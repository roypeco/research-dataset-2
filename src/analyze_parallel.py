import os
import logging
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
from modules.repository_manager import RepositoryManager
from modules.flake8_analyzer import Flake8Analyzer
from modules.data_manager import DataManager
import time

class ParallelRepoAnalyzer:
    """プロジェクトごとに並列処理するリポジトリ分析クラス"""
    
    def __init__(self, max_workers=4, output_format='parquet', track_line_numbers=True):
        self.max_workers = max_workers or min(cpu_count(), 8)  # CPUコア数または8の小さい方
        self.output_format = output_format.lower()  # 'csv' or 'parquet'
        self.track_line_numbers = track_line_numbers  # 行番号追跡（デフォルト有効）
        self.setup_logging()
        self.START_DATE = '2024-01-01'
        self.END_DATE = '2024-12-31'
    
    def setup_logging(self):
        """ログ設定を初期化（全体ログのみ）"""
        # logsディレクトリの作成
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        
        # プロジェクト用ログディレクトリの作成
        project_log_dir = log_dir / 'projects'
        project_log_dir.mkdir(exist_ok=True)
        
        # 全体のログ設定
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_dir / 'out.log', mode='w', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def setup_project_logger(self, pkg_name):
        """プロジェクト専用のロガーを設定"""
        project_logger = logging.getLogger(f"project.{pkg_name}")
        project_logger.setLevel(logging.INFO)
        
        # 既存のハンドラーをクリア（重複を避けるため）
        project_logger.handlers.clear()
        
        # プロジェクト専用のファイルハンドラーを設定
        log_file = Path('logs') / 'projects' / f'{pkg_name}.log'
        handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        project_logger.addHandler(handler)
        
        # プロジェクトロガーが親ロガーに伝播しないよう設定
        project_logger.propagate = False
        
        return project_logger
    
    def analyze_repository_worker(self, repo_data):
        """単一のリポジトリを分析する（並列処理用ワーカー）"""
        repo_url = repo_data['repository_url']
        pkg_name = repo_data['pkgName']
        output_format = repo_data['output_format']  # 出力フォーマット情報を取得
        temp_dir = os.path.abspath(f"temp_{pkg_name}_{os.getpid()}")  # プロセスIDを追加して競合回避
        
        # プロジェクト専用ロガーを設定
        project_logger = self.setup_project_logger(pkg_name)
        
        # 各プロセスで独立したインスタンスを作成
        repo_manager = RepositoryManager()
        flake8_analyzer = Flake8Analyzer()
        data_manager = DataManager()
        
        try:
            project_logger.info(f"Starting analysis of repository: {pkg_name}")
            project_logger.info(f"Repository URL: {repo_url}")
            project_logger.info(f"Date range: {self.START_DATE} to {self.END_DATE}")
            
            # リポジトリのクローン
            project_logger.info(f"Cloning repository: {pkg_name}")
            if not repo_manager.clone_repo(repo_url, temp_dir):
                project_logger.error(f"Failed to clone repository: {repo_url}")
                return {'pkg_name': pkg_name, 'success': False, 'error': 'Clone failed'}
            project_logger.info(f"Successfully cloned repository: {pkg_name}")

            # flake8の使用確認
            project_logger.info(f"Checking flake8 usage in repository: {pkg_name}")
            if not flake8_analyzer.check_flake8_usage(temp_dir):
                project_logger.warning(f"flake8 not used in repository: {pkg_name}")
                return {'pkg_name': pkg_name, 'success': False, 'error': 'No flake8'}
            project_logger.info(f"flake8 is used in repository: {pkg_name}")

            # コミットハッシュ取得
            project_logger.info(f"Getting commits in date range for: {pkg_name}")
            commits = repo_manager.get_commits_in_date_range(temp_dir, self.START_DATE, self.END_DATE)
            if not commits or commits[0] == '':
                project_logger.warning(f"No commits found in date range for: {pkg_name}")
                return {'pkg_name': pkg_name, 'success': False, 'error': 'No commits'}
            project_logger.info(f"Found {len(commits)} commits for {pkg_name}")
            
            # コミット履歴を保存
            project_logger.info(f"Saving commit history for: {pkg_name}")
            data_manager.save_commit_history(commits, pkg_name)
            
            # 最初のコミットにチェックアウト
            project_logger.info(f"Checking out initial commit: {commits[0][:8]} for {pkg_name}")
            repo_manager.checkout_commit(temp_dir, commits[0])
            
            # 違反の初期状態を記録
            project_logger.info(f"Running flake8 on initial commit for: {pkg_name}")
            initial_violations = flake8_analyzer.parse_flake8_output(
                flake8_analyzer.run_flake8(temp_dir), temp_dir
            )
            project_logger.info(f"Found {len(initial_violations)} initial violations for {pkg_name}")
            
            # バッチ処理用のコミットデータを準備
            project_logger.info(f"Preparing commit data for batch processing: {pkg_name}")
            commits_data = [{'commit': commits[0], 'violations': initial_violations}]
            
            # 各コミットで違反の状態を確認（バッチ処理用データ収集）
            project_logger.info(f"Starting commit-by-commit analysis for {pkg_name}")
            processed_commits = 0
            skipped_commits = 0
            total_commits_to_process = len(commits) - 1
            
            for i, commit in enumerate(commits[1:], 1):  # 最初のコミットは除く
                progress_percent = (i / total_commits_to_process) * 100
                project_logger.info(f"Processing commit {i}/{total_commits_to_process} ({progress_percent:.1f}%): {commit[:8]} for {pkg_name}")
                
                # diffにPythonファイルがない場合はスキップ
                if not repo_manager.has_python_files_in_diff(temp_dir, commit):
                    project_logger.info(f"Skipping commit {commit[:8]} (no Python files changed)")
                    skipped_commits += 1
                    continue
                    
                repo_manager.checkout_commit(temp_dir, commit)
                current_violations = flake8_analyzer.parse_flake8_output(
                    flake8_analyzer.run_flake8(temp_dir), temp_dir
                )
                
                project_logger.info(f"Found {len(current_violations)} violations in commit {commit[:8]}")
                
                # バッチ処理用データに追加
                commits_data.append({'commit': commit, 'violations': current_violations})
                processed_commits += 1
                
                # 進捗を定期的にログ出力
                if i % 10 == 0:
                    progress_percent = (i / total_commits_to_process) * 100
                    project_logger.info(f"Progress: {i}/{total_commits_to_process} commits processed ({progress_percent:.1f}%) for {pkg_name}")
            
            # バッチ処理で違反データを一括処理（行番号追跡付き・最適化版）
            project_logger.info(f"Processing violations in batch with optimized line tracking for: {pkg_name}")
            violation_rows = data_manager.process_violations_batch_optimized(
                initial_violations, commits_data, temp_dir, pkg_name
            )
            
            # バッチ処理結果からDataFrameを作成
            project_logger.info(f"Creating DataFrame from batch results for: {pkg_name}")
            if violation_rows:
                import pandas as pd
                headers = data_manager._get_feature_headers()
                data_manager.fix_history_df = pd.DataFrame(violation_rows, columns=headers)
                data_manager.fix_history_df = data_manager.optimize_dataframe_types(data_manager.fix_history_df)
                project_logger.info(f"DataFrame created with {len(data_manager.fix_history_df)} rows for {pkg_name}")
            else:
                project_logger.warning(f"No violation data to process for: {pkg_name}")
                return {'pkg_name': pkg_name, 'success': False, 'error': 'No violation data'}
            
            # DataFrameをファイルに保存
            project_logger.info(f"Saving DataFrame to {output_format.upper()} for: {pkg_name}")
            
            if output_format == 'parquet':
                output_file = data_manager.save_fix_history_to_parquet(pkg_name)
            else:  # デフォルトはCSV
                output_file = data_manager.save_fix_history_to_csv(pkg_name)
            
            if output_file:
                project_logger.info(f"{output_format.upper()} file saved: {output_file}")
                total_violations = len(data_manager.fix_history_df) if hasattr(data_manager, 'fix_history_df') else 0
            else:
                project_logger.error(f"Failed to save {output_format.upper()} file for: {pkg_name}")
                return {'pkg_name': pkg_name, 'success': False, 'error': f'{output_format.upper()} save failed'}
            
            project_logger.info(f"Analysis completed for {pkg_name}")
            project_logger.info(f"Total commits: {len(commits)}, Processed: {processed_commits}, Skipped: {skipped_commits}")
            project_logger.info(f"Processing rate: {processed_commits}/{total_commits_to_process} ({processed_commits/total_commits_to_process*100:.1f}%)")
            
            return {
                'pkg_name': pkg_name,
                'success': True,
                'total_commits': len(commits),
                'processed_commits': processed_commits,
                'skipped_commits': skipped_commits,
                'total_violations': total_violations
            }
            
        except Exception as e:
            project_logger.error(f"Error analyzing repository {pkg_name}: {str(e)}")
            return {'pkg_name': pkg_name, 'success': False, 'error': str(e)}
        finally:
            # 一時ディレクトリの削除
            project_logger.info(f"Cleaning up temporary directory for: {pkg_name}")
            repo_manager.cleanup_temp_dir(temp_dir)
            
            # プロジェクトロガーのハンドラーをクローズ
            for handler in project_logger.handlers:
                handler.close()
                project_logger.removeHandler(handler)
    
    def analyze_all_repositories_parallel(self, json_path):
        """すべてのリポジトリを並列処理する"""
        self.logger.info(f"Starting parallel analysis of all repositories from: {json_path}")
        self.logger.info(f"Using {self.max_workers} parallel workers")
        
        data_manager = DataManager()
        repos = data_manager.load_repos_from_json(json_path)
        valid_repos = [repo for repo in repos if repo['repository_url'] != ""]
        
        self.logger.info(f"Loaded {len(repos)} repositories, {len(valid_repos)} valid repositories")
        
        # 各リポジトリに出力フォーマット情報を追加
        for repo in valid_repos:
            repo['output_format'] = self.output_format
        
        start_time = time.time()
        success_count = 0
        total_count = len(valid_repos)
        
        # プロセスプールでリポジトリを並列処理
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # リポジトリ分析タスクを並列実行
            future_to_repo = {
                executor.submit(self.analyze_repository_worker, repo): repo
                for repo in valid_repos
            }
            
            completed_count = 0
            for future in as_completed(future_to_repo):
                repo = future_to_repo[future]
                completed_count += 1
                
                try:
                    result = future.result()
                    if result['success']:
                        success_count += 1
                        self.logger.info(f"✓ Successfully analyzed: {result['pkg_name']} "
                                        f"({result['processed_commits']}/{result['total_commits']-1} commits processed, "
                                        f"{result['total_violations']} total violations)")
                    else:
                        self.logger.warning(f"✗ Failed to analyze: {result['pkg_name']} - {result.get('error', 'Unknown error')}")
                    
                    # 進捗をログ出力（全体ログのみ）
                    progress_percent = (completed_count / total_count) * 100
                    success_rate_percent = (success_count / completed_count) * 100
                    elapsed_time = time.time() - start_time
                    avg_time_per_repo = elapsed_time / completed_count
                    estimated_remaining = avg_time_per_repo * (total_count - completed_count)
                    
                    self.logger.info(f"Progress: {completed_count}/{total_count} ({progress_percent:.1f}%) "
                                   f"Success rate: {success_rate_percent:.1f}% "
                                   f"Elapsed: {elapsed_time/60:.1f}min "
                                   f"ETA: {estimated_remaining/60:.1f}min")
                    
                except Exception as e:
                    self.logger.error(f"Error processing repository {repo['pkgName']}: {str(e)}")
        
        total_time = time.time() - start_time
        self.logger.info(f"=== Parallel Analysis completed ===")
        self.logger.info(f"Total repositories: {total_count}")
        self.logger.info(f"Successful analyses: {success_count}")
        self.logger.info(f"Failed analyses: {total_count - success_count}")
        self.logger.info(f"Success rate: {success_count/total_count*100:.1f}%")
        self.logger.info(f"Total time: {total_time/60:.1f} minutes")
        self.logger.info(f"Average time per repository: {total_time/total_count:.1f} seconds")
        
        return {
            'total_count': total_count,
            'success_count': success_count,
            'total_time': total_time,
            'success_rate': success_count/total_count*100
        }

def main():
    """メイン関数"""
    import sys
    
    # コマンドライン引数から出力フォーマットを取得
    output_format = 'parquet'  # デフォルト
    
    if len(sys.argv) > 1:
        # 引数：出力フォーマット
        if sys.argv[1].lower() in ['csv', 'parquet']:
            output_format = sys.argv[1].lower()
        else:
            print("Usage: python analyze_parallel.py [csv|parquet]")
            print("Default: parquet")
            print("Note: Line number tracking is always enabled")
            sys.exit(1)
    
    print(f"Output format: {output_format.upper()}")
    print(f"Line number tracking: ENABLED (always on)")
    
    analyzer = ParallelRepoAnalyzer(output_format=output_format)
    result = analyzer.analyze_all_repositories_parallel('jsons/out.json')
    
    print(f"\n=== Final Summary ===")
    print(f"Total repositories: {result['total_count']}")
    print(f"Successful analyses: {result['success_count']}")
    print(f"Success rate: {result['success_rate']:.1f}%")
    print(f"Total execution time: {result['total_time']/60:.1f} minutes")

if __name__ == '__main__':
    main() 