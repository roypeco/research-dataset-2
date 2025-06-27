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
    """並列処理対応のリポジトリ分析クラス（プロジェクト単位のみ並列）"""
    
    def __init__(self, max_workers=None):
        self.max_workers = max_workers or min(cpu_count(), 8)  # CPUコア数または8の小さい方
        self.setup_logging()
        self.START_DATE = '2022-01-01'
        self.END_DATE = '2022-03-31'
    
    def setup_logging(self):
        """ログ設定を初期化"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('out_parallel.log', mode='w', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def analyze_repository_parallel(self, repo_data):
        """単一のリポジトリを分析する（並列処理用）"""
        repo_url = repo_data['repository_url']
        pkg_name = repo_data['pkgName']
        temp_dir = os.path.abspath(f"temp_{pkg_name}")
        
        self.logger.info(f"Starting analysis of repository: {pkg_name}")
        
        try:
            # リポジトリのクローン
            repo_manager = RepositoryManager()
            if not repo_manager.clone_repo(repo_url, temp_dir):
                self.logger.error(f"Failed to clone repository: {pkg_name}")
                return {'pkg_name': pkg_name, 'success': False, 'error': 'Clone failed'}
            
            # flake8の使用確認
            flake8_analyzer = Flake8Analyzer()
            if not flake8_analyzer.check_flake8_usage(temp_dir):
                self.logger.warning(f"flake8 not used in repository: {pkg_name}")
                return {'pkg_name': pkg_name, 'success': False, 'error': 'No flake8'}
            
            # コミットハッシュ取得
            commits = repo_manager.get_commits_in_date_range(temp_dir, self.START_DATE, self.END_DATE)
            if not commits or commits[0] == '':
                self.logger.warning(f"No commits found in date range for: {pkg_name}")
                return {'pkg_name': pkg_name, 'success': False, 'error': 'No commits'}
            
            # データマネージャーでコミット履歴を保存
            data_manager = DataManager()
            data_manager.save_commit_history(commits, pkg_name)
            
            # 最初のコミットにチェックアウト
            repo_manager.checkout_commit(temp_dir, commits[0])
            
            # 違反の初期状態を記録
            initial_violations = flake8_analyzer.parse_flake8_output(
                flake8_analyzer.run_flake8(temp_dir), temp_dir
            )
            
            # CSVファイルの作成
            csv_file = data_manager.create_fix_history_csv(pkg_name, initial_violations, commits[0], temp_dir)
            
            # コミットの逐次処理（並列処理なし）
            commit_results = self.analyze_commits_sequential(temp_dir, commits[1:], csv_file, pkg_name)
            
            return {
                'pkg_name': pkg_name,
                'success': True,
                'total_commits': len(commits),
                'processed_commits': commit_results['processed'],
                'skipped_commits': commit_results['skipped']
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing repository {pkg_name}: {str(e)}")
            return {'pkg_name': pkg_name, 'success': False, 'error': str(e)}
        finally:
            # 一時ディレクトリの削除
            repo_manager.cleanup_temp_dir(temp_dir)
    
    def analyze_commits_sequential(self, temp_dir, commits, csv_file, pkg_name):
        """コミットを逐次処理する（並列処理なし）"""
        self.logger.info(f"Starting sequential commit analysis for {pkg_name} with {len(commits)} commits")
        
        # 各プロセスで独立したインスタンスを作成
        repo_manager = RepositoryManager()
        flake8_analyzer = Flake8Analyzer()
        data_manager = DataManager()
        
        processed_commits = 0
        skipped_commits = 0
        total_commits_to_process = len(commits)
        
        for i, commit in enumerate(commits, 1):
            progress_percent = (i / total_commits_to_process) * 100
            self.logger.info(f"Processing commit {i}/{total_commits_to_process} ({progress_percent:.1f}%): {commit[:8]} for {pkg_name}")
            
            try:
                # diffにPythonファイルがない場合はスキップ
                if not repo_manager.has_python_files_in_diff(temp_dir, commit):
                    self.logger.info(f"Skipping commit {commit[:8]} (no Python files changed)")
                    skipped_commits += 1
                    continue
                
                repo_manager.checkout_commit(temp_dir, commit)
                current_violations = flake8_analyzer.parse_flake8_output(
                    flake8_analyzer.run_flake8(temp_dir), temp_dir
                )
                
                self.logger.info(f"Found {len(current_violations)} violations in commit {commit[:8]}")
                
                # CSVファイルの更新
                data_manager.update_fix_history_csv(csv_file, current_violations, temp_dir, commit)
                processed_commits += 1
                
                # 進捗を定期的にログ出力
                if i % 10 == 0:
                    progress_percent = (i / total_commits_to_process) * 100
                    self.logger.info(f"Progress: {i}/{total_commits_to_process} commits processed ({progress_percent:.1f}%) for {pkg_name}")
                    
            except Exception as e:
                self.logger.error(f"Error processing commit {commit[:8]} for {pkg_name}: {str(e)}")
                skipped_commits += 1
        
        self.logger.info(f"Commit analysis completed for {pkg_name}")
        self.logger.info(f"Total commits: {len(commits)}, Processed: {processed_commits}, Skipped: {skipped_commits}")
        self.logger.info(f"Processing rate: {processed_commits}/{total_commits_to_process} ({processed_commits/total_commits_to_process*100:.1f}%)")
        
        return {'processed': processed_commits, 'skipped': skipped_commits}
    
    def analyze_all_repositories_parallel(self, json_path):
        """すべてのリポジトリを並列処理する（プロジェクト単位のみ）"""
        self.logger.info(f"Starting parallel analysis of all repositories from: {json_path}")
        self.logger.info(f"Using {self.max_workers} parallel workers (project-level only)")
        
        data_manager = DataManager()
        repos = data_manager.load_repos_from_json(json_path)
        valid_repos = [repo for repo in repos if repo['repository_url'] != ""]
        
        self.logger.info(f"Loaded {len(repos)} repositories, {len(valid_repos)} valid repositories")
        
        start_time = time.time()
        success_count = 0
        total_count = len(valid_repos)
        
        # プロセスプールでリポジトリを並列処理
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # リポジトリ分析タスクを並列実行
            future_to_repo = {
                executor.submit(self.analyze_repository_parallel, repo): repo
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
                                        f"({result['processed_commits']}/{result['total_commits']-1} commits processed)")
                    else:
                        self.logger.warning(f"✗ Failed to analyze: {result['pkg_name']} - {result.get('error', 'Unknown error')}")
                    
                    # 進捗をログ出力
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

def main():
    """メイン関数"""
    # CPUコア数を確認
    cpu_cores = cpu_count()
    print(f"Available CPU cores: {cpu_cores}")
    
    # 並列処理のワーカー数を設定（CPUコア数の半分〜全数）
    max_workers = min(cpu_cores, 8)  # 最大8プロセスに制限
    
    analyzer = ParallelRepoAnalyzer(max_workers=max_workers)
    analyzer.analyze_all_repositories_parallel('jsons/test.json')

if __name__ == '__main__':
    main() 