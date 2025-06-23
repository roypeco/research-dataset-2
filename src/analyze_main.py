import os
import logging
from pathlib import Path
from modules.repository_manager import RepositoryManager
from modules.flake8_analyzer import Flake8Analyzer
from modules.data_manager import DataManager

class RepoAnalyzer:
    """リポジトリ分析のメインクラス"""
    
    def __init__(self):
        self.repo_manager = RepositoryManager()
        self.flake8_analyzer = Flake8Analyzer()
        self.data_manager = DataManager()
        self.setup_logging()
    
    def setup_logging(self):
        """ログ設定を初期化"""
        # ログファイルの設定
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('out.log', mode='w', encoding='utf-8'),
                logging.StreamHandler()  # コンソールにも出力
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def analyze_repository(self, repo_url, pkg_name, start_date='2022-01-01', end_date='2022-03-31'):
        """単一のリポジトリを分析する"""
        temp_dir = os.path.abspath(f"temp_{pkg_name}")
        
        self.logger.info(f"Starting analysis of repository: {pkg_name}")
        self.logger.info(f"Repository URL: {repo_url}")
        self.logger.info(f"Date range: {start_date} to {end_date}")
        
        try:
            # リポジトリのクローン
            self.logger.info(f"Cloning repository: {pkg_name}")
            if not self.repo_manager.clone_repo(repo_url, temp_dir):
                self.logger.error(f"Failed to clone repository: {repo_url}")
                return False
            self.logger.info(f"Successfully cloned repository: {pkg_name}")

            # flake8の使用確認
            self.logger.info(f"Checking flake8 usage in repository: {pkg_name}")
            if not self.flake8_analyzer.check_flake8_usage(temp_dir):
                self.logger.warning(f"flake8 not used in repository: {pkg_name}")
                return False
            self.logger.info(f"flake8 is used in repository: {pkg_name}")

            # コミットハッシュ取得
            self.logger.info(f"Getting commits in date range for: {pkg_name}")
            commits = self.repo_manager.get_commits_in_date_range(temp_dir, start_date, end_date)
            if not commits or commits[0] == '':
                self.logger.warning(f"No commits found in date range for: {pkg_name}")
                return False
            
            self.logger.info(f"Found {len(commits)} commits for {pkg_name}")
            
            # コミット履歴を保存
            self.logger.info(f"Saving commit history for: {pkg_name}")
            self.data_manager.save_commit_history(commits, pkg_name)
            
            # 最初のコミットにチェックアウト
            self.logger.info(f"Checking out initial commit: {commits[0][:8]} for {pkg_name}")
            self.repo_manager.checkout_commit(temp_dir, commits[0])
            
            # 違反の初期状態を記録
            self.logger.info(f"Running flake8 on initial commit for: {pkg_name}")
            initial_violations = self.flake8_analyzer.parse_flake8_output(
                self.flake8_analyzer.run_flake8(temp_dir), temp_dir
            )
            self.logger.info(f"Found {len(initial_violations)} initial violations for {pkg_name}")
            
            # CSVファイルの作成
            self.logger.info(f"Creating CSV file for: {pkg_name}")
            csv_file = self.data_manager.create_fix_history_csv(pkg_name, initial_violations, commits[0], temp_dir)
            self.logger.info(f"CSV file created: {csv_file}")

            # 各コミットで違反の状態を確認
            self.logger.info(f"Starting commit-by-commit analysis for {pkg_name}")
            processed_commits = 0
            skipped_commits = 0
            total_commits_to_process = len(commits) - 1
            
            for i, commit in enumerate(commits[1:], 1):  # 最初のコミットは除く
                progress_percent = (i / total_commits_to_process) * 100
                self.logger.info(f"Processing commit {i}/{total_commits_to_process} ({progress_percent:.1f}%): {commit[:8]} for {pkg_name}")
                
                # diffにPythonファイルがない場合はスキップ
                if not self.repo_manager.has_python_files_in_diff(temp_dir, commit):
                    self.logger.info(f"Skipping commit {commit[:8]} (no Python files changed)")
                    skipped_commits += 1
                    continue
                    
                self.repo_manager.checkout_commit(temp_dir, commit)
                current_violations = self.flake8_analyzer.parse_flake8_output(
                    self.flake8_analyzer.run_flake8(temp_dir), temp_dir
                )
                
                self.logger.info(f"Found {len(current_violations)} violations in commit {commit[:8]}")
                
                # CSVファイルの更新
                self.data_manager.update_fix_history_csv(csv_file, current_violations, temp_dir, commit)
                processed_commits += 1
                
                # 進捗を定期的にログ出力
                if i % 10 == 0:
                    progress_percent = (i / total_commits_to_process) * 100
                    self.logger.info(f"Progress: {i}/{total_commits_to_process} commits processed ({progress_percent:.1f}%) for {pkg_name}")
            
            self.logger.info(f"Analysis completed for {pkg_name}")
            self.logger.info(f"Total commits: {len(commits)}, Processed: {processed_commits}, Skipped: {skipped_commits}")
            self.logger.info(f"Processing rate: {processed_commits}/{total_commits_to_process} ({processed_commits/total_commits_to_process*100:.1f}%)")
            return True
            
        except Exception as e:
            self.logger.error(f"Error analyzing repository {pkg_name}: {str(e)}")
            return False
        finally:
            # 一時ディレクトリの削除
            self.logger.info(f"Cleaning up temporary directory for: {pkg_name}")
            self.repo_manager.cleanup_temp_dir(temp_dir)
    
    def analyze_all_repositories(self, json_path):
        """すべてのリポジトリを分析する"""
        self.logger.info(f"Starting analysis of all repositories from: {json_path}")
        
        repos = self.data_manager.load_repos_from_json(json_path)
        self.logger.info(f"Loaded {len(repos)} repositories from JSON file")
        
        success_count = 0
        total_count = len([repo for repo in repos if repo['repository_url'] != ""])
        self.logger.info(f"Total repositories to analyze: {total_count}")
        
        for i, repo in enumerate(repos, 1):
            if repo['repository_url'] == "":
                self.logger.info(f"Skipping repository {i} (no URL)")
                continue
                
            repo_url = repo['repository_url']
            pkg_name = repo['pkgName']
            
            overall_progress_percent = (i / total_count) * 100
            self.logger.info(f"=== Repository {i}/{total_count} ({overall_progress_percent:.1f}%): {pkg_name} ===")
            if self.analyze_repository(repo_url, pkg_name):
                success_count += 1
                self.logger.info(f"✓ Successfully analyzed: {pkg_name}")
            else:
                self.logger.warning(f"✗ Failed to analyze: {pkg_name}")
            
            # 全体の進捗をログ出力
            overall_progress_percent = (i / total_count) * 100
            success_rate_percent = (success_count / i) * 100
            self.logger.info(f"Overall progress: {i}/{total_count} repositories processed ({overall_progress_percent:.1f}%)")
            self.logger.info(f"Success rate: {success_count}/{i} ({success_rate_percent:.1f}%)")
        
        self.logger.info(f"=== Analysis completed ===")
        self.logger.info(f"Total repositories: {total_count}")
        self.logger.info(f"Successful analyses: {success_count}")
        self.logger.info(f"Failed analyses: {total_count - success_count}")
        self.logger.info(f"Success rate: {success_count/total_count*100:.1f}%")

def main():
    """メイン関数"""
    analyzer = RepoAnalyzer()
    analyzer.analyze_all_repositories('jsons/test.json')

if __name__ == '__main__':
    main() 