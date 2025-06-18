import os
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
    
    def analyze_repository(self, repo_url, pkg_name, start_date='2022-01-01', end_date='2022-01-04'):
        """単一のリポジトリを分析する"""
        temp_dir = os.path.abspath(f"temp_{pkg_name}")
        
        try:
            # リポジトリのクローン
            if not self.repo_manager.clone_repo(repo_url, temp_dir):
                print(f"Failed to clone repository: {repo_url}")
                return False

            # flake8の使用確認
            if not self.flake8_analyzer.check_flake8_usage(temp_dir):
                print(f"flake8 not used in repository: {pkg_name}")
                return False

            # コミットハッシュ取得
            commits = self.repo_manager.get_commits_in_date_range(temp_dir, start_date, end_date)
            if not commits or commits[0] == '':
                print(f"No commits found in date range for: {pkg_name}")
                return False
            
            # コミット履歴を保存
            self.data_manager.save_commit_history(commits, pkg_name)
            
            # 最初のコミットにチェックアウト
            self.repo_manager.checkout_commit(temp_dir, commits[0])
            
            # 違反の初期状態を記録
            initial_violations = self.flake8_analyzer.parse_flake8_output(
                self.flake8_analyzer.run_flake8(temp_dir), temp_dir
            )
            
            # CSVファイルの作成
            csv_file = self.data_manager.create_fix_history_csv(pkg_name, initial_violations, commits[0])

            # 各コミットで違反の状態を確認
            for commit in commits[1:]:  # 最初のコミットは除く
                # diffにPythonファイルがない場合はスキップ
                if not self.repo_manager.has_python_files_in_diff(temp_dir, commit):
                    continue
                    
                self.repo_manager.checkout_commit(temp_dir, commit)
                current_violations = self.flake8_analyzer.parse_flake8_output(
                    self.flake8_analyzer.run_flake8(temp_dir), temp_dir
                )
                
                # CSVファイルの更新
                self.data_manager.update_fix_history_csv(csv_file, current_violations, temp_dir, commit)
            
            print(f"Successfully analyzed repository: {pkg_name}")
            return True
            
        except Exception as e:
            print(f"Error analyzing repository {pkg_name}: {str(e)}")
            return False
        finally:
            # 一時ディレクトリの削除
            self.repo_manager.cleanup_temp_dir(temp_dir)
    
    def analyze_all_repositories(self, json_path):
        """すべてのリポジトリを分析する"""
        repos = self.data_manager.load_repos_from_json(json_path)
        
        success_count = 0
        total_count = len([repo for repo in repos if repo['repository_url'] != ""])
        
        for repo in repos:
            if repo['repository_url'] == "":
                continue
                
            repo_url = repo['repository_url']
            pkg_name = repo['pkgName']
            
            print(f"Analyzing {pkg_name}...")
            if self.analyze_repository(repo_url, pkg_name):
                success_count += 1
        
        print(f"\nAnalysis completed: {success_count}/{total_count} repositories processed successfully")

def main():
    """メイン関数"""
    analyzer = RepoAnalyzer()
    analyzer.analyze_all_repositories('jsons/test.json')

if __name__ == '__main__':
    main() 