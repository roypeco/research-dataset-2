import subprocess
import os
import shutil
from pathlib import Path

class RepositoryManager:
    """リポジトリのクローンとGit操作を管理するクラス"""
    
    def __init__(self):
        self.github_token = os.environ.get('GITHUB_TOKEN')
    
    def clone_repo(self, repo_url, temp_dir):
        """リポジトリをクローンする"""
        try:
            if self.github_token:
                repo_url = repo_url.replace('https://github.com/', 
                                          f'https://{self.github_token}:x-oauth-basic@github.com/')
            
            subprocess.run(['git', 'clone', repo_url, temp_dir], check=True)
            return True
        except subprocess.CalledProcessError:
            return False
    
    def get_commits_in_date_range(self, repo_path, start_date, end_date):
        """指定された日付範囲のコミットを取得する"""
        result = subprocess.run([
            'git', 'log', '--since', start_date, '--until', end_date, 
            '--format=%H', '--reverse'
        ], cwd=repo_path, capture_output=True, text=True)
        return result.stdout.strip().split('\n')
    
    def checkout_commit(self, repo_path, commit_hash):
        """指定されたコミットにチェックアウトする"""
        subprocess.run(['git', 'checkout', commit_hash], cwd=repo_path, check=True)
    
    def has_python_files_in_diff(self, repo_path, commit):
        """diffにPythonファイルが含まれているか確認"""
        result = subprocess.run(['git', 'diff', '--name-only', f'{commit}^!'], 
                              cwd=repo_path, capture_output=True, text=True)
        files = result.stdout.strip().split('\n')
        return any(file.endswith('.py') for file in files)
    
    def get_python_files_in_diff(self, repo_path, commit):
        """diffがあったPythonファイルのリストを取得（削除されたファイルは除外）"""
        # diff --name-status を使用して、ファイルの変更状態も取得
        result = subprocess.run(['git', 'diff', '--name-status', f'{commit}^!'], 
                              cwd=repo_path, capture_output=True, text=True)
        
        python_files = []
        for line in result.stdout.strip().split('\n'):
            if line and '\t' in line:
                status, file_path = line.split('\t', 1)
                # 削除されたファイル（D）は除外、追加（A）・変更（M）・リネーム（R）のみ対象
                if file_path.endswith('.py') and not status.startswith('D'):
                    # ファイルが実際に存在するかチェック
                    full_path = os.path.join(repo_path, file_path)
                    if os.path.exists(full_path):
                        python_files.append(file_path)
        
        return python_files
    
    def get_deleted_python_files_in_diff(self, repo_path, commit):
        """diffで削除されたPythonファイルのリストを取得"""
        result = subprocess.run(['git', 'diff', '--name-status', f'{commit}^!'], 
                              cwd=repo_path, capture_output=True, text=True)
        
        deleted_files = []
        for line in result.stdout.strip().split('\n'):
            if line and '\t' in line:
                status, file_path = line.split('\t', 1)
                # 削除されたファイル（D）のみ対象
                if file_path.endswith('.py') and status.startswith('D'):
                    deleted_files.append(file_path)
        
        return deleted_files
    
    def cleanup_temp_dir(self, temp_dir):
        """一時ディレクトリを削除する"""
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir) 