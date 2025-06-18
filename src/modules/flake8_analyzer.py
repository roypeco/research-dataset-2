import subprocess
import os

class Flake8Analyzer:
    """flake8の実行と解析を管理するクラス"""
    
    def check_flake8_usage(self, repo_path):
        """リポジトリでflake8が使用されているか確認"""
        config_files = ['setup.py', 'requirements.txt', 'tox.ini', '.flake8', 'pyproject.toml']
        
        # 設定ファイルを確認
        for file in config_files:
            file_path = os.path.join(repo_path, file)
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    content = f.read().lower()
                    if 'flake8' in content or 'pycodestyle' in content or 'pyflakes' in content:
                        return True
        
        # GitHub Actionsのワークフローを確認
        workflows_dir = os.path.join(repo_path, '.github', 'workflows')
        if os.path.exists(workflows_dir):
            for file in os.listdir(workflows_dir):
                if file.endswith(('.yml', '.yaml')):
                    with open(os.path.join(workflows_dir, file), 'r') as f:
                        content = f.read().lower()
                        if 'flake8' in content or 'pycodestyle' in content or 'pyflakes' in content:
                            return True
        
        return False
    
    def run_flake8(self, repo_path):
        """flake8を実行する"""
        try:
            result = subprocess.run(['flake8'], cwd=repo_path, capture_output=True, text=True)
            return result.stdout
        except subprocess.CalledProcessError as e:
            return e.stdout
    
    def get_violation_context(self, file_path, line_number, context_lines=2):
        """違反が発生した行の前後のコンテキストを取得"""
        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()
                start_line = max(0, int(line_number) - context_lines - 1)
                end_line = min(len(lines), int(line_number) + context_lines)
                context = ''.join(lines[start_line:end_line])
                return context
        except Exception as e:
            print(f"Error reading file {file_path}: {str(e)}")
            return ""
    
    def parse_flake8_output(self, output, repo_path):
        """flake8の出力を解析して違反リストを作成"""
        violations = []
        for line in output.split('\n'):
            if line.strip():
                parts = line.split(':')
                if len(parts) >= 4:
                    file_path = os.path.join(repo_path, parts[0])
                    line_number = parts[1]
                    error_code = parts[3].split()[0]
                    message = parts[3].split(' ', 1)[1] if len(parts[3].split()) > 1 else ''
                    
                    context = self.get_violation_context(file_path, line_number)
                    violation_key = (error_code, parts[0], message, context)
                    violations.append(violation_key)
        return violations
    
    def check_file_exists(self, file_path):
        """ファイルが存在するか確認"""
        return os.path.exists(file_path) 