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
        except Exception as e:
            print(f"Error running flake8: {str(e)}")
            return ""
    
    def run_flake8_on_files(self, repo_path, file_paths):
        """特定のファイルのみに対してflake8を実行する"""
        if not file_paths:
            return ""
        
        try:
            # flake8に対象ファイルのパスを渡す
            cmd = ['flake8'] + file_paths
            result = subprocess.run(cmd, cwd=repo_path, capture_output=True, text=True)
            return result.stdout
        except subprocess.CalledProcessError as e:
            return e.stdout
        except Exception as e:
            print(f"Error running flake8 on specific files: {str(e)}")
            return ""
    
    def get_violation_context(self, file_path, line_number, context_lines=2):
        """違反が発生した行の前後のコンテキストを取得"""
        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()
                
                # context_lines=0の場合は違反行のみを取得（正規化済み）
                if context_lines == 0:
                    line_idx = int(line_number) - 1  # 0ベースのインデックス
                    if 0 <= line_idx < len(lines):
                        # 違反行から空白やタブを削除して正規化
                        normalized_line = lines[line_idx].strip()
                        return normalized_line
                    else:
                        return ""
                
                # 通常の場合は前後のコンテキストを取得
                start_line = max(0, int(line_number) - context_lines - 1)
                end_line = min(len(lines), int(line_number) + context_lines)
                context = ''.join(lines[start_line:end_line])
                return context
        except Exception as e:
            print(f"Error reading file {file_path}: {str(e)}")
            return ""
    
    def parse_flake8_output(self, output, repo_path):
        """flake8の出力を解析して違反リストを作成（行番号付き）"""
        violations = []
        seen_violations = set()  # 重複チェック用
        
        for line in output.split('\n'):
            if line.strip():
                parts = line.split(':')
                if len(parts) >= 4:
                    file_path = parts[0]
                    line_number = parts[1]
                    column_number = parts[2]
                    error_code = parts[3].split()[0]
                    message = parts[3].split(' ', 1)[1] if len(parts[3].split()) > 1 else ''
                    
                    # 行番号を含むコンテキストを取得
                    full_file_path = os.path.join(repo_path, file_path)
                    context = self.get_violation_context(full_file_path, line_number, 0)
                    
                    # 重複チェック用のキーを作成（messageとcontextを除く）
                    violation_key = (error_code, file_path, line_number)
                    
                    # 重複チェック
                    if violation_key not in seen_violations:
                        # 行番号を含む違反キーを作成
                        violation_key_with_line = (error_code, file_path, message, context, line_number)
                        violations.append(violation_key_with_line)
                        seen_violations.add(violation_key)
        
        return violations
    
    def check_file_exists(self, file_path):
        """ファイルが存在するか確認"""
        return os.path.exists(file_path) 