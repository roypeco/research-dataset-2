import json
import subprocess
import os
import csv
import shutil
from datetime import datetime, timedelta
from pathlib import Path

def clone_repo(repo_url, temp_dir):
    try:
        # 環境変数からGitHubトークンを取得
        github_token = os.environ.get('GITHUB_TOKEN')
        if github_token:
            # トークンを使用してURLを修正
            repo_url = repo_url.replace('https://github.com/', f'https://{github_token}:x-oauth-basic@github.com/')
        
        subprocess.run(['git', 'clone', repo_url, temp_dir], check=True)
        return True
    except subprocess.CalledProcessError:
        return False

def get_three_years_commits(repo_path):
    start_date = '2022-01-01'
    end_date = '2022-01-04'
    # end_date = '2024-12-31'
    result = subprocess.run(['git', 'log', '--since', start_date, '--until', end_date, '--format=%H', '--reverse'], 
                          cwd=repo_path, capture_output=True, text=True)
    commits = result.stdout.strip().split('\n')
    return commits

def save_commit_history(commits, pkg_name):
    # コミット履歴を保存するディレクトリを作成
    history_dir = Path('dataset') / pkg_name
    history_dir.mkdir(parents=True, exist_ok=True)
    
    # コミット履歴をCSVに保存（古いものが上になるように逆順）
    with open(history_dir / 'hash.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Commit Hash'])
        for commit in commits:
            writer.writerow([commit])

def check_flake8_usage(repo_path):
    # setup.py, requirements.txt, tox.ini, .flake8 などのファイルを確認
    config_files = ['setup.py', 'requirements.txt', 'tox.ini', '.flake8', 'pyproject.toml']
    for file in config_files:
        file_path = os.path.join(repo_path, file)
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                content = f.read().lower()
                if 'flake8' in content or 'pycodestyle' in content or 'pyflakes' in content:
                    return True
    
    # .github/workflows ディレクトリ内のファイルも確認
    workflows_dir = os.path.join(repo_path, '.github', 'workflows')
    if os.path.exists(workflows_dir):
        for file in os.listdir(workflows_dir):
            if file.endswith('.yml') or file.endswith('.yaml'):
                with open(os.path.join(workflows_dir, file), 'r') as f:
                    content = f.read().lower()
                    if 'flake8' in content or 'pycodestyle' in content or 'pyflakes' in content:
                        return True
    
    return False

def run_flake8(repo_path):
    try:
        result = subprocess.run(['flake8'], cwd=repo_path, capture_output=True, text=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        return e.stdout

def get_violation_context(file_path, line_number, context_lines=2):
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

def parse_flake8_output(output, repo_path):
    violations = []
    for line in output.split('\n'):
        if line.strip():
            parts = line.split(':')
            if len(parts) >= 4:  # ファイルパス:行番号:列番号:エラーコード:メッセージ
                file_path = os.path.join(repo_path, parts[0])  # 絶対パスに変換
                line_number = parts[1]
                column_number = parts[2]
                error_code = parts[3].split()[0]  # エラーコード
                message = parts[3].split(' ', 1)[1] if len(parts[3].split()) > 1 else ''  # エラーメッセージ
                
                # 違反のコンテキストを取得
                context = get_violation_context(file_path, line_number)
                
                # 違反の識別子を作成（位置情報は含めない）
                violation_key = (error_code, parts[0], message, context)  # 相対パスを保存
                violations.append(violation_key)
    return violations

def check_file_exists(file_path):
    """ファイルが存在するか確認"""
    return os.path.exists(file_path)

def has_python_files_in_diff(repo_path, commit):
    """diffにPythonファイルが含まれているか確認"""
    result = subprocess.run(['git', 'diff', '--name-only', f'{commit}^!'], cwd=repo_path, capture_output=True, text=True)
    files = result.stdout.strip().split('\n')
    return any(file.endswith('.py') for file in files)

def main():
    with open('jsons/test.json', 'r') as f:
        repos = json.load(f)

    for repo in repos:
        if repo['repository_url'] == "":
            continue
        repo_url = repo['repository_url']
        pkg_name = repo['pkgName']
        temp_dir = os.path.abspath(f"temp_{pkg_name}")  # 絶対パスを使用
        
        try:
            # リポジトリのクローン
            if not clone_repo(repo_url, temp_dir):
                continue

            # flake8の使用確認
            if not check_flake8_usage(temp_dir):
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
                continue

            # 3年分のコミットハッシュ取得
            commits = get_three_years_commits(temp_dir)
            if not commits or commits[0] == '':
                continue
            
            # 結果を保存するディレクトリを作成
            result_dir = Path('dataset') / pkg_name
            result_dir.mkdir(parents=True, exist_ok=True)
            
            # コミット履歴を保存
            save_commit_history(commits, pkg_name)
            
            # 最初のコミットにチェックアウト
            subprocess.run(['git', 'checkout', commits[0]], cwd=temp_dir, check=True)
            
            # 違反の初期状態を記録
            initial_violations = parse_flake8_output(run_flake8(temp_dir), temp_dir)
            
            # CSVファイルの作成
            csv_file = result_dir / 'fix_history.csv'
            with open(csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['Violation ID', 'File Path', 'Message', 'Context', 'Fixed'])
                for violation in initial_violations:
                    writer.writerow([violation[0], violation[1], violation[2], violation[3], 'False'])

            # 各コミットで違反の状態を確認
            for i, commit in enumerate(commits[1:], 1):  # 最初のコミットは除く
                # diffにPythonファイルがない場合はスキップ
                if not has_python_files_in_diff(temp_dir, commit):
                    continue
                    
                subprocess.run(['git', 'checkout', commit], cwd=temp_dir, check=True)
                current_violations = parse_flake8_output(run_flake8(temp_dir), temp_dir)
                
                # CSVファイルの更新
                with open(csv_file, 'r') as f:
                    reader = csv.reader(f)
                    rows = list(reader)
                
                # 既存の違反の更新と新規違反の追加
                existing_violations = set()
                new_violations = []
                
                # 既存の違反を更新
                with open(csv_file, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(rows[0])  # ヘッダー
                    
                    # 既存の違反を処理
                    for row in rows[1:]:
                        violation_id, file_path, message, context, _ = row
                        violation_key = (violation_id, file_path, message, context)
                        existing_violations.add(violation_key)
                        
                        # ファイルが存在し、かつ違反が修正された場合のみTrueにする
                        if check_file_exists(os.path.join(temp_dir, file_path)) and violation_key not in current_violations:
                            row[4] = 'True'  # 修正された
                        writer.writerow(row)
                    
                    # 新規違反を追加
                    for violation in current_violations:
                        if violation not in existing_violations:
                            writer.writerow([violation[0], violation[1], violation[2], violation[3], 'False'])

        finally:
            # 一時ディレクトリの削除
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

if __name__ == '__main__':
    main()