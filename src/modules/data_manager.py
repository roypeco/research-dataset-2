import csv
import json
from pathlib import Path
import os

class DataManager:
    """データの保存と読み込みを管理するクラス"""
    
    def load_repos_from_json(self, json_path):
        """JSONファイルからリポジトリ情報を読み込む"""
        with open(json_path, 'r') as f:
            return json.load(f)
    
    def save_commit_history(self, commits, pkg_name):
        """コミット履歴をCSVに保存"""
        history_dir = Path('dataset') / pkg_name
        history_dir.mkdir(parents=True, exist_ok=True)
        
        with open(history_dir / 'hash.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Commit Hash'])
            for commit in commits:
                writer.writerow([commit])
    
    def create_fix_history_csv(self, pkg_name, initial_violations, initial_commit):
        """修正履歴のCSVファイルを作成"""
        result_dir = Path('dataset') / pkg_name
        result_dir.mkdir(parents=True, exist_ok=True)
        
        csv_file = result_dir / 'fix_history.csv'
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Violation ID', 'File Path', 'Message', 'Context', 'Error Commit Hash', 'Fix Commit Hash', 'Fixed'])
            for violation in initial_violations:
                writer.writerow([violation[0], violation[1], violation[2], violation[3], initial_commit, '', 'False'])
        
        return csv_file
    
    def update_fix_history_csv(self, csv_file, current_violations, temp_dir, current_commit):
        """修正履歴のCSVファイルを更新"""
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        existing_violations = set()
        violation_data = {}  # 既存の違反データを保持
        
        # 既存の違反データを読み込み
        for row in rows[1:]:
            violation_id, file_path, message, context, error_commit, fix_commit, fixed = row
            violation_key = (violation_id, file_path, message, context)
            existing_violations.add(violation_key)
            violation_data[violation_key] = {
                'error_commit': error_commit,
                'fix_commit': fix_commit,
                'fixed': fixed
            }
        
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(rows[0])  # ヘッダー
            
            # 既存の違反を処理
            for row in rows[1:]:
                violation_id, file_path, message, context, error_commit, fix_commit, fixed = row
                violation_key = (violation_id, file_path, message, context)
                
                # ファイルが存在し、かつ違反が修正された場合
                if self._check_file_exists(os.path.join(temp_dir, file_path)) and violation_key not in current_violations:
                    if fixed == 'False':  # まだ修正されていない場合のみ
                        row[5] = current_commit  # Fix Commit Hashを設定
                        row[6] = 'True'  # FixedをTrueに設定
                writer.writerow(row)
            
            # 新規違反を追加
            for violation in current_violations:
                if violation not in existing_violations:
                    writer.writerow([violation[0], violation[1], violation[2], violation[3], current_commit, '', 'False'])
    
    def _check_file_exists(self, file_path):
        """ファイルが存在するか確認（内部メソッド）"""
        return Path(file_path).exists() 