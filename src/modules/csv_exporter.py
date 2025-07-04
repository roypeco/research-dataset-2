import csv
from pathlib import Path
from .diff_tracker import DiffTracker
import pandas as pd

class CSVExporter:
    """CSV出力に関する処理を担当するクラス"""
    
    def __init__(self, data_manager):
        self.data_manager = data_manager
        self.diff_tracker = DiffTracker()
    
    def create_fix_history_csv(self, pkg_name, initial_violations, initial_commit, temp_dir):
        """修正履歴のCSVファイルを作成（特徴量付き）"""
        result_dir = Path('dataset') / pkg_name
        result_dir.mkdir(parents=True, exist_ok=True)
        
        csv_file = result_dir / 'fix_history.csv'
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(self.data_manager._get_feature_headers())
            
            for violation in initial_violations:
                category = self.data_manager._get_violation_category(violation[0])
                features = self.data_manager._extract_features_for_violation(violation, temp_dir)
                
                # 行番号を取得
                if len(violation) == 5:
                    line_number = violation[4]
                else:
                    line_number = self.data_manager._extract_line_number_from_context(violation[3])
                
                # 基本情報 + 特徴量（行番号をMessageとContextの間に配置）
                row = [
                    violation[0], category, violation[1], violation[2], line_number, violation[3],
                    initial_commit, ''
                ] + features + ['False']
                
                writer.writerow(row)
        
        return csv_file
    
    def update_fix_history_csv(self, csv_file, current_violations, temp_dir, current_commit):
        """修正履歴のCSVファイルを更新（特徴量付き・行番号追跡付き）"""
        # まず、前のコミットで発生した違反の行番号を追跡して更新
        self._update_line_numbers_for_previous_violations(csv_file, temp_dir, current_commit)
        
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        # 現在の違反をセットに変換（行番号ベースの判定）
        current_violations_set = set()
        for violation in current_violations:
            if len(violation) == 5:
                # 行番号を含むキーを作成
                violation_key = (violation[0], violation[1], violation[2], violation[4])  # violation_id, file_path, message, line_number
            else:
                violation_key = violation
            current_violations_set.add(violation_key)
        
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(self.data_manager._get_feature_headers())  # 新しいヘッダーを使用
            
            # 既存の違反を処理
            for row in rows[1:]:
                violation_id, category, file_path, message, line_number, context, error_commit, fix_commit = row[:8]
                fixed = row[-1]  # Fixedは最後の列
                
                # 行番号ベースの違反キーを作成
                existing_violation_key = (violation_id, file_path, message, line_number)
                
                # 既存の違反が現在のコミットで検出されない場合（修正された場合）
                if existing_violation_key not in current_violations_set and fixed == 'False':
                    row[7] = current_commit  # Fix Commit Hashを設定
                    row[-1] = 'True'  # FixedをTrueに設定（最後の列）
                
                writer.writerow(row)
            
            # 新規違反を追加（重複チェック付き）
            added_violations = set()  # 同じコミット内での重複チェック用
            for violation in current_violations:
                # 行番号を含む同一性を判定
                if len(violation) == 5:
                    violation_key = (violation[0], violation[1], violation[2], violation[4])  # violation_id, file_path, message, line_number
                else:
                    violation_key = violation
                
                # 既存の行をチェックして、この違反が既に存在するか確認
                violation_exists = False
                for row in rows[1:]:
                    existing_line_number = row[4]  # Violation Line Number列（新しい位置）
                    existing_violation_key = (row[0], row[2], row[3], existing_line_number)
                    if violation_key == existing_violation_key:
                        violation_exists = True
                        break
                
                if not violation_exists:
                    category = self.data_manager._get_violation_category(violation[0])
                    features = self.data_manager._extract_features_for_violation(violation, temp_dir)
                    
                    # 行番号を取得
                    if len(violation) == 5:
                        line_number = violation[4]
                    else:
                        line_number = self.data_manager._extract_line_number_from_context(violation[3])
                    
                    # 基本情報 + 特徴量（行番号をMessageとContextの間に配置）
                    row = [
                        violation[0], category, violation[1], violation[2], line_number, violation[3],
                        current_commit, ''
                    ] + features + ['False']
                    
                    writer.writerow(row)
                    added_violations.add(violation_key)  # 追加済みとして記録
    
    def write_fix_history_csv_batch(self, csv_file, violation_rows):
        """違反データのリストを一括でCSVに書き込み（効率化版）"""
        try:
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(self.data_manager._get_feature_headers())
                
                # 違反データを一括書き込み
                for row in violation_rows:
                    if row:  # Noneチェック
                        writer.writerow(row)
                        
            return True
            
        except Exception as e:
            print(f"Error writing CSV batch: {str(e)}")
            return False
    
    def save_fix_history_to_csv(self, pkg_name, dataframe):
        """DataFrameをCSVファイルに保存"""
        if dataframe is None:
            return None
        
        result_dir = Path('dataset') / pkg_name
        result_dir.mkdir(parents=True, exist_ok=True)
        
        csv_file = result_dir / 'fix_history.csv'
        
        # Parquetと同じ型最適化を適用
        optimized_df = self.data_manager.optimize_dataframe_types(dataframe.copy())
        optimized_df.to_csv(csv_file, index=False)
        
        return csv_file
    
    def _update_line_numbers_for_previous_violations(self, csv_file, temp_dir, current_commit):
        """前のコミットで発生した違反の行番号を追跡して更新"""
        try:
            # 現在のコミットの前のコミットを取得
            import subprocess
            result = subprocess.run([
                'git', 'log', '--format=%H', '-n', '2', '--skip', '1'
            ], cwd=temp_dir, capture_output=True, text=True)
            
            if result.returncode == 0 and result.stdout.strip():
                previous_commits = result.stdout.strip().split('\n')
                if previous_commits:
                    # 最新の前のコミットを取得
                    previous_commit = previous_commits[0]
                    
                    # 行番号の追跡を実行
                    updated_count = self.diff_tracker.track_violation_movement(
                        temp_dir, csv_file, previous_commit
                    )
                    
                    if updated_count > 0:
                        print(f"Updated {updated_count} violation line numbers from previous commit {previous_commit[:8]}")
                        
        except Exception as e:
            print(f"Error updating line numbers for previous violations: {str(e)}") 