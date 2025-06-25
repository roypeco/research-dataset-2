import csv
import json
from pathlib import Path
import os
from .feature_extractor import FeatureExtractor

class DataManager:
    """データの保存と読み込みを管理するクラス"""
    
    def __init__(self):
        self.feature_extractor = FeatureExtractor()
    
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
    
    def _get_violation_category(self, violation_id):
        """警告IDからカテゴリーを判定する"""
        if not violation_id:
            return ''
        
        # 最初の文字でカテゴリーを判定
        first_char = violation_id[0].upper()
        if first_char == 'E':
            return 'Error'
        elif first_char == 'W':
            return 'Warning'
        elif first_char == 'F':
            return 'Fatal'
        elif first_char == 'C':
            return 'Complexity'
        elif first_char == 'N':
            return 'Naming'
        else:
            return 'Other'
    
    def _get_feature_headers(self):
        """特徴量のヘッダーを取得"""
        return [
            # 基本情報
            'Violation ID', 'Category', 'File Path', 'Message', 'Context', 
            'Error Commit Hash', 'Fix Commit Hash',
            # ファイルレベルの特徴量
            'File Size', 'Total Lines', 'Code Lines', 'Comment Lines', 'Blank Lines',
            'File Depth', 'File Extension', 'Filename Length',
            # 行レベルの特徴量
            'Line Length', 'Line Length No Whitespace', 'Indent Level', 'Line Complexity',
            'Special Chars', 'Variable Count', 'Function Calls', 'Operators',
            # 関数レベルの特徴量
            'In Function', 'Function Name', 'Function Params', 'Function Lines', 'Function Complexity',
            # クラスレベルの特徴量
            'In Class', 'Class Name', 'Class Methods', 'Class Lines',
            # モジュールレベルの特徴量
            'Total Functions', 'Total Classes', 'Total Imports', 'Total Variables',
            'Cyclomatic Complexity', 'File Change Frequency',
            # 時間ベースの特徴量（論文のF40, F46）
            'Lines Added Past 25 Revisions', 'Lines Added Past 3 Months',
            # 修正状態（最終列）
            'Fixed'
        ]
    
    def _extract_features_for_violation(self, violation, temp_dir):
        """違反に対して特徴量を抽出"""
        try:
            # 新しい違反キー形式: (error_code, file_path, message, context, line_number)
            if len(violation) == 5:
                violation_id, file_path, message, context, line_number = violation
            else:
                # 旧形式との互換性
                violation_id, file_path, message, context = violation
                line_number = self._extract_line_number_from_context(context)
            
            full_file_path = os.path.join(temp_dir, file_path)
            
            if line_number and os.path.exists(full_file_path):
                features = self.feature_extractor.extract_features(full_file_path, line_number, temp_dir)
                return self._format_features_for_csv(features)
            else:
                return self._get_default_features()
                
        except Exception as e:
            print(f"Error extracting features for violation: {str(e)}")
            return self._get_default_features()
    
    def _extract_line_number_from_context(self, context):
        """コンテキストから行番号を抽出"""
        try:
            # flake8の出力形式: file:line:col:code message
            # コンテキストから行番号を推定
            lines = context.strip().split('\n')
            if lines:
                # 最初の行のインデントレベルから行番号を推定
                first_line = lines[0]
                indent_level = len(first_line) - len(first_line.lstrip())
                # 簡易的な推定（実際の行番号はflake8の出力から取得する必要があります）
                return str(indent_level + 1)
        except:
            pass
        return "1"  # デフォルト値
    
    def _format_features_for_csv(self, features):
        """特徴量をCSV用の形式に変換"""
        return [
            features.get('file_size', 0),
            features.get('total_lines', 0),
            features.get('code_lines', 0),
            features.get('comment_lines', 0),
            features.get('blank_lines', 0),
            features.get('file_depth', 0),
            features.get('file_extension', ''),
            features.get('filename_length', 0),
            features.get('line_length', 0),
            features.get('line_length_no_whitespace', 0),
            features.get('indent_level', 0),
            features.get('line_complexity', 0),
            features.get('special_chars', 0),
            features.get('variable_count', 0),
            features.get('function_calls', 0),
            features.get('operators', 0),
            features.get('in_function', 0),
            features.get('function_name', ''),
            features.get('function_params', 0),
            features.get('function_lines', 0),
            features.get('function_complexity', 0),
            features.get('in_class', 0),
            features.get('class_name', ''),
            features.get('class_methods', 0),
            features.get('class_lines', 0),
            features.get('total_functions', 0),
            features.get('total_classes', 0),
            features.get('total_imports', 0),
            features.get('total_variables', 0),
            features.get('cyclomatic_complexity', 0),
            features.get('file_change_frequency', 0),
            features.get('lines_added_past_25_revisions', 0),
            features.get('lines_added_past_3_months', 0)
        ]
    
    def _get_default_features(self):
        """デフォルトの特徴量値を取得"""
        return [0] * 34  # 34個の特徴量のデフォルト値
    
    def create_fix_history_csv(self, pkg_name, initial_violations, initial_commit, temp_dir):
        """修正履歴のCSVファイルを作成（特徴量付き）"""
        result_dir = Path('dataset') / pkg_name
        result_dir.mkdir(parents=True, exist_ok=True)
        
        csv_file = result_dir / 'fix_history.csv'
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(self._get_feature_headers())
            
            for violation in initial_violations:
                category = self._get_violation_category(violation[0])
                features = self._extract_features_for_violation(violation, temp_dir)
                
                # 基本情報 + 特徴量
                row = [
                    violation[0], category, violation[1], violation[2], violation[3],
                    initial_commit, ''
                ] + features + ['False']
                
                writer.writerow(row)
        
        return csv_file
    
    def update_fix_history_csv(self, csv_file, current_violations, temp_dir, current_commit):
        """修正履歴のCSVファイルを更新（特徴量付き）"""
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        existing_violations = set()
        violation_data = {}  # 既存の違反データを保持
        
        # 既存の違反データを読み込み
        for row in rows[1:]:
            violation_id, category, file_path, message, context, error_commit, fix_commit = row[:7]
            fixed = row[-1]  # Fixedは最後の列
            # 行番号を除いて同一性を判定（違反ID、ファイルパス、メッセージ、コンテキストのみ）
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
                violation_id, category, file_path, message, context, error_commit, fix_commit = row[:7]
                fixed = row[-1]  # Fixedは最後の列
                # 行番号を除いて同一性を判定
                violation_key = (violation_id, file_path, message, context)
                
                # ファイルが存在し、かつ違反が修正された場合
                if self._check_file_exists(os.path.join(temp_dir, file_path)) and violation_key not in existing_violations:
                    if fixed == 'False':  # まだ修正されていない場合のみ
                        row[6] = current_commit  # Fix Commit Hashを設定
                        row[-1] = 'True'  # FixedをTrueに設定（最後の列）
                writer.writerow(row)
            
            # 新規違反を追加
            for violation in current_violations:
                # 行番号を除いて同一性を判定
                if len(violation) == 5:
                    # 行番号を含む形式から行番号を除く
                    violation_key = (violation[0], violation[1], violation[2], violation[3])
                else:
                    # 旧形式
                    violation_key = violation
                
                if violation_key not in existing_violations:
                    category = self._get_violation_category(violation[0])
                    features = self._extract_features_for_violation(violation, temp_dir)
                    
                    # 基本情報 + 特徴量
                    row = [
                        violation[0], category, violation[1], violation[2], violation[3],
                        current_commit, '', 'False'
                    ] + features + ['False']
                    
                    writer.writerow(row)
    
    def _check_file_exists(self, file_path):
        """ファイルが存在するか確認（内部メソッド）"""
        return Path(file_path).exists() 