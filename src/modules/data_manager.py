import csv
import json
from pathlib import Path
import os
import pandas as pd
from .feature_extractor import FeatureExtractor
from .diff_tracker import DiffTracker
from .csv_exporter import CSVExporter
from .parquet_exporter import ParquetExporter

class DataManager:
    """データの保存と読み込みを管理するクラス"""
    
    def __init__(self):
        self.feature_extractor = FeatureExtractor()
        self.diff_tracker = DiffTracker()
        # DataFrame for storing fix history in memory
        self.fix_history_df = None
        # Exporterインスタンスを初期化
        self.csv_exporter = CSVExporter(self)
        self.parquet_exporter = ParquetExporter(self)
    
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
            'Violation ID', 'Category', 'File Path', 'Message', 'Violation Line Number', 'Context', 
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
        return [0] * 33  # 33個の特徴量のデフォルト値
    
    def optimize_dataframe_types(self, df):
        """DataFrameのデータ型を最適化してファイルサイズを削減（共通メソッド）"""
        # 数値型の最適化
        numeric_columns = [
            'File Size', 'Total Lines', 'Code Lines', 'Comment Lines', 'Blank Lines',
            'File Depth', 'Filename Length', 'Line Length', 'Line Length No Whitespace',
            'Indent Level', 'Line Complexity', 'Special Chars', 'Variable Count',
            'Function Calls', 'Operators', 'Function Params', 'Function Lines',
            'Function Complexity', 'Class Methods', 'Class Lines', 'Total Functions',
            'Total Classes', 'Total Imports', 'Total Variables', 'Cyclomatic Complexity',
            'File Change Frequency', 'Lines Added Past 25 Revisions', 'Lines Added Past 3 Months'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                # 整数型の最適化
                if df[col].dtype in ['int64', 'int32']:
                    df[col] = pd.to_numeric(df[col], downcast='integer')
                # 浮動小数点型の最適化
                elif df[col].dtype in ['float64', 'float32']:
                    df[col] = pd.to_numeric(df[col], downcast='float')
        
        # ブール型の最適化
        boolean_columns = ['In Function', 'In Class', 'Fixed']
        for col in boolean_columns:
            if col in df.columns:
                # 'True'/'False'文字列をブール型に変換
                if df[col].dtype == 'object':
                    df[col] = df[col].map({'True': True, 'False': False, True: True, False: False})
                df[col] = df[col].astype('bool')
        
        # 行番号を整数型に変換
        if 'Violation Line Number' in df.columns:
            try:
                df['Violation Line Number'] = pd.to_numeric(df['Violation Line Number'], errors='coerce')
                df['Violation Line Number'] = df['Violation Line Number'].astype('Int64')  # nullable integer
            except:
                pass
        
        # カテゴリ型の最適化（重複の多い文字列列）
        category_columns = ['Violation ID', 'Category', 'File Extension']
        for col in category_columns:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = df[col].astype('category')
        
        return df
    
    def create_violation_row_data(self, violation, commit_hash, temp_dir, fixed=False, fix_commit=''):
        """違反情報から1行分のCSVデータを作成（バッファリング用）"""
        try:
            category = self._get_violation_category(violation[0])
            features = self._extract_features_for_violation(violation, temp_dir)
            
            # 行番号を取得
            if len(violation) == 5:
                line_number = violation[4]
            else:
                line_number = self._extract_line_number_from_context(violation[3])
            
            # 基本情報 + 特徴量
            row = [
                violation[0], category, violation[1], violation[2], line_number, violation[3],
                commit_hash, fix_commit
            ] + features + [str(fixed)]
            
            return row
            
        except Exception as e:
            print(f"Error creating violation row data: {str(e)}")
            return None

    def write_fix_history_csv_batch(self, csv_file, violation_rows):
        """違反データのリストを一括でCSVに書き込み（効率化版）"""
        return self.csv_exporter.write_fix_history_csv_batch(csv_file, violation_rows)

    def process_violations_batch_optimized(self, initial_violations, commits_data, temp_dir, pkg_name):
        """違反データを一括処理（行番号追跡付き・最適化版）"""
        violation_rows = []
        violation_tracker = {}  # 違反追跡用: key=(violation_id, file_path, line_number), value=row_index
        
        # プロジェクトロガーを取得
        import logging
        project_logger = logging.getLogger(f"project.{pkg_name}")
        project_logger.info(f"Processing {len(initial_violations)} initial violations with optimized line tracking")
        
        # 初期違反を処理
        for violation in initial_violations:
            row = self.create_violation_row_data(violation, commits_data[0]['commit'], temp_dir, fixed=False)
            if row:
                violation_rows.append(row)
                # 追跡用キーを作成
                if len(violation) == 5:
                    violation_key = (violation[0], violation[1], violation[4])
                else:
                    line_number = self._extract_line_number_from_context(violation[3])
                    violation_key = (violation[0], violation[1], line_number)
                violation_tracker[violation_key] = len(violation_rows) - 1
        
        project_logger.info(f"Initial violations processed: {len(violation_rows)} records")
        
        # 各コミットの違反を処理（行番号追跡付き・最適化版）
        total_commits = len(commits_data) - 1
        for commit_index, commit_data in enumerate(commits_data[1:], 1):
            commit_hash = commit_data['commit']
            current_violations = commit_data['violations']
            
            if commit_index % 10 == 0 or commit_index == total_commits:
                project_logger.info(f"Processing commit {commit_index}/{total_commits}: {commit_hash[:8]}")
            
            # 行番号マッピングを計算（前のコミットからの変更）
            line_mappings = self._calculate_line_mappings_for_commit(temp_dir, commit_hash)
            
            # 既存違反の行番号を更新
            updated_violations = self._update_violation_line_numbers_batch(
                violation_tracker, violation_rows, line_mappings
            )
            
            # 現在の違反をセットに変換（更新後の行番号で）
            current_violations_set = set()
            for violation in current_violations:
                if len(violation) == 5:
                    violation_key = (violation[0], violation[1], violation[4])
                else:
                    line_number = self._extract_line_number_from_context(violation[3])
                    violation_key = (violation[0], violation[1], line_number)
                current_violations_set.add(violation_key)
            
            # 既存の違反で修正されたものをチェック（更新後の行番号で）
            fixed_count = 0
            for violation_key, row_index in list(violation_tracker.items()):
                if violation_key not in current_violations_set:
                    # 違反が修正された
                    if violation_rows[row_index][-1] == 'False':  # まだ修正されていない場合
                        violation_rows[row_index][7] = commit_hash  # Fix Commit Hash
                        violation_rows[row_index][-1] = 'True'  # Fixed
                        fixed_count += 1
            
            # 新規違反を追加
            new_violations_count = 0
            for violation in current_violations:
                if len(violation) == 5:
                    violation_key = (violation[0], violation[1], violation[4])
                else:
                    line_number = self._extract_line_number_from_context(violation[3])
                    violation_key = (violation[0], violation[1], line_number)
                
                if violation_key not in violation_tracker:
                    # 新規違反
                    row = self.create_violation_row_data(violation, commit_hash, temp_dir, fixed=False)
                    if row:
                        violation_rows.append(row)
                        violation_tracker[violation_key] = len(violation_rows) - 1
                        new_violations_count += 1
            
            # 重要な変更のみログ出力
            if updated_violations > 0 or fixed_count > 0 or new_violations_count > 0:
                if commit_index % 10 == 0 or any([updated_violations > 5, fixed_count > 5, new_violations_count > 5]):
                    project_logger.info(
                        f"Commit {commit_hash[:8]}: {updated_violations} lines updated, "
                        f"{fixed_count} fixed, {new_violations_count} new violations"
                    )
        
        project_logger.info(f"Optimized batch processing completed: {len(violation_rows)} total records")
        return violation_rows
    
    def _calculate_line_mappings_for_commit(self, repo_path, commit_hash):
        """指定されたコミットの行番号マッピングを計算"""
        try:
            return self.diff_tracker.calculate_line_mapping(repo_path, commit_hash)
        except Exception as e:
            print(f"Error calculating line mappings for {commit_hash}: {str(e)}")
            return {}
    
    def _update_violation_line_numbers_batch(self, violation_tracker, violation_rows, line_mappings):
        """バッチ処理で違反の行番号を更新"""
        updated_count = 0
        
        if not line_mappings:
            return updated_count
        
        # violation_trackerのキーを更新する必要があるため、新しい辞書を作成
        new_violation_tracker = {}
        
        for violation_key, row_index in list(violation_tracker.items()):
            violation_id, file_path, current_line_number = violation_key
            
            # まだ修正されていない違反のみ処理
            if violation_rows[row_index][-1] == 'False':  # Fixed列
                if file_path in line_mappings and str(current_line_number).isdigit():
                    old_line = int(current_line_number)
                    file_mapping = line_mappings[file_path]
                    
                    if old_line in file_mapping:
                        new_line = file_mapping[old_line]
                        if new_line is not None and new_line != old_line:
                            # 行番号を更新
                            violation_rows[row_index][4] = str(new_line)  # Violation Line Number列
                            
                            # 新しいキーでtracker辞書を更新
                            new_key = (violation_id, file_path, str(new_line))
                            new_violation_tracker[new_key] = row_index
                            updated_count += 1
                        else:
                            # 行番号に変更がない場合は元のキーを保持
                            new_violation_tracker[violation_key] = row_index
                    else:
                        # マッピングにない場合は元のキーを保持
                        new_violation_tracker[violation_key] = row_index
                else:
                    # ファイルが変更されていないか、行番号が無効な場合
                    new_violation_tracker[violation_key] = row_index
            else:
                # 既に修正された違反は更新しない
                new_violation_tracker[violation_key] = row_index
        
        # violation_trackerを更新
        violation_tracker.clear()
        violation_tracker.update(new_violation_tracker)
        
        return updated_count
    
    def process_violations_batch_with_line_tracking(self, initial_violations, commits_data, temp_dir, pkg_name):
        """違反データを一括処理（行番号追跡付き）- 最適化版にリダイレクト"""
        return self.process_violations_batch_optimized(initial_violations, commits_data, temp_dir, pkg_name)
    
    def process_violations_batch_fast(self, initial_violations, commits_data, temp_dir, pkg_name, track_line_numbers=True):
        """バッチ処理（行番号追跡は必須）"""
        # 行番号追跡は必須のため、常に最適化版を使用
        return self.process_violations_batch_optimized(
            initial_violations, commits_data, temp_dir, pkg_name
        )
    
    def create_fix_history_csv(self, pkg_name, initial_violations, initial_commit, temp_dir):
        """修正履歴のCSVファイルを作成（特徴量付き）"""
        return self.csv_exporter.create_fix_history_csv(pkg_name, initial_violations, initial_commit, temp_dir)
    
    def update_fix_history_csv(self, csv_file, current_violations, temp_dir, current_commit):
        """修正履歴のCSVファイルを更新（特徴量付き・行番号追跡付き）"""
        return self.csv_exporter.update_fix_history_csv(csv_file, current_violations, temp_dir, current_commit)
    
    def _check_file_exists(self, file_path):
        """ファイルが存在するか確認（内部メソッド）"""
        return Path(file_path).exists()

    def initialize_fix_history_dataframe(self, initial_violations, initial_commit, temp_dir):
        """修正履歴のDataFrameを初期化"""
        rows = []
        
        for violation in initial_violations:
            category = self._get_violation_category(violation[0])
            features = self._extract_features_for_violation(violation, temp_dir)
            
            # 行番号を取得
            if len(violation) == 5:
                line_number = violation[4]
            else:
                line_number = self._extract_line_number_from_context(violation[3])
            
            # 基本情報 + 特徴量
            row_data = {
                'Violation ID': violation[0],
                'Category': category,
                'File Path': violation[1],
                'Message': violation[2],
                'Violation Line Number': line_number,
                'Context': violation[3],
                'Error Commit Hash': initial_commit,
                'Fix Commit Hash': ''
            }
            
            # 特徴量を追加
            feature_names = [
                'File Size', 'Total Lines', 'Code Lines', 'Comment Lines', 'Blank Lines',
                'File Depth', 'File Extension', 'Filename Length',
                'Line Length', 'Line Length No Whitespace', 'Indent Level', 'Line Complexity',
                'Special Chars', 'Variable Count', 'Function Calls', 'Operators',
                'In Function', 'Function Name', 'Function Params', 'Function Lines', 'Function Complexity',
                'In Class', 'Class Name', 'Class Methods', 'Class Lines',
                'Total Functions', 'Total Classes', 'Total Imports', 'Total Variables',
                'Cyclomatic Complexity', 'File Change Frequency',
                'Lines Added Past 25 Revisions', 'Lines Added Past 3 Months'
            ]
            
            for i, feature_name in enumerate(feature_names):
                row_data[feature_name] = features[i] if i < len(features) else 0
            
            row_data['Fixed'] = False
            rows.append(row_data)
        
        self.fix_history_df = pd.DataFrame(rows)
        return True
    
    def update_fix_history_dataframe(self, current_violations, temp_dir, current_commit):
        """修正履歴のDataFrameを更新"""
        if self.fix_history_df is None:
            return False
        
        # 前のコミットで発生した違反の行番号を追跡して更新
        self._update_dataframe_line_numbers_for_previous_violations(temp_dir, current_commit)
        
        # 現在の違反をセットに変換
        current_violations_set = set()
        for violation in current_violations:
            if len(violation) == 5:
                violation_key = (violation[0], violation[1], violation[4])
            else:
                line_number = self._extract_line_number_from_context(violation[3])
                violation_key = (violation[0], violation[1], line_number)
            current_violations_set.add(violation_key)
        
        # 既存の違反で修正されたものをチェック
        for index, row in self.fix_history_df.iterrows():
            if not row['Fixed']:  # まだ修正されていない場合
                existing_violation_key = (
                    row['Violation ID'], 
                    row['File Path'], 
                    row['Violation Line Number']
                )
                
                if existing_violation_key not in current_violations_set:
                    # 違反が修正された
                    self.fix_history_df.loc[index, 'Fix Commit Hash'] = current_commit
                    self.fix_history_df.loc[index, 'Fixed'] = True
        
        # 新規違反を追加
        new_rows = []
        for violation in current_violations:
            if len(violation) == 5:
                violation_key = (violation[0], violation[1], violation[4])
            else:
                line_number = self._extract_line_number_from_context(violation[3])
                violation_key = (violation[0], violation[1], line_number)
            
            # 既存の違反かチェック
            existing_violation = False
            for _, row in self.fix_history_df.iterrows():
                existing_key = (
                    row['Violation ID'], 
                    row['File Path'], 
                    row['Violation Line Number']
                )
                if violation_key == existing_key:
                    existing_violation = True
                    break
            
            if not existing_violation:
                # 新規違反
                category = self._get_violation_category(violation[0])
                features = self._extract_features_for_violation(violation, temp_dir)
                
                # 行番号を取得
                if len(violation) == 5:
                    line_number = violation[4]
                else:
                    line_number = self._extract_line_number_from_context(violation[3])
                
                row_data = {
                    'Violation ID': violation[0],
                    'Category': category,
                    'File Path': violation[1],
                    'Message': violation[2],
                    'Violation Line Number': line_number,
                    'Context': violation[3],
                    'Error Commit Hash': current_commit,
                    'Fix Commit Hash': ''
                }
                
                # 特徴量を追加
                feature_names = [
                    'File Size', 'Total Lines', 'Code Lines', 'Comment Lines', 'Blank Lines',
                    'File Depth', 'File Extension', 'Filename Length',
                    'Line Length', 'Line Length No Whitespace', 'Indent Level', 'Line Complexity',
                    'Special Chars', 'Variable Count', 'Function Calls', 'Operators',
                    'In Function', 'Function Name', 'Function Params', 'Function Lines', 'Function Complexity',
                    'In Class', 'Class Name', 'Class Methods', 'Class Lines',
                    'Total Functions', 'Total Classes', 'Total Imports', 'Total Variables',
                    'Cyclomatic Complexity', 'File Change Frequency',
                    'Lines Added Past 25 Revisions', 'Lines Added Past 3 Months'
                ]
                
                for i, feature_name in enumerate(feature_names):
                    row_data[feature_name] = features[i] if i < len(features) else 0
                
                row_data['Fixed'] = False
                new_rows.append(row_data)
        
        # 新規違反をDataFrameに追加
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            self.fix_history_df = pd.concat([self.fix_history_df, new_df], ignore_index=True)
        
        return True
    
    def save_fix_history_to_csv(self, pkg_name):
        """DataFrameをCSVファイルに保存"""
        if self.fix_history_df is None:
            return None
        
        csv_file = self.csv_exporter.save_fix_history_to_csv(pkg_name, self.fix_history_df)
        
        # DataFrameをクリア
        self.fix_history_df = None
        
        return csv_file
    
    def save_fix_history_to_parquet(self, pkg_name):
        """DataFrameをParquetファイルに保存"""
        if self.fix_history_df is None:
            return None
        
        parquet_file = self.parquet_exporter.save_fix_history_to_parquet(pkg_name, self.fix_history_df)
        
        # DataFrameをクリア
        self.fix_history_df = None
        
        return parquet_file
    
    def _update_dataframe_line_numbers_for_previous_violations(self, temp_dir, current_commit):
        """前のコミットで発生した違反の行番号を追跡してDataFrameを更新"""
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
                    updated_count = self._track_violation_movement_dataframe(
                        temp_dir, previous_commit
                    )
                    
                    if updated_count > 0:
                        print(f"Updated {updated_count} violation line numbers from previous commit {previous_commit[:8]}")
                        
        except Exception as e:
            print(f"Error updating line numbers for previous violations: {str(e)}")
    
    def _track_violation_movement_dataframe(self, repo_path, commit_hash):
        """違反の移動を追跡してDataFrameを更新"""
        print(f"Tracking violation movement for commit {commit_hash}")
        
        # 行番号マッピングを計算
        line_mappings = self.diff_tracker.calculate_line_mapping(repo_path, commit_hash)
        
        if not line_mappings:
            print(f"No line mappings found for commit {commit_hash}")
            return 0
        
        # DataFrameの行番号を更新
        updated_count = self._update_dataframe_violation_line_numbers(line_mappings, commit_hash)
        
        print(f"Updated {updated_count} violation line numbers for commit {commit_hash}")
        return updated_count
    
    def _update_dataframe_violation_line_numbers(self, line_mappings, commit_hash):
        """DataFrameの違反行番号を更新"""
        updated_count = 0
        
        for index, row in self.fix_history_df.iterrows():
            file_path = row['File Path']
            current_line_number = row['Violation Line Number']
            fix_commit = row['Fix Commit Hash']
            
            # 行番号更新の条件
            # 1. まだ修正されていない違反
            # 2. ファイルに変更がある
            # 3. 有効な行番号
            if (fix_commit == '' and 
                file_path in line_mappings and
                str(current_line_number).isdigit()):
                
                old_line = int(current_line_number)
                file_mapping = line_mappings[file_path]
                
                if old_line in file_mapping:
                    new_line = file_mapping[old_line]
                    if new_line is not None:
                        self.fix_history_df.loc[index, 'Violation Line Number'] = str(new_line)
                        updated_count += 1
        
        return updated_count 

    def clear_all_caches(self):
        """全てのキャッシュをクリア（メモリ節約）"""
        self.feature_extractor.clear_cache()
        # 他のキャッシュも必要に応じてクリア
        if hasattr(self, 'csv_exporter') and hasattr(self.csv_exporter, 'clear_cache'):
            self.csv_exporter.clear_cache()
        if hasattr(self, 'parquet_exporter') and hasattr(self.parquet_exporter, 'clear_cache'):
            self.parquet_exporter.clear_cache() 