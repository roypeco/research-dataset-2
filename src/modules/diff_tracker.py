import subprocess
import re
from pathlib import Path
import csv
import os

class DiffTracker:
    """Git diffを解析して行番号の変更を追跡するクラス"""
    
    def __init__(self):
        pass
    
    def get_diff_for_commit(self, repo_path, commit_hash):
        """指定されたコミットのdiffを取得"""
        try:
            result = subprocess.run([
                'git', 'show', '--unified=0', '--no-color', commit_hash
            ], cwd=repo_path, capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"Error getting diff for commit {commit_hash}: {result.stderr}")
                return {}
            
            return self._parse_diff_output(result.stdout)
            
        except Exception as e:
            print(f"Error getting diff for commit {commit_hash}: {str(e)}")
            return {}
    
    def _parse_diff_output(self, diff_output):
        """diff出力を解析して行番号マッピングを作成"""
        line_mappings = {}
        current_file = None
        current_hunks = []
        
        for line in diff_output.split('\n'):
            # ファイルパス行を検出
            if line.startswith('diff --git'):
                # 前のファイルのhunkを処理
                if current_file and current_hunks:
                    line_mappings[current_file] = self._process_hunks(current_hunks)
                current_file = None
                current_hunks = []
                continue
            
            # ファイル名行を検出
            if line.startswith('--- a/') or line.startswith('+++ b/'):
                if line.startswith('+++ b/'):
                    current_file = line[6:]  # '+++ b/' を除去
                continue
            
            # 行番号情報行を検出
            if line.startswith('@@'):
                if current_file:
                    current_hunks.append(line)
        
        # 最後のファイルのhunkを処理
        if current_file and current_hunks:
            line_mappings[current_file] = self._process_hunks(current_hunks)
        
        return line_mappings
    
    def _process_hunks(self, hunk_lines):
        """hunk行を処理して行番号マッピングを作成"""
        line_mapping = {}
        
        for hunk_line in hunk_lines:
            mapping = self._parse_hunk_header(hunk_line)
            if mapping:
                line_mapping.update(mapping)
        
        return line_mapping
    
    def _parse_hunk_header(self, hunk_line):
        """hunkヘッダーを解析して行番号マッピングを作成"""
        # @@ -old_start,old_count +new_start,new_count @@
        pattern = r'^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@'
        match = re.match(pattern, hunk_line)
        
        if not match:
            return {}
        
        old_start = int(match.group(1))
        old_count = int(match.group(2)) if match.group(2) else 1
        new_start = int(match.group(3))
        new_count = int(match.group(4)) if match.group(4) else 1
        
        mapping = {}
        
        # 削除された行のマッピング（削除された行は新しいファイルには存在しない）
        for i in range(old_count):
            old_line = old_start + i
            mapping[old_line] = None  # 削除された行
        
        # 追加された行のマッピング（追加された行は古いファイルには存在しない）
        for i in range(new_count):
            new_line = new_start + i
            mapping[f"new_{new_line}"] = new_line
        
        return mapping
    
    def calculate_line_mapping(self, repo_path, commit_hash):
        """コミットの行番号マッピングを計算"""
        diff_mappings = self.get_diff_for_commit(repo_path, commit_hash)
        line_mappings = {}
        
        for file_path, file_mappings in diff_mappings.items():
            line_mappings[file_path] = self._calculate_file_line_mapping(file_mappings)
        
        return line_mappings
    
    def _calculate_file_line_mapping(self, file_mappings):
        """ファイル内の行番号マッピングを計算"""
        # 削除された行と追加された行を分離
        deleted_lines = []
        added_lines = []
        
        for old_line, new_line in file_mappings.items():
            if isinstance(old_line, int) and new_line is None:
                deleted_lines.append(old_line)
            elif isinstance(old_line, str) and old_line.startswith('new_'):
                added_lines.append(new_line)
        
        deleted_lines.sort()
        added_lines.sort()
        
        # 行番号のオフセットを計算
        line_mapping = {}
        offset = 0
        deleted_index = 0
        added_index = 0
        
        # 最大行番号を計算
        max_line = 1
        if deleted_lines:
            max_line = max(max_line, max(deleted_lines))
        if added_lines:
            max_line = max(max_line, max(added_lines))
        
        for line_num in range(1, max_line + 1):
            # 削除された行をチェック
            while deleted_index < len(deleted_lines) and deleted_lines[deleted_index] <= line_num:
                if deleted_lines[deleted_index] == line_num:
                    offset -= 1
                deleted_index += 1
            
            # 追加された行をチェック
            while added_index < len(added_lines) and added_lines[added_index] <= line_num + offset:
                offset += 1
                added_index += 1
            
            line_mapping[line_num] = line_num + offset
        
        return line_mapping
    
    def get_detailed_diff(self, repo_path, commit_hash):
        """詳細なdiff情報を取得（行の内容も含む）"""
        try:
            result = subprocess.run([
                'git', 'show', '--unified=3', '--no-color', commit_hash
            ], cwd=repo_path, capture_output=True, text=True)
            
            if result.returncode != 0:
                return {}
            
            return self._parse_detailed_diff(result.stdout)
            
        except Exception as e:
            print(f"Error getting detailed diff for commit {commit_hash}: {str(e)}")
            return {}
    
    def _parse_detailed_diff(self, diff_output):
        """詳細なdiff出力を解析"""
        file_diffs = {}
        current_file = None
        current_hunk = None
        
        for line in diff_output.split('\n'):
            # ファイルパス行を検出
            if line.startswith('diff --git'):
                current_file = None
                continue
            
            # ファイル名行を検出
            if line.startswith('--- a/') or line.startswith('+++ b/'):
                if line.startswith('+++ b/'):
                    current_file = line[6:]
                    file_diffs[current_file] = []
                continue
            
            # hunkヘッダーを検出
            if line.startswith('@@'):
                if current_file:
                    current_hunk = self._parse_hunk_header(line)
                    if current_hunk:
                        file_diffs[current_file].append(current_hunk)
                continue
        
        return file_diffs
    
    def update_violation_line_numbers(self, csv_file, line_mappings, commit_hash):
        """CSVファイルの違反行番号を更新"""
        updated_rows = []
        updated_count = 0
        
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)
            updated_rows.append(headers)
            
            for row in reader:
                if len(row) < len(headers):
                    continue
                
                file_path = row[2]  # File Path列
                current_line_number = row[4]  # Violation Line Number列（新しい位置）
                error_commit = row[6]  # Error Commit Hash列（新しい位置）
                fix_commit = row[7]  # Fix Commit Hash列（新しい位置）
                
                # 行番号更新の条件を修正
                # 1. まだ修正されていない違反
                # 2. ファイルに変更がある
                # 3. 有効な行番号
                # 4. 違反が発生したコミットより後のコミットで行番号追跡を実行
                if (fix_commit == '' and 
                    file_path in line_mappings and
                    current_line_number.isdigit()):
                    
                    old_line = int(current_line_number)
                    file_mapping = line_mappings[file_path]
                    
                    if old_line in file_mapping:
                        new_line = file_mapping[old_line]
                        if new_line is not None:
                            row[4] = str(new_line)  # Violation Line Numberを更新（新しい位置）
                            updated_count += 1
                
                updated_rows.append(row)
        
        # 更新されたCSVファイルを書き込み
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(updated_rows)
        
        return updated_count
    
    def track_violation_movement(self, repo_path, csv_file, commit_hash):
        """違反の移動を追跡してCSVファイルを更新"""
        print(f"Tracking violation movement for commit {commit_hash}")
        
        # 行番号マッピングを計算
        line_mappings = self.calculate_line_mapping(repo_path, commit_hash)
        
        if not line_mappings:
            print(f"No line mappings found for commit {commit_hash}")
            return 0
        
        # CSVファイルの行番号を更新
        updated_count = self.update_violation_line_numbers(csv_file, line_mappings, commit_hash)
        
        print(f"Updated {updated_count} violation line numbers for commit {commit_hash}")
        return updated_count 