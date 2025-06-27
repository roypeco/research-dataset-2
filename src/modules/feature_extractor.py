import ast
import os
import re
from pathlib import Path
from collections import defaultdict

class FeatureExtractor:
    """静的解析警告の特徴量を抽出するクラス"""
    
    def __init__(self):
        self.complexity_metrics = {}
    
    def extract_features(self, file_path, line_number, repo_path):
        """指定されたファイルと行から特徴量を抽出"""
        features = {}
        
        # ファイルレベルの特徴量
        features.update(self._extract_file_features(file_path, repo_path))
        
        # 行レベルの特徴量
        features.update(self._extract_line_features(file_path, line_number))
        
        # 関数レベルの特徴量
        features.update(self._extract_function_features(file_path, line_number))
        
        # クラスレベルの特徴量
        features.update(self._extract_class_features(file_path, line_number))
        
        # モジュールレベルの特徴量
        features.update(self._extract_module_features(file_path, repo_path))
        
        features['violation_line_number'] = line_number
        
        return features
    
    def _extract_file_features(self, file_path, repo_path):
        """ファイルレベルの特徴量を抽出"""
        features = {}
        
        try:
            # ファイルサイズ
            file_size = os.path.getsize(file_path)
            features['file_size'] = file_size
            
            # ファイルの行数
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
                features['total_lines'] = len(lines)
                features['code_lines'] = len([l for l in lines if l.strip() and not l.strip().startswith('#')])
                features['comment_lines'] = len([l for l in lines if l.strip().startswith('#')])
                features['blank_lines'] = len([l for l in lines if not l.strip()])
            
            # ファイルの深さ（ディレクトリ階層）
            rel_path = os.path.relpath(file_path, repo_path)
            features['file_depth'] = len(Path(rel_path).parts)
            
            # ファイル拡張子
            features['file_extension'] = Path(file_path).suffix
            
            # ファイル名の長さ
            features['filename_length'] = len(Path(file_path).name)
            
        except Exception as e:
            print(f"Error extracting file features for {file_path}: {str(e)}")
            features.update({
                'file_size': 0,
                'total_lines': 0,
                'code_lines': 0,
                'comment_lines': 0,
                'blank_lines': 0,
                'file_depth': 0,
                'file_extension': '',
                'filename_length': 0
            })
        
        return features
    
    def _extract_line_features(self, file_path, line_number):
        """行レベルの特徴量を抽出"""
        features = {}
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
                
            if 0 <= int(line_number) - 1 < len(lines):
                line = lines[int(line_number) - 1]
                
                # 行の長さ
                features['line_length'] = len(line)
                features['line_length_no_whitespace'] = len(line.strip())
                
                # インデントレベル
                features['indent_level'] = len(line) - len(line.lstrip())
                
                # 行の複雑さ
                features['line_complexity'] = self._calculate_line_complexity(line)
                
                # 特殊文字の数
                features['special_chars'] = len(re.findall(r'[^\w\s]', line))
                
                # 変数名の数
                features['variable_count'] = len(re.findall(r'\b[a-zA-Z_]\w*\s*=', line))
                
                # 関数呼び出しの数
                features['function_calls'] = len(re.findall(r'\b\w+\s*\(', line))
                
                # 演算子の数
                features['operators'] = len(re.findall(r'[+\-*/=<>!&|^~]', line))
                
            else:
                features.update({
                    'line_length': 0,
                    'line_length_no_whitespace': 0,
                    'indent_level': 0,
                    'line_complexity': 0,
                    'special_chars': 0,
                    'variable_count': 0,
                    'function_calls': 0,
                    'operators': 0
                })
                
        except Exception as e:
            print(f"Error extracting line features for {file_path}:{line_number}: {str(e)}")
            features.update({
                'line_length': 0,
                'line_length_no_whitespace': 0,
                'indent_level': 0,
                'line_complexity': 0,
                'special_chars': 0,
                'variable_count': 0,
                'function_calls': 0,
                'operators': 0
            })
        
        return features
    
    def _extract_function_features(self, file_path, line_number):
        """関数レベルの特徴量を抽出"""
        features = {}
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            tree = ast.parse(content)
            line_num = int(line_number)
            
            # 関数内の行かどうか
            in_function = False
            function_name = ''
            function_params = 0
            function_lines = 0
            function_complexity = 0
            
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    if node.lineno <= line_num <= node.end_lineno:
                        in_function = True
                        function_name = node.name
                        function_params = len(node.args.args)
                        function_lines = node.end_lineno - node.lineno + 1
                        function_complexity = self._calculate_function_complexity(node)
                        break
            
            features['in_function'] = 1 if in_function else 0
            features['function_name'] = function_name
            features['function_params'] = function_params
            features['function_lines'] = function_lines
            features['function_complexity'] = function_complexity
            
        except Exception as e:
            print(f"Error extracting function features for {file_path}:{line_number}: {str(e)}")
            features.update({
                'in_function': 0,
                'function_name': '',
                'function_params': 0,
                'function_lines': 0,
                'function_complexity': 0
            })
        
        return features
    
    def _extract_class_features(self, file_path, line_number):
        """クラスレベルの特徴量を抽出"""
        features = {}
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            tree = ast.parse(content)
            line_num = int(line_number)
            
            # クラス内の行かどうか
            in_class = False
            class_name = ''
            class_methods = 0
            class_lines = 0
            
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    if node.lineno <= line_num <= node.end_lineno:
                        in_class = True
                        class_name = node.name
                        class_lines = node.end_lineno - node.lineno + 1
                        class_methods = len([n for n in node.body if isinstance(n, ast.FunctionDef)])
                        break
            
            features['in_class'] = 1 if in_class else 0
            features['class_name'] = class_name
            features['class_methods'] = class_methods
            features['class_lines'] = class_lines
            
        except Exception as e:
            print(f"Error extracting class features for {file_path}:{line_number}: {str(e)}")
            features.update({
                'in_class': 0,
                'class_name': '',
                'class_methods': 0,
                'class_lines': 0
            })
        
        return features
    
    def _extract_module_features(self, file_path, repo_path):
        """モジュールレベルの特徴量を抽出"""
        features = {}
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            tree = ast.parse(content)
            
            # モジュール全体の統計
            features['total_functions'] = len([n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)])
            features['total_classes'] = len([n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)])
            features['total_imports'] = len([n for n in ast.walk(tree) if isinstance(n, (ast.Import, ast.ImportFrom))])
            features['total_variables'] = len([n for n in ast.walk(tree) if isinstance(n, ast.Assign)])
            
            # 循環複雑度
            features['cyclomatic_complexity'] = self._calculate_cyclomatic_complexity(tree)
            
            # ファイルの変更頻度（簡易版）
            features['file_change_frequency'] = self._estimate_file_change_frequency(file_path, repo_path)
            
            # F40: 過去25回のリビジョンでファイルに追加されたコード行数
            features['lines_added_past_25_revisions'] = self._get_lines_added_past_revisions(file_path, repo_path, 25)
            
            # F46: 過去3ヶ月でパッケージに追加されたコード行数
            features['lines_added_past_3_months'] = self._get_lines_added_past_months(file_path, repo_path, 3)
            
        except Exception as e:
            print(f"Error extracting module features for {file_path}: {str(e)}")
            features.update({
                'total_functions': 0,
                'total_classes': 0,
                'total_imports': 0,
                'total_variables': 0,
                'cyclomatic_complexity': 0,
                'file_change_frequency': 0,
                'lines_added_past_25_revisions': 0,
                'lines_added_past_3_months': 0
            })
        
        return features
    
    def _calculate_line_complexity(self, line):
        """行の複雑さを計算"""
        complexity = 0
        complexity += len(re.findall(r'\bif\b|\bfor\b|\bwhile\b|\band\b|\bor\b', line))
        complexity += len(re.findall(r'[+\-*/=<>!&|^~]', line))
        return complexity
    
    def _calculate_function_complexity(self, node):
        """関数の循環複雑度を計算"""
        complexity = 1  # 基本の複雑度
        
        for child in ast.walk(node):
            if isinstance(child, (ast.If, ast.For, ast.While, ast.AsyncFor, ast.AsyncWith)):
                complexity += 1
            elif isinstance(child, ast.BoolOp):
                complexity += len(child.values) - 1
            elif isinstance(child, ast.ExceptHandler):
                complexity += 1
        
        return complexity
    
    def _calculate_cyclomatic_complexity(self, tree):
        """モジュール全体の循環複雑度を計算"""
        complexity = 0
        
        for node in ast.walk(tree):
            if isinstance(node, (ast.If, ast.For, ast.While, ast.AsyncFor, ast.AsyncWith)):
                complexity += 1
            elif isinstance(node, ast.BoolOp):
                complexity += len(node.values) - 1
            elif isinstance(node, ast.ExceptHandler):
                complexity += 1
        
        return complexity
    
    def _estimate_file_change_frequency(self, file_path, repo_path):
        """ファイルの変更頻度を推定（簡易版）"""
        try:
            # Gitの履歴から変更回数を取得
            import subprocess
            result = subprocess.run(
                ['git', 'log', '--oneline', '--', os.path.relpath(file_path, repo_path)],
                cwd=repo_path, capture_output=True, text=True
            )
            return len(result.stdout.strip().split('\n')) if result.stdout.strip() else 0
        except:
            return 0
    
    def _get_lines_added_past_revisions(self, file_path, repo_path, num_revisions):
        """過去N回のリビジョンでファイルに追加されたコード行数を取得（F40）"""
        try:
            import subprocess
            rel_path = os.path.relpath(file_path, repo_path)
            
            # 過去N回のコミットを取得
            result = subprocess.run(
                ['git', 'log', '--format=%H', '-n', str(num_revisions), '--', rel_path],
                cwd=repo_path, capture_output=True, text=True
            )
            
            if not result.stdout.strip():
                return 0
            
            total_lines_added = 0
            commits = result.stdout.strip().split('\n')
            
            for commit in commits:
                if commit:
                    # 各コミットでの追加行数を取得
                    lines_added = self._get_lines_added_in_commit(repo_path, commit, rel_path)
                    total_lines_added += lines_added
            
            return total_lines_added
            
        except Exception as e:
            print(f"Error getting lines added in past {num_revisions} revisions: {str(e)}")
            return 0
    
    def _get_lines_added_past_months(self, file_path, repo_path, months):
        """過去Nヶ月でパッケージに追加されたコード行数を取得（F46）"""
        try:
            import subprocess
            from datetime import datetime, timedelta
            
            # Nヶ月前の日付を計算
            end_date = datetime.now()
            start_date = end_date - timedelta(days=months * 30)
            
            # 日付範囲でコミットを取得
            result = subprocess.run([
                'git', 'log', '--format=%H', 
                '--since', start_date.strftime('%Y-%m-%d'),
                '--until', end_date.strftime('%Y-%m-%d'),
                '--', os.path.relpath(file_path, repo_path)
            ], cwd=repo_path, capture_output=True, text=True)
            
            if not result.stdout.strip():
                return 0
            
            total_lines_added = 0
            commits = result.stdout.strip().split('\n')
            
            for commit in commits:
                if commit:
                    # 各コミットでの追加行数を取得
                    rel_path = os.path.relpath(file_path, repo_path)
                    lines_added = self._get_lines_added_in_commit(repo_path, commit, rel_path)
                    total_lines_added += lines_added
            
            return total_lines_added
            
        except Exception as e:
            print(f"Error getting lines added in past {months} months: {str(e)}")
            return 0
    
    def _get_lines_added_in_commit(self, repo_path, commit_hash, file_path):
        """特定のコミットでファイルに追加された行数を取得"""
        try:
            import subprocess
            
            # コミットでの変更統計を取得
            result = subprocess.run([
                'git', 'show', '--stat', '--format=', commit_hash, '--', file_path
            ], cwd=repo_path, capture_output=True, text=True)
            
            if not result.stdout.strip():
                return 0
            
            # 統計情報から追加行数を抽出
            # 例: " 10 files changed, 150 insertions(+), 50 deletions(-)"
            lines = result.stdout.strip().split('\n')
            for line in lines:
                if 'insertions' in line:
                    # 正規表現で追加行数を抽出
                    import re
                    match = re.search(r'(\d+)\s+insertions?', line)
                    if match:
                        return int(match.group(1))
            
            return 0
            
        except Exception as e:
            print(f"Error getting lines added in commit {commit_hash}: {str(e)}")
            return 0 