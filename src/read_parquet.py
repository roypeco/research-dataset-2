import pandas as pd
import os

def read_parquet_file():
    """
    相対パスを使用してparquetファイルを読み込み、DataFrameで表示する関数
    """
    # 相対パスを指定してparquetファイルを読み込み
    parquet_path = "dataset/Flask/fix_history.parquet"
    
    try:
        # parquetファイルをDataFrameとして読み込み
        df = pd.read_parquet(parquet_path)
        
        # 基本的な情報を表示
        print("=== Parquetファイルの内容 ===")
        print(f"ファイルパス: {parquet_path}")
        print(f"データ形状: {df.shape}")
        print(f"カラム数: {len(df.columns)}")
        print(f"行数: {len(df)}")
        
        print("\n=== カラム情報 ===")
        for i, col in enumerate(df.columns):
            print(f"{i+1:2d}. {col}")
        
        print("\n=== データ型 ===")
        print(df.dtypes)
        
        print("\n=== 最初の5行 ===")
        print(df.head())
        
        print("\n=== 基本統計情報 ===")
        print(df.describe())
        
        print("\n=== 欠損値の確認 ===")
        print(df.isnull().sum())
        
        return df
        
    except FileNotFoundError:
        print(f"エラー: ファイル '{parquet_path}' が見つかりません。")
        print("現在のディレクトリ:", os.getcwd())
        print("想定されるファイル位置:", os.path.abspath(parquet_path))
        return None
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        return None

def main():
    """メイン関数"""
    df = read_parquet_file()
    
    if df is not None:
        # 追加の詳細情報を表示
        print("\n=== 追加情報 ===")
        print(f"メモリ使用量: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        
        # 特定のカラムの値の分布を表示（例：Category列）
        if 'Category' in df.columns:
            print("\n=== Categoryカラムの分布 ===")
            print(df['Category'].value_counts())
        
        # 特定のカラムの値の分布を表示（例：Violation ID列）
        if 'Violation ID' in df.columns:
            print("\n=== Violation IDカラムの分布 ===")
            print(df['Violation ID'].value_counts())

if __name__ == "__main__":
    main() 