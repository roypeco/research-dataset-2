import pandas as pd
import logging
from pathlib import Path

class ParquetExporter:
    """Parquet出力に関する処理を担当するクラス"""
    
    def __init__(self, data_manager):
        self.data_manager = data_manager
        self.logger = logging.getLogger(__name__)
    
    def save_fix_history_to_parquet(self, pkg_name, dataframe):
        """DataFrameをParquetファイルに保存"""
        if dataframe is None:
            return None
        
        result_dir = Path('dataset') / pkg_name
        result_dir.mkdir(parents=True, exist_ok=True)
        
        parquet_file = result_dir / 'fix_history.parquet'
        
        try:
            # データ型の最適化
            optimized_df = self.data_manager.optimize_dataframe_types(dataframe.copy())
            
            # Parquetファイルに保存（圧縮あり）
            optimized_df.to_parquet(
                parquet_file, 
                engine='pyarrow',  # pyarrowエンジンを使用
                compression='snappy',  # 高速圧縮
                index=False
            )
            
            return parquet_file
            
        except ImportError as e:
            self.logger.error(f"pyarrow not installed. Install with: pip install pyarrow")
            self.logger.error(f"ImportError details: {str(e)}")
            self.logger.warning("FALLBACK: Saving as CSV instead of Parquet")
            # フォールバックとしてCSV保存
            from .csv_exporter import CSVExporter
            csv_exporter = CSVExporter(self.data_manager)
            return csv_exporter.save_fix_history_to_csv(pkg_name, dataframe)
        except Exception as e:
            self.logger.error(f"Failed to save to parquet: {str(e)}")
            self.logger.error(f"Exception type: {type(e).__name__}")
            self.logger.warning("FALLBACK: Saving as CSV instead of Parquet")
            # フォールバックとしてCSV保存
            from .csv_exporter import CSVExporter
            csv_exporter = CSVExporter(self.data_manager)
            return csv_exporter.save_fix_history_to_csv(pkg_name, dataframe)