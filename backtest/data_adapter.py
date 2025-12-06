"""
数据适配器 - 将数据库列名映射到回测框架标准列名
"""

import pandas as pd
from typing import Dict, List, Optional


class DataAdapter:
    """数据适配器，将数据库列名转换为回测框架标准列名"""
    
    # 标准列名映射
    STANDARD_COLUMNS = {
        # 价格相关
        "成交价": "close",
        "开盘价": "open", 
        "最高价": "high",
        "最低价": "low",
        "前收盘": "prev_close",
        
        # 成交量相关
        "成交量": "volume",
        "成交额": "amount",
        "成交笔数": "trade_count",
        "当日累计成交量": "cum_volume",
        "当日成交额": "cum_amount",
        
        # 买卖盘口
        "申买价1": "bid1",
        "申买量1": "bid1_volume",
        "申卖价1": "ask1",
        "申卖量1": "ask1_volume",
        
        # 基本信息
        "万得代码": "symbol",
        "交易所代码": "exchange",
        "自然日": "date",
        "时间": "time"
    }
    
    @classmethod
    def adapt_dataframe(cls, df: pd.DataFrame, required_columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        适配DataFrame，将数据库列名转换为标准列名
        
        Args:
            df: 原始数据DataFrame
            required_columns: 需要的标准列名列表
            
        Returns:
            pd.DataFrame: 适配后的DataFrame
        """
        if df.empty:
            return df
        
        # 创建列名映射字典
        column_mapping = {}
        for db_col, std_col in cls.STANDARD_COLUMNS.items():
            if db_col in df.columns:
                column_mapping[db_col] = std_col
        
        # 重命名列
        adapted_df = df.rename(columns=column_mapping)
        
        # 如果指定了必需列，检查并填充缺失值
        if required_columns:
            for col in required_columns:
                if col not in adapted_df.columns:
                    # 如果列缺失，根据列类型填充默认值
                    if col in ["close", "open", "high", "low"]:
                        # 价格列用成交价填充
                        adapted_df[col] = adapted_df.get("close", df.get("成交价", 0))
                    elif col in ["volume", "amount"]:
                        # 成交量相关列用0填充
                        adapted_df[col] = 0
                    elif col == "symbol":
                        # 股票代码用第一个非空值填充
                        if "万得代码" in df.columns and not df["万得代码"].empty:
                            adapted_df[col] = df["万得代码"].iloc[0]
                        else:
                            adapted_df[col] = "UNKNOWN"
        
        return adapted_df
    
    @classmethod
    def get_standard_column_name(cls, db_column: str) -> str:
        """
        获取数据库列名对应的标准列名
        
        Args:
            db_column: 数据库列名
            
        Returns:
            str: 标准列名，如果不存在映射则返回原列名
        """
        return cls.STANDARD_COLUMNS.get(db_column, db_column)
    
    @classmethod
    def get_required_columns(cls) -> List[str]:
        """
        获取回测框架必需的标准列名
        
        Returns:
            List[str]: 必需的标准列名列表
        """
        return ["close", "high", "low", "volume", "symbol"]
    
    @classmethod
    def create_bar_dict(cls, bar: pd.Series) -> Dict:
        """
        创建标准的bar字典，适配数据库列名
        
        Args:
            bar: 原始数据Series
            
        Returns:
            Dict: 适配后的bar字典
        """
        bar_dict = bar.to_dict()
        
        # 添加datetime索引
        if hasattr(bar, 'name') and bar.name is not None:
            bar_dict["datetime"] = bar.name
        
        # 映射列名
        adapted_dict = {}
        for db_col, value in bar_dict.items():
            std_col = cls.get_standard_column_name(db_col)
            adapted_dict[std_col] = value
        
        # 确保必需列存在
        required_cols = cls.get_required_columns()
        for col in required_cols:
            if col not in adapted_dict:
                # 使用默认值或从其他列推导
                if col == "close" and "成交价" in bar_dict:
                    adapted_dict[col] = bar_dict["成交价"]
                elif col == "high" and "最高价" in bar_dict:
                    adapted_dict[col] = bar_dict["最高价"]
                elif col == "low" and "最低价" in bar_dict:
                    adapted_dict[col] = bar_dict["最低价"]
                elif col == "volume" and "成交量" in bar_dict:
                    adapted_dict[col] = bar_dict["成交量"]
                elif col == "symbol" and "万得代码" in bar_dict:
                    adapted_dict[col] = bar_dict["万得代码"]
                else:
                    # 设置默认值
                    if col in ["close", "high", "low"]:
                        adapted_dict[col] = bar_dict.get("成交价", 0)
                    elif col == "volume":
                        adapted_dict[col] = 0
                    elif col == "symbol":
                        adapted_dict[col] = "UNKNOWN"
        
        return adapted_dict