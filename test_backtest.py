"""
测试回测框架适配新的数据库格式
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import QuantDatabase
from backtest.data_adapter import DataAdapter
import pandas as pd

def test_data_adapter():
    """测试数据适配器"""
    print("=== 测试数据适配器 ===")
    
    # 加载一些测试数据
    db = QuantDatabase(read_only=True)
    df = db.load_quotes('000001.SZ', '2025-01-02', '2025-01-02')
    
    if df.empty:
        print("没有找到测试数据，使用模拟数据")
        # 创建模拟数据
        df = pd.DataFrame({
            '万得代码': ['000001.SZ'] * 10,
            '成交价': [10.0, 10.1, 10.2, 10.1, 10.0, 9.9, 10.0, 10.1, 10.2, 10.3],
            '最高价': [10.1, 10.2, 10.3, 10.2, 10.1, 10.0, 10.1, 10.2, 10.3, 10.4],
            '最低价': [9.9, 10.0, 10.1, 10.0, 9.9, 9.8, 9.9, 10.0, 10.1, 10.2],
            '成交量': [1000, 1500, 2000, 1200, 800, 1000, 1500, 2000, 1800, 2200],
            '成交额': [10000, 15150, 20400, 12120, 8000, 9900, 15000, 20200, 18360, 22660]
        })
        df.index = pd.date_range('2025-01-02 09:30:00', periods=10, freq='1min')
    
    print("原始数据列名:", list(df.columns))
    print("数据形状:", df.shape)
    
    # 测试数据适配器
    adapted_df = DataAdapter.adapt_dataframe(df)
    print("适配后数据列名:", list(adapted_df.columns))
    
    # 测试单个bar转换
    if not df.empty:
        bar = df.iloc[0]
        bar_dict = DataAdapter.create_bar_dict(bar)
        print("单个bar转换结果:")
        for key, value in bar_dict.items():
            if key in ['close', 'high', 'low', 'volume', 'symbol']:
                print(f"  {key}: {value}")
    
    db.close()
    print("数据适配器测试完成！\n")

def test_backtest_integration():
    """测试回测集成"""
    print("=== 测试回测集成 ===")
    
    try:
        from backtest.utils import run_backtest
        from backtest.strategies import MovingAverageStrategy
        
        print("尝试运行回测...")
        
        # 尝试运行回测
        analysis = run_backtest(
            symbol="000001.SZ",
            start_date="2025-01-02",
            end_date="2025-01-02",
            strategy_class=MovingAverageStrategy,
            short_window=3,
            long_window=5,
            initial_cash=100000
        )
        
        if analysis:
            print("回测运行成功！")
            analysis.print_summary()
        else:
            print("回测返回None，可能没有数据")
            
    except Exception as e:
        print(f"回测运行出错: {e}")
        print("错误类型:", type(e).__name__)
        import traceback
        traceback.print_exc()
    
    print("回测集成测试完成！\n")

if __name__ == "__main__":
    print("开始测试回测框架适配...\n")
    
    test_data_adapter()
    test_backtest_integration()
    
    print("所有测试完成！")