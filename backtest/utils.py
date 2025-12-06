import pandas as pd
from typing import Dict, List, Union
from datetime import datetime, date
from database import QuantDatabase
from .Strategy import Strategy
from .Backtesting import Backtesting
from .Analysis import BacktestAnalysis


def run_backtest(symbol: str, start_date: Union[str, date, datetime], 
                 end_date: Union[str, date, datetime], 
                 strategy_class: type = None, 
                 initial_cash: float = 100000, **strategy_kwargs):
    """
    运行回测
    
    Args:
        symbol: 股票代码
        start_date: 开始日期
        end_date: 结束日期
        strategy_class: 策略类
        initial_cash: 初始资金
        **strategy_kwargs: 策略参数
    
    Returns:
        BacktestAnalysis: 回测分析结果
    """
    if strategy_class is None:
        from .strategies import MovingAverageStrategy
        strategy_class = MovingAverageStrategy
    
    # 加载数据，选择重要的价格和成交量指标
    db = QuantDatabase(read_only=True)
    df = db.load_quotes(symbol, start_date, end_date)
    
    if df.empty:
        print(f"没有找到 {symbol} 在 {start_date} 到 {end_date} 的数据")
        db.close()
        return None
    
    # 创建策略
    strategy = strategy_class(**strategy_kwargs)
    
    # 运行回测
    backtest = Backtesting(strategy, df, initial_cash=initial_cash)
    backtest.run()
    
    # 分析结果
    results_df = backtest.results()
    analysis = BacktestAnalysis(results_df, backtest.broker.trades, initial_cash)
    analysis.print_summary()
    
    db.close()
    
    return analysis


def load_multiple_symbols(symbols: List[str], start_date: Union[str, date, datetime], 
                         end_date: Union[str, date, datetime]) -> Dict[str, pd.DataFrame]:
    """
    加载多个股票的数据
    
    Args:
        symbols: 股票代码列表
        start_date: 开始日期
        end_date: 结束日期
    
    Returns:
        Dict[str, pd.DataFrame]: 股票数据字典
    """
    db = QuantDatabase(read_only=True)
    data_dict = {}
    
    for symbol in symbols:
        df = db.load_quotes(symbol, start_date, end_date)
        if not df.empty:
            data_dict[symbol] = df
        else:
            print(f"警告: 没有找到 {symbol} 的数据")
    
    db.close()
    return data_dict


def create_portfolio_backtest(strategy_class: type, data_dict: Dict[str, pd.DataFrame], 
                             initial_cash: float = 100000, **strategy_kwargs):
    """
    创建投资组合回测
    
    Args:
        strategy_class: 策略类
        data_dict: 股票数据字典
        initial_cash: 初始资金
        **strategy_kwargs: 策略参数
    
    Returns:
        Dict[str, BacktestAnalysis]: 各股票回测结果
    """
    results = {}
    
    for symbol, df in data_dict.items():
        print(f"\n=== 回测 {symbol} ===")
        strategy = strategy_class(**strategy_kwargs)
        backtest = Backtesting(strategy, df, initial_cash=initial_cash)
        backtest.run()
        
        results_df = backtest.results()
        analysis = BacktestAnalysis(results_df, backtest.broker.trades, initial_cash)
        results[symbol] = analysis
    
    return results