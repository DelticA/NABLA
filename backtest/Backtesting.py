import pandas as pd
from typing import Dict
from .Strategy import Strategy
from .Broker import Broker
from .data_adapter import DataAdapter


class Backtesting:
    """回测引擎"""
    
    def __init__(self, strategy: Strategy, df: pd.DataFrame, 
                 initial_cash: float = 100000, fee_rate: float = 0.0005, 
                 slippage: float = 0.0001):
        self.strategy = strategy
        self.data = df
        self.broker = Broker(initial_cash=initial_cash, fee_rate=fee_rate, 
                           slippage=slippage)
        self.strategy.set_broker(self.broker)
        self.history = []

    def run(self):
        """运行回测"""
        self.strategy.on_init()
        
        for idx, bar in self.data.iterrows():
            # 使用数据适配器创建标准的bar字典
            bar_dict = DataAdapter.create_bar_dict(bar)
            
            self.strategy.on_bar(bar_dict)
            trades = self.broker.execute_orders(bar_dict)
            
            for trade in trades:
                self.strategy.on_trade(trade)
            
            self.record(bar_dict)
        
        self.strategy.on_finish()

    def record(self, bar: Dict):
        """记录回测数据"""
        equity = self.broker.cash
        
        # 计算持仓市值
        for symbol, pos in self.broker.positions.items():
            if pos["quantity"] > 0:
                equity += pos["quantity"] * bar["close"]
        
        self.history.append({
            "timestamp": bar["datetime"],
            "equity": equity,
            "cash": self.broker.cash,
            "close": bar["close"],
            "positions": self.broker.positions.copy()
        })

    def results(self) -> pd.DataFrame:
        """获取回测结果"""
        return pd.DataFrame(self.history)