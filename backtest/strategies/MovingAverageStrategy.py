import numpy as np
from ..Strategy import Strategy


class MovingAverageStrategy(Strategy):
    """移动平均线策略"""
    
    def __init__(self, short_window: int = 5, long_window: int = 20):
        super().__init__()
        self.short_window = short_window
        self.long_window = long_window
        self.prices = []

    def on_init(self):
        print("移动平均线策略初始化")

    def on_bar(self, bar: dict):
        price = bar["close"]  # 使用标准列名
        symbol = bar.get("symbol", "000001.SZ")  # 默认使用第一个股票
        
        self.prices.append(price)
        
        if len(self.prices) < self.long_window:
            return
        
        short_ma = np.mean(self.prices[-self.short_window:])
        long_ma = np.mean(self.prices[-self.long_window:])
        pos = self.get_position(symbol)
        
        # 金叉买入，死叉卖出
        if short_ma > long_ma and pos["quantity"] == 0:
            # 买入
            qty = self.broker.cash * 0.95 / price  # 使用95%资金
            if qty > 0:
                self.buy(symbol, price, qty, "MARKET")
                print(f"[{bar['datetime']}] 买入 {symbol} {qty:.2f} 股 @ {price:.2f}")
        
        elif short_ma < long_ma and pos["quantity"] > 0:
            # 卖出
            self.sell(symbol, price, pos["quantity"], "MARKET")
            print(f"[{bar['datetime']}] 卖出 {symbol} {pos['quantity']:.2f} 股 @ {price:.2f}")

    def on_trade(self, trade: dict):
        print(f"[成交] {trade['side']} {trade['symbol']} {trade['quantity']:.2f} @ {trade['price']:.2f}")

    def on_finish(self):
        print("移动平均线策略回测完成")