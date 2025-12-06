"""
回测框架使用示例
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from backtest.utils import run_backtest, load_multiple_symbols, create_portfolio_backtest
from backtest.strategies import MovingAverageStrategy


def single_strategy_example():
    """单策略回测示例"""
    print("=== 单策略回测示例 ===")
    
    analysis = run_backtest(
        symbol="000001.SZ",
        start_date="2025-01-02",
        end_date="2025-01-02",
        strategy_class=MovingAverageStrategy,
        short_window=5,
        long_window=10,
        initial_cash=100000
    )
    
    return analysis


def portfolio_example():
    """投资组合回测示例"""
    print("\n=== 投资组合回测示例 ===")
    
    symbols = ["000001.SZ", "000002.SZ"]  # 示例股票代码
    
    # 加载多只股票数据
    data_dict = load_multiple_symbols(
        symbols=symbols,
        start_date="2025-01-02",
        end_date="2025-01-02"
    )
    
    # 对每只股票运行回测
    results = create_portfolio_backtest(
        strategy_class=MovingAverageStrategy,
        data_dict=data_dict,
        initial_cash=100000,
        short_window=5,
        long_window=10
    )
    
    # 汇总结果
    print("\n=== 投资组合汇总 ===")
    for symbol, analysis in results.items():
        metrics = analysis.get_metrics()
        print(f"{symbol}: 收益率 {metrics['total_return_pct']:.2f}%")
    
    return results


def custom_strategy_example():
    """自定义策略示例"""
    print("\n=== 自定义策略示例 ===")
    
    from backtest.Strategy import Strategy
    
    class MyCustomStrategy(Strategy):
        """自定义策略示例"""
        
        def __init__(self, threshold: float = 0.01):
            super().__init__()
            self.threshold = threshold
            self.last_price = None
        
        def on_init(self):
            print("自定义策略初始化")
        
        def on_bar(self, bar):
            price = bar["close"]
            symbol = bar.get("symbol", "000001.SZ")
            
            if self.last_price is None:
                self.last_price = price
                return
            
            price_change = (price - self.last_price) / self.last_price
            pos = self.get_position(symbol)
            
            if price_change > self.threshold and pos["quantity"] == 0:
                # 价格上涨超过阈值，买入
                qty = self.broker.cash * 0.5 / price  # 使用50%资金
                if qty > 0:
                    self.buy(symbol, price, qty, "MARKET")
                    print(f"[{bar['datetime']}] 买入 {symbol} {qty:.2f} 股 @ {price:.2f}")
            
            elif price_change < -self.threshold and pos["quantity"] > 0:
                # 价格下跌超过阈值，卖出
                self.sell(symbol, price, pos["quantity"], "MARKET")
                print(f"[{bar['datetime']}] 卖出 {symbol} {pos['quantity']:.2f} 股 @ {price:.2f}")
            
            self.last_price = price
        
        def on_trade(self, trade):
            print(f"[成交] {trade['side']} {trade['symbol']} {trade['quantity']:.2f} @ {trade['price']:.2f}")
        
        def on_finish(self):
            print("自定义策略回测完成")
    
    # 运行自定义策略
    analysis = run_backtest(
        symbol="000001.SZ",
        start_date="2025-01-02",
        end_date="2025-01-02",
        strategy_class=MyCustomStrategy,
        threshold=0.02,  # 2%阈值
        initial_cash=100000
    )
    
    return analysis


if __name__ == "__main__":
    # 运行所有示例
    single_strategy_example()
    portfolio_example()
    custom_strategy_example()