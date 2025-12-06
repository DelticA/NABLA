import pandas as pd
import numpy as np
from typing import List, Dict


class BacktestAnalysis:
    """回测分析"""
    
    def __init__(self, results_df: pd.DataFrame, trades: List[Dict], 
                 initial_cash: float, risk_free_rate: float = 0.0):
        self.results_df = results_df.copy()
        self.trades = trades
        self.initial_cash = initial_cash
        self.risk_free_rate = risk_free_rate
        
        # 计算收益率
        self.results_df['equity_pct'] = self.results_df['equity'] / self.initial_cash * 100
        if not self.results_df.empty:
            self.results_df['price_pct'] = self.results_df['close'] / self.results_df['close'].iloc[0] * 100
        
        self.calculate_metrics()

    def calculate_metrics(self):
        """计算回测指标"""
        if self.results_df.empty:
            return
        
        # 计算最大回撤
        self.results_df['max_equity'] = self.results_df['equity_pct'].cummax()
        self.results_df['drawdown'] = self.results_df['equity_pct'] - self.results_df['max_equity']
        self.max_drawdown = self.results_df['drawdown'].min()
        
        # 计算总收益率
        self.total_return_pct = self.results_df['equity_pct'].iloc[-1] - 100
        
        # 计算夏普比率
        returns = self.results_df['equity_pct'].pct_change().dropna()
        if len(returns) > 0:
            N = returns.count()
            daily_rf = self.risk_free_rate / N
            r_bar = returns.mean()
            sigma = returns.std()
            self.sharpe_ratio = (r_bar - daily_rf) / sigma * np.sqrt(N) if sigma > 0 else 0
        else:
            self.sharpe_ratio = 0

    def print_summary(self):
        """打印回测摘要"""
        print("\n=== 回测结果摘要 ===")
        print(f"初始资金: {self.initial_cash:,.2f}")
        print(f"最终权益: {self.results_df['equity'].iloc[-1]:,.2f}")
        print(f"总收益率: {self.total_return_pct:.2f}%")
        print(f"最大回撤: {self.max_drawdown:.2f}%")
        print(f"夏普比率: {self.sharpe_ratio:.4f}")
        print(f"交易次数: {len(self.trades)}")
        
        if self.trades:
            buy_trades = [t for t in self.trades if t['side'] == 'BUY']
            sell_trades = [t for t in self.trades if t['side'] == 'SELL']
            print(f"买入次数: {len(buy_trades)}")
            print(f"卖出次数: {len(sell_trades)}")

    def get_metrics(self) -> Dict:
        """获取所有指标"""
        return {
            'initial_cash': self.initial_cash,
            'final_equity': self.results_df['equity'].iloc[-1] if not self.results_df.empty else 0,
            'total_return_pct': self.total_return_pct,
            'max_drawdown': self.max_drawdown,
            'sharpe_ratio': self.sharpe_ratio,
            'total_trades': len(self.trades),
            'buy_trades': len([t for t in self.trades if t['side'] == 'BUY']),
            'sell_trades': len([t for t in self.trades if t['side'] == 'SELL'])
        }