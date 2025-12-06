"""
回测框架入口文件
"""

from backtest.utils import run_backtest
from backtest.strategies import MovingAverageStrategy


def main():
    """主函数"""
    # 示例回测
    analysis = run_backtest(
        symbol="000001.SZ",
        start_date="2025-01-02",
        end_date="2025-01-02",
        strategy_class=MovingAverageStrategy,
        short_window=5,
        long_window=10,
        initial_cash=100000
    )
    
    if analysis:
        print("\n回测完成！")
    else:
        print("回测失败！")


if __name__ == "__main__":
    main()