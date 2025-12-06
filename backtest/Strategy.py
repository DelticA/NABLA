from abc import ABC, abstractmethod
from typing import Dict, List, Optional


class Strategy(ABC):
    """策略基类"""
    
    def __init__(self):
        self.broker = None
    
    def set_broker(self, broker):
        self.broker = broker

    @abstractmethod
    def on_init(self):
        """策略初始化"""
        pass

    @abstractmethod
    def on_bar(self, bar: Dict):
        """每个bar的处理逻辑"""
        pass

    @abstractmethod
    def on_trade(self, trade: Dict):
        """成交回调"""
        pass

    @abstractmethod
    def on_finish(self):
        """回测结束"""
        pass

    def buy(self, symbol: str, price: float, qty: float, order_type: str = "LIMIT") -> Dict:
        """买入"""
        return self.broker.submit_order("BUY", symbol, price, qty, order_type)

    def sell(self, symbol: str, price: float, qty: float, order_type: str = "LIMIT") -> Dict:
        """卖出"""
        return self.broker.submit_order("SELL", symbol, price, qty, order_type)

    def cancel_order(self, order_id: int) -> bool:
        """取消订单"""
        return self.broker.cancel_order(order_id)

    def get_position(self, symbol: str) -> Dict:
        """获取持仓"""
        return self.broker.get_position(symbol)

    def get_account_info(self) -> Dict:
        """获取账户信息"""
        return self.broker.get_account_info()

    def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict]:
        """获取未成交订单"""
        return self.broker.get_open_orders(symbol)