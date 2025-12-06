from typing import Dict, List, Optional


class Broker:
    """交易经纪商"""
    
    def __init__(self, initial_cash: float = 100000, fee_rate: float = 0.0005, 
                 slippage: float = 0.0001):
        self.cash = initial_cash
        self.positions: Dict[str, Dict] = {}  # {symbol: {"quantity": 0, "avg_price": 0}}
        self.orders: List[Dict] = []
        self.trades: List[Dict] = []
        self.order_id = 0
        self.fee_rate = fee_rate
        self.slippage = slippage

    def submit_order(self, side: str, symbol: str, price: float, qty: float, 
                    order_type: str) -> Dict:
        """提交订单"""
        order = {
            "id": self.order_id,
            "side": side,
            "symbol": symbol,
            "price": price,
            "quantity": qty,
            "type": order_type,
            "status": "OPEN"
        }
        self.order_id += 1
        self.orders.append(order)
        return order

    def cancel_order(self, order_id: int) -> bool:
        """取消订单"""
        for order in self.orders:
            if order["id"] == order_id and order["status"] == "OPEN":
                order["status"] = "CANCELED"
                return True
        return False

    def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict]:
        """获取未成交订单"""
        return [o for o in self.orders if o["status"] == "OPEN" 
                and (symbol is None or o["symbol"] == symbol)]

    def get_position(self, symbol: str) -> Dict:
        """获取持仓"""
        return self.positions.get(symbol, {"quantity": 0, "avg_price": 0.0})

    def update_position(self, symbol: str, position: Dict):
        """更新持仓"""
        self.positions[symbol] = position

    def get_account_info(self) -> Dict:
        """获取账户信息"""
        return {"cash": self.cash, "positions": self.positions.copy()}

    def execute_orders(self, bar: Dict) -> List[Dict]:
        """执行订单"""
        trades = []
        active_orders = self.get_open_orders()
        
        for order in active_orders:
            symbol = order["symbol"]
            match_price = None
            filled = False

            if order["type"] == "MARKET":
                # 市价单，用滑点价格
                match_price = bar["close"] * (1 + self.slippage if order["side"] == "BUY" 
                                            else 1 - self.slippage)
                filled = True

            elif order["type"] == "LIMIT":
                # 限价单，在最高价和最低价之间判为能成交
                if (order["side"] == "BUY" and order["price"] >= bar["low"]) or \
                   (order["side"] == "SELL" and order["price"] <= bar["high"]):
                    match_price = order["price"]
                    filled = True

            if not filled:
                continue

            cost = match_price * order["quantity"]
            fee = cost * self.fee_rate
            pos = self.get_position(symbol)

            if order["side"] == "BUY" and self.cash >= (cost + fee):
                self.cash -= (cost + fee)
                total_qty = pos["quantity"] + order["quantity"]
                if total_qty > 0:
                    pos["avg_price"] = (pos["avg_price"] * pos["quantity"] + 
                                       match_price * order["quantity"]) / total_qty
                pos["quantity"] = total_qty
                order["status"] = "FILLED"

            elif order["side"] == "SELL" and pos["quantity"] >= order["quantity"]:
                self.cash += (cost - fee)
                pos["quantity"] -= order["quantity"]
                order["status"] = "FILLED"

            if order["status"] == "FILLED":
                self.update_position(symbol, pos)
                trade = {
                    "order_id": order["id"],
                    "symbol": symbol,
                    "side": order["side"],
                    "price": match_price,
                    "quantity": order["quantity"],
                    "fee": fee,
                    "timestamp": bar.get("datetime")  # 使用datetime字段作为时间戳
                }
                self.trades.append(trade)
                trades.append(trade)
        
        return trades