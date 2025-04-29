import datetime as dt
import binance.enums as be

from collections import defaultdict, deque

from backtrader.broker import BrokerBase
from backtrader.order import *
from backtrader.position import Position

class BinanceBroker(BrokerBase):
    _ORDER_TYPES = {
        Order.Limit: be.ORDER_TYPE_LIMIT,
        Order.Market: be.ORDER_TYPE_MARKET,
        Order.Stop: be.ORDER_TYPE_STOP_LOSS,
        Order.StopLimit: be.ORDER_TYPE_STOP_LOSS_LIMIT,
    }

    def __init__(self, store):
        super(BinanceBroker, self).__init__()

        self.notifs = deque()
        self.positions = defaultdict(Position)

        self.startingcash = self.cash = 0
        self.startingvalue = self.value = self.cash

        self.open_orders = dict()
    
        self._store = store
        self._store.binance_socket.start_user_socket(self._handle_user_socket_message)

    def start(self):
        pass

    def _handle_user_socket_message(self, msg):
        """https://binance-docs.github.io/apidocs/spot/en/#payload-order-update"""
        # print(msg)
        # {'e': 'executionReport', 'E': 1707120960762, 's': 'ETHUSDT', 'c': 'oVoRofmTTXJCqnGNuvcuEu', 'S': 'BUY', 'o': 'MARKET', 'f': 'GTC', 'q': '0.00220000', 'p': '0.00000000', 'P': '0.00000000', 'F': '0.00000000', 'g': -1, 'C': '', 'x': 'NEW', 'X': 'NEW', 'r': 'NONE', 'i': 15859894465, 'l': '0.00000000', 'z': '0.00000000', 'L': '0.00000000', 'n': '0', 'N': None, 'T': 1707120960761, 't': -1, 'I': 33028455024, 'w': True, 'm': False, 'M': False, 'O': 1707120960761, 'Z': '0.00000000', 'Y': '0.00000000', 'Q': '0.00000000', 'W': 1707120960761, 'V': 'EXPIRE_MAKER'}

        # {'e': 'executionReport', 'E': 1707120960762, 's': 'ETHUSDT', 'c': 'oVoRofmTTXJCqnGNuvcuEu', 'S': 'BUY', 'o': 'MARKET', 'f': 'GTC', 'q': '0.00220000', 'p': '0.00000000', 'P': '0.00000000', 'F': '0.00000000', 'g': -1, 'C': '',
        # 'x': 'TRADE', 'X': 'FILLED', 'r': 'NONE', 'i': 15859894465, 'l': '0.00220000', 'z': '0.00220000', 'L': '2319.53000000', 'n': '0.00000220', 'N': 'ETH', 'T': 1707120960761, 't': 1297224255, 'I': 33028455025, 'w': False,
        # 'm': False, 'M': True, 'O': 1707120960761, 'Z': '5.10296600', 'Y': '5.10296600', 'Q': '0.00000000', 'W': 1707120960761, 'V': 'EXPIRE_MAKER'}
        if msg['e'] == 'executionReport':
            if msg['s'] in self._store.symbols:
                for binance_id, order in self.open_orders.items():
                    if binance_id == msg['i']:
                        trade = {'qty': msg['l'], 'price': msg['L'], 'commission': msg['n']}
                        self._process_trading_message(order, msg['X'], msg['T'], [trade])
                        break
        elif msg['e'] == 'error':
            raise msg
    
    def _submit(self, order):
        exectype = self._ORDER_TYPES.get(order.exectype, be.ORDER_TYPE_MARKET)
        symbol = order.data.symbol
        side = be.SIDE_BUY if order.ordtype == Order.Buy else be.SIDE_SELL
        size = abs(order.size) if order.size else None
        binance_order = self._store.create_order(symbol, side, exectype, size, order.price, **order.info)
        
        order.info['binance_id'] = binance_order['orderId']
        order.executed.remsize = float(binance_order['executedQty'])
        order.submit()
        # print(1111, binance_order)
        # 1111 {'symbol': 'ETHUSDT', 'orderId': 15860400971, 'orderListId': -1, 'clientOrderId': 'EO7lLPcYNZR8cNEg8AOEPb', 'transactTime': 1707124560731, 'price': '0.00000000', 'origQty': '0.00220000', 'executedQty': '0.00220000', 'cummulativeQuoteQty': '5.10356000', 'status': 'FILLED', 'timeInForce': 'GTC', 'type': 'MARKET', 'side': 'BUY', 'workingTime': 1707124560731, 'fills': [{'price': '2319.80000000', 'qty': '0.00220000', 'commission': '0.00000220', 'commissionAsset': 'ETH', 'tradeId': 1297261843}], 'selfTradePreventionMode': 'EXPIRE_MAKER'}
        # order = BinanceOrder(owner, data, exectype, binance_order)
        self._process_trading_message(
            order, binance_order['status'], 
            binance_order['transactTime'], 
            binance_order['fills']
        )
        
        return order
    
    def _process_trading_message(self, order, status, transact_time, trades):
        match status:
            case be.ORDER_STATUS_NEW:
                self.open_orders.update({order.info['binance_id']: order})
                order.accept()
            case be.ORDER_STATUS_PARTIALLY_FILLED:
                self.open_orders.update({order.info['binance_id']: order})
                self._process_order_trades(order, transact_time, trades)
                order.partial()
            case be.ORDER_STATUS_FILLED:
                self.open_orders.pop(order.info['binance_id'], None)
                self._process_order_trades(order, transact_time, trades)                
                order.completed()    
            case be.ORDER_STATUS_REJECTED:
                order.reject()
        
        self.notify(order)
    
    def _process_order_trades(self, order, transact_time, trades):
        comminfo = self.getcommissioninfo(order.data)
        position = self.positions[order.data]
        pprice_old = position.price
        date = dt.datetime.fromtimestamp(int(transact_time)/1000)
        
        for trade in trades:
            price = float(trade['price'])
            size = float(trade['qty']) if order.ordtype == Order.Buy else -float(trade['qty'])
            psize, pprice, opened, closed = position.pseudoupdate(size, price)
            openedvalue = opened * price if opened else 0.0
            openedcomm = float(trade['commission']) if opened else 0.0
            closedvalue = closed * price if closed else 0.0
            closedcomm = float(trade['commission']) if closed else 0.0
            margin = comminfo.margin
            pnl = comminfo.profitandloss(-closed, pprice_old, pprice)

            order.execute(date, size, price,
                closed, closedvalue, closedcomm,
                opened, openedvalue, openedcomm,
                margin, pnl,
                psize, pprice)
            order.addcomminfo(comminfo)

            pos = self.getposition(order.data, clone=False)
            pos.update(size, price, date)
            self.cash -= abs(openedvalue)
            self.cash += abs(closedvalue)

    def buy(self, owner, data, size, price=None, plimit=None,
            exectype=None, valid=None, tradeid=0, oco=None,
            trailamount=None, trailpercent=None,
            **kwargs):
        order = BuyOrder(owner=owner, data=data,
                         size=size, price=price, pricelimit=plimit,
                         exectype=exectype, valid=valid)
        order.addinfo(**kwargs)
        return self._submit(order)

    def cancel(self, order):
        order_id = order.info['binance_id']
        symbol = order.data.symbol
        self._store.cancel_order(symbol=symbol, order_id=order_id)
        
    def format_price(self, value):
        return self._store.format_price(value)

    def get_asset_balance(self, asset):
        return self._store.get_asset_balance(asset)

    def getcash(self):
        return self.cash

    def get_notification(self):
        if not self.notifs:
            return None

        return self.notifs.popleft()

    def getposition(self, data, clone=True):
        pos = self.positions[data]
        if clone:
            pos = pos.clone()
        return pos

    def getvalue(self, datas=None):
        if datas is not None:
            value = 0
            for data in datas:
                value += self.getposition(data).size * data.close[0]            
            self.value = value + self.cash

        return self.value

    def notify(self, order):
        self.notifs.append(order)

    def sell(self, owner, data, size, price=None, plimit=None,
             exectype=None, valid=None, tradeid=0, oco=None,
             trailamount=None, trailpercent=None,
             **kwargs):
        order = SellOrder(owner=owner, data=data,
                         size=size, price=price, pricelimit=plimit,
                         exectype=exectype, valid=valid)
        order.addinfo(**kwargs)
        return self._submit(order)
        
    def set_cash(self, cash):
        '''Sets the cash parameter (alias: ``setcash``)'''
        self._store.get_balance()

        binance_cash = self._store._cash

        self.startingcash = self.cash = self.value = cash if cash <= binance_cash else binance_cash

    setcash = set_cash

    def add_cash(self, cash):
        self._store.get_balance()
        
        if self.cash + cash <= self._store._cash:
            self.cash += cash