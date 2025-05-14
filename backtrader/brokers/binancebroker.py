from collections import deque

import binance.enums as be

from backtrader import BackBroker, CommInfoBase
from backtrader.order import *


class BinanceOrder(Order):
    _ORDER_TYPES = {
        Order.Limit: be.ORDER_TYPE_LIMIT,
        Order.Market: be.ORDER_TYPE_MARKET,
        Order.Stop: be.ORDER_TYPE_STOP_LOSS,
        Order.StopLimit: be.ORDER_TYPE_STOP_LOSS_LIMIT,
    }

    def __init__(self, ordtype, quoteOrderQty):
        self.ordtype = ordtype
        self.quoteOrderQty = quoteOrderQty

        super().__init__()

        self._side = be.SIDE_BUY if self.ordtype == Order.Buy else be.SIDE_SELL
        self._exectype = self._ORDER_TYPES.get(self.exectype, be.ORDER_TYPE_MARKET)
        self._symbol = self.data.symbol
        self._size = abs(self.size) if self.size else None
        self.binance = None

    def transmit(self):
        self.binance = self.data._store.create_order(
            self._symbol,
            self._side,
            self._exectype,
            self._size,
            self.price,
            quoteOrderQty=self.quoteOrderQty
        )

        if self._exectype == be.ORDER_TYPE_MARKET:
            self.price = sum(float(trade["price"]) for trade in self.binance['fills']) / len(self.binance['fills'])
            if self.size:
                self.executed.remsize = self.size
            else:
                self.executed.remsize = Decimal(self.binance['executedQty'])

class BinanceBroker(BackBroker):
    params = (
        ('cash', Decimal('1000.0')),
    )

    def __init__(self, store):
        super(BinanceBroker, self).__init__()

        self.order_trades = deque(maxlen=100)
        self._store = store
        self._store.binance_socket.start_user_socket(self._handle_user_socket_message)

    def init(self):
        super(BinanceBroker, self).init()
        self.setcommission(
            commission=0.00075,
            commtype=CommInfoBase.COMM_PERC,
            stocklike=True
        )

    def _handle_user_socket_message(self, msg):
        """https://developers.binance.com/docs/binance-spot-api-docs/user-data-stream#order-update"""
        # print(msg)
        # {'e': 'executionReport', 'E': 1707120960762, 's': 'ETHUSDT', 'c': 'oVoRofmTTXJCqnGNuvcuEu', 'S': 'BUY', 'o': 'MARKET', 'f': 'GTC', 'q': '0.00220000', 'p': '0.00000000', 'P': '0.00000000', 'F': '0.00000000', 'g': -1, 'C': '', 'x': 'NEW', 'X': 'NEW', 'r': 'NONE', 'i': 15859894465, 'l': '0.00000000', 'z': '0.00000000', 'L': '0.00000000', 'n': '0', 'N': None, 'T': 1707120960761, 't': -1, 'I': 33028455024, 'w': True, 'm': False, 'M': False, 'O': 1707120960761, 'Z': '0.00000000', 'Y': '0.00000000', 'Q': '0.00000000', 'W': 1707120960761, 'V': 'EXPIRE_MAKER'}

        # {'e': 'executionReport', 'E': 1707120960762, 's': 'ETHUSDT', 'c': 'oVoRofmTTXJCqnGNuvcuEu', 'S': 'BUY', 'o': 'MARKET', 'f': 'GTC', 'q': '0.00220000', 'p': '0.00000000', 'P': '0.00000000', 'F': '0.00000000', 'g': -1, 'C': '',
        # 'x': 'TRADE', 'X': 'FILLED', 'r': 'NONE', 'i': 15859894465, 'l': '0.00220000', 'z': '0.00220000', 'L': '2319.53000000', 'n': '0.00000220', 'N': 'ETH', 'T': 1707120960761, 't': 1297224255, 'I': 33028455025, 'w': False,
        # 'm': False, 'M': True, 'O': 1707120960761, 'Z': '5.10296600', 'Y': '5.10296600', 'Q': '0.00000000', 'W': 1707120960761, 'V': 'EXPIRE_MAKER'}
        if msg['e'] == 'executionReport':
            if msg['s'] in self._store.symbols and msg['X'] in [be.ORDER_STATUS_FILLED, be.ORDER_STATUS_PARTIALLY_FILLED]:
                for order in self.pending:
                    if order.binance['orderId'] == msg['i']:
                        self.order_trades.append({
                            'orderId': msg['i'],
                            'status': msg['X'],
                            'size': msg['l'],
                            'price': msg['L'],
                            'commAmount': msg['n'],
                            'commAsset': msg['N']
                        })
                        break
        elif msg['e'] == 'error':
            raise msg

    def buy(self, owner, data, size, price=None, plimit=None,
            exectype=None, valid=None, tradeid=0, oco=None,
            trailamount=None, trailpercent=None,
            **kwargs):

        if 'quoteOrderQty' in kwargs:
            order = BinanceOrder(owner=owner, data=data, ordtype=Order.Buy,
                             size=size, price=price, pricelimit=plimit,
                             exectype=exectype, valid=valid, quoteOrderQty=kwargs['quoteOrderQty'])
        else:
            order = BinanceOrder(owner=owner, data=data, ordtype=Order.Buy,
                             size=size, price=price, pricelimit=plimit,
                             exectype=exectype, valid=valid, quoteOrderQty=None)

        order.addinfo(**kwargs)
        self._ocoize(order, oco)

        return self.submit(order)

    def cancel(self, order, bracket=False):
        order_id = order.binance['orderId']
        symbol = order.data.symbol
        self._store.cancel_order(symbol=symbol, order_id=order_id)
        super().cancel(order, bracket)
        
    def format_price(self, value):
        return self._store.format_price(value)

    def get_asset_balance(self, asset):
        return self._store.get_asset_balance(asset)

    def sell(self, owner, data, size, price=None, plimit=None,
             exectype=None, valid=None, tradeid=0, oco=None,
             trailamount=None, trailpercent=None,
             **kwargs):

        if 'quoteOrderQty' in kwargs:
            order = BinanceOrder(owner=owner, data=data, ordtype=Order.Sell,
                                 size=size, price=price, pricelimit=plimit,
                                 exectype=exectype, valid=valid, quoteOrderQty=kwargs['quoteOrderQty'])
        else:
            order = BinanceOrder(owner=owner, data=data, ordtype=Order.Sell,
                                 size=size, price=price, pricelimit=plimit,
                                 exectype=exectype, valid=valid, quoteOrderQty=None)

        order.addinfo(**kwargs)
        self._ocoize(order, oco)

        return self.submit(order)
        
    def set_cash(self, cash):
        '''Sets the cash parameter (alias: ``setcash``)'''
        self._store.get_balance()

        binance_cash = self._store._cash
        new_cash = cash if cash <= binance_cash else binance_cash

        super().set_cash(new_cash)

    setcash = set_cash

    def add_cash(self, cash):
        self._store.get_balance()
        
        if self.get_cash() + cash <= self._store._cash:
            super().add_cash(cash)

    def next(self):
        while self._toactivate:
            self._toactivate.popleft().activate()

        if self.p.checksubmit:
            self.check_submitted()

        # Discount any cash for positions hold
        credit = Decimal('0.0')
        for data, pos in self.positions.items():
            if pos:
                comminfo = self.getcommissioninfo(data)
                dt0 = data.datetime.datetime()
                dcredit = comminfo.get_credit_interest(data, pos, dt0)
                self.d_credit[data] += dcredit
                credit += dcredit
                pos.datetime = dt0  # mark last credit operation

        self.cash -= credit

        self._process_order_history()

        # Iterate once over all elements of the pending queue
        self.pending.append(None)
        while True:
            order = self.pending.popleft()
            if order is None:
                break

            if order.expire():
                self.notify(order)
                self._ococheck(order)
                self._bracketize(order, cancel=True)

            elif not order.active():
                self.pending.append(order)  # cannot yet be processed

            else:
                trades = [trade for trade in self.order_trades if trade['orderId'] == order.binance['orderId']]

                for trade in trades:
                    self._execute(order, ago=0, price=Decimal(str(order.price)))
                    self.order_trades.remove(trade)

                if trades:
                    self.notify(order)
                    self._ococheck(order)

                if order.alive():
                    self.pending.append(order)

                elif order.status == Order.Completed:
                    # a bracket parent order may have been executed
                    self._bracketize(order)

        # Operations have been executed ... adjust cash end of bar
        for data, pos in self.positions.items():
            # futures change cash every bar
            if pos:
                comminfo = self.getcommissioninfo(data)
                self.cash += comminfo.cashadjust(pos.size,
                                                 pos.adjbase,
                                                 data.close[0])
                # record the last adjustment price
                pos.adjbase = data.close[0]

        self._get_value()  # update value

    def _execute(self, order, ago=None, price=None, cash=None, position=None,
                 dtcoc=None, commission=None):
        # ago = None is used a flag for pseudo execution
        if ago is not None and price is None:
            return  # no psuedo exec no price - no execution

        if self.p.filler is None or ago is None:
            # Order gets full size or pseudo-execution
            size = order.executed.remsize
        else:
            # Execution depends on volume filler
            size = self.p.filler(order, price, ago)
            if not order.isbuy():
                size = -size

        # Get comminfo object for the data
        comminfo = self.getcommissioninfo(order.data)

        # Check if something has to be compensated
        if order.data._compensate is not None:
            data = order.data._compensate
            cinfocomp = self.getcommissioninfo(data)  # for actual commission
        else:
            data = order.data
            cinfocomp = comminfo

        # Adjust position with operation size
        if ago is not None:
            # Real execution with date
            position = self.positions[data]
            pprice_orig = position.price

            psize, pprice, opened, closed = position.pseudoupdate(size, price)

            # if part/all of a position has been closed, then there has been
            # a profitandloss ... record it
            pnl = comminfo.profitandloss(-closed, pprice_orig, price)
            cash = self.cash
        else:
            pnl = 0
            if not self.p.coo:
                price = pprice_orig = order.created.price
            else:
                # When doing cheat on open, the price to be considered for a
                # market order is the opening price and not the default closing
                # price with which the order was created
                if order.exectype == Order.Market:
                    price = pprice_orig = order.data.open[0]
                else:
                    price = pprice_orig = order.created.price

            psize, pprice, opened, closed = position.update(size, price)

        # "Closing" totally or partially is possible. Cash may be re-injected
        if closed:
            # Adjust to returned value for closed items & acquired opened items
            if self.p.shortcash:
                closedvalue = comminfo.getvaluesize(-closed, pprice_orig)
            else:
                closedvalue = comminfo.getoperationcost(closed, pprice_orig)

            closecash = closedvalue
            if closedvalue > 0:  # long position closed
                closecash /= comminfo.get_leverage()  # inc cash with lever

            # Calculate and substract commission
            closedcomm = comminfo.getcommission(closed, price)
            pnl -= closedcomm

            cash += closecash + pnl * comminfo.stocklike

            if ago is not None:
                # Cashadjust closed contracts: prev close vs exec price
                # The operation can inject or take cash out
                cash += comminfo.cashadjust(-closed,
                                            position.adjbase,
                                            price)

                # Update system cash
                self.cash = cash
        else:
            closedvalue = closedcomm = Decimal('0.0')

        popened = opened
        if opened:
            if self.p.shortcash:
                openedvalue = comminfo.getvaluesize(opened, price)
            else:
                openedvalue = comminfo.getoperationcost(opened, price)

            opencash = openedvalue if openedvalue <= cash else cash
            if openedvalue > 0:  # long position being opened
                opencash /= comminfo.get_leverage()  # dec cash with level

            cash -= opencash  # original behavior

            openedcomm = cinfocomp.getcommission(opened, price)
            cash -= openedcomm

            if cash < 0.0:
                # execution is not possible - nullify
                opened = Decimal('0.0')
                openedvalue = openedcomm = Decimal('0.0')

            elif ago is not None:  # real execution
                if abs(psize) > abs(opened):
                    # some futures were opened - adjust the cash of the
                    # previously existing futures to the operation price and
                    # use that as new adjustment base, because it already is
                    # for the new futures At the end of the cycle the
                    # adjustment to the close price will be done for all open
                    # futures from a common base price with regards to the
                    # close price
                    adjsize = psize - opened
                    cash += comminfo.cashadjust(adjsize,
                                                position.adjbase, price)

                # record adjust price base for end of bar cash adjustment
                position.adjbase = price

                # update system cash - checking if opened is still != 0
                self.cash = cash
        else:
            openedvalue = openedcomm = Decimal('0.0')

        if ago is None:
            # return cash from pseudo-execution
            return cash

        execsize = closed + opened

        if execsize:
            # Confimrm the operation to the comminfo object
            comminfo.confirmexec(execsize, price)

            # do a real position update if something was executed
            position.update(execsize, price, data.datetime.datetime())

            if closed and self.p.int2pnl:  # Assign accumulated interest data
                closedcomm += self.d_credit.pop(data, Decimal('0.0'))

            # Execute and notify the order
            order.execute(dtcoc or data.datetime[ago],
                          execsize, price,
                          closed, closedvalue, closedcomm,
                          opened, openedvalue, openedcomm,
                          comminfo.margin, pnl,
                          psize, pprice)

            order.addcomminfo(comminfo)

        if popened and not opened:
            # opened was not executed - not enough cash
            order.margin()
            self.notify(order)
            self._ococheck(order)
            self._bracketize(order, cancel=True)

    def transmit(self, order, check=True):
        order.transmit()

        # print(1111, binance_order)
        # 1111 {'symbol': 'ETHUSDT', 'orderId': 15860400971, 'orderListId': -1, 'clientOrderId': 'EO7lLPcYNZR8cNEg8AOEPb', 'transactTime': 1707124560731, 'price': '0.00000000', 'origQty': '0.00220000', 'executedQty': '0.00220000', 'cummulativeQuoteQty': '5.10356000', 'status': 'FILLED', 'timeInForce': 'GTC', 'type': 'MARKET', 'side': 'BUY', 'workingTime': 1707124560731, 'fills': [{'price': '2319.80000000', 'qty': '0.00220000', 'commission': '0.00000220', 'commissionAsset': 'ETH', 'tradeId': 1297261843}], 'selfTradePreventionMode': 'EXPIRE_MAKER'}
        # order = BinanceOrder(owner, data, exectype, binance_order)
        execsize = float(order.binance['executedQty'])

        if execsize:
            for trade in order.binance['fills']:
                self.order_trades.append({
                    'orderId': order.binance['orderId'],
                    'status': order.binance['status'],
                    'size': execsize,
                    'price': trade['price'],
                    'commAmount': trade['commission'],
                    'commAsset': trade['commissionAsset']
                })

        return super().transmit(order, check)

    def submit_accept(self, order):
        self.orders.append(order)
        return super().submit_accept(order)

    def _get_value(self, datas=None, lever=False):
        pos_value = Decimal('0.0')
        pos_value_unlever = Decimal('0.0')
        unrealized = Decimal('0.0')

        while self._cash_addition:
            c = self._cash_addition.popleft()
            self._fundshares += c / self._fundval
            self.cash += c

        for data in datas or self.positions:
            comminfo = self.getcommissioninfo(data)
            position = self.positions[data]
            if not self.p.shortcash:
                dvalue = comminfo.getvalue(position, Decimal(str(data.close[0])))
            else:
                dvalue = comminfo.getvaluesize(position.size, Decimal(str(data.close[0])))

            dunrealized = comminfo.profitandloss(position.size, position.price,
                                                 Decimal(str(data.close[0])))
            if datas and len(datas) == 1:
                if lever and dvalue > Decimal('0'):
                    dvalue -= dunrealized
                    return (dvalue / comminfo.get_leverage()) + dunrealized
                return dvalue

            if not self.p.shortcash:
                dvalue = abs(dvalue)

            pos_value += dvalue
            unrealized += dunrealized

            if dvalue > Decimal('0'):
                dvalue -= dunrealized
                pos_value_unlever += (dvalue / comminfo.get_leverage())
                pos_value_unlever += dunrealized
            else:
                pos_value_unlever += dvalue

        if not self._fundhist:
            self._value = v = self.cash + pos_value_unlever
            self._fundval = self._value / self._fundshares
        else:
            fval, fvalue = self._process_fund_history()

            self._value = fvalue
            self.cash = fvalue - pos_value_unlever
            self._fundval = fval
            self._fundshares = fvalue / fval
            lev = pos_value / (pos_value_unlever or Decimal('1.0'))

            pos_value_unlever = fvalue
            pos_value = fvalue * lev

        self._valuemkt = pos_value_unlever

        self._valuelever = self.cash + pos_value
        self._valuemktlever = pos_value

        self._leverage = pos_value / (pos_value_unlever or Decimal('1.0'))
        self._unrealized = unrealized

        return self._value if not lever else self._valuelever