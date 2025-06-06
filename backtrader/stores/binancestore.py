import time
from functools import wraps
from math import floor

from binance import Client, ThreadedWebsocketManager
from binance.enums import *
from binance.exceptions import BinanceAPIException
from requests.exceptions import ConnectTimeout, ConnectionError

from backtrader.dataseries import TimeFrame


class BinanceStore(object):
    _GRANULARITIES = {
        (TimeFrame.Minutes, 1): KLINE_INTERVAL_1MINUTE,
        (TimeFrame.Minutes, 3): KLINE_INTERVAL_3MINUTE,
        (TimeFrame.Minutes, 5): KLINE_INTERVAL_5MINUTE,
        (TimeFrame.Minutes, 15): KLINE_INTERVAL_15MINUTE,
        (TimeFrame.Minutes, 30): KLINE_INTERVAL_30MINUTE,
        (TimeFrame.Minutes, 60): KLINE_INTERVAL_1HOUR,
        (TimeFrame.Minutes, 120): KLINE_INTERVAL_2HOUR,
        (TimeFrame.Minutes, 240): KLINE_INTERVAL_4HOUR,
        (TimeFrame.Minutes, 360): KLINE_INTERVAL_6HOUR,
        (TimeFrame.Minutes, 480): KLINE_INTERVAL_8HOUR,
        (TimeFrame.Minutes, 720): KLINE_INTERVAL_12HOUR,
        (TimeFrame.Days, 1): KLINE_INTERVAL_1DAY,
        (TimeFrame.Days, 3): KLINE_INTERVAL_3DAY,
        (TimeFrame.Weeks, 1): KLINE_INTERVAL_1WEEK,
        (TimeFrame.Months, 1): KLINE_INTERVAL_1MONTH,
    }

    def __init__(self, api_key, api_secret, coin_target, testnet=False, retries=5, tld='com'):  # coin_refer, coin_target
        self.binance = Client(api_key, api_secret, testnet=testnet, tld=tld)
        self.binance_socket = ThreadedWebsocketManager(api_key, api_secret, testnet=testnet)
        self.binance_socket.daemon = True
        self.binance_socket.start()
        # self.coin_refer = coin_refer
        self.coin_target = coin_target  # USDT
        # self.symbol = coin_refer + coin_target
        self.symbols = []  # symbols
        self.retries = retries

        self._cash = 0
        self._value = 0
        self.get_balance()

        self._asset_filters = {}

        self._broker = None
        self._data = None
        self._datas = {}

    def _format_value(self, value, step):
        precision = step.find('1') - 1
        if precision > 0:
            return '{:0.0{}f}'.format(float(value), precision)
        return floor(value)
        
    def retry(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            for attempt in range(1, self.retries + 1):
                time.sleep(60 / 1200) # API Rate Limit
                try:
                    return func(self, *args, **kwargs)
                except (BinanceAPIException, ConnectTimeout, ConnectionError) as err:
                    if isinstance(err, BinanceAPIException) and err.code == -1021:
                        # Recalculate timestamp offset between local and Binance's server
                        res = self.binance.get_server_time()
                        self.binance.timestamp_offset = res['serverTime'] - int(time.time() * 1000)
                    
                    if attempt == self.retries:
                        raise
        return wrapper

    @retry
    def cancel_open_orders(self, symbol):
        orders = self.binance.get_open_orders(symbol=symbol)
        if len(orders) > 0:
            self.binance._request_api('delete', 'openOrders', signed=True, data={ 'symbol': symbol })

    @retry
    def cancel_order(self, symbol, order_id):
        try:
            self.binance.cancel_order(symbol=symbol, orderId=order_id)
        except BinanceAPIException as api_err:
            if api_err.code == -2011:  # Order filled
                return
            else:
                raise api_err
        except Exception as err:
            raise err
    
    @retry
    def create_order(self, symbol, side, type, size, price, **params):
        if type == None: type = ORDER_TYPE_MARKET

        if type != ORDER_TYPE_MARKET:
            params.update({
                'price': self.format_price(symbol, price)
            })
           
        if type in [ORDER_TYPE_LIMIT, ORDER_TYPE_STOP_LOSS_LIMIT]:
            params.update({
                'timeInForce': TIME_IN_FORCE_GTC
            })
        elif type == ORDER_TYPE_STOP_LOSS:
            params.update({
                'stopPrice': self.format_price(symbol, price)
            })
        
        if size is not None: size = self.format_quantity(symbol, size)
                
        return self.binance.create_order(
            symbol=symbol,
            side=side,
            type=type,
            quantity=size,
            newOrderRespType='FULL',
            **params)

    def format_price(self, symbol, price):
        return self._format_value(price, self._asset_filters[symbol]['tickSize'])
    
    def format_quantity(self, symbol, size):
        return self._format_value(size, self._asset_filters[symbol]['stepSize'])

    @retry
    def get_asset_balance(self, asset):
        balance = self.binance.get_asset_balance(asset)
        return float(balance['free']), float(balance['locked'])

    def get_symbol_balance(self, symbol):
        """Get symbol balance in symbol"""
        balance = 0
        try:
            symbol = symbol[0:len(symbol)-len(self.coin_target)]
            balance = self.binance.get_asset_balance(symbol)
            balance = float(balance['free'])
        except Exception as e:
            print("Error:", e)
        return balance, symbol  # float(balance['locked'])

    def get_balance(self, ):
        """Balance in USDT for example - in coin target"""
        free, locked = self.get_asset_balance(self.coin_target)
        self._cash = free
        self._value = free + locked

    def get_filters(self, symbol):
        if not self._asset_filters:
            self._asset_filters[symbol] = self._get_filters(symbol)

        return self._asset_filters[symbol]

    def _get_filters(self, symbol):
        symbol_info = self.get_symbol_info(symbol)
        filters = dict()
        for f in symbol_info['filters']:
            if f['filterType'] == 'LOT_SIZE':
                filters.update({
                    'stepSize': f['stepSize'],
                    'minQty': f['minQty']
                })
            elif f['filterType'] == 'PRICE_FILTER':
                filters.update({
                    'tickSize': f['tickSize']
                })
            elif f['filterType'] == 'NOTIONAL':
                filters.update({
                    'minNotional': f['minNotional']
                })

        return filters

    def get_interval(self, timeframe, compression):
        return self._GRANULARITIES.get((timeframe, compression))

    @retry
    def get_symbol_info(self, symbol):
        return self.binance.get_symbol_info(symbol)

    def stop_socket(self):
        self.binance_socket.stop()
        self.binance_socket.join(5)