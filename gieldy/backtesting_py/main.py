import vectorbt as vbt

from gieldy.APIs.API_exchange_initiator import ExchangeAPI
from gieldy.CCXT.CCXT_functions_builtin import get_pairs_prices, get_pairs_precisions_status
from gieldy.CCXT.CCXT_functions_mine import pairs_list_BTC
from gieldy.general.utils import excel_save_formatted
from gieldy.CCXT.get_full_history import GetFullHistory


class Backtest:

    def __init__(self):
        self.PAIRS_MODE = 2
        self.CORES_USED = 8
        self.API_spot = ExchangeAPI().binance_spot_read()
        self.BTC_PRICE = get_pairs_prices(self.API_spot).loc["BTC/USDT"]["price"]
        self.MIN_VOL_USD = 150_000
        self.MIN_VOL_BTC = self.MIN_VOL_USD / self.BTC_PRICE

    def pairs_list_spot_BTC(self):
        """Only pairs on Binance spot BTC"""
        pairs_list = pairs_list_BTC(self.API_spot)

        return pairs_list

