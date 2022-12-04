import vectorbt as vbt

from gieldy.APIs.API_exchange_initiator import ExchangeAPI
from gieldy.CCXT.CCXT_functions_builtin import get_pairs_prices, get_pairs_precisions_status
from gieldy.CCXT.CCXT_functions_mine import get_pairs_list_BTC, get_history_of_all_pairs_on_list
from gieldy.general.utils import excel_save_formatted
from gieldy.CCXT.get_full_history import GetFullHistory


class Backtest:

    def __init__(self):
        self.PAIRS_MODE = 2
        self.CORES_USED = 8
        self.API_spot = ExchangeAPI().binance_spot_read()

        self.START_DATE = "10.10.2021"
        self.END_DATE = "10.10.2022"
        self.TIMEFRAME = "1h"
        self.MIN_VOL_USD = 100_000
        self.BTC_PRICE = get_pairs_prices(self.API_spot).loc["BTC/USDT"]["price"]
        self.MIN_VOL_BTC = self.MIN_VOL_USD / self.BTC_PRICE

    def pairs_list_spot_BTC(self):
        """Only pairs on Binance spot BTC"""
        pairs_list = get_pairs_list_BTC(self.API_spot)

        return pairs_list

    def all_pairs_history(self):
        pairs_list = self.pairs_list_spot_BTC()
        all_pairs_history = get_history_of_all_pairs_on_list(timeframe=self.TIMEFRAME, since=self.START_DATE,
                                                             end=self.END_DATE)


if __name__ == "__main__":
