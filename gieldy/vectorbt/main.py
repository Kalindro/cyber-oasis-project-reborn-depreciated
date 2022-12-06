import vectorbt as vbt

from gieldy.API.API_exchange_initiator import ExchangeAPI
from gieldy.CCXT.CCXT_functions_builtin import get_pairs_prices, get_pairs_precisions_status
from gieldy.CCXT.CCXT_functions_mine import get_pairs_list_BTC, get_history_of_all_pairs_on_list
from gieldy.general.utils import excel_save_formatted
from gieldy.CCXT.get_full_history import GetFullHistory


class Backtest:

    def __init__(self):
        self.PAIRS_MODE = 1
        self.CORES_USED = 8
        self.API_spot = ExchangeAPI().binance_spot_read_only()

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

    def select_pairs_list_and_API(self):
        """Depending on the PAIRS_MODE, return correct paris list and API"""
        fut_API = ExchangeAPI().binance_futures_read_only()
        spot_API = ExchangeAPI().binance_spot_read_only()

        if self.PAIRS_MODE == 1:
            pairs_list = ["BTC/USDT", "ETH/USDT"]
            API = spot_API
        elif self.PAIRS_MODE == 2:
            pairs_list = self.pairs_list_futures_USDT()
            API = fut_API
        elif self.PAIRS_MODE == 3:
            pairs_list = self.pairs_list_spot_USDT()
            API = spot_API
        elif self.PAIRS_MODE == 4:
            pairs_list = self.pairs_list_spot_BTC()
            API = spot_API
        else:
            raise ValueError("Invalid mode: " + str(self.PAIRS_MODE))

    def all_pairs_history(self):
        pairs_list = self.pairs_list_spot_BTC()
        all_pairs_history = get_history_of_all_pairs_on_list(
            pairs_list=["BTC/USDT", "ETH/USDT"],
            timeframe=self.TIMEFRAME,
            last_n_candles=50, save_load=False, API=self.API_spot)
        print(all_pairs_history)


if __name__ == "__main__":
    Backtest().all_pairs_history()
    vbt.data()