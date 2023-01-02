import vectorbt as vbt

from gieldy.CCXT.CCXT_functions_mine import get_history_of_all_pairs_on_list, get_pairs_list_BTC, get_pairs_list_USDT
from gieldy.API.API_exchange_initiator import ExchangeAPISelect


class Backtest:

    def __init__(self):
        self.PAIRS_MODE = 2
        self.CORES_USED = 6
        self.min_vol_USD = 150_000
        self.timeframe = "1h"
        self.API_spot = ExchangeAPISelect().binance_spot_read_only()
        self.API_fut = ExchangeAPISelect().binance_futures_read_only()
        self.START_DATE = "10.10.2021"
        self.END_DATE = "10.10.2022"
        self.TIMEFRAME = "1h"

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


if __name__ == "__main__":
