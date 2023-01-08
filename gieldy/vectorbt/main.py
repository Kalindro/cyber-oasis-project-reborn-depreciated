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



if __name__ == "__main__":
    pass