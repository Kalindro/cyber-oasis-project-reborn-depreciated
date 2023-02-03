import pandas as pd

from API.API_exchange_initiator import ExchangeAPISelect
from CCXT.CCXT_functions_builtin import get_pairs_prices
from general.log_config import ConfigureLoguru

pd.set_option('display.max_rows', 0)
pd.set_option('display.max_columns', 0)
pd.set_option('display.width', 0)

logger = ConfigureLoguru().info_level()


class _BaseSettings:
    """
    Script for performance of all coins in a table. Pairs modes:
    - test (1)
    - futures_USDT (2)
    - spot_BTC (3)
    - spot_USDT (4)
    """

    def __init__(self):
        self.PAIRS_MODE = 2
        self.CORES_USED = 6
        self.min_vol_USD = 150_000
        self.timeframe = "1h"
        self.number_of_last_candles = 2000
        self.API_spot = ExchangeAPISelect().binance_spot_read_only()
        self.API_fut = ExchangeAPISelect().binance_futures_read_only()
        self.BTC_price = get_pairs_prices(self.API_spot).loc["BTC/USDT"]["price"]
        self.min_vol_BTC = self.min_vol_USD / self.BTC_price
