import pandas as pd
import vectorbt as vbt

from gieldy.CCXT.CCXT_functions_builtin import get_pairs_prices
from gieldy.CCXT.CCXT_functions_mine import get_history_of_all_pairs_on_list, select_exchange_mode, \
    select_pairs_list_mode
from gieldy.general.log_config import ConfigureLoguru

pd.set_option('display.max_rows', 0)
pd.set_option('display.max_columns', 0)
pd.set_option('display.width', 0)

logger = ConfigureLoguru().info_level()


class _BaseSettings:
    """
    Script for performance of all coins in a table. Pairs modes:
    """

    def __init__(self):
        """
        :EXCHANGE_MODE: 1 - Binance Spot; 2 - Binance Futures; 3 - Kucoin Spot
        :PAIRS_MODE: 1 - Test single; 2 - Test multi; 3 - BTC; 4 - USDT
        """
        self.EXCHANGE_MODE = 1
        self.PAIRS_MODE = 1
        self.timeframe = "1h"
        self.min_vol_USD = 150_000
        self.CORES_USED = 6

        self.since = "01.01.2022"
        self.end = "31.12.2022"
        self.period = 10
        self.deviation = 0.05

        self.API = select_exchange_mode(self.EXCHANGE_MODE)
        self.pairs_list = select_pairs_list_mode(self.PAIRS_MODE, self.API)
        self.BTC_price = get_pairs_prices(self.API).loc["BTC/USDT"]["price"]
        self.min_vol_BTC = self.min_vol_USD / self.BTC_price

    def backtest_envelope(self):
        all_coins_history_df_dict = get_history_of_all_pairs_on_list(pairs_list=self.pairs_list, timeframe=self.timeframe,
                                                           save_load_history=True, since=self.since, end=self.end,
                                                           API=self.API)
        stacked_history_df = pd.concat(all_coins_history_df_dict, axis=1)
        x = vbt.Data.from_data(all_coins_history_df_dict)
        print(x)
        price = stacked_history_df["close"]
        upper_band = vbt.MA.run(price * (1 + self.deviation), window=self.period)
        lower_band = vbt.MA.run(price * (1 - self.deviation), window=self.period)
        entry_signal = lower_band.close_crossed_below(lower_band)
        exit_signal = upper_band.close_crossed_below(upper_band)

        vbt.settings.portfolio['init_cash'] = 1000
        vbt.settings.portfolio['fees'] = 0.0025
        vbt.settings.portfolio['slippage'] = 0.0025

        pf = vbt.Portfolio.from_signals(price, entry_signal, exit_signal)
        print(pf.stats())
        # pf.plot().show()


if __name__ == "__main__":
    _BaseSettings().backtest_envelope()
