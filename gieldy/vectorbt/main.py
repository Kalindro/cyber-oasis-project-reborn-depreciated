import pandas as pd
import vectorbt as vbt
from gieldy.general.log_config import ConfigureLoguru

from gieldy.CCXT.CCXT_functions_builtin import get_pairs_prices
from gieldy.CCXT.CCXT_functions_mine import get_history_of_all_pairs_on_list, select_exchange_mode, \
    select_pairs_list_mode

logger = ConfigureLoguru().info_level()


class _BaseSettings:
    """
    Script for performance of all coins in a table. Pairs modes:
    """

    def __init__(self):
        """
        :EXCHANGE_MODE: 1 - Binance Spot; 2 - Binance Futures; 3 - Kucoin Spot
        :PAIRS_MODE: 1 - Test; 2 - BTC; 3 - USDT
        """
        self.EXCHANGE_MODE = 1
        self.PAIRS_MODE = 1
        self.timeframe = "1h"
        self.number_of_last_candles = 2000
        self.min_vol_USD = 150_000
        self.CORES_USED = 6

        self.since = "01.01.2022"
        self.end = "31.12.2022"
        self.period = 10
        self.deviation = 5

        self.API = select_exchange_mode(self.EXCHANGE_MODE)
        self.pairs_list = select_pairs_list_mode(self.PAIRS_MODE, self.API)
        self.BTC_price = get_pairs_prices(self.API).loc["BTC/USDT"]["price"]
        self.min_vol_BTC = self.min_vol_USD / self.BTC_price

    def backtest_envelope(self):
        history_df_list = get_history_of_all_pairs_on_list(pairs_list=self.pairs_list, timeframe=self.timeframe,
                                                           save_load_history=False, since=self.since, end=self.end,
                                                           API=self.API)
        stacked_history_df = pd.concat(history_df_list, axis=1)

        ma = vbt.MA(stacked_history_df, self.period)
        upper_band = ma * (1 + self.deviation)
        lower_band = ma * (1 - self.deviation)

        entry_signal = ma.cross_above(stacked_history_df, upper_band)
        exit_signal = ma.cross_below(stacked_history_df, lower_band)

        vbt.settings.portfolio['init_cash'] = 100  # 100$
        vbt.settings.portfolio['fees'] = 0.0025  # 0.25%
        vbt.settings.portfolio['slippage'] = 0.0025  # 0.25%

        backtest = vbt.Backtest(entry_signal, exit_signal, stacked_history_df)

        # Run the backtest and print the results
        backtest.run()
        print(backtest.results)


if __name__ == "__main__":
    _BaseSettings().backtest_envelope()
