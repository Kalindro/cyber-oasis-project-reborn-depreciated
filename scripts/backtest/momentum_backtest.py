import os
from dataclasses import dataclass

import numpy as np
import pandas as pd
import vectorbtpro as vbt

from exchange.fundamental_template import FundamentalTemplate
from exchange.get_history import GetFullHistoryDF
from scripts.backtest.backtest_template import BacktestTemplate
from scripts.backtest.momentum_allocation import MomentumStrat
from utils.log_config import ConfigureLoguru
from utils.utils import excel_save_formatted_naive

logger = ConfigureLoguru().info_level()

vbt.settings['plotting']['layout']['width'] = 1800
vbt.settings['plotting']['layout']['height'] = 800
vbt.settings.set_theme("seaborn")

vbt.settings.portfolio['init_cash'] = 1000
vbt.settings.portfolio['fees'] = 0.001
vbt.settings.portfolio['slippage'] = 0
vbt.settings.portfolio.stats['incl_unrealized'] = True


@dataclass
class _BaseTemplate(FundamentalTemplate):
    def __init__(self):
        self.EXCHANGE_MODE: int = 1
        self.PAIRS_MODE: int = 4
        super().__init__(exchange_mode=self.EXCHANGE_MODE, pairs_mode=self.PAIRS_MODE)

        self.PERIODS = dict(MOMENTUM=[20],  # np.arange(2, 60, 2),
                            NATR=False,  # np.arange(2, 100, 2)
                            BTC_SMA=False,  # np.arange(2, 100, 2)
                            TOP_NUMBER=20,
                            REBALANCE=["1d"],  # ["8h", "12h", "1d", "2d", "4d"]
                            )

        self.SAVE_LOAD_HISTORY: bool = True
        self.VOL_QUANTILE_DROP = 0.2
        self.TIMEFRAME: str = "4h"
        self.START: str = "01.01.2022"
        self.END: str = "01.01.2023"


class MainBacktest(_BaseTemplate, BacktestTemplate):
    """Main class with backtesting template"""

    def __init__(self):
        super().__init__()
        self.current_strat = MomentumStrat().momentum_strat

    def main(self):
        backtest_pickle_name = "backtest.pickle"
        if os.path.exists(backtest_pickle_name):
            pf = vbt.Portfolio.load(backtest_pickle_name)
        else:
            self.vbt_data = self._get_history()
            pf = self.current_strat(vbt_data=self.vbt_data, periods=self.PERIODS)
            pf.save(backtest_pickle_name)
        analytics = pf.stats(agg_func=None)
        trades = pf.get_trade_history()
        print(analytics.to_string())

        try:
            trades[["Index"]] = trades[["Index"]]
            analytics[["Start", "End"]] = analytics[["Start", "End"]]
            if isinstance(trades, pd.DataFrame):
                excel_save_formatted_naive(dataframe=trades, filename="trades_analytics.xlsx")
            if isinstance(analytics, pd.DataFrame):
                excel_save_formatted_naive(dataframe=analytics, filename="backtest_analytics.xlsx")
        except Exception as err:
            print(err)

    def _get_history(self):
        vbt_data = GetFullHistoryDF(pairs_list=self.pairs_list, start=self.START, end=self.END,
                                    timeframe=self.TIMEFRAME, API=self.API, save_load_history=self.SAVE_LOAD_HISTORY,
                                    vol_quantile_drop=self.VOL_QUANTILE_DROP).get_full_history()

        return vbt_data


if __name__ == "__main__":
    MainBacktest().main()
    a = np.log(5)  # So formatter won't remove np from import
