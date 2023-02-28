from dataclasses import dataclass

import numpy as np
import pandas as pd
import vectorbt as vbt

from generic.funcs_for_pairs_lists import get_full_history_for_pairs_list
from generic.select_mode import FundamentalSettings
from utils import plot_base
from utils.log_config import ConfigureLoguru

logger = ConfigureLoguru().info_level()

vbt.settings['plotting']['layout']['width'] = 1800
vbt.settings['plotting']['layout']['height'] = 800
vbt.settings.set_theme("seaborn")

vbt.settings.portfolio['init_cash'] = 1000
vbt.settings.portfolio['fees'] = 0.0025
vbt.settings.portfolio['slippage'] = 0


@dataclass
class _BaseSettings(FundamentalSettings):
    def __init__(self):
        self.EXCHANGE_MODE: int = 1
        self.PAIRS_MODE: int = 4
        super().__init__(exchange_mode=self.EXCHANGE_MODE, pairs_mode=self.PAIRS_MODE)

        self.PERIODS = dict(KELTNER=168,
                            DEVIATION=128,
                            )
        self.SAVE_LOAD_HISTORY: bool = True
        self.PLOTTING: bool = True

        self.TIMEFRAME: str = "1h"
        self.since: str = "01.01.2022"
        self.end: str = "31.12.2022"

        self.MIN_VOLUME = 10
        self.MIN_DATA_LENGTH = 10
        self.validate_inputs()

    def validate_inputs(self) -> None:
        """Validate input parameters"""
        if self.PAIRS_MODE != 1:
            self.PLOTTING = False


class MainBacktest(_BaseSettings):
    """Main class with backtesting template"""

    def main(self):
        price_df = self._get_history()
        entries, exits, keltner = self.keltner_strat(price_df=price_df)
        pf = vbt.Portfolio.from_signals(open=price_df["open"], close=price_df["close"], high=price_df["high"],
                                        low=price_df["low"], size=np.inf, entries=entries, exits=exits)

        print(pf.stats())
        if self.PLOTTING:
            fig = plot_base(portfolio=pf, price_df=price_df)
            fig = self.keltner_print(keltner=keltner, fig=fig)
            fig.show()

    def _get_history(self):
        all_coins_history_df_list = get_full_history_for_pairs_list(pairs_list=self.pairs_list,
                                                                    timeframe=self.TIMEFRAME,
                                                                    save_load_history=self.SAVE_LOAD_HISTORY,
                                                                    since=self.since, end=self.end, API=self.API,
                                                                    min_data_length=self.MIN_DATA_LENGTH)
        price_df = pd.concat(all_coins_history_df_list, axis=1)

        return price_df


if __name__ == "__main__":
    MainBacktest().main()
