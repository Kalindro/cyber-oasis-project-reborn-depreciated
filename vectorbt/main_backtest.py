from dataclasses import dataclass

import pandas as pd
import vectorbt as vbt

from prime_functions.funcs_for_pairs_lists import get_full_history_for_pairs_list
from prime_functions.select_mode import FundamentalSettings
from utils.log_config import ConfigureLoguru

logger = ConfigureLoguru().info_level()

vbt.settings['plotting']['layout']['width'] = 1800
vbt.settings['plotting']['layout']['height'] = 800
vbt.settings.set_theme("seaborn")

vbt.settings.portfolio['init_cash'] = 1000
vbt.settings.portfolio['fees'] = 0.0025
vbt.settings.portfolio['slippage'] = 0
vbt.settings.portfolio.stats['incl_unrealized'] = True


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
        pf = self._momentum_strategy()

        print(pf.stats())
        if self.PLOTTING:
            fig = self._plot_base(portfolio=pf, price_df=price_df)
            fig.show()

    def _momentum_strategy(self):
        pass

    def _get_history(self):
        pairs_history_df_list = get_full_history_for_pairs_list(pairs_list=self.pairs_list,
                                                                    timeframe=self.TIMEFRAME,
                                                                    save_load_history=self.SAVE_LOAD_HISTORY,
                                                                    since=self.since, end=self.end, API=self.API,
                                                                    min_data_length=self.MIN_DATA_LENGTH)
        # price_df = pd.concat(pairs_history_df_list, axis=1)

        return pairs_history_df_list

    @staticmethod
    def _plot_base(portfolio, price_df):
        pf = portfolio
        fig = pf.plot(subplots=[("price", dict(title="Price", group_id_labels=True, yaxis_kwargs=dict(title="Price"))
                                 ), "value", "trades", "cum_returns", "drawdowns", "cash"])
        fig = price_df.vbt.ohlc.plot(plot_type="candlestick", show_volume=False,
                                     ohlc_add_trace_kwargs=dict(row=1, col=1), xaxis=dict(rangeslider_visible=False),
                                     fig=fig)
        fig = pf.orders.plot(add_trace_kwargs=dict(row=1, col=1), buy_trace_kwargs=dict(marker=dict(color="blue")),
                             sell_trace_kwargs=dict(marker=dict(color="black")),
                             close_trace_kwargs=dict(opacity=0, line=dict(color="black")), fig=fig)

        return fig


if __name__ == "__main__":
    MainBacktest().main()
