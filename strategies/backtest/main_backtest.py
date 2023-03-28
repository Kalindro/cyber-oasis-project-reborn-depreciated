from dataclasses import dataclass

import vectorbtpro as vbt

from exchange.get_history import GetFullHistoryDF
from exchange.select_mode import FundamentalSettings
from general_functions.momentums import allocation_momentum_ranking
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
        self.PAIRS_MODE: int = 2
        super().__init__(exchange_mode=self.EXCHANGE_MODE, pairs_mode=self.PAIRS_MODE)

        self.PERIODS = dict(MOMENTUM=168,
                            NATR=128,
                            TOP_NUMBER=20,
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

    def __init__(self):
        super().__init__()
        self.data = self._get_history()
        self.vbt_data = vbt.Data.from_data(data=self.data)
        print(self.vbt_data.run("natr").real)

    def main(self):
        pf = self._momentum_strat(self.vbt_data)
        print(pf.stats())

    def _momentum_strat(self, vbt_data):
        pf_opt = vbt.PFO.from_allocate_func(
            vbt_data.symbol_wrapper,
            self._allocation_function,
            vbt.RepEval("wrapper.columns"),
            vbt.Rep("i"),
            vbt.Param(self.PERIODS["MOMENTUM"]),
            vbt.Param(self.PERIODS["NATR"]),
            vbt.Param(self.PERIODS["TOP_NUMBER"]),
            on=vbt.RepEval("wrapper.index")
        )

        return pf_opt.simulate(vbt_data)

    def _allocation_function(self, group_idx, i, momentum_period, NATR_period, top_number):
        if i == 0:
            self.allocations = allocation_momentum_ranking(pairs_history_df_dict=self.data,
                                                           momentum_period=momentum_period,
                                                           NATR_period=NATR_period,
                                                           top_number=top_number)

        return self.allocations[group_idx].iloc[i]

    def _get_history(self):
        pairs_history_df_list = GetFullHistoryDF().get_full_history(pairs_list=self.pairs_list,
                                                                    timeframe=self.TIMEFRAME,
                                                                    save_load_history=self.SAVE_LOAD_HISTORY,
                                                                    since=self.since, end=self.end,
                                                                    API=self.API,
                                                                    min_data_length=self.MIN_DATA_LENGTH)

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
