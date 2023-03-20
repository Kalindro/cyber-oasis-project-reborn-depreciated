from dataclasses import dataclass

import vectorbtpro as vbt

from exchange.get_history import GetFullHistoryDF
from exchange.select_mode import FundamentalSettings
from general_functions.momentums import momentum_ranking_with_parity
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
        data = self._get_history()
        vbt_data = vbt.Data.from_data(data=data)
        pf = self.momentum_strat(vbt_data)

        print(pf.stats())

        # if self.PLOTTING:
        #     price_df = self._get_history()
        #     fig = self._plot_base(portfolio=pf, price_df=price_df)
        #     fig.show()

    def momentum_strat(self, vbt_data):
        allocations = momentum_ranking_with_parity(momentum_period=self.PERIODS["MOMENTUM"],
                                                   NATR_period=self.PERIODS["NATR"],
                                                   pairs_history_df_dict=dict(vbt_data.data))

        wrapper = vbt_data.get("close").vbt.wrapper
        # pf = vbt.PortfolioOptimizer.from_allocate_func(
        #     wrapper,
        #     allocations_func,
        #     every="1D"
        # )

        pf_optimizer = vbt.PortfolioOptimizer.from_allocations(
            wrapper,
            allocations
        )
        print(pf_optimizer)
        pf = pf_optimizer.simulate(vbt_data, freq="1h")

        return pf

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
