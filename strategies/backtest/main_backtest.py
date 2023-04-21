import os
from dataclasses import dataclass

import numpy as np
import pandas as pd
import vectorbtpro as vbt

from exchange.get_history import GetFullHistoryDF
from exchange.select_mode import FundamentalSettings
from strategies.backtest.momentums import MomentumAllocation
from utils.log_config import ConfigureLoguru
from utils.utils import resample_datetime_index

logger = ConfigureLoguru().info_level()

vbt.settings['plotting']['layout']['width'] = 1800
vbt.settings['plotting']['layout']['height'] = 800
vbt.settings.set_theme("seaborn")

vbt.settings.portfolio['init_cash'] = 1000
vbt.settings.portfolio['fees'] = 0.001
vbt.settings.portfolio['slippage'] = 0
vbt.settings.portfolio.stats['incl_unrealized'] = True


@dataclass
class _BaseSettings(FundamentalSettings):
    def __init__(self):
        self.EXCHANGE_MODE: int = 1
        self.PAIRS_MODE: int = 4
        super().__init__(exchange_mode=self.EXCHANGE_MODE, pairs_mode=self.PAIRS_MODE)

        self.PERIODS = dict(MOMENTUM=[20],  # np.arange(2, 60, 2),
                            NATR=False,  # np.arange(2, 100, 2)
                            BTC_SMA=False,  # np.arange(2, 100, 2)
                            TOP_NUMBER=20,
                            REBALANCE=["30min"],  # ["8h", "12h", "1d", "2d", "4d"]
                            )
        self.SAVE_LOAD_HISTORY: bool = True
        self.PLOTTING: bool = True

        self.TIMEFRAME: str = "30min"
        self.start: str = "01.02.2022"
        self.end: str = "01.06.2022"

        self.VOL_QUANTILE_DROP = 0.2
        self._validate_inputs()

    def _validate_inputs(self) -> None:
        """Validate input parameters"""
        if self.PAIRS_MODE != 1:
            self.PLOTTING = False


class MainBacktest(_BaseSettings):
    """Main class with backtesting template"""

    def __init__(self):
        super().__init__()
        self.vbt_data = None

    def main(self):
        backtest_pickle_name = "backtest.pickle"
        if os.path.exists(backtest_pickle_name):
            pf = vbt.Portfolio.load(backtest_pickle_name)
        else:
            self.vbt_data = self._get_history()
            pf = self._momentum_strat(self.vbt_data)
            pf.save(backtest_pickle_name)
        analytics = pf.stats(agg_func=None)
        trades = pf.get_trade_history()
        print(analytics)

        trades[["Index"]] = trades[["Index"]].astype(str)
        analytics[["Start", "End"]] = analytics[["Start", "End"]].astype(str)
        try:
            trades.to_excel("trades_analytics.xlsx")
            analytics.to_excel("backtest_analytics.xlsx")
        except Exception as err:
            print(err)

    def _momentum_strat(self, vbt_data: vbt.Data):
        rebalance_indexes = {
            f"{rebalance_tf}_rl_tf": resample_datetime_index(dt_index=vbt_data.index, resample_tf=rebalance_tf) for
            rebalance_tf in self.PERIODS["REBALANCE"]}

        pf_opt = vbt.PFO.from_allocate_func(
            vbt_data.symbol_wrapper,
            self._allocation_function,
            vbt.RepEval("wrapper.columns"),
            vbt.Rep("i"),
            vbt.Param(self.PERIODS["MOMENTUM"]),
            vbt.Param(self.PERIODS["NATR"]),
            vbt.Param(self.PERIODS["BTC_SMA"]),
            vbt.Param(self.PERIODS["TOP_NUMBER"]),
            on=vbt.Param(rebalance_indexes),
        )

        return pf_opt.simulate(vbt_data, bm_close=vbt_data.get(symbols="BTC/USDT", columns="Close"))

    def _allocation_function(self, columns: pd.DataFrame.columns, i: int, momentum_period: int, NATR_period: int,
                             btc_sma_p: int, top_number: int):
        if i == 0:
            self.allocations = MomentumAllocation().allocation_momentum_ranking(vbt_data=self.vbt_data,
                                                                                momentum_period=momentum_period,
                                                                                NATR_period=NATR_period,
                                                                                btc_sma_p=btc_sma_p,
                                                                                top_number=top_number)

        return self.allocations[columns].iloc[i]

    def _get_history(self):
        vbt_data = GetFullHistoryDF(pairs_list=self.pairs_list, start=self.start, end=self.end,
                                    timeframe=self.TIMEFRAME, API=self.API, save_load_history=self.SAVE_LOAD_HISTORY,
                                    vol_quantile_drop=self.VOL_QUANTILE_DROP).get_full_history()

        return vbt_data

    @staticmethod
    def _plot_ohlc(portfolio, price_df):
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

    @staticmethod
    def _plot_base(portfolio):
        pf = portfolio
        fig = pf.plot(subplots=["value", "trades", "cum_returns", "drawdowns", "cash"])
        return fig

    @staticmethod
    def _benchmark_DD_metric():
        max_winning_streak = (
            'max_winning_streak',
            dict(
                title='Max Winning Streak',
                calc_func=lambda self, group_by:
                self.get_trades(group_by=group_by).winning_streak.max()
            )
        )


if __name__ == "__main__":
    MainBacktest().main()
    a = np.log(5)  # So formatter won't remove np from import
