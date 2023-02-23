from dataclasses import dataclass

import numpy as np
import pandas as pd
import vectorbt as vbt

from ext_projects.CCXT.functions_mine import get_full_history_for_pairs_list, select_exchange_mode
from ext_projects.CCXT.functions_pairs_list import select_pairs_list_mode
from general_funcs.log_config import ConfigureLoguru

logger = ConfigureLoguru().info_level()

vbt.settings['plotting']['layout']['width'] = 1800
vbt.settings['plotting']['layout']['height'] = 800
vbt.settings.set_theme("seaborn")

vbt.settings.portfolio['init_cash'] = 1000
vbt.settings.portfolio['fees'] = 0.0025


@dataclass
class _BaseSettings:
    """
    Modes available:
    :EXCHANGE_MODE: 1 - Binance Spot; 2 - Binance Futures; 3 - Kucoin Spot
    :PAIRS_MODE: 1 - Test single; 2 - Test multi; 3 - BTC; 4 - USDT
    """
    EXCHANGE_MODE: int = 1
    PAIRS_MODE: int = 4
    SAVE_LOAD_HISTORY: bool = True
    PLOTTING: bool = True

    TIMEFRAME: str = "1h"
    since: str = "01.01.2022"
    end: str = "31.12.2022"
    PERIOD: int = 20
    DEVIATION: int = 2

    def __post_init__(self):
        self.API = select_exchange_mode(self.EXCHANGE_MODE)
        self.pairs_list = select_pairs_list_mode(self.PAIRS_MODE, self.API)
        self.min_data_length = 100
        self.validate_inputs()

    def validate_inputs(self) -> None:
        """Validate input parameters"""
        if self.PAIRS_MODE != 1:
            self.PLOTTING = False


class MainBacktest(_BaseSettings):
    """Main class with backtesting template"""

    def plot_base(self, portfolio, price_df):
        pf = portfolio
        fig = pf.plot(subplots=[("price", dict(title="Price", group_id_labels=True, yaxis_kwargs=dict(title="Price"))
                                 ), "value", "trades", "cum_returns", "drawdowns", "cash"])
        fig = price_df.vbt.ohlc.plot(plot_type="candlestick", show_volume=False,
                                     ohlc_add_trace_kwargs=dict(row=1, col=1), xaxis=dict(rangeslider_visible=False),
                                     fig=fig)
        fig = pf.orders.plot(add_trace_kwargs=dict(row=1, col=1), buy_trace_kwargs=dict(marker=dict(color="blue")),
                             sell_trace_kwargs=dict(marker=dict(color="black")),
                             close_trace_kwargs=dict(opacity=0, line=dict(color="black")), fig=fig)
        # fig = entries.vbt.signals.plot_as_entry_markers(price_df["close"], add_trace_kwargs=dict(row=1, col=1),
        #                                                 trace_kwargs=dict(marker=dict(color="deepskyblue")), fig=fig)
        # fig = exits.vbt.signals.plot_as_exit_markers(price_df["close"], add_trace_kwargs=dict(row=1, col=1),
        #                                              trace_kwargs=dict(marker=dict(color="orange")), fig=fig)

        return fig

    def main(self):
        all_coins_history_df_list = get_full_history_for_pairs_list(pairs_list=self.pairs_list,
                                                                    timeframe=self.TIMEFRAME,
                                                                    save_load_history=self.SAVE_LOAD_HISTORY,
                                                                    since=self.since, end=self.end, API=self.API,
                                                                    min_data_length=self.min_data_length)
        price_df = pd.concat(all_coins_history_df_list, axis=1)
        entries, exits, keltner = self.keltner_strat(price_df=price_df)

        pf = vbt.Portfolio.from_signals(open=price_df["open"], close=price_df["close"], high=price_df["high"],
                                        low=price_df["low"], size=np.inf, entries=entries, exits=exits)
        print(pf.stats())
        if self.PLOTTING:
            fig = self.plot_base(portfolio=pf, price_df=price_df)
            fig = self.keltner_print(keltner=keltner, fig=fig)
            fig.show()

    def keltner_strat(self, price_df):
        keltner = vbt.IndicatorFactory.from_pandas_ta("kc").run(high=price_df["high"], low=price_df["low"],
                                                                close=price_df["close"], length=self.PERIOD,
                                                                scalar=self.DEVIATION)
        upper_band = keltner.kcue.to_numpy()
        lower_band = keltner.kcle.to_numpy()
        trend = np.where(price_df.close < lower_band, 1, 0)
        trend = np.where(price_df.close > upper_band, -1, trend)

        entries = trend == 1
        exits = trend == -1

        return entries, exits, keltner

    def keltner_print(self, keltner, fig):
        fig = keltner.kcue.vbt.plot(
            trace_kwargs=dict(name="Upper Band", opacity=0.55, line=dict(color="darkslateblue")),
            add_trace_kwargs=dict(row=1, col=1), fig=fig)
        fig = keltner.kcle.vbt.plot(
            trace_kwargs=dict(name="Lower Band", opacity=0.55, line=dict(color="darkslateblue")),
            add_trace_kwargs=dict(row=1, col=1), fig=fig)

        return fig


if __name__ == "__main__":
    MainBacktest().main()
