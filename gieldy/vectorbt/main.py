import numpy as np
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

vbt.settings['plotting']['layout']['width'] = 1300
vbt.settings['plotting']['layout']['height'] = 650
vbt.settings.set_theme("seaborn")

vbt.settings.portfolio['init_cash'] = 1000
vbt.settings.portfolio['fees'] = 0.0025
vbt.settings.portfolio['slippage'] = 0.0025


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
        self.period = 20
        self.deviation = 2

        self.API = select_exchange_mode(self.EXCHANGE_MODE)
        self.pairs_list = select_pairs_list_mode(self.PAIRS_MODE, self.API)
        self.BTC_price = get_pairs_prices(self.API).loc["BTC/USDT"]["price"]
        self.min_vol_BTC = self.min_vol_USD / self.BTC_price

    def plot_base(self, portfolio, price_df, entries, exits):
        pf = portfolio
        fig = pf.plot(
            subplots=[("price", dict(title="Price", yaxis_kwargs=dict(title="Price"))), "trades", "trade_pnl", "cash",
                      "cum_returns", "drawdowns", ])
        fig = price_df.vbt.ohlc.plot(plot_type="candlestick", show_volume=False,
                                     ohlc_add_trace_kwargs=dict(row=1, col=1),
                                     fig=fig, xaxis=dict(rangeslider_visible=False))
        fig = entries.vbt.signals.plot_as_entry_markers(price_df["close"], add_trace_kwargs=dict(row=1, col=1),
                                                        trace_kwargs=dict(marker=dict(color="deepskyblue")), fig=fig)
        fig = fig
        fig = exits.vbt.signals.plot_as_exit_markers(price_df["close"], add_trace_kwargs=dict(row=1, col=1),
                                                     trace_kwargs=dict(marker=dict(color="orange")), fig=fig)
        return fig

    def keltner_strat(self, price_df):
        keltner = vbt.IndicatorFactory.from_pandas_ta("kc").run(high=price_df["high"], low=price_df["low"],
                                                                close=price_df["close"],
                                                                length=self.period, scalar=self.deviation)
        entries = keltner.close_crossed_below(keltner.kcle)
        exits = keltner.close_crossed_above(keltner.kcue)
        return entries, exits, keltner

    def keltner_print(self, keltner, fig):
        fig = keltner.kcue.vbt.plot(
            trace_kwargs=dict(name="Upper Band", opacity=0.6, line=dict(color="darkslateblue")),
            add_trace_kwargs=dict(row=1, col=1), fig=fig)
        fig = keltner.kcle.vbt.plot(
            trace_kwargs=dict(name="Lower Band", opacity=0.6, line=dict(color="darkslateblue")),
            add_trace_kwargs=dict(row=1, col=1), fig=fig)
        return fig

    def main(self):
        all_coins_history_df_list = get_history_of_all_pairs_on_list(pairs_list=self.pairs_list,
                                                                     timeframe=self.timeframe,
                                                                     save_load_history=True, since=self.since,
                                                                     end=self.end,
                                                                     API=self.API)
        price_df = pd.concat(all_coins_history_df_list, axis=1)
        entries, exits, keltner = self.keltner_strat(price_df=price_df)

        pf = vbt.Portfolio.from_signals(open=price_df["open"], close=price_df["close"], high=price_df["high"],
                                        low=price_df["low"], size=np.inf, entries=entries, exits=exits)
        print(pf.stats())

        fig = self.plot_base(portfolio=pf, price_df=price_df, entries=entries, exits=exits)
        fig = self.keltner_print(keltner=keltner, fig=fig)
        fig.show()


if __name__ == "__main__":
    _BaseSettings().main()
