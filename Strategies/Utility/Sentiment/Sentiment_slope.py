from Gieldy.Binance.API_initiation_Binance_spot import API_initiation
from Gieldy.Refractor_general.Get_history import get_history_full
from datetime import timedelta
from talib import NATR, ROCP
from pandas import DataFrame as df
from functools import partial
import pandas as pd
import datetime as dt
import numpy as np
from scipy.stats import linregress


class SentimentATR:
    def __init__(self):
        self.PAIR_LIST = ["BTC", "ETH", "BNB", "ADA", "SOL", "AVAX", "FTM", "DOT", "GMT", "NEAR", "APE"]
        self.TIMEFRAME = "1H"
        self.PERIOD = 24
        self.START = dt.datetime.now() - timedelta(days=14)

    def regression_slope(self, data):
        returns = np.log(data)
        x = np.arange(len(returns))
        slope, _, _, _, _ = linregress(x, returns)
        regression_slope = slope * 1000
        return regression_slope

    def get_history(self):
        API_binance = API_initiation()
        ready_pair_list = [symbol + "/USDT" for symbol in self.PAIR_LIST]

        ranking = df()
        get_history_full_with_args = partial(get_history_full, timeframe=self.TIMEFRAME, fresh_live_history_no_save_read=True,
                                             start=self.START, API=API_binance)

        all_pairs_history = list(map(get_history_full_with_args, ready_pair_list))
        for pair_history in all_pairs_history:
            pair_history["NATR"] = NATR(high=pair_history["high"], low=pair_history["low"], close=pair_history["close"], timeperiod=self.PERIOD)
            pair_history["regression_slope"] = pair_history["close"].rolling(self.PERIOD, self.PERIOD).apply(self.regression_slope)
            ranking = pd.concat([ranking, pair_history.tail(1)])

        ultimate_rank = ranking[["symbol", "NATR", "regression_slope"]].copy()
        ultimate_rank.sort_values("regression_slope", ascending=False, inplace=True)

        pd.options.display.precision = 2
        print(ultimate_rank)


if __name__ == "__main__":
    SentimentATR().get_history()
