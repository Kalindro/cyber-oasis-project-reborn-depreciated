from Gieldy.Binance.Manual_initiation.API_initiation_manual_Binance_Spot_Futures_USDT import API_initiation
from Gieldy.Refractor_general.Get_history import get_history_full
from datetime import timedelta
from talib import NATR, ROCP
from pandas import DataFrame as df
from functools import partial
import pandas as pd
import datetime as dt


class SentimentATR:
    def __init__(self):
        self.PAIR_LIST = ["LUNA", "SOL", "AVAX", "FLOW", "NEAR", "WAVES", "APE"]
        self.TIMEFRAME = "1D"
        self.PERIOD = 14
        self.START = dt.datetime.now() - timedelta(days=self.PERIOD + 14)

    def get_history(self):
        API_binance = API_initiation()
        ready_pair_list = [symbol + "/USDT" for symbol in self.PAIR_LIST]

        fresh_ranking = df()
        get_history_full_with_args = partial(get_history_full, timeframe=self.TIMEFRAME, fresh_live_history_no_save_read=True,
                                             start=self.START, API=API_binance)

        all_pairs_history = list(map(get_history_full_with_args, ready_pair_list))
        for pair_history in all_pairs_history:
            pair_history["NATR"] = NATR(high=pair_history["high"], low=pair_history["low"], close=pair_history["close"], timeperiod=self.PERIOD)
            pair_history["NATRR"] = 1 / pair_history["NATR"]
            pair_history["ROCP"] = ROCP(pair_history["close"], timeperiod=2) * 100
            fresh_ranking = pd.concat([fresh_ranking, pair_history.tail(1)])

        NATR_avg = fresh_ranking["NATRR"].mean()

        ready_rank = df()
        for pair_history in all_pairs_history:
            pair_history["weight"] = pair_history["NATRR"] / NATR_avg
            pair_history["ROCP_adj"] = pair_history["ROCP"] * pair_history["weight"]
            ready_rank = pd.concat([ready_rank, pair_history.tail(1)])

        ultimate_rank = ready_rank[["symbol", "NATR", "ROCP_adj"]].copy()
        ultimate_rank.sort_values("ROCP_adj", inplace=True)

        pd.options.display.precision = 2
        print(ultimate_rank)


if __name__ == "__main__":
    SentimentATR().get_history()
