import time
import datetime as dt
from pandas import DataFrame as df


def get_history_fragment_CCXT_REST_for_func(pair, timeframe, since, API):
    general_client = API["general_client"]
    candle_limit = 800

    candles = general_client.fetchOHLCV(symbol=pair, timeframe=timeframe,
                                        since=since, limit=candle_limit)

    columns_ordered = ["date", "open", "high", "low", "close", "volume"]
    history_dataframe_new = df(candles, columns=columns_ordered)

    return history_dataframe_new
