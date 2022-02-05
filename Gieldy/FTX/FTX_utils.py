import time
from pandas import DataFrame as df
import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


def FTX_REST_history_fragment(pair, timeframe, since, to, API):
    general_client = API["general_client"]

    pair = pair.replace("/", "_")

    if timeframe.upper() == "15MIN":
        timeframe = "900"
    if timeframe.upper() == "1H":
        timeframe = "3_600"
    if timeframe.upper() == "4H":
        timeframe = "14_400"
    if timeframe.upper() == "1D":
        timeframe = "86_400"

    since = int(time.mktime(since.timetuple()))
    to = int(time.mktime(to.timetuple()))

    candles = general_client.get_historical_data(market_name=pair, resolution=timeframe, start_time=since, end_time=to, limit=1000)

    history_dataframe_new = df()
    history_dataframe_new["date"] = [float(candle["time"]) for candle in candles]
    history_dataframe_new["open"] = [candle["open"] for candle in candles]
    history_dataframe_new["high"] = [candle["high"] for candle in candles]
    history_dataframe_new["low"] = [candle["low"] for candle in candles]
    history_dataframe_new["close"] = [candle["close"] for candle in candles]
    history_dataframe_new["volume"] = [candle["volume"] for candle in candles]

    return history_dataframe_new
