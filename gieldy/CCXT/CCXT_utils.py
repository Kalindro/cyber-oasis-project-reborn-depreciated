import ccxt
import ciso8601
import datetime
from pandas import DataFrame as df


def CCXT_REST_history_fragment(pair, timeframe, since, API):
    general_client = API["general_client"]

    if timeframe.upper() == "15MIN":
        timeframe = "15m"
    if timeframe.upper() == "30MIN":
        timeframe = "30m"
    if timeframe.upper() == "1H":
        timeframe = "1h"
    if timeframe.upper() == "4H":
        timeframe = "4h"
    if timeframe.upper() == "1D":
        timeframe = "1d"
    if timeframe.upper() == "3D":
        timeframe = "3d"

    since = ciso8601.parse_datetime(since)

    candles = general_client.fetchOHLCV(symbol=pair, timeframe=timeframe, since=since)
    print(candles)
    history_dataframe_new = df()
    history_dataframe_new["date"] = [float(candle[0]) for candle in candles]
    history_dataframe_new["open"] = [candle[1] for candle in candles]
    history_dataframe_new["high"] = [candle[2] for candle in candles]
    history_dataframe_new["low"] = [candle[3] for candle in candles]
    history_dataframe_new["close"] = [candle[4] for candle in candles]
    history_dataframe_new["volume"] = [candle[7] for candle in candles]

    return history_dataframe_new
