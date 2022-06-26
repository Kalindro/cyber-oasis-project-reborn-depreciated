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


def get_pairs_precisions_status(API):
    general_client = API["general_client"]

    tickers = df(general_client.fetch_markets())


def get_pairs_prices(API):
    general_client = API["general_client"]

    raw_pairs = general_client.fetch_tickers()
    pairs_prices_df = df.from_dict(raw_pairs, orient="index", columns=["average"])
    pairs_prices_df.rename(columns={"average": "price"}, inplace=True)
    print(pairs_prices_df)
