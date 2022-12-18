import pandas as pd
from pandas import DataFrame as df


def get_history_one_fragment(pair: str, timeframe: str, since: int, API: dict) -> pd.DataFrame:
    """Private, get fragment of history"""
    exchange_client = API["client_uninvoked"]()
    candle_limit = 800
    candles_list = exchange_client.fetchOHLCV(symbol=pair, timeframe=timeframe, since=since, limit=candle_limit)
    columns_ordered = ["date", "open", "high", "low", "close", "volume"]
    history_dataframe_new = df(candles_list, columns=columns_ordered)
    return history_dataframe_new


def get_exchange_timestamp(API):
    """Get exchange time for timezone setting"""
    exchange_client = API["client_uninvoked"]()
    exchange_status = exchange_client.fetch_status()
    exchange_timestamp = exchange_status["updated"]
    return exchange_timestamp


def get_pairs_precisions_status(API):
    """Get exchange pairs with trading precisions and active status"""
    exchange_client = API["client_uninvoked"]()
    pairs_precisions_status = exchange_client.fetch_markets()
    pairs_precisions_status = df(pairs_precisions_status,
                                 columns=["symbol", "base", "quote", "active", "precision", "limits"])
    pairs_precisions_status = pairs_precisions_status.astype({"active": str})
    pairs_precisions_status.set_index("symbol", inplace=True)
    return pairs_precisions_status


def get_pairs_prices(API):
    """Get exchange pairs with current prices"""
    exchange_client = API["client_uninvoked"]()
    raw_pairs = exchange_client.fetch_tickers()
    pairs_prices_df = df.from_dict(raw_pairs, orient="index", columns=["average"])
    pairs_prices_df.rename(columns={"average": "price"}, inplace=True)
    return pairs_prices_df
