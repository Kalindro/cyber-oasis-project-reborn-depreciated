from pandas import DataFrame as df


def get_exchange_timestamp(API):
    """ Get exchange time for timezone setting"""
    general_client = API["general_client"]

    exchange_status = general_client.fetch_status()
    print(exchange_status["updated"])


def get_history_fragment_CCXT_REST_for_func(pair, timeframe, since, API):
    """ Get fragment of history, only used for other bigger function. """
    general_client = API["general_client"]
    candle_limit = 800

    candles = general_client.fetchOHLCV(symbol=pair, timeframe=timeframe,
                                        since=since, limit=candle_limit)

    columns_ordered = ["date", "open", "high", "low", "close", "volume"]
    history_dataframe_new = df(candles, columns=columns_ordered)

    return history_dataframe_new


def get_pairs_precisions_status(API):
    """ Get exchange pairs with trading precisions and active status. """
    general_client = API["general_client"]

    pairs_precisions = general_client.fetch_markets()
    pairs_precisions_df = df(pairs_precisions, columns=["symbol", "base", "quote", "active", "precision", "limits"])
    pairs_precisions_df.set_index("symbol", inplace=True)

    return pairs_precisions_df


def get_pairs_prices(API):
    """ Get exchange pairs with current prices. """
    general_client = API["general_client"]

    raw_pairs = general_client.fetch_tickers()
    pairs_prices_df = df.from_dict(raw_pairs, orient="index", columns=["average"])
    pairs_prices_df.rename(columns={"average": "price"}, inplace=True)

    return pairs_prices_df
