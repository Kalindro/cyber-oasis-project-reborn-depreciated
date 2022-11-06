from pandas import DataFrame as df


def get_exchange_timestamp(API):
    """Get exchange time for timezone setting"""
    general_client = API["general_client"]

    exchange_status = general_client.fetch_status()
    exchange_timestamp = exchange_status["updated"]

    return exchange_timestamp


def get_pairs_precisions_status(API):
    """Get exchange pairs with trading precisions and active status"""
    general_client = API["general_client"]

    pairs_precisions = general_client.fetch_markets()
    pairs_precisions_df = df(pairs_precisions,
                             columns=["symbol", "base", "quote", "active", "precision", "limits"])
    pairs_precisions_df.set_index("symbol", inplace=True)

    return pairs_precisions_df


def get_pairs_prices(API):
    """Get exchange pairs with current prices"""
    general_client = API["general_client"]

    raw_pairs = general_client.fetch_tickers()
    pairs_prices_df = df.from_dict(raw_pairs, orient="index", columns=["average"])
    pairs_prices_df.rename(columns={"average": "price"}, inplace=True)

    return pairs_prices_df
