import logging

import pandas as pd
from pandas import DataFrame as df

from gieldy.general.log_config import configure_logging

logger3 = configure_logging(logging.WARNING)


def get_exchange_timestamp(API: dict) -> str:
    """Get exchange time for timezone setting"""
    exchange_client = API["client"]
    exchange_status = exchange_client.fetch_status()
    exchange_timestamp = exchange_status["updated"]
    return exchange_timestamp


def get_pairs_precisions_status(API: dict) -> pd.DataFrame:
    """Get exchange pairs with trading precisions and active status"""
    logger3.info("Getting pairs precisions status...")
    exchange_client = API["client"]
    pairs_precisions_status_df = exchange_client.fetch_markets()
    pairs_precisions_status_df = df(pairs_precisions_status_df,
                                    columns=["symbol", "base", "quote", "active", "precision", "limits"])
    pairs_precisions_status_df = pairs_precisions_status_df.astype({"active": str})
    pairs_precisions_status_df.set_index("symbol", inplace=True)
    logger3.info("Pairs precisions status completed, returning")
    return pairs_precisions_status_df


def get_pairs_prices(API: dict) -> pd.DataFrame:
    """Get exchange pairs with current prices"""
    logger3.info("Getting pairs prices...")
    exchange_client = API["client"]
    raw_pairs = exchange_client.fetch_tickers()
    pairs_prices_df = df.from_dict(raw_pairs, orient="index", columns=["average"])
    pairs_prices_df.rename(columns={"average": "price"}, inplace=True)
    logger3.info("Pairs prices completed, returning")
    return pairs_prices_df
