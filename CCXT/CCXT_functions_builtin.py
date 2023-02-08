import pandas as pd
from loguru import logger
from pandas import DataFrame as df

def get_exchange_timestamp(API: dict) -> str:
    """Get exchange time for timezone setting"""
    exchange_client = API["client"]
    exchange_status = exchange_client.fetch_status()
    exchange_timestamp = exchange_status["updated"]
    return exchange_timestamp


def get_pairs_with_precisions_status(API: dict) -> pd.DataFrame:
    """Get exchange pairs with trading precisions and active status"""
    logger.info("Getting pairs with precisions and status...")
    exchange_client = API["client"]
    pairs_precisions_status_df = exchange_client.fetch_markets()
    pairs_precisions_status_df = df(pairs_precisions_status_df,
                                    columns=["symbol", "base", "quote", "active", "precision", "limits"])
    pairs_precisions_status_df = pairs_precisions_status_df.astype({"active": str})
    pairs_precisions_status_df.set_index("symbol", inplace=True)
    logger.debug("Pairs with precisions and status completed, returning")
    return pairs_precisions_status_df


def get_pairs_prices(API: dict) -> pd.DataFrame:
    """Get exchange pairs with current prices"""
    logger.info("Getting pairs prices...")
    exchange_client = API["client"]
    raw_pairs = exchange_client.fetch_tickers()
    pairs_prices_df = df.from_dict(raw_pairs, orient="index", columns=["average"])
    pairs_prices_df.rename(columns={"average": "price"}, inplace=True)
    logger.debug("Pairs prices completed, returning")
    return pairs_prices_df


def change_leverage_and_mode_on_all_pairs_on_list(leverage: int, pairs_list: list, API: dict) -> None:
    exchange_client = API["client"]
    for pair in pairs_list:
        logger.info(f"Changing leverage and margin for {pair}")
        if "bybit" in API["name"].lower():
            try:
                exchange_client.set_margin_mode(marginMode="ISOLATED", symbol=pair, params={"leverage": leverage})
            except Exception as err:
                if "not modified" in str(err):
                    pass
                else:
                    print(err)
        else:
            exchange_client.set_leverage(leverage=leverage, symbol=pair)
            exchange_client.set_margin_mode(marginMode="ISOLATED", symbol=pair)
        logger.info(f"{pair} leverage changed to {leverage}, margin mode to isolated")
