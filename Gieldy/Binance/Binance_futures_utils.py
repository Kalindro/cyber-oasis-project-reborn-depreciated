from decimal import Decimal
from pandas import DataFrame as df
import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


def binance_futures_get_pair_prices(API):
    general_client = API["general_client"]

    prices_dataframe = df(general_client.futures_mark_price())
    prices_dataframe = prices_dataframe.apply(pd.to_numeric, errors="ignore")
    prices_dataframe.rename(columns={"symbol": "pair", "markPrice": "mark_price"}, inplace=True)
    col = prices_dataframe["pair"].str[:-4]
    prices_dataframe.insert(1, "symbol", col)
    prices_dataframe["quote"] = prices_dataframe["pair"].str[-4:]
    prices_dataframe = prices_dataframe[prices_dataframe.quote.str.contains("USDT")]
    prices_dataframe.pop("quote")
    prices_dataframe.set_index("symbol", inplace=True)

    return prices_dataframe


def binance_futures_get_balance(API):
    general_client = API["general_client"]

    balances_df = df(general_client.futures_account_balance())
    balances_df.drop(["accountAlias", "updateTime"], axis=1, inplace=True)
    balances_df.rename(columns={"asset": "symbol", "balance": "total", "withdrawAvailable": "available"}, inplace=True)
    balances_df.set_index("symbol", inplace=True)

    return balances_df


def binance_futures_positions(API):
    general_client = API["general_client"]

    positions_df = df(general_client.futures_account()["positions"])
    positions_df = positions_df[positions_df.symbol.str.contains("USDT")]
    positions_df = positions_df.apply(pd.to_numeric, errors="ignore")
    positions_df["pair"] = positions_df["symbol"]
    positions_df["symbol"] = positions_df["symbol"].str[:-4]
    positions_df.set_index("symbol", inplace=True)
    positions_df.drop(["updateTime", "positionSide", "bidNotional", "askNotional", "openOrderInitialMargin", "maxNotional"], axis=1, inplace=True)

    return positions_df


def binance_futures_get_pairs_precisions_status(API):
    general_client = API["general_client"]

    pairs_dataframe = df(general_client.futures_exchange_info()["symbols"])
    pairs_dataframe.rename(columns={"baseCurrency": "symbol", "quoteAsset": "quote", "quantityPrecision": "amount_precision",
                                    "pricePrecision": "price_precision"}, inplace=True)

    pairs_dataframe["symbol"] = pairs_dataframe["symbol"].str[:-4]

    pairs_dataframe = pairs_dataframe[pairs_dataframe.quote.str.contains("USDT")]
    pairs_dataframe["min_order_amount"] = pairs_dataframe.apply(lambda row: row["filters"][2]["minQty"], axis=1)
    pairs_dataframe["min_order_value"] = pairs_dataframe.apply(lambda row: row["filters"][5]["notional"], axis=1)
    pairs_dataframe.set_index("symbol", inplace=True)

    return pairs_dataframe


def binance_futures_open_market_long(API, pair, amount):
    general_client = API["general_client"]

    general_client.futures_create_order(symbol=pair, side="BUY", type="MARKET", quantity=amount)


def binance_futures_open_market_short(API, pair, amount):
    general_client = API["general_client"]

    general_client.futures_create_order(symbol=pair, side="SELL", type="MARKET", quantity=amount)


def binance_futures_close_market_long(API, pair, amount):
    general_client = API["general_client"]

    general_client.futures_create_order(symbol=pair, side="SELL", type="MARKET", quantity=amount, reduceOnly=True)


def binance_futures_close_market_short(API, pair, amount):
    general_client = API["general_client"]

    general_client.futures_create_order(symbol=pair, side="BUY", type="MARKET", quantity=amount, reduceOnly=True)


def binance_futures_open_limit_long(API, pair, amount, price):
    general_client = API["general_client"]

    general_client.futures_create_order(symbol=pair, side="BUY", type="LIMIT", quantity=amount, price=price)


def binance_futures_open_limit_short(API, pair, amount, price):
    general_client = API["general_client"]

    general_client.futures_create_order(symbol=pair, side="SELL", type="LIMIT", quantity=amount, price=price)


def binance_futures_close_limit_long(API, pair, amount, price):
    general_client = API["general_client"]

    general_client.futures_create_order(symbol=pair, side="SELL", type="LIMIT", quantity=amount, price=price, reduceOnly=True)


def binance_futures_close_limit_short(API, pair, amount, price):
    general_client = API["general_client"]

    general_client.futures_create_order(symbol=pair, side="BUY", type="LIMIT", quantity=amount, price=price, reduceOnly=True)


def binance_futures_change_leverage(API, pair, leverage):
    general_client = API["general_client"]

    general_client.futures_change_leverage(symbol=pair, leverage=leverage)


def binance_futures_change_marin_type(API, pair, type):
    """  "ISOLATED" or "CROSSED" """

    general_client = API["general_client"]

    general_client.futures_change_margin_type(symbol=pair, marginType=type)


def binance_futures_margin_leverage_check(API, leverage, cross=True):
    margin = "CROSSED" if cross else "ISOLATED"
    binance_positions = binance_futures_positions(API)

    if (~binance_positions.leverage.isin([str(leverage)])).any():
        for _, row in binance_positions.iterrows():
            if row["leverage"] != leverage:
                print("Changing leverage")
                binance_futures_change_leverage(API=API, pair=row["pair"], leverage=leverage)

    if binance_positions.isolated.any():
        for _, row in binance_positions.iterrows():
            if row["isolated"]:
                print("Changing margin type")
                binance_futures_change_marin_type(API=API, pair=row["pair"], type=margin)
