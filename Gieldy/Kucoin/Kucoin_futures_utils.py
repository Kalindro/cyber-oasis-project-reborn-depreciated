from decimal import Decimal
from pandas import DataFrame as df
import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


def kucoin_futures_get_pair_prices(API):
    general_client = API["general_client"]

    prices_dataframe = df(general_client.get_contracts_list())
    prices_dataframe = prices_dataframe.apply(pd.to_numeric, errors="ignore")
    prices_dataframe.rename(columns={"symbol": "pair", "markPrice": "mark_price", "baseCurrency": "symbol", "quoteCurrency": "quote"}, inplace=True)
    prices_dataframe = prices_dataframe[prices_dataframe.quote.str.contains("USDT")]
    prices_dataframe = prices_dataframe.apply(pd.to_numeric, errors="ignore")
    prices_dataframe.set_index("symbol", inplace=True)

    return prices_dataframe


def kucoin_futures_get_balance(API):
    user_client = API["user_client"]

    balances_df = df(user_client.get_account_overview("USDT"), index=[0])
    balances_df.drop(["accountAlias", "updateTime"], axis=1, inplace=True)
    balances_df = balances_df.apply(pd.to_numeric, errors="ignore")
    balances_df.rename(columns={"accountEquity": "total", "availableBalance": "available"}, inplace=True)

    return balances_df


def kucoin_futures_positions(API):
    trade_client = API["trade_client"]

    precisions_dataframe = kucoin_futures_get_pairs_precisions_status(API)
    positions_df = df(trade_client.get_all_position())
    if len(positions_df) == 0: return positions_df
    positions_df.rename(columns={"settleCurrency": "quote", "symbol": "pair"}, inplace=True)
    positions_df = positions_df[positions_df.quote.str.contains("USDT")]
    positions_df["symbol"] = positions_df["pair"].str[:-5]
    positions_df["coins_lot_size"] = positions_df.apply(lambda row: precisions_dataframe.loc[positions_df["symbol"], "coins_lot_size"], axis=1).astype(float)
    positions_df["amount"] = positions_df.apply(lambda row: positions_df["coins_lot_size"] * row["currentQty"], axis=1).astype(float)
    positions_df = positions_df.apply(pd.to_numeric, errors="ignore")
    positions_df.set_index("symbol", inplace=True)

    return positions_df


def kucoin_futures_get_pairs_precisions_status(API):
    general_client = API["general_client"]

    precisions_dataframe = df(general_client.get_contracts_list())
    precisions_dataframe.rename(columns={"symbol": "pair", "markPrice": "mark_price", "baseCurrency": "symbol", "quoteCurrency": "quote",
                                    "multiplier": "coins_lot_size"}, inplace=True)

    precisions_dataframe = precisions_dataframe[precisions_dataframe.quote.str.contains("USDT")]
    precisions_dataframe = precisions_dataframe.apply(pd.to_numeric, errors="ignore")
    precisions_dataframe["min_order_amount"] = precisions_dataframe["coins_lot_size"]
    precisions_dataframe["min_order_value"] = precisions_dataframe.apply(lambda row: row["mark_price"] * row["min_order_amount"], axis=1)
    precisions_dataframe["amount_precision"] = precisions_dataframe.apply(lambda row: int(abs(Decimal(str(row["min_order_amount"])).normalize().as_tuple().exponent)), axis=1)
    precisions_dataframe["price_precision"] = precisions_dataframe.apply(lambda row: int(abs(Decimal(str(row["tickSize"])).normalize().as_tuple().exponent)), axis=1)
    precisions_dataframe = precisions_dataframe.apply(pd.to_numeric, errors="ignore")
    precisions_dataframe.set_index("symbol", inplace=True)

    return precisions_dataframe


def kucoin_futures_open_market_long(API, pair, amount, leverage):
    trade_client = API["trade_client"]

    trade_client.create_market_order(symbol=pair, side="buy", size=amount, lever=leverage)


def binance_futures_open_market_short(API, pair, amount, leverage):
    trade_client = API["trade_client"]

    trade_client.create_market_order(symbol=pair, side="sell", size=amount, lever=leverage)


def binance_futures_close_market_long(API, pair, leverage):
    trade_client = API["trade_client"]

    trade_client.create_market_order(symbol=pair, side="sell", lever=leverage, closeOrder=True)


def binance_futures_close_market_short(API, pair, leverage):
    trade_client = API["trade_client"]

    trade_client.create_market_order(symbol=pair, side="buy", lever=leverage, closeOrder=True)


def binance_futures_open_limit_long(API, pair, amount, price):
    general_client = API["general_client"]

    general_client.futures_create_order(symbol=pair, side="BUY", type="LIMIT", quantity=amount, price=price)


def kucoin_futures_change_margin_type(API, pair, cross=True):
    trade_client = API["trade_client"]

    trade_client.modify_auto_deposit_margin(symbol=pair, status=cross)


def kucoin_futures_margin_check(API, cross=True):
    kucoin_positions = kucoin_futures_positions(API)

    if (~kucoin_positions.autoDeposit).any():
        for _, row in kucoin_positions.iterrows():
            if not row["autoDeposit"]:
                print("Changing margin")
                kucoin_futures_change_margin_type(API=API, pair=row["pair"], cross=cross)
