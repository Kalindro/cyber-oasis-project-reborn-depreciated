import time
from math import fabs
from decimal import Decimal
from pandas import DataFrame as df
import pandas as pd
from binance.exceptions import BinanceAPIException

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


def binance_REST_history_fragment(pair, timeframe, since, to, API):
    general_client = API["general_client"]

    pair = pair.replace("/", "")

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

    since = int(time.mktime(since.timetuple()) * 1000)
    to = int(time.mktime(to.timetuple()) * 1000)

    candles = general_client.get_historical_klines(symbol=pair, interval=timeframe, start_str=since, end_str=to)

    history_dataframe_new = df()
    history_dataframe_new["date"] = [float(candle[0]) for candle in candles]
    history_dataframe_new["open"] = [candle[1] for candle in candles]
    history_dataframe_new["high"] = [candle[2] for candle in candles]
    history_dataframe_new["low"] = [candle[3] for candle in candles]
    history_dataframe_new["close"] = [candle[4] for candle in candles]
    history_dataframe_new["volume"] = [candle[7] for candle in candles]

    return history_dataframe_new


def binance_cancel_pair_orders(pair, API):
    try:
        general_client = API["general_client"]

        pair = pair.replace("/", "")

        canceled_pair_orders = general_client.cancel_orders(symbol=pair)
        canceled_pair_orders_amount = len([canceled_orders["orderId"] for canceled_orders in canceled_pair_orders])
        return canceled_pair_orders_amount

    except BinanceAPIException as err:
        print(err, "No orders to cancel")


def binance_cancel_all_orders(API):
    general_client = API["general_client"]

    pairs_to_cancel = []
    all_open_orders = general_client.get_open_orders()
    for pair_data in all_open_orders:
        pairs_to_cancel.append(pair_data["symbol"])
    pairs_to_cancel = list(set(pairs_to_cancel))
    for pair in pairs_to_cancel:
        print("Cancelling", pair)
        binance_cancel_pair_orders(pair=pair, API=API)

    print(f"Canceled all orders")


def binance_fetch_order(pair, order_ID, API):
    general_client = API["general_client"]

    pair = pair.replace("/", "")

    order = general_client.get_order(symbol=pair, orderId=order_ID)
    order_data = {"price": float(order["price"]),
                  "timestamp": int(order["updateTime"])
                  }

    return order_data


def binance_fetch_open_pair_orders(pair, API):
    general_client = API["general_client"]

    pair = pair.replace("/", "")

    open_pair_orders = general_client.get_open_orders(symbol=pair)

    return open_pair_orders


def binance_get_pairs_precisions_status(API):
    general_client = API["general_client"]

    all_pairs = general_client.get_exchange_info()["symbols"]

    pair_dataframe = df()
    pair_dataframe[["pair", "symbol", "base", "status", "amount_precision", "price_precision", "min_order_amount",
                    "min_order_value"]] = [
        [pair_data["baseAsset"] + "/" + pair_data["quoteAsset"],
         pair_data["baseAsset"],
         pair_data["quoteAsset"],
         pair_data["status"],
         round(min(float(pair_data["baseAssetPrecision"]),
                   fabs(Decimal(pair_data["filters"][2]["stepSize"]).normalize().as_tuple().exponent))) if "stepSize" in
                                                                                                           pair_data["filters"][2] else 0,
         round(min(fabs(Decimal(pair_data["filters"][0]["minPrice"]).normalize().as_tuple().exponent),
                   fabs(Decimal(pair_data["filters"][0]["tickSize"]).normalize().as_tuple().exponent))),
         float(pair_data["filters"][2]["minQty"]) if "minQty" in pair_data["filters"][2] else 0,
         float(pair_data["filters"][3]["minNotional"]) if "minNotional" in pair_data["filters"][3] else 0] for pair_data in all_pairs]
    pair_dataframe.set_index("pair", inplace=True)

    return pair_dataframe


def binance_get_pairs_prices(API):
    general_client = API["general_client"]

    all_tickers = general_client.get_ticker()

    tickers_dataframe = df()
    tickers_dataframe[["pair", "price"]] = [[ticker["symbol"],
                                             float(ticker["lastPrice"])] for ticker in all_tickers if
                                            ticker["symbol"].endswith(("BTC", "ETH", "USDT"))]
    for index, row in tickers_dataframe.iterrows():
        if row["pair"].endswith("USDT"):
            tickers_dataframe.loc[index, "pair"] = row["pair"][:-4] + "/USDT"
        if row["pair"].endswith("BTC"):
            tickers_dataframe.loc[index, "pair"] = row["pair"][:-3] + "/BTC"
        if row["pair"].endswith("ETH"):
            tickers_dataframe.loc[index, "pair"] = row["pair"][:-3] + "/ETH"
    tickers_dataframe.set_index("pair", inplace=True)

    return tickers_dataframe


def binance_get_balances(API):
    general_client = API["general_client"]

    all_balances = general_client.get_account()["balances"]

    balances_dataframe = df()
    balances_dataframe[["symbol", "available", "frozen", "total"]] = [
        [Balance["asset"], float(Balance["free"]), float(Balance["locked"]), float(Balance["free"]) + float(Balance["locked"])] for Balance
        in all_balances]
    balances_dataframe.set_index("symbol", inplace=True)

    return balances_dataframe


def binance_create_buy_limit_order(pair, size, price, API):
    general_client = API["general_client"]

    pair = pair.replace("/", "")

    order = general_client.order_limit_buy(symbol=pair, quantity=size, price=price)
    order_data = {"ID": order["orderId"]}

    return order_data


def binance_create_sell_limit_order(pair, size, price, API):
    general_client = API["general_client"]

    pair = pair.replace("/", "")

    order = general_client.order_limit_sell(symbol=pair, quantity=size, price=price)
    order_data = {"ID": order["orderId"]}

    return order_data


def binance_create_sell_market_order(pair, size, API):
    general_client = API["general_client"]

    pair = pair.replace("/", "")

    order = general_client.order_market_sell(symbol=pair, quantity=size)
    order_data = {"ID": order["orderId"]}

    return order_data


def binance_trading_enabled_bool(pair_precisions_status):
    trading_enabled = True if "TRADING" in pair_precisions_status["status"].upper() else False

    return trading_enabled


def binance_get_futures_pair_prices_rates(API):
    general_client = API["general_client"]

    prices_dataframe = df(general_client.futures_mark_price())
    prices_dataframe = prices_dataframe.apply(pd.to_numeric, errors="ignore")
    prices_dataframe.rename(columns={"symbol": "pair", "markPrice": "mark_price", "lastFundingRate": "funding_rate"}, inplace=True)
    col = prices_dataframe["pair"].str[:-4]
    prices_dataframe.insert(1, "symbol", col)
    prices_dataframe["quote"] = prices_dataframe["pair"].str[-4:]
    prices_dataframe = prices_dataframe[prices_dataframe.quote.str.contains("USDT")]
    prices_dataframe["pair"] = prices_dataframe["symbol"] + "/" + prices_dataframe["quote"]
    prices_dataframe.pop("quote")
    prices_dataframe["funding_rate_APR"] = (prices_dataframe["funding_rate"] * 3 * 365.25).round(2)
    prices_dataframe.set_index("symbol", inplace=True)

    return prices_dataframe


def binance_get_futures_balance(API):
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
    positions_df["pair"] = positions_df["symbol"]
    positions_df["symbol"] = positions_df["symbol"].str[:-4]
    positions_df.set_index("symbol", inplace=True)
    positions_df.drop(["updateTime", "positionSide", "bidNotional", "askNotional", "openOrderInitialMargin", "maxNotional"], axis=1, inplace=True)

    return positions_df


def binance_futures_open_market_long(API):
    general_client = API["general_client"]

    general_client.futures_create_order(symbol=None, side="BUY", type="MARKET", quantity=None, price=None, close_position=False)


def binance_futures_open_limit_long(API):
    general_client = API["general_client"]

    general_client.futures_create_order(symbol=None, side="BUY", type="LIMIT", quantity=None, price=None, close_position=False)


def binance_futures_open_market_short(API):
    general_client = API["general_client"]

    general_client.futures_create_order(symbol=None, side="SELL", type="MARKET", quantity=None, price=None, close_position=False)


def binance_futures_open_limit_short(API):
    general_client = API["general_client"]

    general_client.futures_create_order(symbol=None, side="SELL", type="LIMIT", quantity=None, price=None, close_position=False)


def binance_futures_close_market_long(API):
    general_client = API["general_client"]

    general_client.futures_create_order(symbol=None, side="SELL", type="MARKET", quantity=None, price=None, close_position=False)


def binance_futures_close_limit_long(API):
    general_client = API["general_client"]

    general_client.futures_create_order(symbol=None, side="SELL", type="LIMIT", quantity=None, price=None, close_position=True)


def binance_futures_close_market_short(API):
    general_client = API["general_client"]

    general_client.futures_create_order(symbol=None, side="BUY", type="MARKET", quantity=None, price=None, close_position=False)


def binance_futures_close_limit_short(API):
    general_client = API["general_client"]

    general_client.futures_create_order(symbol=None, side="BUY", type="LIMIT", quantity=None, price=None, close_position=True)





def binance_futures_change_leverage(API, pair, leverage):
    general_client = API["general_client"]

    general_client.futures_change_leverage(symbol=pair, leverage=leverage)


def binance_futures_change_marin_type(API, pair, type):
    """  "ISOLATED" or "CROSSED" """

    general_client = API["general_client"]

    general_client.futures_change_margin_type(symbol=pair, marginType=type)

