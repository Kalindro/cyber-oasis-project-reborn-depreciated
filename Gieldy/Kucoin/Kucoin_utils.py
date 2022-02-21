import time
from math import fabs
from decimal import Decimal
from pandas import DataFrame as df
import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


def kucoin_REST_history_fragment(pair, timeframe, since, to, API):
    general_client = API["general_client"]

    pair = pair.replace("/", "-")

    if timeframe.upper() == "15MIN":
        timeframe = "15min"
    if timeframe.upper() == "30MIN":
        timeframe = "30min"
    if timeframe.upper() == "1H":
        timeframe = "1hour"
    if timeframe.upper() == "4H":
        timeframe = "4hour"
    if timeframe.upper() == "1D":
        timeframe = "1day"

    since = int(time.mktime(since.timetuple()))
    to = int(time.mktime(to.timetuple()))

    candles = general_client.get_kline_data(symbol=pair, kline_type=timeframe, start=since, end=to)

    history_dataframe_new = df()
    history_dataframe_new["date"] = [float(Candle[0]) * 1000 for Candle in candles]
    history_dataframe_new["open"] = [candle[1] for candle in candles]
    history_dataframe_new["high"] = [candle[3] for candle in candles]
    history_dataframe_new["low"] = [candle[4] for candle in candles]
    history_dataframe_new["close"] = [candle[2] for candle in candles]
    history_dataframe_new["volume"] = [candle[6] for candle in candles]

    return history_dataframe_new


def kucoin_cancel_pair_orders(pair, API):
    trade_client = API["trade_client"]

    pair = pair.replace("/", "-")

    canceled_pair_orders = trade_client.cancel_all_orders(symbol=pair)
    canceled_pair_orders_amount = len(canceled_pair_orders["cancelledOrderIds"])

    return canceled_pair_orders_amount


def kucoin_cancel_all_orders(API):
    trade_client = API["trade_client"]

    canceled_all_orders = trade_client.cancel_all_orders()

    print(f"Canceled all orders")
    return canceled_all_orders


def kucoin_fetch_order(order_ID, API):
    general_client = API["general_client"]

    order = general_client.get_order(order_id=str(order_ID))

    order_data = {"price": float(order["price"]),
                  "timestamp": int(order["createdAt"])
                  }

    return order_data


def kucoin_fetch_open_pair_orders(pair, API):
    general_client = API["general_client"]

    pair = pair.replace("/", "-")

    open_pair_orders = general_client.get_orders(symbol=pair)

    return open_pair_orders


def kucoin_get_pairs_precisions_status(API):
    general_client = API["general_client"]

    all_pairs = general_client.get_symbols()

    pair_dataframe = df()
    pair_dataframe[["pair", "symbol", "base", "status", "amount_precision", "price_precision", "min_order_amount",
                    "min_order_value"]] = [
        [pair_data["baseCurrency"].upper() + "/" + pair_data["quoteCurrency"],
         pair_data["baseCurrency"].upper(),
         pair_data["quoteCurrency"],
         pair_data["enableTrading"],
         round(fabs(Decimal(pair_data["baseIncrement"]).normalize().as_tuple().exponent)),
         round(fabs(Decimal(pair_data["priceIncrement"]).normalize().as_tuple().exponent)),
         float(pair_data["baseMinSize"]),
         float(pair_data["quoteMinSize"])] for pair_data in all_pairs]
    pair_dataframe.set_index("pair", inplace=True)

    return pair_dataframe


def kucoin_get_pairs_prices(API):
    general_client = API["general_client"]

    all_tickers = general_client.get_ticker()["ticker"]

    tickers_dataframe = df()
    tickers_dataframe[["pair", "price"]] = [[Ticker["symbol"].upper(), float(Ticker["sell"])] for Ticker in all_tickers if
                                            Ticker["symbol"].endswith(("BTC", "ETH", "USDT"))]
    for index, row in tickers_dataframe.iterrows():
        if row["pair"].endswith("USDT"):
            tickers_dataframe.loc[index, "pair"] = row["pair"][:-5] + "/USDT"
        if row["pair"].endswith("BTC"):
            tickers_dataframe.loc[index, "pair"] = row["pair"][:-4] + "/BTC"
        if row["pair"].endswith("ETH"):
            tickers_dataframe.loc[index, "pair"] = row["pair"][:-4] + "/ETH"
    tickers_dataframe.set_index("pair", inplace=True)

    return tickers_dataframe


def kucoin_get_balances(API):
    general_client = API["general_client"]

    all_balances = general_client.get_accounts()

    balances_dataframe = df()
    balances_dataframe[["symbol", "available", "frozen", "total"]] = [
        [Balance["currency"].upper(), float(Balance["available"]), float(Balance["holds"]), float(Balance["balance"])] for Balance in
        all_balances if Balance["type"] == "trade"]
    balances_dataframe.set_index("symbol", inplace=True)
    balances_dataframe.drop_duplicates(inplace=True)

    return balances_dataframe


def kucoin_create_buy_limit_order(pair, size, price, API):
    general_client = API["general_client"]

    pair = pair.replace("/", "-")

    order = general_client.create_limit_order(symbol=pair, size=size, price=price, side=general_client.SIDE_BUY)
    order_data = {"ID": order["orderId"]}

    return order_data


def kucoin_create_sell_limit_order(pair, size, price, API):
    general_client = API["general_client"]

    pair = pair.replace("/", "-")

    order = general_client.create_limit_order(symbol=pair, size=size, price=price, side=general_client.SIDE_SELL)
    order_data = {"ID": order["orderId"]}

    return order_data


def kucoin_create_sell_market_order(pair, size, API):
    general_client = API["general_client"]

    pair = pair.replace("/", "-")

    order = general_client.create_market_order(symbol=pair, size=size, side=general_client.SIDE_SELL)
    order_data = {"ID": order["orderId"]}

    return order_data


def kucoin_trading_enabled_bool(pair_precisions_status):
    trading_enabled = pair_precisions_status["status"]

    return trading_enabled
