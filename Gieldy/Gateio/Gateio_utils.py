import time
from pandas import DataFrame as df
import pandas as pd
import gate_api

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


def gateio_REST_history_fragment(pair, timeframe, since, to, API):
    general_client = API["general_client"]

    pair = pair.replace("/", "_")

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

    since = int(time.mktime(since.timetuple()))
    to = int(time.mktime(to.timetuple()))

    candles = general_client.list_candlesticks(pair, interval=timeframe, _from=since, to=to)

    history_dataframe_new = df()
    history_dataframe_new["date"] = [float(candle[0]) * 1000 for candle in candles]
    history_dataframe_new["open"] = [candle[2] for candle in candles]
    history_dataframe_new["high"] = [candle[3] for candle in candles]
    history_dataframe_new["low"] = [candle[4] for candle in candles]
    history_dataframe_new["close"] = [candle[5] for candle in candles]
    history_dataframe_new["volume"] = [candle[1] for candle in candles]

    return history_dataframe_new


def gateio_cancel_pair_orders(pair, API):
    trade_client = API["trade_client"]

    pair = pair.replace("/", "_")

    canceled_pair_orders = trade_client.cancel_orders(currency_pair=pair)
    canceled_pair_orders_amount = len([canceled_orders.id for canceled_orders in canceled_pair_orders])

    return canceled_pair_orders_amount


def gateio_cancel_all_orders(API):
    trade_client = API["trade_client"]

    pairs_to_cancel = []
    all_open_orders = trade_client.list_all_open_orders()
    while len(all_open_orders) > 0:
        for Pair_data in all_open_orders:
            if Pair_data.currency_pair not in pairs_to_cancel:
                pairs_to_cancel.append(Pair_data.currency_pair)
        for pair in pairs_to_cancel:
            print("Cancelling", pair)
            gateio_cancel_pair_orders(pair=pair, API=API)
        all_open_orders = trade_client.list_all_open_orders()


def gateio_fetch_order(pair, order_ID, API):
    trade_client = API["trade_client"]

    pair = pair.replace("/", "_")

    order = trade_client.get_order(currency_pair=pair, order_id=order_ID)
    order_data = {"price": float(order.price),
                  "timestamp": int(order.update_time) * 1000
                  }

    return order_data


def gateio_fetch_open_pair_orders(pair, API):
    trade_client = API["trade_client"]

    pair = pair.replace("/", "_")
    open_pair_orders = trade_client.Trade_client(currency_pair=pair, status="open")

    return open_pair_orders


def gateio_get_pairs_precisions_status(API):
    general_client = API["general_client"]

    all_pairs = general_client.list_currency_pairs()

    pair_dataframe = df()
    pair_dataframe[["pair", "symbol", "base", "status", "amount_precision", "price_precision", "min_order_amount",
                    "min_order_value"]] = [
        [pair_data.base + "/" + pair_data.quote,
         pair_data.base,
         pair_data.quote,
         pair_data.trade_status,
         pair_data.amount_precision,
         pair_data.precision,
         float(pair_data.min_base_amount if (pair_data.min_base_amount is not None) else 0),
         float(pair_data.min_quote_amount if (pair_data.min_quote_amount is not None) else 0)] for pair_data in all_pairs]
    pair_dataframe.set_index("pair", inplace=True)

    return pair_dataframe


def gateio_get_pairs_prices(API):
    general_client = API["general_client"]

    all_tickers = general_client.list_tickers()

    tickers_dataframe = df()
    tickers_dataframe[["pair", "price"]] = [[ticker.currency_pair, float(ticker.last)] for ticker in all_tickers if
                                            ticker.currency_pair.endswith(("BTC", "ETH", "USDT"))]
    for index, row in tickers_dataframe.iterrows():
        if row["pair"].endswith("USDT"):
            tickers_dataframe.loc[index, "pair"] = row["pair"][:-5] + "/USDT"
        if row["pair"].endswith("BTC"):
            tickers_dataframe.loc[index, "pair"] = row["pair"][:-4] + "/BTC"
        if row["pair"].endswith("ETH"):
            tickers_dataframe.loc[index, "pair"] = row["pair"][:-4] + "/ETH"
    tickers_dataframe.set_index("pair", inplace=True)

    return tickers_dataframe


def gateio_get_balances(API):
    trade_client = API["trade_client"]

    all_balances = trade_client.list_spot_accounts()

    balances_dataframe = df()
    balances_dataframe[["symbol", "available", "frozen", "total"]] = [
        [balance.currency, float(balance.available), float(balance.locked), float(balance.available) + float(balance.locked)] for balance in
        all_balances]
    balances_dataframe.set_index("symbol", inplace=True)

    return balances_dataframe


def gateio_create_buy_limit_order(pair, size, price, API):
    trade_client = API["trade_client"]

    pair = pair.replace("/", "_")

    order_spec = gate_api.Order(currency_pair=pair, amount=size, price=price, side="buy")
    order = trade_client.create_order(order_spec)
    order_data = {"ID": order.id}

    return order_data


def gateio_create_sell_limit_order(pair, size, price, API):
    trade_client = API["trade_client"]

    pair = pair.replace("/", "_")

    order_spec = gate_api.Order(currency_pair=pair, amount=size, price=price, side="sell")
    order = trade_client.create_order(order_spec)
    order_data = {"ID": order.id}

    return order_data


def gateio_trading_enabled_bool(pair_precisions_status):
    trading_enabled = True if "TRADABLE" in pair_precisions_status["status"].upper() else False

    return trading_enabled
