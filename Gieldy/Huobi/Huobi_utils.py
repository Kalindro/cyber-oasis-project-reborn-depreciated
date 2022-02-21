import time, gzip, json, pprint, threading, websocket

from pandas import DataFrame as df
import pandas as pd

from huobi.constant import *

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


def huobi_REST_history_fragment(pair, timeframe, API):
    market_client = API["market_client"]

    pair = pair.replace("/", "")
    pair = pair.lower()

    if timeframe.upper() == "15MIN":
        timeframe = "15min"
    if timeframe.upper() == "1H":
        timeframe = "60min"
    if timeframe.upper() == "1D":
        timeframe = "1day"

    candles = market_client.get_candlestick(symbol=pair, period=timeframe, size=1500)

    history_dataframe_new = df()
    history_dataframe_new["date"] = [float(candle.id) * 1000 for candle in candles]
    history_dataframe_new["open"] = [candle.open for candle in candles]
    history_dataframe_new["high"] = [candle.high for candle in candles]
    history_dataframe_new["low"] = [candle.low for candle in candles]
    history_dataframe_new["close"] = [candle.close for candle in candles]
    history_dataframe_new["volume"] = [candle.vol for candle in candles]

    return history_dataframe_new


def huobi_websocket_history_fragment(pair, timeframe, since, to):
    printing = False

    pair = pair.replace("/", "")
    pair = pair.lower()

    if timeframe.upper() == "1H":
        timeframe = "60min"
    elif timeframe.upper() == "4H":
        timeframe = "4hour"
    elif timeframe.upper() == "1D":
        timeframe = "1day"

    since_fixed = int(time.mktime(since.timetuple()))
    to_fixed = int(time.mktime(to.timetuple()))

    history_dataframe_new = df()

    def get_current_timestamp():
        return int(round(time.time() * 1000))

    def send_message(ws, message_dict):
        data = json.dumps(message_dict).encode()
        if printing:
            print("Sending Message:")
            pprint.pprint(message_dict)
        ws.send(data)

    def on_message(ws, message):
        unzipped_data = gzip.decompress(message).decode()
        msg_dict = json.loads(unzipped_data)
        if printing:
            print("Received Message: ")
            pprint.pprint(msg_dict)

        candles = msg_dict["data"]
        history_dataframe_new["date"] = [float(candle["id"]) * 1000 for candle in candles]
        history_dataframe_new["open"] = [candle["open"] for candle in candles]
        history_dataframe_new["high"] = [candle["high"] for candle in candles]
        history_dataframe_new["low"] = [candle["low"] for candle in candles]
        history_dataframe_new["close"] = [candle["close"] for candle in candles]
        history_dataframe_new["volume"] = [candle["amount"] for candle in candles]
        time.sleep(0.1)
        ws.close()

        return history_dataframe_new

    def on_error(ws, error):
        print("Error: " + str(error))
        error = gzip.decompress(error).decode()
        print(error)

    def on_close(ws):
        if printing:
            print("### Closed ###")

    def on_open(ws):
        def run(*args):
            data = {
                "req": "market." + pair + ".kline." + timeframe,
                "id": str(get_current_timestamp()),
                "from": int(since_fixed),
                "to": int(to_fixed),
            }

            send_message(ws, data)

        t = threading.Thread(target=run, args=())
        t.start()

    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(
        "wss://api-aws.huobi.pro/ws",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    ws.run_forever()

    return history_dataframe_new


def huobi_cancel_pair_orders(pair, API):
    trade_client = API["trade_client"]
    account_ID = API["account_ID"]

    pair = pair.replace("/", "")
    pair = pair.lower()

    canceled_pair_orders = trade_client.cancel_open_orders(symbols=pair, account_id=account_ID)
    canceled_pair_orders_amount = canceled_pair_orders.success_count

    return canceled_pair_orders_amount


def huobi_cancel_all_orders(API):
    trade_client = API["trade_client"]
    account_ID = API["account_ID"]

    canceled_all_orders = trade_client.cancel_open_orders(account_id=account_ID)
    while int(canceled_all_orders.success_count) > 0:
        canceled_all_orders = trade_client.cancel_open_orders(account_id=account_ID)

    print(f"Canceled all orders")
    return canceled_all_orders


def huobi_fetch_order(order_ID, API):
    trade_client = API["trade_client"]

    order = trade_client.get_order(order_id=order_ID)
    order_data = {"price": float(order.price),
                  "timestamp": int(order.finished_at)
                  }

    return order_data


def huobi_fetch_open_pair_orders(pair, API):
    trade_client = API["trade_client"]
    account_ID = API["account_ID"]

    pair = pair.replace("/", "")
    pair = pair.lower()

    open_pair_orders = trade_client.get_open_orders(symbol=pair, account_id=account_ID)

    return open_pair_orders


def huobi_get_pairs_precisions_status(API):
    generic_client = API["generic_client"]

    all_pairs = generic_client.get_exchange_symbols()

    pair_dataframe = df()
    pair_dataframe[["pair", "symbol", "base", "status", "amount_precision", "price_precision", "min_order_amount", "min_order_value"]] = [
        [pair_data.base_currency.upper() + "/" + pair_data.quote_currency.upper(),
         pair_data.base_currency.upper(),
         pair_data.quote_currency.upper(),
         pair_data.state,
         float(pair_data.amount_precision),
         float(pair_data.price_precision),
         float(pair_data.min_order_amt),
         float(pair_data.min_order_value)] for pair_data in all_pairs]
    pair_dataframe.set_index("pair", inplace=True)

    return pair_dataframe


def huobi_get_pairs_prices(API):
    market_client = API["market_client"]

    all_tickers = market_client.get_market_tickers()

    tickers_dataframe = df()
    tickers_dataframe[["pair", "price"]] = [[Ticker.symbol.upper(), float(Ticker.close)] for Ticker in all_tickers if
                                            Ticker.symbol.upper().endswith(("BTC", "ETH", "USDT"))]
    for index, row in tickers_dataframe.iterrows():
        if row["Pair"].endswith("USDT"):
            tickers_dataframe.loc[index, "Pair"] = row["Pair"][:-4] + "/USDT"
        if row["Pair"].endswith("BTC"):
            tickers_dataframe.loc[index, "Pair"] = row["Pair"][:-3] + "/BTC"
        if row["Pair"].endswith("ETH"):
            tickers_dataframe.loc[index, "Pair"] = row["Pair"][:-3] + "/ETH"
    tickers_dataframe.set_index("pair", inplace=True)

    return tickers_dataframe


def huobi_get_balances(API):
    account_client = API["account_client"]

    account_list = account_client.get_accounts()
    account_ID = [Account.id for Account in account_list if Account.type == "spot"][0]
    all_balances = account_client.get_balance(account_id=account_ID)

    balances_dataframe = df()
    balances_dataframe[["symbol", "available"]] = [[balance.currency.upper(), balance.balance]
                                                   for balance in all_balances if balance.type == "trade"]
    balances_dataframe[["symbol", "frozen"]] = [[Balance.currency.upper(), Balance.balance]
                                                for Balance in all_balances if Balance.type == "frozen"]
    balances_dataframe["total"] = balances_dataframe["available"].astype(float) + balances_dataframe["frozen"].astype(float)
    balances_dataframe = balances_dataframe.set_index("symbol")

    return balances_dataframe


def huobi_create_buy_limit_order(pair, size, price, API):
    trade_client = API["trade_client"]
    account_ID = API["account_ID"]

    pair = pair.replace("/", "")
    pair = pair.lower()

    order = trade_client.create_order(symbol=pair, amount=size, price=price, order_type=OrderType.BUY_LIMIT,
                                      source=OrderSource.API, account_id=account_ID)
    order_data = {"ID": order}

    return order_data


def huobi_create_sell_limit_order(pair, size, price, API):
    trade_client = API["trade_client"]
    account_ID = API["account_ID"]

    pair = pair.replace("/", "")
    pair = pair.lower()

    order = trade_client.create_order(symbol=pair, amount=size, price=price, order_type=OrderType.SELL_LIMIT,
                                      source=OrderSource.API, account_id=account_ID, )
    order_data = {"ID": order}

    return order_data


def huobi_trading_enabled_bool(pair_precisions_status):
    trading_enabled = True if "online" in pair_precisions_status["Status"].lower() else False

    return trading_enabled
