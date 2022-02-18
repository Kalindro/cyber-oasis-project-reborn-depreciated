from Gieldy.Huobi.Huobi_utils import *
from Gieldy.Binance.Binance_utils import *
from Gieldy.Kucoin.Kucoin_utils import *
from Gieldy.Gateio.Gateio_utils import *
from Gieldy.Okx.Okx_utils import *
from Gieldy.FTX.FTX_utils import *

import pandas as pd
import numpy as np

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


def cancel_pair_orders(pair, API):
    name = API["name"]

    if "huobi" in name.lower():
        return huobi_cancel_pair_orders(pair=pair, API=API)

    if "kucoin" in name.lower():
        return kucoin_cancel_pair_orders(pair=pair, API=API)

    if "binance" in name.lower():
        return binance_cancel_pair_orders(pair=pair, API=API)

    if "gate" in name.lower():
        return gateio_cancel_pair_orders(pair=pair, API=API)

    if "okex" in name.lower():
        return okex_cancel_pair_orders(pair=pair, API=API)

    else:
        raise Exception(f"{name.lower()} not added for this function")


def cancel_all_orders(API):
    print("Canceling all orders")

    name = API["name"]

    if "huobi" in name.lower():
        return huobi_cancel_all_orders(API)

    if "kucoin" in name.lower():
        return kucoin_cancel_all_orders(API)

    if "binance" in name.lower():
        return binance_cancel_all_orders(API)

    if "gate" in name.lower():
        return gateio_cancel_all_orders(API)

    if "okx" in name.lower():
        return okx_cancel_all_orders(API)

    else:
        raise Exception(f"{name.lower()} not added for this function")

    print("Canceled all orders")


def fetch_order(pair, order_ID, API):
    name = API["name"]

    if "huobi" in name.lower():
        return huobi_fetch_order(order_ID=order_ID, API=API)

    if "kucoin" in name.lower():
        return kucoin_fetch_order(order_ID=order_ID, API=API)

    if "binance" in name.lower():
        return binance_fetch_order(pair=pair, order_ID=order_ID, API=API)

    if "gate" in name.lower():
        return gateio_fetch_order(pair=pair, order_ID=order_ID, API=API)

    else:
        raise Exception(f"{name.lower()} not added for this function")


def fetch_open_pair_orders(pair, API):
    name = API["name"]

    if "huobi" in name.lower():
        return huobi_fetch_open_pair_orders(pair=pair, API=API)

    if "kucoin" in name.lower():
        return kucoin_fetch_open_pair_orders(pair=pair, API=API)

    if "binance" in name.lower():
        return binance_fetch_open_pair_orders(pair=pair, API=API)

    if "gate" in name.lower():
        return gateio_fetch_open_pair_orders(pair=pair, API=API)

    else:
        raise Exception(f"{name.lower()} not added for this function")


def get_pairs_precisions_status(API):
    name = API["name"]

    print("Getting all pairs precisions and status")

    if "huobi" in name.lower():
        return huobi_get_pairs_precisions_status(API)

    if "kucoin" in name.lower():
        return kucoin_get_pairs_precisions_status(API)

    if "binance" in name.lower():
        return binance_get_pairs_precisions_status(API)

    if "gate" in name.lower():
        return gateio_get_pairs_precisions_status(API)

    else:
        raise Exception(f"{name.lower()} not added for this function")


def get_pairs_prices(API):
    name = API["name"]

    print("Getting all pairs prices")

    if "huobi" in name.lower():
        return huobi_get_pairs_prices(API)

    if "kucoin" in name.lower():
        return kucoin_get_pairs_prices(API)

    if "binance" in name.lower():
        return binance_get_pairs_prices(API)

    if "gate" in name.lower():
        return gateio_get_pairs_prices(API)

    else:
        raise Exception(f"{name.lower()} not added for this function")


def get_balances(API):
    name = API["name"]

    print("Getting all coins balances")

    if "huobi" in name.lower():
        return huobi_get_balances(API)

    if "kucoin" in name.lower():
        return kucoin_get_balances(API)

    if "binance" in name.lower():
        return binance_get_balances(API)

    if "gate" in name.lower():
        return gateio_get_balances(API)

    else:
        raise Exception(f"{name.lower()} not added for this function")


def create_buy_limit_order(pair, size, price, API):
    name = API["name"]

    price = np.format_float_positional(price, trim="-")
    size = np.format_float_positional(size, trim="-")

    if "huobi" in name.lower():
        return huobi_create_buy_limit_order(pair=pair, size=size, price=price, API=API)

    if "kucoin" in name.lower():
        return kucoin_create_buy_limit_order(pair=pair, size=size, price=price, API=API)

    if "binance" in name.lower():
        return binance_create_buy_limit_order(pair=pair, size=size, price=price, API=API)

    if "gate" in name.lower():
        return gateio_create_buy_limit_order(pair=pair, size=size, price=price, API=API)

    else:
        raise Exception(f"{name.lower()} not added for this function")


def create_sell_limit_order(pair, size, price, API):
    name = API["name"]

    price = np.format_float_positional(price, trim="-")
    size = np.format_float_positional(size, trim="-")

    if "huobi" in name.lower():
        return huobi_create_sell_limit_order(pair=pair, size=size, price=price, API=API)

    if "kucoin" in name.lower():
        return kucoin_create_sell_limit_order(pair=pair, size=size, price=price, API=API)

    if "binance" in name.lower():
        return binance_create_sell_limit_order(pair=pair, size=size, price=price, API=API)

    if "gate" in name.lower():
        return gateio_create_sell_limit_order(pair=pair, size=size, price=price, API=API)

    else:
        raise Exception(f"{name.lower()} not added for this function")


def create_sell_market_order(pair, size, API):
    name = API["name"]

    if "kucoin" in name.lower():
        return kucoin_create_sell_market_order(pair=pair, size=size, API=API)

    if "binance" in name.lower():
        return binance_create_sell_market_order(pair=pair, size=size, API=API)

    else:
        raise Exception(f"{name.lower()} not added for this function")


def trading_enabled_bool(pair_precisions_status, API):
    name = API["name"]

    if "huobi" in name.lower():
        return huobi_trading_enabled_bool(pair_precisions_status=pair_precisions_status)

    if "kucoin" in name.lower():
        return kucoin_trading_enabled_bool(pair_precisions_status=pair_precisions_status)

    if "binance" in name.lower():
        return binance_trading_enabled_bool(pair_precisions_status=pair_precisions_status)

    if "gate" in name.lower():
        return gateio_trading_enabled_bool(pair_precisions_status=pair_precisions_status)

    else:
        raise Exception(f"{name.lower()} not added for this function")


def get_history_fragment_for_func(pair, timeframe, since, to, days_of_history, API):
    name = API["name"]

    if "huobi" in name.lower():
        if days_of_history < 32:
            return huobi_REST_history_fragment(pair=pair, timeframe=timeframe, API=API)
        else:
            return huobi_websocket_history_fragment(pair=pair, timeframe=timeframe, Since=since, To=to)

    if "kucoin" in name.lower():
        return kucoin_REST_history_fragment(pair=pair, timeframe=timeframe, since=since, to=to, API=API)

    if "binance" in name.lower():
        return binance_REST_history_fragment(pair=pair, timeframe=timeframe, since=since, to=to, API=API)

    if "gate" in name.lower():
        return gateio_REST_history_fragment(pair=pair, timeframe=timeframe, since=since, to=to, API=API)

    if "ftx" in name.lower():
        return FTX_REST_history_fragment(pair=pair, timeframe=timeframe, since=since, to=to, API=API)

    else:
        raise Exception(f"{name.lower()} not added for this function")
