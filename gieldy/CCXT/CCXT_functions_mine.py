from gieldy.CCXT.CCXT_functions_builtin import get_pairs_precisions_status
from gieldy.CCXT.get_full_history import QueryHistory
from functools import partial


def get_pairs_list_USDT(API):
    """Get all USDT active pairs list"""
    pairs_precisions_status = get_pairs_precisions_status(API)
    pairs_precisions_status = pairs_precisions_status[pairs_precisions_status["active"] == "True"]
    pairs_list_original = list(pairs_precisions_status.index)
    pairs_list = [str(pair) for pair in pairs_list_original if str(pair).endswith("/USDT")]
    return pairs_list


def get_pairs_list_BTC(API):
    """Get all BTC active pairs list"""
    pairs_precisions_status = get_pairs_precisions_status(API)
    pairs_precisions_status = pairs_precisions_status[pairs_precisions_status["active"] == "True"]
    pairs_list_original = list(pairs_precisions_status.index)
    pairs_list = [str(pair) for pair in pairs_list_original if str(pair).endswith("/BTC")]
    return pairs_list


def get_history_of_all_pairs_on_list(pairs_list, timeframe, save_load, API, last_n_candles):
    partial_get_full_history = partial(QueryHistory, timeframe=timeframe, save_load=save_load, API=API,
                                       last_n_candles=last_n_candles)

    all_coins_history = list(map(partial_get_full_history, pairs_list))
    print(all_coins_history[-1].main())
    print(all_coins_history)
