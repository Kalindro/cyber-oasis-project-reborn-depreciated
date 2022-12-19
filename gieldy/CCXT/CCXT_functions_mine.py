import concurrent.futures
from typing import Optional

import pandas as pd

from gieldy.CCXT.CCXT_functions_builtin import get_pairs_precisions_status
from gieldy.CCXT.get_full_history import GetFullCleanHistoryDataframe


def get_pairs_list_USDT(API: dict) -> list[str]:
    """Get all USDT active pairs list"""
    pairs_precisions_status = get_pairs_precisions_status(API)
    pairs_precisions_status = pairs_precisions_status[pairs_precisions_status["active"] == "True"]
    pairs_list_original = list(pairs_precisions_status.index)
    pairs_list = [str(pair) for pair in pairs_list_original if str(pair).endswith("/USDT")]
    return pairs_list


def get_pairs_list_BTC(API: dict) -> list[str]:
    """Get all BTC active pairs list"""
    pairs_precisions_status = get_pairs_precisions_status(API)
    pairs_precisions_status = pairs_precisions_status[pairs_precisions_status["active"] == "True"]
    pairs_list_original = list(pairs_precisions_status.index)
    pairs_list = [str(pair) for pair in pairs_list_original if str(pair).endswith("/BTC")]
    return pairs_list


def get_history_of_all_pairs_on_list_TP(pairs_list: list, timeframe: str, save_load_history: bool, API: dict,
                                        number_of_last_candles: Optional[int] = None, since: Optional[str] = None,
                                        end: Optional[str] = None) -> list[pd.DataFrame]:
    delegate_history = GetFullCleanHistoryDataframe(timeframe=timeframe, save_load_history=save_load_history,
                                                    number_of_last_candles=number_of_last_candles, since=since,
                                                    end=end, API=API)
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        all_coins_history = list(executor.map(delegate_history.main, pairs_list))
    return all_coins_history


def get_history_of_all_pairs_on_list_PP(pairs_list: list, timeframe: str, save_load_history: bool, API: dict,
                                        number_of_last_candles: Optional[int] = None, since: Optional[str] = None,
                                        end: Optional[str] = None) -> list[pd.DataFrame]:
    delegate_history = GetFullCleanHistoryDataframe(timeframe=timeframe, save_load_history=save_load_history,
                                                    number_of_last_candles=number_of_last_candles, since=since,
                                                    end=end, API=API)
    with concurrent.futures.ProcessPoolExecutor(max_workers=3) as executor:
        all_coins_history = list(executor.map(delegate_history.main, pairs_list))
    return all_coins_history


# def get_history_of_all_pairs_on_list(pairs_list: list, timeframe: str, save_load_history: bool, API: dict,
#                                      number_of_last_candles: Optional[int] = None, since: Optional[str] = None,
#                                      end: Optional[str] = None) -> list[pd.DataFrame]:
#     history_delegate = GetFullCleanHistoryDataframe(timeframe=timeframe, save_load_history=save_load_history,
#                                                     number_of_last_candles=number_of_last_candles, since=since,
#                                                     end=end, API=API)
#     all_coins_history = list(map(history_delegate.main, pairs_list))
#     return all_coins_history
