import concurrent.futures
from typing import Optional

import pandas as pd
from loguru import logger

from gieldy.CCXT.CCXT_functions_builtin import get_pairs_precisions_status
from gieldy.CCXT.get_full_history import GetFullCleanHistoryDataframe

logger.remove()


def get_pairs_list_USDT(API: dict) -> list[str]:
    """Get all USDT active pairs list"""
    logger.info("Getting USDT pairs list...")
    pairs_precisions_status = get_pairs_precisions_status(API)
    pairs_precisions_status = pairs_precisions_status[pairs_precisions_status["active"] == "True"]
    pairs_list_original = list(pairs_precisions_status.index)
    pairs_list = [str(pair) for pair in pairs_list_original if str(pair).endswith("/USDT")]
    logger.success("Pairs list completed, returning")
    return pairs_list


def get_pairs_list_BTC(API: dict) -> list[str]:
    """Get all BTC active pairs list"""
    logger.info("Getting BTC pairs list...")
    pairs_precisions_status = get_pairs_precisions_status(API)
    pairs_precisions_status = pairs_precisions_status[pairs_precisions_status["active"] == "True"]
    pairs_list_original = list(pairs_precisions_status.index)
    pairs_list = [str(pair) for pair in pairs_list_original if str(pair).endswith("/BTC")]
    logger.success("Pairs list completed, returning")
    return pairs_list


def get_history_of_all_pairs_on_list(pairs_list: list, timeframe: str, save_load_history: bool, API: dict,
                                     number_of_last_candles: Optional[int] = None, since: Optional[str] = None,
                                     end: Optional[str] = None) -> list[pd.DataFrame]:
    logger.info("Getting history of all the coins on provided pairs list...")
    delegate_history = GetFullCleanHistoryDataframe(timeframe=timeframe, save_load_history=save_load_history,
                                                    number_of_last_candles=number_of_last_candles, since=since,
                                                    end=end, API=API)
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        all_coins_history = list(executor.map(delegate_history.main, pairs_list))
    logger.success("History of all the coins completed, returning")
    return all_coins_history
