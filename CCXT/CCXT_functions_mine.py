import concurrent.futures
from functools import partial
from typing import Optional

import pandas as pd
from loguru import logger

from API.API_exchange_initiator import ExchangeAPISelect
from CCXT.CCXT_functions_builtin import get_pairs_with_precisions_status
from CCXT.get_full_history import GetFullHistoryDF


def get_pairs_list_test_single() -> list[str]:
    """Get test pairs list"""
    return ["BTC/USDT"]


def get_pairs_list_test_multi() -> list[str]:
    """Get test pairs list"""
    return ["BTC/USDT", "ETH/USDT"]


def get_pairs_list_BTC(API: dict) -> list[str]:
    """Get all BTC active pairs list"""
    logger.info("Getting BTC pairs list...")
    pairs_precisions_status = get_pairs_with_precisions_status(API)
    pairs_precisions_status = pairs_precisions_status[pairs_precisions_status["active"] == "True"]
    pairs_list_original = list(pairs_precisions_status.index)
    pairs_list = [str(pair) for pair in pairs_list_original if str(pair).endswith("/BTC")]
    logger.debug("Pairs list completed, returning")
    return pairs_list


def get_pairs_list_USDT(API: dict) -> list[str]:
    """Get all USDT active pairs list"""
    logger.info("Getting USDT pairs list...")
    pairs_precisions_status = get_pairs_with_precisions_status(API)
    pairs_precisions_status = pairs_precisions_status[pairs_precisions_status["active"] == "True"]
    pairs_list_original = list(pairs_precisions_status.index)
    pairs_list = [str(pair) for pair in pairs_list_original if str(pair).endswith("/USDT")]
    logger.debug("Pairs list completed, returning")
    return pairs_list


def get_history_of_all_pairs_on_list(pairs_list: list, timeframe: str, save_load_history: bool, API: dict,
                                     number_of_last_candles: Optional[int] = None, since: Optional[str] = None,
                                     end: Optional[str] = None) -> dict[str: pd.DataFrame]:
    workers = 3
    logger.info("Getting history of all the coins on provided pairs list...")
    delegate_history_partial = partial(GetFullHistoryDF().main, timeframe=timeframe,
                                       save_load_history=save_load_history,
                                       API=API, number_of_last_candles=number_of_last_candles,
                                       since=since, end=end)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        all_coins_history = list(executor.map(delegate_history_partial, pairs_list))
    logger.success("History of all the coins completed, returning")
    all_coins_history = [df for df in all_coins_history if df is not None]
    return all_coins_history


def select_exchange_mode(EXCHANGE_MODE) -> dict:
    """Depending on the PAIRS_MODE, return correct pairs list"""
    exchanges_dict = {1: ExchangeAPISelect().binance_spot_read_only,
                      2: ExchangeAPISelect().binance_futures_read_only,
                      3: ExchangeAPISelect().kucoin_spot_read_only,
                      }
    exchange = exchanges_dict.get(EXCHANGE_MODE)
    if exchange is None:
        raise ValueError("Invalid mode: " + str(EXCHANGE_MODE))
    return exchange()


def select_pairs_list_mode(PAIRS_MODE, API) -> list[str]:
    """Depending on the PAIRS_MODE, return correct pairs list"""
    pairs_list = {1: partial(get_pairs_list_test_single),
                  2: partial(get_pairs_list_test_multi),
                  3: partial(get_pairs_list_BTC, API),
                  4: partial(get_pairs_list_USDT, API),
                  }
    pairs_list = pairs_list.get(PAIRS_MODE)
    if pairs_list is None:
        raise ValueError("Invalid mode: " + str(PAIRS_MODE))
    return pairs_list()
