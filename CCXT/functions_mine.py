import concurrent.futures
from functools import partial
from typing import Optional

import pandas as pd
from loguru import logger

from API.API_exchange_initiator import ExchangeAPISelect
from CCXT.functions_pairs_list import get_pairs_list_ALL
from CCXT.get_full_history import GetFullHistoryDF
from general_funcs.utils import dataframe_is_not_none_and_has_elements


def get_full_history_for_pairs_list(pairs_list: list, timeframe: str, API: dict, save_load_history: bool = False,
                                    number_of_last_candles: Optional[int] = None, since: Optional[str] = None,
                                    end: Optional[str] = None) -> dict[str: pd.DataFrame]:
    """Get history of all pairs on list"""
    workers = 2
    logger.info("Getting history of all the coins on provided pairs list...")
    delegate_history_partial = partial(GetFullHistoryDF().main, timeframe=timeframe,
                                       save_load_history=save_load_history,
                                       API=API, number_of_last_candles=number_of_last_candles,
                                       since=since, end=end)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        all_coins_history = list(executor.map(delegate_history_partial, pairs_list))
    all_coins_history = [df for df in all_coins_history if dataframe_is_not_none_and_has_elements(df)]
    logger.success("History of all the coins completed, returning")

    return all_coins_history


def select_exchange_mode(exchange_mode) -> dict:
    """Depending on the PAIRS_MODE, return correct pairs list"""
    exchanges_dict = {1: ExchangeAPISelect().binance_spot_read_only,
                      2: ExchangeAPISelect().binance_futures_read_only,
                      3: ExchangeAPISelect().kucoin_spot_read_only,
                      }
    exchange = exchanges_dict.get(exchange_mode)
    if exchange is None:
        raise ValueError("Invalid mode: " + str(exchange_mode))

    return exchange()


def chagne_leverage_n_mode_for_pairs_list(leverage: int, pairs_list: list, isolated: bool, API: dict) -> None:
    """Change leverage and margin mode on all pairs on list"""
    exchange_client = API["client"]
    mmode = "ISOLATED" if isolated else "CROSS"

    for pair in pairs_list:
        logger.info(f"Changing leverage and margin for {pair}")
        if "bybit" in API["name"].lower():
            try:
                exchange_client.set_margin_mode(marginMode=mmode, symbol=pair, params={"leverage": leverage})
            except Exception as err:
                if "not modified" in str(err):
                    pass
                else:
                    print(err)
        else:
            exchange_client.set_leverage(leverage=leverage, symbol=pair)
            exchange_client.set_margin_mode(marginMode=mmode, symbol=pair)
        logger.info(f"{pair} leverage changed to {leverage}, margin mode to isolated")


def change_leverage_n_mode_for_all_exchange_pairs(leverage: int, isolated: bool, API: dict) -> None:
    """Change leverage and margin mode on all exchange pairs"""
    logger.info("Changing leverage and margin mode on all pairs on exchange")
    pairs_list = get_pairs_list_ALL(API=API)
    chagne_leverage_n_mode_for_pairs_list(leverage=leverage, pairs_list=pairs_list, isolated=isolated,
                                          API=API)
    logger.success("Finished changing leverage and margin mode on all")