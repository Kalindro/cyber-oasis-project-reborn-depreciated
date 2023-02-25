import concurrent.futures
from functools import partial

import pandas as pd
from loguru import logger

from CCXT.funcs_get_pairs_list import get_pairs_list_ALL
from CCXT.get_full_history import GetFullHistoryDF
from utils.utils import dataframe_is_not_none_and_not_empty


def get_full_history_for_pairs_list(pairs_list: list, timeframe: str, API: dict, min_dat_length: int = None,
                                    min_volume: int = None, **kwargs) -> list[pd.DataFrame]:
    """Get history of all pairs on list
    kwargs:
        save_load_history
        number_of_last_n_candles
        since
        end
    """
    workers = 2
    logger.info("Getting history of all the coins on provided pairs list...")
    delegate_history_partial = partial(GetFullHistoryDF().main, timeframe=timeframe, API=API, **kwargs)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        pairs_history_df_list = list(executor.map(delegate_history_partial, pairs_list))
    pairs_history_df_list = [df for df in pairs_history_df_list if dataframe_is_not_none_and_not_empty(df)]

    if min_dat_length:
        pairs_history_df_list = [df for df in pairs_history_df_list if len(df) < min_dat_length]
    if min_volume:
        raise AssertionError("Not implemented yet")

    logger.success("History of all the coins completed, returning")

    return pairs_history_df_list


def change_leverage_and_mode_for_pairs_list(leverage: int, pairs_list: list, isolated: bool, API: dict) -> None:
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


def change_leverage_and_mode_for_whole_exchange(leverage: int, isolated: bool, API: dict) -> None:
    """Change leverage and margin mode on all exchange pairs"""
    logger.info("Changing leverage and margin mode on all pairs on exchange")
    pairs_list = get_pairs_list_ALL(API=API)
    change_leverage_and_mode_for_pairs_list(leverage=leverage, pairs_list=pairs_list, isolated=isolated, API=API)
    logger.success("Finished changing leverage and margin mode on all")
