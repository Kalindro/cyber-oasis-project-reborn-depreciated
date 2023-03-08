import concurrent.futures
from functools import partial

import numpy as np
import pandas as pd
from loguru import logger

from exchange.base_functions import change_leverage_and_mode_one_pair
from exchange.get_full_history import GetFullHistoryDF
from exchange.get_pairs_list import get_pairs_list_ALL
from utils.utils import dataframe_is_not_none_and_not_empty


def get_full_history_for_pairs_list(pairs_list: list[str], timeframe: str, API: dict, min_data_length: int = None,
                                    vol_quantile_drop: float = None, **kwargs) -> list[pd.DataFrame]:
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

    if min_data_length:
        pairs_history_df_list = [df for df in pairs_history_df_list if len(df) > min_data_length]
    if vol_quantile_drop:
        pairs_history_df_list = _drop_bottom_quantile_vol(pairs_history_df_list=pairs_history_df_list,
                                                          quantile=vol_quantile_drop)

    logger.success("History of all the coins completed, returning")

    return pairs_history_df_list


def _drop_bottom_quantile_vol(pairs_history_df_list: list[pd.DataFrame], quantile: float):
    mean_volumes = [pair_history["volume"].mean() for pair_history in pairs_history_df_list]
    threshold = np.quantile(mean_volumes, 0.25)
    pairs_history_df_list_quantiled = [pair_dataframe for index, pair_dataframe in enumerate(pairs_history_df_list) if
                                       mean_volumes[index] > threshold]
    logger.success(f"Dropped bottom {quantile * 100}% volume coins")

    return pairs_history_df_list_quantiled


def change_leverage_and_mode_for_whole_exchange(leverage: int, isolated: bool, API: dict) -> None:
    """Change leverage and margin mode on all exchange pairs"""
    logger.info("Changing leverage and margin mode on all pairs on exchange")
    pairs_list = get_pairs_list_ALL(API=API)
    change_leverage_and_mode_for_pairs_list(leverage=leverage, pairs_list=pairs_list, isolated=isolated, API=API)
    logger.success("Finished changing leverage and margin mode on all")


def change_leverage_and_mode_for_pairs_list(leverage: int, pairs_list: list, isolated: bool, API: dict) -> None:
    """Change leverage and margin mode on all pairs on list"""
    for pair in pairs_list:
        change_leverage_and_mode_one_pair(pair=pair, leverage=leverage, isolated=isolated, API=API)
