from loguru import logger

from exchange.base_functions import change_leverage_and_mode_one_pair
from exchange.get_pairs_list import get_pairs_list_ALL


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
