from functools import partial

from loguru import logger

from ext_projects.CCXT.functions_base import get_pairs_with_precisions_status


def remove_shit_from_pairs_list(pairs_list):
    """Remove bs pairs from ever list"""
    forbidden_symbols = ("EUR", "USD", "GBP", "AUD", "NZD", "CNY", "JPY", "CAD", "CHF")
    forbidden_ending = ("UP", "DOWN", "BEAR", "BULL")

    def symbol_func(pair): return str(pair).split("/")[0]

    pairs_list = [str(pair) for pair in pairs_list if not (symbol_func(pair) in forbidden_symbols)]
    pairs_list = [str(pair) for pair in pairs_list if not symbol_func(pair).endswith(forbidden_ending)]
    return pairs_list


def _get_pairs_list_base(API: dict):
    pairs_precisions_status = get_pairs_with_precisions_status(API)
    pairs_precisions_status = pairs_precisions_status[pairs_precisions_status["active"] == "True"]
    pairs_list_original = list(pairs_precisions_status.index)
    pairs_list_original = remove_shit_from_pairs_list(pairs_list=pairs_list_original)

    return pairs_list_original


def get_pairs_list_test_single() -> list[str]:
    """Get test pairs list"""
    return ["GNS/USDT"]


def get_pairs_list_test_multi() -> list[str]:
    """Get test pairs list"""
    return ["ACH/USDT", "BTC/USDT", "ETH/USDT"]


def get_pairs_list_BTC(API: dict) -> list[str]:
    """Get all BTC active pairs list"""
    logger.info("Getting BTC pairs list...")
    pairs_list_original = _get_pairs_list_base(API=API)
    pairs_list = [str(pair) for pair in pairs_list_original if str(pair).endswith(("/BTC", ":BTC"))]
    logger.debug("Pairs list completed, returning")

    return pairs_list


def get_pairs_list_USDT(API: dict) -> list[str]:
    """Get all USDT active pairs list"""
    logger.info("Getting USDT pairs list...")
    pairs_list_original = _get_pairs_list_base(API=API)
    pairs_list = [str(pair) for pair in pairs_list_original if str(pair).endswith(("/USDT", ":USDT"))]
    logger.debug("Pairs list completed, returning")

    return pairs_list


def get_pairs_list_ALL(API: dict) -> list[str]:
    """Get ALL active pairs list"""
    logger.info("Getting ALL pairs list...")
    pairs_list_original = _get_pairs_list_base(API=API)
    pairs_list = [str(pair) for pair in pairs_list_original if
                  str(pair).endswith(("/USDT", ":USDT", "/BTC", ":BTC", "/ETH", ":ETH"))]
    logger.debug("Pairs list completed, returning")

    return pairs_list


def select_pairs_list_mode(pairs_mode, API) -> list[str]:
    """Depending on the PAIRS_MODE, return correct pairs list"""
    pairs_list = {1: partial(get_pairs_list_test_single),
                  2: partial(get_pairs_list_test_multi),
                  3: partial(get_pairs_list_BTC, API),
                  4: partial(get_pairs_list_USDT, API),
                  }
    pairs_list = pairs_list.get(pairs_mode)
    if pairs_list is None:
        raise ValueError("Invalid mode: " + str(pairs_mode))

    return pairs_list()
