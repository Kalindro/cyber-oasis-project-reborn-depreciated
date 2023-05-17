from loguru import logger

from CyberOasisProjectReborn.CEFI.exchange.base_functions import get_pairs_with_precisions_status


def get_pairs_list_test_single() -> list[str]:
    """Get _tests pairs list"""
    return ["BTC/USDT"]


def get_pairs_list_test_multi() -> list[str]:
    """Get _tests pairs list"""
    return ["NEO/USDT", "BTC/USDT", "ETH/USDT", "BNB/USDT", "LTC/USDT"]


def get_pairs_list_BTC(API: dict) -> list[str]:
    """Get all BTC active pairs list"""
    logger.info("Getting BTC pairs list...")
    pairs_list_original = _get_pairs_list_base(API=API)
    pairs_list = [str(pair) for pair in pairs_list_original if str(pair).endswith(("/BTC", ":BTC"))]
    pairs_list_final = []
    [pairs_list_final.append(pair) for pair in pairs_list if pair not in pairs_list_final]
    logger.debug("Pairs list completed, returning")

    return pairs_list_final


def get_pairs_list_USDT(API: dict) -> list[str]:
    """Get all USDT active pairs list"""
    logger.info("Getting USDT pairs list...")
    pairs_list_original = _get_pairs_list_base(API=API)
    pairs_list = [str(pair) for pair in pairs_list_original if str(pair).endswith(("/USDT", ":USDT"))]
    pairs_list_final = []
    [pairs_list_final.append(pair) for pair in pairs_list if pair not in pairs_list_final]

    logger.debug("Pairs list completed, returning")

    return pairs_list_final


def get_pairs_list_ALL(API: dict) -> list[str]:
    """Get ALL active pairs list"""
    logger.info("Getting ALL pairs list...")
    pairs_list_original = _get_pairs_list_base(API=API)
    pairs_list = [str(pair) for pair in pairs_list_original if
                  str(pair).endswith(("/USDT", ":USDT", "/BTC", ":BTC", "/ETH", ":ETH"))]
    pairs_list_final = []
    [pairs_list_final.append(pair) for pair in pairs_list if pair not in pairs_list_final]
    logger.debug("Pairs list completed, returning")

    return pairs_list_final


def _remove_shit_from_pairs_list(pairs_list: list[str]):
    """Remove bs pairs from ever list"""
    forbidden_symbols_lying = ("LUNA", "FTT", "DREP")
    forbidden_symbols_fiat = ("EUR", "USD", "GBP", "AUD", "NZD", "CNY", "JPY", "CAD", "CHF")
    forbidden_symbols_stables = (
    "USDC", "USDT", "USDP", "TUSD", "BUSD", "DAI", "USDO", "FRAX", "USDD", "GUSD", "LUSD", "USTC")
    forbidden_symbols = forbidden_symbols_lying + forbidden_symbols_fiat + forbidden_symbols_stables
    forbidden_symbol_ending = ("UP", "DOWN", "BEAR", "BULL")

    def get_symbol(pair): return str(pair).split("/")[0]

    pairs_list = [str(pair) for pair in pairs_list if not get_symbol(pair) in forbidden_symbols]
    pairs_list = [str(pair) for pair in pairs_list if not get_symbol(pair).endswith(forbidden_symbol_ending)]

    return pairs_list


def _get_pairs_list_base(API: dict):
    pairs_precisions_status = get_pairs_with_precisions_status(API)
    pairs_precisions_status = pairs_precisions_status[pairs_precisions_status["active"] == "True"]
    pairs_list_original = list(pairs_precisions_status.index)
    pairs_list_original = _remove_shit_from_pairs_list(pairs_list=pairs_list_original)

    return pairs_list_original
