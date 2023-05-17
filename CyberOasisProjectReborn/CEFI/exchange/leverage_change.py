from concurrent.futures import ThreadPoolExecutor
from functools import partial

from loguru import logger

from CyberOasisProjectReborn.CEFI.exchange.get_pairs_list import get_pairs_list_ALL


def change_leverage_and_mode_for_whole_exchange(leverage: int,
                                                isolated: bool,
                                                API: dict
                                                ) -> None:
    """Change leverage and margin mode on all exchange pairs"""
    logger.info("Changing leverage and margin mode on all pairs on exchange")
    pairs_list = get_pairs_list_ALL(API=API)
    change_leverage_and_mode_for_pairs_list(leverage=leverage, pairs_list=pairs_list, isolated=isolated, API=API)
    logger.success("Finished changing leverage and margin mode on all")


def change_leverage_and_mode_for_pairs_list(leverage: int,
                                            pairs_list: list[str],
                                            isolated: bool,
                                            API: dict
                                            ) -> None:
    """Change leverage and margin mode on all pairs on list"""
    with ThreadPoolExecutor(max_workers=2) as executor:
        change_lev_partial = partial(change_leverage_and_mode_one_pair, leverage=leverage, isolated=isolated, API=API)
        output = dict(zip(pairs_list, executor.map(change_lev_partial, pairs_list)))


def change_leverage_and_mode_one_pair(pair: str,
                                      leverage: int,
                                      isolated: bool,
                                      API: dict
                                      ) -> None:
    """Change leverage and margin mode for one pairs_list"""

    exchange_client = API["client"]
    mmode = "ISOLATED" if isolated else "CROSS"

    logger.info(f"Changing leverage and margin for {pair}")
    if "bybit" in API["name"].lower():
        try:
            exchange_client.set_leverage(leverage=leverage, symbol=pair)
            exchange_client.set_margin_mode(marginMode=mmode, symbol=pair, params={"leverage": leverage})
        except Exception as err:
            if "not modified" in str(err):
                pass
            else:
                print(err)
    elif "okx" in API["name"].lower():
        try:
            exchange_client.set_leverage(leverage=leverage, symbol=pair)
            exchange_client.set_margin_mode(marginMode=mmode, symbol=pair, params={"lever": leverage})
        except Exception as err:
            if "Margin trading is not supported" in str(err) or "Leverage exceeds" in str(err):
                pass
            else:
                print(err)
    else:
        exchange_client.set_leverage(leverage=leverage, symbol=pair)
        exchange_client.set_margin_mode(marginMode=mmode, symbol=pair)

    logger.info(f"{pair} leverage changed to {leverage}, margin mode to {mmode}")
