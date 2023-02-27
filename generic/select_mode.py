import typing as tp
from functools import partial

from API.API_exchange_initiator import ExchangeAPISelect
from generic.get_pairs_list import get_pairs_list_test_single, get_pairs_list_test_multi, get_pairs_list_BTC, \
    get_pairs_list_USDT


class FundamentalSettings:
    """
    Modes available:
    :EXCHANGE_MODE: 1 - Binance Spot; 2 - Binance Futures; 3 - Kucoin Spot
    :PAIRS_MODE: 1 - Test single; 2 - Test multi; 3 - BTC; 4 - USDT
    """

    def __init__(self):
        self.EXCHANGE_MODE: tp.Union[int, None] = None
        self.PAIRS_MODE: tp.Union[int, None] = None

        if self.EXCHANGE_MODE:
            self.API = select_exchange_mode(self.EXCHANGE_MODE)
        if self.PAIRS_MODE:
            self.pairs_list = select_pairs_list_mode(self.PAIRS_MODE, self.API)


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
