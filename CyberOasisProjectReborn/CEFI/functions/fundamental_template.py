from __future__ import annotations

from abc import ABC
from functools import partial
from typing import TYPE_CHECKING

from CyberOasisProjectReborn.CEFI.exchange.exchanges import *

if TYPE_CHECKING:
    from CyberOasisProjectReborn.CEFI.functions.exchange_functions import Exchange


class FundamentalTemplate(ABC):
    """
    Modes available:
    :EXCHANGE_MODE: 1 - Binance Spot Read; 2 - Binance Futures Read; 3 - Bybit Read; 4 - Bybit Trade;
    :PAIRS_MODE: 1 - Test single; 2 - Test multi; 3 - BTC; 4 - USDT
    """

    def __init__(self, exchange_mode: int = None, pairs_mode: int = None):
        if exchange_mode:
            self.exchange = self.select_exchange_mode(exchange_mode)
        if pairs_mode:
            self.pairs_list = self.select_pairs_list_mode(pairs_mode)

    def select_exchange_mode(self, exchange_mode: int) -> Exchange:
        """Depending on the PAIRS_MODE, return correct pairs list"""
        exchanges_dict = {1: BinanceSpotReadOnly,
                          2: BinanceFuturesReadOnly,
                          3: BybitReadOnly,
                          4: BybitTrade,
                          }
        exchange = exchanges_dict.get(exchange_mode)
        if exchange is None:
            raise ValueError("Invalid mode: " + str(exchange_mode))
        exchange = exchange()
        return exchange

    def select_pairs_list_mode(self, pairs_mode: int) -> list[str]:
        """Depending on the PAIRS_MODE, return correct pairs list"""
        pairs_list = {1: partial(self.exchange.functions.get_pairs_list_test_single),
                      2: partial(self.exchange.functions.get_pairs_list_test_multi),
                      3: partial(self.exchange.functions.get_pairs_list_BTC),
                      4: partial(self.exchange.functions.get_pairs_list_USDT),
                      }
        pairs_list = pairs_list.get(pairs_mode)
        if pairs_list is None:
            raise ValueError("Invalid mode: " + str(pairs_mode))
        return pairs_list()
