from dataclasses import dataclass

from CCXT.functions_mine import select_exchange_mode


@dataclass
class _BaseSettings:
    """
    Modes available:
    :EXCHANGE_MODE: 1 - Binance Spot; 2 - Binance Futures; 3 - Kucoin Spot
    :PAIRS_MODE: 1 - Test single; 2 - Test multi; 3 - BTC; 4 - USDT
    """
    EXCHANGE_MODE: int = 1
    TIMEFRAME: str = "1h"
    PAIR_LONG: str = "RNDR/USDT"
    PAIR_SHORT: str = "BTC/USDT"
    PAIR_BENCHMARK: str = "BTC/USDT"
    INVESTMENT: int = 2300
    NUMBER_OF_LAST_CANDLES: int = 170

    def __post_init__(self):
        self.API = select_exchange_mode(self.EXCHANGE_MODE)


class PortfolioParity:
    def main(self):
        pass


if __name__ == "__main__":
    PortfolioParity().main()
