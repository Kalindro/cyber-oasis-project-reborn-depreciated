from dataclasses import dataclass

from CCXT.functions_mine import select_exchange_mode
from CCXT.functions_pairs_list import select_pairs_list_mode
from general_funcs.calculations import calc_portfolio_parity
from general_funcs.log_config import ConfigureLoguru

logger = ConfigureLoguru().info_level()


@dataclass
class _BaseSettings:
    """
    Modes available:
    :EXCHANGE_MODE: 1 - Binance Spot; 2 - Binance Futures; 3 - Kucoin Spot
    :PAIRS_MODE: 1 - Test single; 2 - Test multi; 3 - BTC; 4 - USDT
    """
    EXCHANGE_MODE: int = 1
    PAIRS_MODE: int = 2
    TIMEFRAME: str = "1h"
    INVESTMENT: int = 1000
    NUMBER_OF_LAST_CANDLES: int = 170
    PERIOD: int = 20

    def __post_init__(self):
        self.API = select_exchange_mode(self.EXCHANGE_MODE)
        self.pairs_list = select_pairs_list_mode(self.PAIRS_MODE, self.API)


class PortfolioParity(_BaseSettings):
    def main(self):
        parity = calc_portfolio_parity(pairs_list=self.pairs_list, investment=self.INVESTMENT, period=self.PERIOD,
                                       timeframe=self.TIMEFRAME, number_of_last_candles=self.NUMBER_OF_LAST_CANDLES,
                                       API=self.API, min_data_length=50)
        print(parity)


if __name__ == "__main__":
    PortfolioParity().main()
