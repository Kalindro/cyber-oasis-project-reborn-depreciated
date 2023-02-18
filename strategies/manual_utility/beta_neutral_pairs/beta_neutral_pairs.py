from dataclasses import dataclass

from CCXT.functions_mine import select_exchange_mode
from general_funcs.calculations import calc_beta_neutral_allocation_for_two_pairs
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
    TIMEFRAME: str = "1h"
    PAIR_LONG: str = "RNDR/USDT"
    PAIR_SHORT: str = "BTC/USDT"
    PAIR_BENCHMARK: str = "BTC/USDT"
    INVESTMENT: int = 2300
    NUMBER_OF_LAST_CANDLES: int = 170

    def __post_init__(self):
        self.API = select_exchange_mode(self.EXCHANGE_MODE)


class BetaNeutralPairs(_BaseSettings):
    """Main class with pairs balance calculations"""

    def main(self):
        allocation_df = calc_beta_neutral_allocation_for_two_pairs(pair_long=self.PAIR_LONG,
                                                                   pair_short=self.PAIR_SHORT,
                                                                   investment=self.INVESTMENT,
                                                                   timeframe=self.TIMEFRAME,
                                                                   number_of_last_candles=self.NUMBER_OF_LAST_CANDLES,
                                                                   API=self.API)
        print(allocation_df)


if __name__ == "__main__":
    BetaNeutralPairs().main()
