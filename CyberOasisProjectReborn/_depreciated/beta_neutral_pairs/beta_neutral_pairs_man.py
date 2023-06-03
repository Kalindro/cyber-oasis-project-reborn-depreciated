from general_functions.portfolio_alocations import calc_beta_neutral_allocation_for_two_pairs
from CyberOasisProjectReborn.CEFI.exchange import FundamentalTemplate
from CyberOasisProjectReborn.utils.logger_custom import Loguru

logger = Loguru().info_level()


class _BaseTemplate(FundamentalTemplate):
    def __init__(self):
        self.EXCHANGE_MODE: int = 1
        super().__init__(exchange_mode=self.EXCHANGE_MODE)

        self.PERIODS = dict(BETA=20,
                            )
        self.TIMEFRAME = "1h"
        self.NUMBER_OF_LAST_CANDLES = 1000

        self.INVESTMENT = 1000
        self.PAIR_LONG: str = "RNDR/USDT"
        self.PAIR_SHORT: str = "BTC/USDT"
        self.PAIR_BENCHMARK: str = "BTC/USDT"


class BetaNeutralPairs(_BaseTemplate):
    """Calculate beta neutral allocation for two pairs"""

    def main(self):
        allocation_df = calc_beta_neutral_allocation_for_two_pairs(pair_long=self.PAIR_LONG,
                                                                   pair_short=self.PAIR_SHORT,
                                                                   benchmark=self.PAIR_BENCHMARK,
                                                                   timeframe=self.TIMEFRAME,
                                                                   number_of_last_candles=self.NUMBER_OF_LAST_CANDLES,
                                                                   beta_period=self.PERIODS["BETA"],
                                                                   investment=self.INVESTMENT, API=self.API)
        print(allocation_df)


if __name__ == "__main__":
    BetaNeutralPairs().main()
