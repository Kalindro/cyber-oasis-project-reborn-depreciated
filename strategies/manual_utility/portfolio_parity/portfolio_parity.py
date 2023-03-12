from prime_functions.portfolio_alocations import calc_portfolio_parity
from exchange.get_full_history import get_full_history_for_pairs_list
from exchange.select_mode import FundamentalSettings
from utils.log_config import ConfigureLoguru

logger = ConfigureLoguru().info_level()


class _BaseSettings(FundamentalSettings):
    def __init__(self):
        self.EXCHANGE_MODE: int = 1
        self.PAIRS_MODE: int = 2
        super().__init__(exchange_mode=self.EXCHANGE_MODE, pairs_mode=self.PAIRS_MODE)

        self.PERIODS = dict(NATR=20
                            )
        self.TIMEFRAME = "1h"
        self.NUMBER_OF_LAST_CANDLES = 1000

        self.INVESTMENT = 1000


class PortfolioParity(_BaseSettings):
    def main(self):
        pairs_history_df_list = get_full_history_for_pairs_list(pairs_list=self.pairs_list,
                                                                timeframe=self.TIMEFRAME,
                                                                number_of_last_candles=self.NUMBER_OF_LAST_CANDLES,
                                                                API=self.API, min_data_length=50)
        parity = calc_portfolio_parity(pairs_history_df_list=pairs_history_df_list, investment=self.INVESTMENT,
                                       NATR_period=self.PERIODS["NATR"])
        print(parity)


if __name__ == "__main__":
    PortfolioParity().main()
