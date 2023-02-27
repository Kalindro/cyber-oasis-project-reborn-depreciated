from generic.funcs_for_pairs_histories import calc_portfolio_parity
from generic.funcs_for_pairs_lists import get_full_history_for_pairs_list
from generic.select_mode import FundamentalSettings
from utils.log_config import ConfigureLoguru

logger = ConfigureLoguru().info_level()


class _BaseSettings(FundamentalSettings):
    def __init__(self):
        super().__init__()
        self.EXCHANGE_MODE: int = 1
        self.PAIRS_MODE: int = 2

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
                                       NATR_period=self.NATR_period)
        print(parity)


if __name__ == "__main__":
    PortfolioParity().main()
