from dataclasses import dataclass

from generic.funcs_for_pairs_histories import calc_portfolio_parity
from generic.funcs_for_pairs_lists import get_full_history_for_pairs_list
from generic.select_mode import FundamentalSettings
from utils.log_config import ConfigureLoguru

logger = ConfigureLoguru().info_level()


@dataclass
class _BaseSettings(FundamentalSettings):
    EXCHANGE_MODE: int = 1
    PAIRS_MODE: int = 2
    TIMEFRAME: str = "1h"
    INVESTMENT: int = 1000
    NUMBER_OF_LAST_CANDLES: int = 170

    PERIODS = dict(NATR=20)


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
