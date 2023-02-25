from dataclasses import dataclass

from generic.funcs_for_pairs_lists import get_full_history_for_pairs_list
from generic.select_mode import select_exchange_mode, select_pairs_list_mode
from generic.funcs_for_pairs_histories import calc_portfolio_parity
from utils.log_config import ConfigureLoguru

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

    def __init__(self):
        self.API = select_exchange_mode(self.EXCHANGE_MODE)
        self.pairs_list = select_pairs_list_mode(self.PAIRS_MODE, self.API)
        self.pairs_history_df_list = get_full_history_for_pairs_list(pairs_list=self.pairs_list,
                                                                     timeframe=self.TIMEFRAME,
                                                                     number_of_last_candles=self.NUMBER_OF_LAST_CANDLES,
                                                                     API=self.API, min_data_length=50)


class PortfolioParity(_BaseSettings):
    def main(self):
        parity = calc_portfolio_parity(pairs_history_df_list=self.pairs_history_df_list, investment=self.INVESTMENT,
                                       period=self.PERIOD)
        print(parity)


if __name__ == "__main__":
    PortfolioParity().main()
