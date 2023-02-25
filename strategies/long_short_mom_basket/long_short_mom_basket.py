from dataclasses import dataclass

from generic.funcs_for_pairs_lists import get_full_history_for_pairs_list
from generic.select_mode import select_exchange_mode, select_pairs_list_mode
from generic.funcs_for_pairs_histories import momentum_ranking_for_pairs_histories
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
    PAIRS_MODE: int = 4
    TOP_DECIMAL = 0.2
    PERIOD = 168
    MIN_VOLUME = 10
    MIN_DATA_LENGTH = 10

    TIMEFRAME: str = "1h"
    NUMBER_OF_LAST_CANDLES: int = 1000

    def __init__(self):
        self.API = select_exchange_mode(self.EXCHANGE_MODE)
        self.pairs_list = select_pairs_list_mode(self.PAIRS_MODE, self.API)


class LongShortMomBasket(_BaseSettings):
    def main(self):
        pairs_history_df_list = get_full_history_for_pairs_list(pairs_list=self.pairs_list, timeframe=self.TIMEFRAME,
                                                                number_of_last_candles=self.NUMBER_OF_LAST_CANDLES,
                                                                min_data_length=self.MIN_DATA_LENGTH, API=self.API)
        top_coins, bottom_coins = momentum_ranking_for_pairs_histories(pairs_history_df_list=pairs_history_df_list,
                                                                       period=self.PERIOD,
                                                                       top_decimal=self.TOP_DECIMAL)

        print(top_coins, bottom_coins)


if __name__ == "__main__":
    LongShortMomBasket().main()
