from dataclasses import dataclass

from generic.funcs_for_pairs_histories import calc_portfolio_parity
from generic.funcs_for_pairs_histories import momentum_ranking_for_pairs_histories
from generic.funcs_for_pairs_lists import get_full_history_for_pairs_list
from generic.select_mode import select_exchange_mode, select_pairs_list_mode
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
    TOP_NUMBER = 20
    MOMENTUM_PERIOD = 168
    NATR_PERIOD = 168
    MIN_VOLUME = 10
    MIN_DATA_LENGTH = 10
    VOL_QUANTILE = 0.35

    TIMEFRAME: str = "1h"
    NUMBER_OF_LAST_CANDLES: int = 1000

    def __post_init__(self):
        self.API = select_exchange_mode(self.EXCHANGE_MODE)
        self.pairs_list = select_pairs_list_mode(self.PAIRS_MODE, self.API)


class LongShortMomBasket(_BaseSettings):
    def main(self):
        pairs_history_df_list = get_full_history_for_pairs_list(pairs_list=self.pairs_list, timeframe=self.TIMEFRAME,
                                                                number_of_last_candles=self.NUMBER_OF_LAST_CANDLES,
                                                                min_data_length=self.MIN_DATA_LENGTH,
                                                                vol_quantile=self.VOL_QUANTILE, API=self.API)
        top_coins_history, bottom_coins_history = momentum_ranking_for_pairs_histories(
            pairs_history_df_list=pairs_history_df_list,
            momentum_period=self.MOMENTUM_PERIOD,
            top_number=self.TOP_NUMBER)
        top_coins_parity = calc_portfolio_parity(pairs_history_df_list=top_coins_history, NATR_period=self.NATR_PERIOD,
                                                 winsor_trim=True)
        bottom_coins_parity = calc_portfolio_parity(pairs_history_df_list=bottom_coins_history,
                                                    NATR_period=self.NATR_PERIOD, winsor_trim=True)
        allocation_top_now = {pair_history["pair"][-1]: pair_history["weight_ccy"][-1] for pair_history in
                              top_coins_parity}
        allocation_bottom_now = {pair_history["pair"][-1]: pair_history["weight_ccy"][-1] for pair_history in
                                 bottom_coins_parity}



if __name__ == "__main__":
    LongShortMomBasket().main()
