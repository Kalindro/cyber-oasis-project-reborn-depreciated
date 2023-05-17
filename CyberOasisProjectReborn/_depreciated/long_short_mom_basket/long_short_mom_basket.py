from prime_functions.portfolio_alocations import calc_portfolio_parity
from prime_functions.momentums import momentum_calculation_for_pairs_histories
from CEFI.get_full_history import get_full_history_for_pairs_list
from CyberOasisProjectReborn.CEFI.exchange.fundamental_template import FundamentalTemplate
from CyberOasisProjectReborn.utils.log_config import ConfigureLoguru

logger = ConfigureLoguru().info_level()


class _BaseTemplate(FundamentalTemplate):
    def __init__(self):
        self.EXCHANGE_MODE: int = 1
        self.PAIRS_MODE: int = 4
        super().__init__(exchange_mode=self.EXCHANGE_MODE, pairs_mode=self.PAIRS_MODE)

        self.PERIODS = dict(MOMENTUM=168,
                            NATR=128,
                            )
        self.TIMEFRAME = "1h"
        self.NUMBER_OF_LAST_CANDLES = 1000

        self.MIN_VOLUME = 10
        self.MIN_DATA_LENGTH = 10
        self.VOL_QUANTILE_DROP = 0.35
        self.TOP_NUMBER = 20


class LongShortMomBasket(_BaseTemplate):
    def main(self):
        pairs_history_df_list = get_full_history_for_pairs_list(pairs_list=self.pairs_list, timeframe=self.TIMEFRAME,
                                                                number_of_last_candles=self.NUMBER_OF_LAST_CANDLES,
                                                                min_data_length=self.MIN_DATA_LENGTH,
                                                                vol_quantile_drop=self.VOL_QUANTILE_DROP, API=self.API)
        top_coins_history, bottom_coins_history = momentum_calculation_for_pairs_histories(
            pairs_history_df_list=pairs_history_df_list,
            momentum_period=self.PERIODS["MOMENTUM"],
            top_number=self.TOP_NUMBER)
        top_coins_parity = calc_portfolio_parity(pairs_history_df_list=top_coins_history,
                                                 NATR_period=self.PERIODS["NATR"],
                                                 winsor_trim=True)
        bottom_coins_parity = calc_portfolio_parity(pairs_history_df_list=bottom_coins_history,
                                                    NATR_period=self.PERIODS["NATR"], winsor_trim=True)
        allocation_top_now = {pair_history["pairs_list"][-1]: pair_history["weight_ccy"][-1] for pair_history in
                              top_coins_parity}
        allocation_bottom_now = {pair_history["pairs_list"][-1]: pair_history["weight_ccy"][-1] for pair_history in
                                 bottom_coins_parity}


if __name__ == "__main__":
    LongShortMomBasket().main()
