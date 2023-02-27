from generic.funcs_for_pairs_histories import calc_portfolio_parity
from generic.funcs_for_pairs_histories import momentum_ranking_for_pairs_histories
from generic.funcs_for_pairs_lists import get_full_history_for_pairs_list
from generic.select_mode import FundamentalSettings
from utils.log_config import ConfigureLoguru

logger = ConfigureLoguru().info_level()


class _BaseSettings(FundamentalSettings):
    def __init__(self):
        super().__init__()
        self.EXCHANGE_MODE: int = 1
        self.PAIRS_MODE: int = 4
        self.SETTINGS = dict(MIN_VOLUME=10,
                             MIN_DATA_LENGTH=10,
                             VOL_QUANTILE_DROP=0.35,
                             TOP_NUMBER=20, )
        self.PERIODS = dict(MOMENTUM=168,
                            NATR=128,
                            )
        self.HISTORY = dict(TIMEFRAME="1h",
                            NUMBER_OF_LAST_CANDLES=1000,
                            )


class LongShortMomBasket(_BaseSettings):
    def main(self):
        pairs_history_df_list = get_full_history_for_pairs_list(pairs_list=self.pairs_list, timeframe=self.TIMEFRAME,
                                                                number_of_last_candles=self.NUMBER_OF_LAST_CANDLES,
                                                                min_data_length=self.MIN_DATA_LENGTH,
                                                                vol_quantile_drop=self.VOL_QUANTILE_DROP, API=self.API)
        top_coins_history, bottom_coins_history = momentum_ranking_for_pairs_histories(
            pairs_history_df_list=pairs_history_df_list,
            momentum_period=self.PERIODS,
            top_number=self.TOP_NUMBER)
        top_coins_parity = calc_portfolio_parity(pairs_history_df_list=top_coins_history, NATR_period=self.NATR_PERIOD,
                                                 winsor_trim=True)
        bottom_coins_parity = calc_portfolio_parity(pairs_history_df_list=bottom_coins_history,
                                                    NATR_period=self.NATR_PERIOD, winsor_trim=True)
        allocation_top_now = {pair_history["pair"][-1]: pair_history["weight_ccy"][-1] for pair_history in
                              top_coins_parity}
        allocation_bottom_now = {pair_history["pair"][-1]: pair_history["weight_ccy"][-1] for pair_history in
                                 bottom_coins_parity}

        # print(f"Bottom: {allocation_bottom_now}")


if __name__ == "__main__":
    LongShortMomBasket().main()
