from dataclasses import dataclass
from functools import partial

import numpy as np
from scipy.stats import linregress

from CCXT.CCXT_functions_mine import select_exchange_mode
from CCXT.get_full_history import GetFullHistoryDF
from general.log_config import ConfigureLoguru

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
    PAIR_SHORT: str = "ETH/USDT"
    PAIR_BENCHMARK: str = "BTC/USDT"
    INVESTMENT: int = 2300
    NUMBER_OF_LAST_CANDLES: int = 170

    def __post_init__(self):
        self.API = select_exchange_mode(self.EXCHANGE_MODE)


class PairsBalance(_BaseSettings):

    def beta_neutral(self):
        pairs = {
            "long_pair": self.PAIR_LONG,
            "short_pair": self.PAIR_SHORT,
            "benchmark": self.PAIR_BENCHMARK
        }

        hist_func_partial = partial(GetFullHistoryDF.main, timeframe=self.TIMEFRAME, API=self.API,
                                    number_of_last_candles=self.NUMBER_OF_LAST_CANDLES)
        histories = {pair_side: hist_func_partial(pair=pair_name) for pair_side, pair_name in pairs.items()}

        for pair_side, history_df in histories.items():
            history_df["returns"] = np.log(history_df["close"])
        for pair_side, history_df in histories.items():
            history_df["slope"] = linregress(x=histories["benchmark"]["returns"], y=history_df["returns"])[0]

        beta_long_pair = histories["long_pair"]["slope"][-1]
        beta_short_pair = histories["short_pair"]["slope"][-1]
        total_beta = beta_long_pair + beta_short_pair
        allocation_long_pair = (total_beta - beta_long_pair) / total_beta * self.INVESTMENT
        allocation_short_pair = (total_beta - beta_short_pair) / total_beta * -self.INVESTMENT

        print(f"Allocation to {self.PAIR_LONG}: {allocation_long_pair}")
        print(f"Allocation to {self.PAIR_SHORT}: {allocation_short_pair}")


if __name__ == "__main__":
    PairsBalance().beta_neutral()
