import numpy as np

from CCXT.CCXT_functions_mine import select_exchange_mode
from CCXT.get_full_history import GetFullHistoryDF
from general.log_config import ConfigureLoguru

logger = ConfigureLoguru().info_level()


class _BaseSettings:

    def __init__(self):
        """
        Modes available:
        :EXCHANGE_MODE: 1 - Binance Spot; 2 - Binance Futures; 3 - Kucoin Spot
        :PAIRS_MODE: 1 - Test single; 2 - Test multi; 3 - BTC; 4 - USDT
        """
        self.EXCHANGE_MODE = 1
        self.TIMEFRAME = "1h"
        self.PAIR_LONG = "RNDR/USDT"
        self.PAIR_SHORT = "ETH/USDT"
        self.PAIR_BENCHMARK = "ETH/USDT"
        self.INVESTMENT = 1000
        self.NUMBER_OF_LAST_CANDLES = 100

        self.API = select_exchange_mode(self.EXCHANGE_MODE)


class PairsBalance(_BaseSettings):

    def beta_neutral(self):
        history_a = GetFullHistoryDF.main(pair=self.PAIR_LONG, timeframe=self.TIMEFRAME, API=self.API,
                                          number_of_last_candles=self.NUMBER_OF_LAST_CANDLES)
        history_b = GetFullHistoryDF.main(pair=self.PAIR_SHORT, timeframe=self.TIMEFRAME, API=self.API,
                                          number_of_last_candles=self.NUMBER_OF_LAST_CANDLES)
        history_benchmark = GetFullHistoryDF.main(pair=self.PAIR_BENCHMARK, timeframe=self.TIMEFRAME, API=self.API,
                                                  number_of_last_candles=self.NUMBER_OF_LAST_CANDLES)

        pair_A_returns = history_a["close"].pct_change().dropna()
        pair_B_returns = history_b["close"].pct_change().dropna()
        pair_BENCHMARK_returns = history_benchmark["close"].pct_change().dropna()

        beta_pair_A = np.cov(pair_A_returns, pair_BENCHMARK_returns)[0, 1] / np.var(pair_BENCHMARK_returns)
        beta_pair_B = np.cov(pair_B_returns, pair_BENCHMARK_returns)[0, 1] / np.var(pair_BENCHMARK_returns)

        total_beta = beta_pair_A + beta_pair_B

        allocation_PAIR_A = (total_beta - beta_pair_A) / total_beta * self.INVESTMENT
        allocation_PAIR_B = (total_beta - beta_pair_B) / total_beta * -self.INVESTMENT

        print(f"Allocation to {self.PAIR_LONG}: {allocation_PAIR_A}")
        print(f"Allocation to {self.PAIR_SHORT}: {allocation_PAIR_B}")


if __name__ == "__main__":
    PairsBalance().beta_neutral()
