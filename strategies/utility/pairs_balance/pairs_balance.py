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
        self.TIMEFRAME = "1h"  # Don't change
        self.PAIR_A = "ETH/USDT"
        self.PAIR_B = "RNDR/USDT"
        self.INVESTMENT = 1000
        self.NUMBER_OF_LAST_CANDLES = 1000  # Don't change
        self.CORES_USED = 6

        self.API = select_exchange_mode(self.EXCHANGE_MODE)


class PairsBalance(_BaseSettings):
    def main(self):
        history_a = GetFullHistoryDF.main(pair=self.PAIR_A, timeframe=self.TIMEFRAME, API=self.API,
                                          number_of_last_candles=self.NUMBER_OF_LAST_CANDLES)
        history_b = GetFullHistoryDF.main(pair=self.PAIR_B, timeframe=self.TIMEFRAME, API=self.API,
                                          number_of_last_candles=self.NUMBER_OF_LAST_CANDLES)

        returns_a = history_a["close"].pct_change().dropna()
        returns_b = history_b["close"].pct_change().dropna()
        std_a = np.std(returns_a)
        std_b = np.std(returns_b)
        correlation = np.corrcoef(returns_a, returns_b)[0][1]

        if std_a > std_b:
            position_a = self.INVESTMENT / 2 * (1 - abs(correlation))
            position_b = self.INVESTMENT / 2 * (1 + abs(correlation))
        else:
            position_a = self.INVESTMENT / 2 * (1 + abs(correlation))
            position_b = self.INVESTMENT / 2 * (1 - abs(correlation))

        print(f"Correlation: {correlation}")
        print(f"Position A: {position_a}")
        print(f"Position B: {position_b}")


if __name__ == "__main__":
    PairsBalance().main()
