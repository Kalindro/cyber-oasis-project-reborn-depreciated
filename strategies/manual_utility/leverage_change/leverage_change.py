from dataclasses import dataclass

from generic.funcs_for_pairs_lists import change_leverage_and_mode_for_whole_exchange
from generic.select_mode import select_exchange_mode
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
    LEVERAGE = 3
    ISOLATED = True

    def __post_init__(self):
        self.API = select_exchange_mode(self.EXCHANGE_MODE)


class LeverageChange(_BaseSettings):
    """Used to manually change leverage and margin mode on all pairs on the exchange"""

    def main(self):
        change_leverage_and_mode_for_whole_exchange(leverage=self.LEVERAGE, isolated=self.ISOLATED, API=self.API)


if __name__ == "__main__":
    LeverageChange().main()
