from dataclasses import dataclass

from generic.funcs_for_pairs_lists import change_leverage_and_mode_for_whole_exchange
from generic.select_mode import FundamentalSettings
from utils.log_config import ConfigureLoguru

logger = ConfigureLoguru().info_level()


@dataclass
class _BaseSettings(FundamentalSettings):
    EXCHANGE_MODE: int = 1
    LEVERAGE = 3
    ISOLATED = True


class LeverageChange(_BaseSettings):
    """Used to manually change leverage and margin mode on all pairs on the exchange"""

    def main(self):
        change_leverage_and_mode_for_whole_exchange(leverage=self.LEVERAGE, isolated=self.ISOLATED, API=self.API)


if __name__ == "__main__":
    LeverageChange().main()
