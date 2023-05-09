from exchange.leverage_change import change_leverage_and_mode_for_whole_exchange
from exchange.fundamental_template import FundamentalTemplate
from utils.log_config import ConfigureLoguru

logger = ConfigureLoguru().info_level()


class _BaseTemplate(FundamentalTemplate):
    def __init__(self):
        self.EXCHANGE_MODE: int = 6
        super().__init__(exchange_mode=self.EXCHANGE_MODE)

        self.LEVERAGE = 5
        self.ISOLATED = True


class LeverageChange(_BaseTemplate):
    """Used to manually change leverage and margin mode on all pairs on the exchange"""

    def main(self):
        change_leverage_and_mode_for_whole_exchange(leverage=self.LEVERAGE, isolated=self.ISOLATED, API=self.API)


if __name__ == "__main__":
    LeverageChange().main()
