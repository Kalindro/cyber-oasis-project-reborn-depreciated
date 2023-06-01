from CyberOasisProjectReborn.CEFI.exchange import ExchangeFunctions
from CyberOasisProjectReborn.CEFI.exchange import FundamentalTemplate
from CyberOasisProjectReborn.utils.log_config import Loguru

logger = Loguru().info_level()


class _BaseTemplate(FundamentalTemplate):
    def __init__(self):
        self.EXCHANGE_MODE: int = 6
        super().__init__(exchange_mode=self.EXCHANGE_MODE)

        self.LEVERAGE = 5
        self.ISOLATED = True


class LeverageChange(_BaseTemplate):
    """Used to manually change leverage and margin mode on all pairs on the exchange"""

    def main(self):
        ExchangeFunctions(self.API).change_leverage_and_mode_for_whole_exchange(leverage=self.LEVERAGE, isolated=self.ISOLATED)


if __name__ == "__main__":
    LeverageChange().main()
