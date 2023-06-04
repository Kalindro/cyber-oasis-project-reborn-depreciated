from CyberOasisProjectReborn.CEFI.functions.fundamental_template import FundamentalTemplate
from CyberOasisProjectReborn.utils.logger_custom import LoggerCustom

logger = LoggerCustom().info_level()


class _BaseTemplate(FundamentalTemplate):
    def __init__(self):
        self.EXCHANGE_MODE: int = 4
        super().__init__(exchange_mode=self.EXCHANGE_MODE)

        self.LEVERAGE = 5
        self.ISOLATED = True


class LeverageChange(_BaseTemplate):
    """Used to manually change leverage and margin mode on all pairs on the exchange"""

    def main(self):
        self.exchange.functions.change_leverage_and_mode_for_whole_exchange(self.LEVERAGE, isolated=self.ISOLATED)


if __name__ == "__main__":
    logger = LoggerCustom().info_level()
    LeverageChange().main()
