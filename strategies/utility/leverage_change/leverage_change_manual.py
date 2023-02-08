from API.API_exchange_initiator import ExchangeAPISelect
from CCXT.CCXT_functions_mine import change_leverage_and_mode_on_all_exchange_pairs
from general.log_config import ConfigureLoguru

logger = ConfigureLoguru().info_level()


class LeverageChange:
    def __init__(self):
        self.LEVERAGE = 2
        self.ISOLATED = True
        self.API = ExchangeAPISelect().bybit_trade()

    def main(self):
        change_leverage_and_mode_on_all_exchange_pairs(leverage=self.LEVERAGE, isolated=self.ISOLATED, API=self.API)


if __name__ == "__main__":
    LeverageChange().main()
