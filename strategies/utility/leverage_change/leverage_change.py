from API.API_exchange_initiator import ExchangeAPISelect
from CCXT.CCXT_functions_mine import change_leverage_and_mode_on_all_exchange_pairs
from general.log_config import ConfigureLoguru

logger = ConfigureLoguru().info_level()


class LeverageChange:
    def __init__(self):
        self.LEVERAGE = 2
        self.ISOLATED = True

    def main(self, API):
        change_leverage_and_mode_on_all_exchange_pairs(leverage=self.LEVERAGE, API=API)


if __name__ == "__main__":
    API = ExchangeAPISelect().bybit_trade()
    LeverageChange().main(API=API)
