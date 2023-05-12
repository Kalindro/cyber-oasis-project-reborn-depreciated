import numpy as np
import vectorbtpro as vbt

from exchange.fundamental_template import FundamentalTemplate
from scripts.backtest.backtest_template import BacktestTemplate
from scripts.backtest.momentum_rank.momentum_allocation import MomentumStrat
from utils.log_config import ConfigureLoguru

logger = ConfigureLoguru().info_level()

vbt.settings['plotting']['layout']['width'] = 1800
vbt.settings['plotting']['layout']['height'] = 800
vbt.settings.set_theme("seaborn")

vbt.settings.portfolio['init_cash'] = 1000
vbt.settings.portfolio['fees'] = 0.001
vbt.settings.portfolio['slippage'] = 0
vbt.settings.portfolio.stats['incl_unrealized'] = True


class _BaseTemplate(FundamentalTemplate):
    def __init__(self):
        self.EXCHANGE_MODE: int = 1
        self.PAIRS_MODE: int = 4
        super().__init__(exchange_mode=self.EXCHANGE_MODE, pairs_mode=self.PAIRS_MODE)

        self.PERIODS = dict(MOMENTUM=[20],  # np.arange(2, 60, 2),
                            NATR=False,  # np.arange(2, 100, 2)
                            BTC_SMA=False,  # np.arange(2, 100, 2)
                            TOP_NUMBER=20,
                            REBALANCE=["1d"],  # ["8h", "12h", "1d", "2d", "4d"]
                            )

        self.SAVE_LOAD_HISTORY: bool = True
        self.VOL_QUANTILE_DROP = 0.2
        self.TIMEFRAME: str = "4h"
        self.START: str = "01.01.2022"
        self.END: str = "01.01.2023"


class MainBacktest(_BaseTemplate, BacktestTemplate):
    """Main class with backtesting template"""

    def __init__(self):
        super().__init__()
        self.current_strat = MomentumStrat().momentum_strat

    def run(self):
        self._run()


if __name__ == "__main__":
    MainBacktest().run()
    a = np.log(5)  # So formatter won't remove np from import
