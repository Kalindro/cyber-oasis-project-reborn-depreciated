import time

from Gieldy.Binance.API_initiation_Binance_AB_USDT import API_initiation as API_Binance_AB_USDT
from Gieldy.Refractor_general.Get_history import get_history_full
from Strategies.Utility.Global_cancel_all_orders.AB_global_cancel_all_orders import AB_global_cancel_all_orders

from datetime import datetime, timedelta
from talib import ROCP, NATR

import schedule
import shelve


class GlobalCancelOverseer:
    def __init__(self):
        self.natr_period = 23
        self.overseer_refresh_minutes = 2
        self.natr_multiplier = 0.75
        self.time_now = datetime.now()
        self.start = datetime.now() - timedelta(days=self.natr_period + 7)

    def global_cancel(self):
        btc_history = get_history_full(pair="BTC/USDT", timeframe="1D", start=self.start, end=self.time_now,
                                       fresh_live_history=True, API=API_Binance_AB_USDT())
        btc_history["rocp"] = round(ROCP(btc_history["close"], timeperiod=1), 4)
        btc_history["natr"] = round(NATR(btc_history["high"], btc_history["low"], btc_history["close"],
                                         timeperiod=self.natr_period), 4) * self.natr_multiplier / 100

        A = btc_history["rocp"][-1] < -btc_history["natr"][-1]
        B = btc_history["rocp"][-2] < -btc_history["natr"][-2]
        C = btc_history["rocp"][-1] + btc_history["rocp"][-2] < (-btc_history["natr"][-1]) + (-btc_history["natr"][-2])
        D = btc_history["rocp"][-1] + btc_history["rocp"][-2] + btc_history["rocp"][-3] < (
            -btc_history["natr"][-1]) + (-btc_history["natr"][-2]) + (-btc_history["natr"][-3])

        shelf = shelve.open("config\config")
        if A or B or C or D:
            if not shelf["canceled"]:
                print("Emergency situation")
                AB_global_cancel_all_orders()
                shelf["canceled"] = True
            else:
                print("Emergency situation, already canceled")
        else:
            print("Under control, no cancellation")
            shelf["canceled"] = False

        shelf.close()

    def run_global_cancel_overseer(self):
        self.global_cancel()
        schedule.every(self.overseer_refresh_minutes).minutes.do(self.global_cancel)
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == "__main__":

    GlobalCancelOverseer().run_global_cancel_overseer()
