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
        self.natr_period = 21
        self.overseer_refresh_minutes = 2
        self.natr_multiplier = 0.75
        self.time_now = datetime.now()
        self.start = datetime.now() - timedelta(days=180)

    def global_cancel(self):
        btc_history = get_history_full(pair="BTC/USDT", timeframe="1D", start=self.start, end=self.time_now,
                                       fresh_live_history=True, API=API_Binance_AB_USDT())

        btc_history["high_2d"] = btc_history["high"].rolling(2).max()
        btc_history["low_2d"] = btc_history["low"].rolling(2).min()
        btc_history["close_2d"] = btc_history["close"]

        btc_history["high_3d"] = btc_history["high"].rolling(3).max()
        btc_history["low_3d"] = btc_history["low"].rolling(3).min()
        btc_history["close_3d"] = btc_history["close"]

        btc_history["natr_1d"] = round(NATR(btc_history["high"], btc_history["low"], btc_history["close"],
                                         timeperiod=self.natr_period) * self.natr_multiplier / 100, 4)
        btc_history["rocp_1d"] = round(ROCP(btc_history["close"], timeperiod=1), 4)

        btc_history["natr_2d"] = round(NATR(btc_history["high_2d"], btc_history["low_2d"], btc_history["close_2d"],
                                            timeperiod=self.natr_period) * self.natr_multiplier / 100, 4)
        btc_history["rocp_2d"] = round(ROCP(btc_history["close"], timeperiod=2), 4)

        btc_history["natr_3d"] = round(NATR(btc_history["high_3d"], btc_history["low_3d"], btc_history["close_3d"],
                                         timeperiod=self.natr_period) * self.natr_multiplier / 100, 4)
        btc_history["rocp_3d"] = round(ROCP(btc_history["close"], timeperiod=3), 4)

        A_1d = btc_history["rocp_1d"][-1] < -btc_history["natr_1d"][-1]
        B_1d = btc_history["rocp_1d"][-2] < -btc_history["natr_1d"][-2]

        A_2d = btc_history["rocp_2d"][-1] < -btc_history["natr_2d"][-1]
        B_2d = btc_history["rocp_2d"][-2] < -btc_history["natr_2d"][-2]

        A_3d = btc_history["rocp_3d"][-1] < -btc_history["natr_3d"][-1]
        B_3d = btc_history["rocp_3d"][-2] < -btc_history["natr_3d"][-2]

        shelf = shelve.open("config\config")

        if "canceled" not in shelf: shelf["canceled"] = None
        if A_1d or B_1d or A_3d or B_3d:
            if A_1d: print(f'1D_ROCP: {btc_history["rocp_1d"][-1]} surpassed 1D_NATR: {-btc_history["natr_1d"][-1]} (now)')
            if B_1d: print(f'1D_ROCP: {btc_history["rocp_1d"][-2]} surpassed 1D_NATR: {-btc_history["natr_1d"][-2]} (yesterday)')
            if A_2d: print(f'2D_ROCP: {btc_history["rocp_2d"][-1]} surpassed 2D_NATR: {-btc_history["natr_2d"][-1]} (now)')
            if B_2d: print(f'2D_ROCP: {btc_history["rocp_2d"][-2]} surpassed 2D_NATR: {-btc_history["natr_2d"][-2]} (yesterday)')
            if A_3d: print(f'3D_ROCP: {btc_history["rocp_3d"][-1]} surpassed 3D_NATR: {-btc_history["natr_3d"][-1]} (now)')
            if B_3d: print(f'3D_ROCP: {btc_history["rocp_3d"][-2]} surpassed 3D_NATR: {-btc_history["natr_3d"][-2]} (yesterday)')
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
