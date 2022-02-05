import time
import pandas as pd
from pandas import DataFrame as df
from datetime import datetime

import schedule
import telegram_send
import dataframe_image as dfi
import logging


logging.getLogger('matplotlib.font_manager').disabled = True

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

def jebany_performance():
    def performance(Performance_dataframe):
        Today_ROI = Performance_dataframe.iloc[-1]["ROI"]
        Today_profit = Performance_dataframe.iloc[-1]["Profit_value"]
        Last_day_ROI = Performance_dataframe.iloc[-2]["ROI"]
        Last_day_profit = Performance_dataframe.iloc[-2]["Profit_value"]
        Week_profit = Performance_dataframe.iloc[-1]["Week_profit"]
        In_play = Performance_dataframe.iloc[-1]["In_play"]
        One_performance = {"Today ROI": Today_ROI, "Today profit": Today_profit, "Last day ROI": Last_day_ROI,
                           "Last day profit": Last_day_profit, "7 days profit": Week_profit, "In play": In_play}
        return One_performance

    Kucoin_USDT_performance = performance(pd.read_excel("AB_Bot_Kucoin/Performance.xlsx"))
    Binance_USDT_performance = performance(pd.read_excel("Portfolio_clean_Binance/Performance.xlsx"))
    Huobi_USDT_performance = performance(pd.read_excel("AB_Bot_Huobi_USDT/Performance.xlsx"))
    Gateio_USDT_performance = performance(pd.read_excel("AB_Bot_Gateio_USDT/Performance.xlsx"))
    # Kucoin_BTC_performance = performance(pd.read_excel("AB_Bot_Kucoin_BTC/Performance.xlsx"))
    # Huobi_BTC_performance = performance(pd.read_excel("AB_Bot_Huobi_BTC/Performance.xlsx"))

    Global_performance = {"Kucoin USDT": Kucoin_USDT_performance,
                          "Binance USDT": Binance_USDT_performance,
                          "Huobi USDT": Huobi_USDT_performance,
                          "Gateio USDT": Gateio_USDT_performance,}
                          # "Kucoin BTC": Kucoin_BTC_performance,
                          # "Huobi BTC": Huobi_BTC_performance,}

    Global_performance_dataframe = df(Global_performance)
    Global_performance_dataframe.loc[Global_performance_dataframe.index[0], "SUM/MEAN"] = Global_performance_dataframe.mean(axis = 1)["Today ROI"]
    Global_performance_dataframe.loc[Global_performance_dataframe.index[1], "SUM/MEAN"] = Global_performance_dataframe.sum(axis = 1)["Today profit"]
    Global_performance_dataframe.loc[Global_performance_dataframe.index[2], "SUM/MEAN"] = Global_performance_dataframe.mean(axis = 1)["Last day ROI"]
    Global_performance_dataframe.loc[Global_performance_dataframe.index[3], "SUM/MEAN"] = Global_performance_dataframe.sum(axis = 1)["Last day profit"]
    Global_performance_dataframe.loc[Global_performance_dataframe.index[4], "SUM/MEAN"] = Global_performance_dataframe.sum(axis = 1)["7 days profit"]
    Global_performance_dataframe.loc[Global_performance_dataframe.index[5], "SUM/MEAN"] = Global_performance_dataframe.mean(axis = 1)["In play"]

    For_print_global_performance_dataframe = Global_performance_dataframe.copy()
    For_print_global_performance_dataframe.iloc[0] = [f"{x:.2%}" for x in For_print_global_performance_dataframe.iloc[0]]
    For_print_global_performance_dataframe.iloc[1] = [f"${x:,.2f}" for x in For_print_global_performance_dataframe.iloc[1]]
    For_print_global_performance_dataframe.iloc[2] = [f"{x:.2%}" for x in For_print_global_performance_dataframe.iloc[2]]
    For_print_global_performance_dataframe.iloc[3] = [f"${x:,.2f}" for x in For_print_global_performance_dataframe.iloc[3]]
    For_print_global_performance_dataframe.iloc[4] = [f"${x:,.2f}" for x in For_print_global_performance_dataframe.iloc[4]]
    For_print_global_performance_dataframe.iloc[5] = [f"{x:.2%}" for x in For_print_global_performance_dataframe.iloc[5]]

    Exact_time_now = datetime.now()
    print(f"\nTime now: {Exact_time_now.strftime('%H:%M:%S')}")
    print(For_print_global_performance_dataframe)
    dfi.export(For_print_global_performance_dataframe,"Global_performance.png", table_conversion = "matplotlib")
    with open("Global_performance.png", "rb") as f:
        telegram_send.send(messages = [f"Time now: {Exact_time_now.strftime('%H:%M:%S')}"], images = [f])
    f.close()

jebany_performance()
schedule.every(6).minutes.do(jebany_performance)

while True:
    try:
        schedule.run_pending()
        time.sleep(5)
    except Exception as Error:
        print(f"O CHUJ {Error}")
