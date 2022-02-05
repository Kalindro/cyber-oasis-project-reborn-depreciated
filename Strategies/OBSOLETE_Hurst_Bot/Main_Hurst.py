import time
from time import gmtime, strftime

from beepy import beep

import pandas as pd
from hurst import compute_Hc
from pandas import DataFrame as df

from Hurst_Bot.Exchange_initiation import exchange_initiation, name
from Get_history import get_history
from Get_tickers import get_tickers

Name = name()
Exchange = exchange_initiation()
Results_dataframe = df(columns = ["Pair", "Hurst"])

Tickers = get_tickers(Exchange = Exchange, Base = "USDT")

for Pair in Tickers:
    print("Working on:", Pair)
    History_dataframe = get_history(Exchange = Exchange, Pair = Pair, Timeframe = "1d", Days_of_history = 360)
    Price = History_dataframe["Close"]
    if len(History_dataframe) > 100:
        H, c, val = compute_Hc(Price)
        print(Pair, "Hurst exponent = {:.4f}".format(H))
        data = {"Pair": Pair,
                "Hurst": H,
                }

        Results_dataframe = Results_dataframe.append(data, ignore_index=True)
    else:
        print(Pair, "not 100 lenght, cya")
        pass

Results_dataframe.sort_values(by = ["Hurst"], inplace = True, ascending = False)
Results_dataframe.reset_index(drop = True, inplace = True)

writer = pd.ExcelWriter("Binance Hurst " + strftime("%Y_%m_%d %H", gmtime()) + ".xlsx", engine = "xlsxwriter")
Workbook = writer.book
Results_dataframe.to_excel(writer, sheet_name = "Hurst")
Hurst_sheet = writer.sheets["Hurst"]
Format_1 = Workbook.add_format({'num_format': '#,##0.00'})
Format_2 = Workbook.add_format({'num_format': '0.00%'})
Hurst_sheet.set_column("B:B", 13)
Hurst_sheet.set_column("C:C", 13)
writer.save()
print("Results saved")

# Beep

print("BEEPING")
beep(sound = 1)
time.sleep(1)
beep(sound = 1)