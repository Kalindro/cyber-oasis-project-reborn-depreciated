from AB_Bot.Backtest_AB import backtest_ab
from Gieldy.Refractor_general.Get_filtered_tickers import refractor_get_backtesting_tickers

import pandas as pd
from datetime import datetime


def backtest_ab_auto(Base, API):

    Name = API["Name"]

    Tickers_list = refractor_get_backtesting_tickers(Base = Base, API = API)

    print("Short backtest start")
    Short_backtest_results_DF, Basic_file_name = backtest_ab(Desired_days = 30, Tickers_list = Tickers_list, Base = Base, API = API)
    print("Long backtest start")
    Long_backtest_results_DF, Basic_file_name = backtest_ab(Desired_days = 150, Tickers_list = Tickers_list, Base = Base, API = API)

    Ultimate_backtest_results_DF = Short_backtest_results_DF[Short_backtest_results_DF["Symbol"].isin(Long_backtest_results_DF["Symbol"])]
    Ultimate_backtest_results_DF.reset_index(drop = True, inplace = True)
    Basic_file_name = f"Ultimate {Basic_file_name}"
    Addition_name = f"{datetime.now().strftime('%Y-%m-%d %H@%M')}"
    Writer = pd.ExcelWriter(f"{Basic_file_name}, {Addition_name}.xlsx", engine = "xlsxwriter")
    Workbook = Writer.book
    Ultimate_backtest_results_DF.to_excel(Writer)

    Format_1 = Workbook.add_format({'num_format': '#,##0.00'})
    Format_2 = Workbook.add_format({'num_format': '0.00%'})
    for Sheet_name in list(Workbook.sheetnames.keys()):
        Sheet = Writer.sheets[Sheet_name]
        Results_sheet = Sheet
        Results_sheet.set_column("B:B", 10)
        Results_sheet.set_column("C:C", 13)
        Results_sheet.set_column("D:D", 13, Format_1)
        Results_sheet.set_column("E:E", 13, Format_2)
        Results_sheet.set_column("F:F", 13, Format_1)
        Results_sheet.set_column("G:G", 13, Format_2)
        Results_sheet.set_column("H:H", 13)
        Results_sheet.set_column("I:I", 13)
    Writer.save()
