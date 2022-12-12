import time
import math
import datetime as dt
import dateutil.parser
import pandas as pd
from pandas import ExcelWriter as ExcelWriter


def date_string_to_datetime(date_string):
    date_datetime = dateutil.parser.parse(date_string)
    return date_datetime


def datetime_to_timestamp_ms(date_datetime):
    date_timestamp = int(time.mktime(date_datetime.timetuple()) * 1000)
    return date_timestamp


def timestamp_ms_to_datetime(timestamp_ms):
    date_datetime = dt.datetime.fromtimestamp(timestamp_ms / 1000.0)
    return date_datetime


def dataframe_is_not_none_and_has_elements(dataframe: pd.DataFrame) -> bool:
    return dataframe is not None and not dataframe.empty


def excel_save_formatted(dataframe, column_size, cash_cols, rounded_cols, perc_cols):
    writer = ExcelWriter("performance.xlsx", engine="xlsxwriter")
    with writer as writer:
        dataframe.to_excel(writer, sheet_name="main")
        workbook = writer.book
        for worksheet_name in writer.sheets.keys():
            worksheet = writer.sheets[worksheet_name]
            worksheet.set_column("B:AA", column_size)
            header_format = workbook.add_format({
                "valign": "vcenter",
                "align": "center",
                "bold": True,
            })
            cash_format = workbook.add_format({"num_format": "$#,##0"})
            rounded_format = workbook.add_format({"num_format": "0.00"})
            perc_format = workbook.add_format({"num_format": "0.00%"})
            worksheet.set_row(0, cell_format=header_format)
            worksheet.set_column(cash_cols, None, cash_format)
            worksheet.set_column(rounded_cols, None, rounded_format)
            worksheet.set_column(perc_cols, None, perc_format)


def timeframe_to_timestamp_ms(timeframe):
    if timeframe.lower() == "1m":
        timestamp_ms = 60000
    elif timeframe.lower() == "5m":
        timestamp_ms = 300000
    elif timeframe.lower() == "15m":
        timestamp_ms = 900000
    elif timeframe.lower() == "30m":
        timestamp_ms = 1800000
    elif timeframe.lower() == "1h" or timeframe.lower() == "60m":
        timestamp_ms = 3600000
    elif timeframe.lower() == "4h":
        timestamp_ms = 14400000
    elif timeframe.lower() == "12h":
        timestamp_ms = 43200000
    elif timeframe.lower() == "24h" or timeframe.lower() == "1d":
        timestamp_ms = 86400000
    elif timeframe.lower() == "7d" or timeframe.lower() == "1w":
        timestamp_ms = 604800000
    else:
        raise ValueError("Unsupported timeframe to convert to timestamp")
    return int(timestamp_ms)


def round_down(x):
    return math.trunc(x * 4) / 4


def round_up(x):
    return math.ceil(x * 4) / 4