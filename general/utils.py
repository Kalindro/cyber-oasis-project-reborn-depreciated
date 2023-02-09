import datetime as dt
import math
import time

import dateutil.parser
import pandas as pd
from pandas import ExcelWriter


def date_string_to_datetime(date_string) -> dt.datetime:
    date_datetime = dateutil.parser.parse(date_string)
    return date_datetime


def datetime_to_timestamp_ms(date_datetime) -> int:
    date_timestamp = int(time.mktime(date_datetime.timetuple()) * 1000)
    return date_timestamp


def timestamp_ms_to_datetime(timestamp_ms) -> dt.datetime:
    date_datetime = dt.datetime.fromtimestamp(timestamp_ms / 1000.0)
    return date_datetime


def dataframe_is_not_none_and_has_elements(dataframe: pd.DataFrame) -> bool:
    if (dataframe is not None) and (not dataframe.empty):
        if len(dataframe) >= 2:
            return True


def excel_save_formatted(dataframe, global_cols_size, cash_cols, cash_cols_size, rounded_cols, rounded_cols_size,
                         perc_cols, perc_cols_size) -> None:
    writer = ExcelWriter("performance.xlsx", engine="xlsxwriter")
    with writer as writer:
        dataframe.to_excel(writer, sheet_name="main")
        workbook = writer.book
        for worksheet_name in writer.sheets.keys():
            worksheet = writer.sheets[worksheet_name]
            header_format = workbook.add_format({
                "valign": "vcenter",
                "align": "center",
                "bold": True,
            })
            cash_formatting = workbook.add_format({"num_format": "$#,##0"})
            rounded_formatting = workbook.add_format({"num_format": "0.00"})
            perc_formatting = workbook.add_format({"num_format": "0.00%"})
            worksheet.set_row(0, cell_format=header_format)
            worksheet.set_column("A:AAA", global_cols_size, None)

            columns = [(cash_cols, cash_cols_size, cash_formatting),
                       (rounded_cols, rounded_cols_size, rounded_formatting),
                       (perc_cols, perc_cols_size, perc_formatting)]

            for cols, cols_size, formatting in columns:
                if cols is not None:
                    args = [cols] if cols_size is None else [cols, cols_size]
                    worksheet.set_column(*args, formatting)


def timeframe_to_timestamp_ms(timeframe) -> int:
    timeframe = timeframe.lower()
    if timeframe == "1m" or timeframe == "1min":
        timestamp_ms = 60000
    elif timeframe == "5m" or timeframe == "5min":
        timestamp_ms = 300000
    elif timeframe == "15m" or timeframe == "15min":
        timestamp_ms = 900000
    elif timeframe == "30m" or timeframe == "30min":
        timestamp_ms = 1800000
    elif timeframe == "1h" or timeframe == "60m" or timeframe == "60min":
        timestamp_ms = 3600000
    elif timeframe == "4h":
        timestamp_ms = 14400000
    elif timeframe == "12h":
        timestamp_ms = 43200000
    elif timeframe == "24h" or timeframe == "1d":
        timestamp_ms = 86400000
    elif timeframe == "7d" or timeframe == "1w":
        timestamp_ms = 604800000
    else:
        raise ValueError("Unsupported TIMEFRAME to convert to timestamp")
    return int(timestamp_ms)


def round_down(x) -> float:
    return math.trunc(x * 4) / 4


def round_up(x) -> float:
    return math.ceil(x * 4) / 4


def omit_arg(d, *args):
    if isinstance(d, dict):
        result = d.copy()
        for arg in args:
            if type(arg) is list:
                for key in arg:
                    if key in result:
                        del result[key]
            else:
                if arg in result:
                    del result[arg]
        return result
    return d
