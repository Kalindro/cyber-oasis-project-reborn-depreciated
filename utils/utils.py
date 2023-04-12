import datetime as dt
import math
import time
from datetime import timedelta

import numpy as np
import pandas as pd
import vectorbtpro as vbt
from cleantext import clean
from loguru import logger
from pandas import ExcelWriter
from scipy.stats import linregress
from vectorbtpro.utils.datetime_ import get_local_tz


def clean_string(message: str) -> str:
    message = clean(message, no_urls=True, replace_with_url="", no_emoji=True)
    return message


def datetime_now_in_UTC() -> dt.datetime:
    date_datetime = dt.datetime.now(dt.timezone.utc)
    return date_datetime


def date_string_to_UTC_datetime(date_string: str) -> dt.datetime:
    date_datetime = pd.to_datetime(date_string, dayfirst=True).tz_localize(get_local_tz()).tz_convert("UTC")
    return date_datetime


def datetime_to_timestamp_ms(date_datetime: dt.datetime) -> int:
    date_timestamp = int(time.mktime(date_datetime.timetuple()) * 1000)
    return date_timestamp


def timestamp_ms_to_datetime(timestamp_ms: int) -> dt.datetime:
    date_datetime = pd.to_datetime(timestamp_ms / 1000.0, dayfirst=True).tz_localize(get_local_tz()).tz_convert("UTC")
    return date_datetime


def cut_exact_df_dates(pre_dataframe: pd.DataFrame, start: dt.datetime, end: dt.datetime) -> pd.DataFrame:
    """Cut the dataframe to exactly match the desired since/end, small quirk here as end_datetime can be precise to
     the second while the TIMEFRAME may be 1D - it would never return correctly, mainly when the end is now"""
    cut_dataframe = pre_dataframe.loc[start:min(pre_dataframe.iloc[-1].name, end)]

    return cut_dataframe


def dataframe_is_not_none_and_not_empty(dataframe: pd.DataFrame) -> bool:
    if dataframe is not None:
        if not dataframe.empty:
            return True


def excel_save_formatted(dataframe: pd.DataFrame, filename: str,
                         global_cols_size: int = None,
                         cash_cols: str = None,
                         cash_cols_size: int = None,
                         rounded_cols: str = None,
                         rounded_cols_size: int = None,
                         perc_cols: str = None,
                         perc_cols_size: int = None,
                         str_cols: str = None,
                         str_cols_size: int = None) -> None:
    """Save dataframe as formatted excel"""
    count = 1
    while True:
        try:
            writer = ExcelWriter(filename, engine="xlsxwriter")
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
                    str_formatting = workbook.add_format(None)
                    worksheet.set_row(0, cell_format=header_format)
                    worksheet.set_column("A:AAA", global_cols_size, None)

                    columns = [(cash_cols, cash_cols_size, cash_formatting),
                               (rounded_cols, rounded_cols_size, rounded_formatting),
                               (perc_cols, perc_cols_size, perc_formatting),
                               (str_cols, str_cols_size, str_formatting)]

                    for cols, cols_size, formatting in columns:
                        if cols is not None:
                            worksheet.set_column(cols, width=cols_size, cell_format=formatting)
                break

        except PermissionError as e:
            if "denied" in str(e):
                filename = f"{filename.split('.')[0].split('_')[0]}_{count}.xlsx"
                logger.warning(f"Trying to save as v{count} because access denied")
                count += 1
            else:
                raise e


def timeframe_to_timestamp_ms(timeframe: str) -> int:
    """Convert timeframe string to timestamp in milliseconds"""
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
        raise ValueError("Unsupported timeframe to convert to timestamp")

    return int(timestamp_ms)


def timeframe_to_timedelta(timeframe: str) -> dt.timedelta:
    """Convert timeframe string to timestamp in milliseconds"""
    timeframe = timeframe.lower()
    if timeframe == "1m" or timeframe == "1min":
        my_timedelta = timedelta(minutes=1)
    elif timeframe == "5m" or timeframe == "5min":
        my_timedelta = timedelta(minutes=5)
    elif timeframe == "15m" or timeframe == "15min":
        my_timedelta = timedelta(minutes=15)
    elif timeframe == "30m" or timeframe == "30min":
        my_timedelta = timedelta(minutes=30)
    elif timeframe == "1h" or timeframe == "60m" or timeframe == "60min":
        my_timedelta = timedelta(hours=1)
    elif timeframe == "4h":
        my_timedelta = timedelta(hours=4)
    elif timeframe == "12h":
        my_timedelta = timedelta(hours=12)
    elif timeframe == "24h" or timeframe == "1d":
        my_timedelta = timedelta(hours=24)
    elif timeframe == "7d" or timeframe == "1w":
        my_timedelta = timedelta(days=7)
    else:
        raise ValueError("Unsupported timeframe to convert to timedelta")

    return my_timedelta


def round_down(x: float) -> float:
    return math.trunc(x * 4) / 4


def round_up(x: float) -> float:
    return math.ceil(x * 4) / 4


def _momentum_calc_for_vbt_data(vbt_data: vbt.Data, momentum_period: int) -> dict[str: pd.DataFrame]:
    """Calculate momentum ranking for list of history dataframes"""
    logger.info("Calculating momentum ranking for pairs histories")

    return vbt_data.close.rolling(momentum_period).apply(_legacy_momentum_calculate)


def _legacy_momentum_calculate(price_closes: pd.DataFrame) -> float:
    """Calculating momentum from close"""
    returns = np.log(price_closes)
    x = np.arange(len(returns))
    slope, _, rvalue, _, _ = linregress(x, returns)
    slope = slope * 100

    return (((np.exp(slope) ** 252) - 1) * 100) * (rvalue ** 2)
