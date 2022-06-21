import datetime
import os
import time
import pandas as pd
import datetime as dt

from random import randint
from datetime import timedelta, date
from pandas import DataFrame as df
from pathlib import Path

from Gieldy.Refractor_general.Main_refracting import get_history_fragment_for_func

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


# Getting history function
def get_history_full(pair, timeframe, fresh_live_history_no_save_read, API, last_n_candles=None, start=None, end=None):
    if (last_n_candles is None) and (start is None):
        raise AssertionError("Error, either last_n_candles or start has to be provided")

    print(f"Getting {pair} history")
    current_path = os.path.dirname(os.path.abspath(__file__))
    project_path = Path(current_path).parent.parent
    name = API["name"]

    if end is None: end = dt.datetime.now()

    if "huobi" in name.lower():
        exchange = "Huobi"
        request_limit_sleep = randint(2, 10)

    if "kucoin" in name.lower():
        exchange = "Kucoin"
        request_limit_sleep = randint(45, 60)

    if "binance" in name.lower():
        exchange = "Binance"
        request_limit_sleep = randint(2, 10)

    if "gate" in name.lower():
        exchange = "Gateio"
        request_limit_sleep = randint(2, 10)

    if "ftx" in name.lower():
        exchange = "FTX"
        request_limit_sleep = randint(2, 10)

    if type(start) == datetime.datetime: start = start.date()
    if type(end) == datetime.datetime: end = end.date()
    pair_for_data = pair.replace("/", "-")
    last_date = date.fromisoformat("2018-01-01")
    history_dataframe_saved = None
    ohlc = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum", "pair": "last", "symbol": "last"}

    if not fresh_live_history_no_save_read:
        try:
            history_dataframe_saved = pd.read_csv(f"{project_path}/History_data/{exchange}/15MIN/{pair_for_data}_15MIN.csv", index_col=0,
                                                  parse_dates=True)
            print("Saved history found")
            if history_dataframe_saved.iloc[-1].name.date() >= end:
                history_dataframe_saved = history_dataframe_saved.resample(f"{timeframe.upper()}").agg(ohlc).fillna(method="ffill")
                return history_dataframe_saved.loc[max(history_dataframe_saved.iloc[0].name.date(), start):end]
            else:
                last_date = history_dataframe_saved.iloc[-1].name.date() - timedelta(days=2)
                print("Just no fresh days")
                pass

        except Exception as err:
            print(f"{err}, No saved history for {pair} or too short")
            pass

    print("Getting fresh history")

    candles_limit = 800  # 33 days
    addition_limit = 75

    timeframe_for_data = "15MIN" if not fresh_live_history_no_save_read else timeframe.upper()

    if timeframe_for_data == "15MIN":
        days = candles_limit / 24 / 4
        addition = addition_limit / 24 / 4
    if timeframe_for_data == "30MIN":
        days = candles_limit / 24 / 2
        addition = addition_limit / 24 / 2
    if timeframe_for_data == "1H":
        days = candles_limit / 24
        addition = addition_limit / 24
    if timeframe_for_data == "4H":
        days = candles_limit / 6
        addition = addition_limit / 6
    if timeframe_for_data == "1D":
        days = candles_limit
        addition = addition_limit
    if timeframe_for_data == "3D":
        days = candles_limit * 3
        addition = addition_limit * 3

    delta = end - start
    days_of_history = delta.days
    since = start if fresh_live_history_no_save_read else last_date
    to = since + timedelta(days=days) + timedelta(days=addition)
    history_dataframe_final = df()

    # Start the loop for history sets merging

    while since < date.today() + timedelta(days=1):
        while True:
            try:
                time.sleep(request_limit_sleep / 10)
                history_dataframe_fresh = get_history_fragment_for_func(pair=pair, timeframe=timeframe_for_data, since=since, to=to, days_of_history=days_of_history, API=API)
                history_dataframe_final = pd.concat([history_dataframe_final, history_dataframe_fresh])
                since = since + timedelta(days=days)
                to = since + timedelta(days=days) + timedelta(days=addition)
                break
            except Exception as e:
                print(f"{e}, error on history fragments loop")

    # Adding column names and changing data type
    if len(history_dataframe_final) > 1:
        history_dataframe_final = history_dataframe_final.astype(float)
        history_dataframe_final["date"] = pd.to_datetime(history_dataframe_final["date"], unit="ms")
        history_dataframe_final.set_index("date", inplace=True)
        history_dataframe_final.sort_index(inplace=True)
        history_dataframe_final["pair"] = pair
        history_dataframe_final["symbol"] = pair[:-5] if pair.endswith("/USDT") else pair[:-4]

    else:
        print(f"{pair} is broken or too short, returning 0 len DF")
        return df()

    if history_dataframe_saved is not None:
        history_dataframe_final = pd.concat([history_dataframe_saved, history_dataframe_final])
        history_dataframe_final.sort_index(inplace=True)

    # Cleaning dataframe
    history_dataframe_final.dropna(inplace=True)
    history_dataframe_final.drop_duplicates(keep="last", inplace=True)

    if not fresh_live_history_no_save_read and timeframe_for_data == "15MIN":
        history_dataframe_final.to_csv(f"{project_path}/History_data/{exchange}/15MIN/{pair_for_data}_15MIN.csv")

        print(f"Saved {pair} CSV")

    history_dataframe_final = history_dataframe_final.resample(f"{timeframe.upper()}").agg(ohlc).fillna(method="ffill")

    return history_dataframe_final.loc[max(history_dataframe_final.iloc[0].name.date(), start):end]
