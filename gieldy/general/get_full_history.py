import datetime as dt
import time
import os
from random import randint
import pandas as pd
from pathlib import Path
from pandas import DataFrame as df

from gieldy.CCXT.CCXT_utils import get_history_fragment_CCXT_REST

current_path = os.path.dirname(os.path.abspath(__file__))
project_path = Path(current_path).parent.parent


class GetFullHistory:

    def __init__(self, pair, timeframe, since, API, end=None):
        self.pair = pair
        self.timeframe = timeframe.lower()
        self.since = since
        self.end = end
        self.API = API
        self.name = self.API["name"]
        self.exchange = self.exchange_check()

    def exchange_check(self):
        if "kucoin" in self.name.lower():
            return "kucoin"

        if "binance" in self.name.lower():
            return "binance"

    @staticmethod
    def date_string_to_timestampms(date_string):
        if "/" in date_string:
            date_datetime = dt.datetime.strptime(date_string, "%d/%m/%Y")
        elif "." in date_string:
            date_datetime = dt.datetime.strptime(date_string, "%d.%m.%Y")
        else:
            raise Exception("Wrong date string format")

        date_timestamp = int(time.mktime(date_datetime.timetuple()) * 1000)

        return date_timestamp

    @staticmethod
    def history_clean(hist_dataframe, pair):

        if len(hist_dataframe) > 1:
            hist_dataframe.set_index("date", inplace=True)
            hist_dataframe.sort_index(inplace=True)
            hist_dataframe.dropna(inplace=True)
            hist_dataframe.drop_duplicates(keep="last", inplace=True)
            hist_dataframe["pair"] = pair
            hist_dataframe["symbol"] = pair[:-5] if pair.endswith("/USDT") else pair[:-4]
        else:
            print(f"{pair} is broken or too short, returning 0 len DF")
            return df()

        hist_dataframe.dropna(inplace=True)
        hist_dataframe.drop_duplicates(keep="last", inplace=True)

        return hist_dataframe

    def get_full_history(self):
        print(f"Getting {self.pair} history")

        since_timestamp = self.date_string_to_timestampms(self.since)
        original_since_timestamp = since_timestamp

        if self.end is None:
            end_timestamp = int(time.time() * 1000)
        else:
            end_timestamp = self.date_string_to_timestampms(self.end)

        hist_df_full = df()
        while True:
            try:
                time.sleep(randint(2, 5) / 10)
                hist_df_fresh = get_history_fragment_CCXT_REST(pair=self.pair,
                                                               timeframe=self.timeframe,
                                                               since=since_timestamp, API=self.API)

                if len(hist_df_full) > 1:
                    if hist_df_fresh.iloc[-1].date == hist_df_full.iloc[-1].date: break

                hist_df_full = pd.concat([hist_df_full, hist_df_fresh])
                since_timestamp = int(hist_df_fresh.iloc[-5].date)

                if len(hist_df_full) > 1:
                    if hist_df_full.iloc[-1].date >= end_timestamp: break

            except Exception as e:
                print(f"{e}, error on history fragments loop")

        hist_df_final = self.history_clean(hist_df_full, pair=self.pair)
        hist_df_final = hist_df_final.loc[
                        max(int(hist_df_final.iloc[0].name), original_since_timestamp):
                        min(int(hist_df_final.iloc[-1].name), end_timestamp)]
        hist_df_final.index = pd.to_datetime(hist_df_final.index, unit="ms")

        return hist_df_final

    def load_data(self):
        try:
            history_df_saved = pd.read_csv(
                f"{project_path}/history_data/{self.exchange}/{self.timeframe}/{self.pair}_{self.timeframe}.csv",
                index_col=0, parse_dates=True)
            print("Saved history found")

            return history_df_saved

        except Exception as err:
            print(f"{err}, no saved history for {self.pair}")
            pass

    def save_data(self, df_to_save):
        df_to_save.to_csv(f"{project_path}/History_data/{self.exchange}/15MIN/{self.pair}_15MIN.csv")
