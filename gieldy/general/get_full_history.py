import time
import os
import datetime as dt
import pandas as pd
from random import randint
from pathlib import Path
from pandas import DataFrame as df

from gieldy.CCXT.CCXT_utils import get_history_fragment_CCXT_REST_for_func

current_path = os.path.dirname(os.path.abspath(__file__))
project_path = Path(current_path).parent.parent


class GetFullHistory:
    """ Get full history of pair between desired periods, default from since to now. """
    def __init__(self, pair, timeframe, since, API, end=None):
        self.save_load = True
        self.pair = pair
        self.pair_for_data = self.pair.replace("/", "-")
        self.timeframe = timeframe.lower()
        self.API = API
        self.name = self.API["name"]
        self.exchange = self.exchange_check()
        self.data_location = f"{project_path}/history_data/{self.exchange}/{self.timeframe}"
        self.since_datetime = self.date_string_to_datetime(since)
        self.since_timestamp = self.datetime_to_timestamp_ms(self.since_datetime)
        if end is None:
            self.end_datetime = dt.datetime.now()
        else:
            self.end_datetime = self.date_string_to_datetime(end)
        self.end_timestamp = self.datetime_to_timestamp_ms(self.end_datetime)
        self.day_in_timestamp_ms = 86_400_000

    def exchange_check(self):
        if "kucoin" in self.name.lower():
            return "kucoin"

        if "binance" in self.name.lower():
            return "binance"

    @staticmethod
    def date_string_to_datetime(date_string):
        date_datetime = dt.datetime.strptime(date_string, "%d/%m/%Y")

        return date_datetime

    @staticmethod
    def datetime_to_timestamp_ms(date_datetime):
        date_timestamp = int(time.mktime(date_datetime.timetuple()) * 1000)

        return date_timestamp

    def load_data(self):
        try:
            if not os.path.exists(self.data_location):
                os.makedirs(self.data_location)
            history_df_saved = pd.read_pickle(
                f"{self.data_location}/{self.pair_for_data}_{self.timeframe}.pickle")
            print("Saved history pickle found")

            return history_df_saved

        except Exception as err:
            print(f"No saved history for {self.pair}, {err}")
            pass

    def save_data(self, df_to_save):
        df_to_save.to_pickle(f"{self.data_location}/{self.pair_for_data}_{self.timeframe}.pickle")
        print("Saved history dataframe as pickle")

    @staticmethod
    def history_clean(hist_dataframe, pair):
        if len(hist_dataframe) > 1:
            hist_dataframe.set_index("date", inplace=True)
            hist_dataframe.sort_index(inplace=True)
            hist_dataframe.index = pd.to_datetime(hist_dataframe.index, unit="ms")
            hist_dataframe.dropna(inplace=True)
            hist_dataframe.drop_duplicates(keep="last", inplace=True)
            hist_dataframe["pair"] = pair
            hist_dataframe["symbol"] = pair[:-5] if pair.endswith("/USDT") else pair[:-4]
        else:
            print(f"{pair} is broken or too short, returning 0 len DF")
            return df()

        return hist_dataframe

    def return_dataframe(self, final_dataframe):
        return final_dataframe.loc[
               self.since_datetime:min(final_dataframe.iloc[-1].name, self.end_datetime)]

    def get_full_history(self):
        print(f"Getting {self.pair} history")

        if self.save_load:
            hist_df_full = self.load_data()
            if hist_df_full is not None and len(hist_df_full) > 1:
                if (hist_df_full.iloc[-1].name > self.end_datetime) and (
                        hist_df_full.iloc[0].name < self.since_datetime):
                    print("Saved data is sufficient, returning")
                    self.return_dataframe(hist_df_full)
                else:
                    hist_df_full = df()
                    print("Saved data found, not sufficient data range, getting fresh")
        else:
            hist_df_full = df()

        local_since_timestamp = self.since_timestamp - self.day_in_timestamp_ms
        stable_loop_timestamp_delta = 0
        while True:
            try:
                time.sleep(randint(2, 5) / 10)
                hist_df_fresh = get_history_fragment_CCXT_REST_for_func(pair=self.pair,
                                                                        timeframe=self.timeframe,
                                                                        since=local_since_timestamp,
                                                                        API=self.API)
                hist_df_full = pd.concat([hist_df_full, hist_df_fresh])

                loop_timestamp_delta = int(hist_df_fresh.iloc[-1].date - hist_df_fresh.iloc[0].date)
                stable_loop_timestamp_delta = max(stable_loop_timestamp_delta, loop_timestamp_delta)
                local_since_timestamp += int(stable_loop_timestamp_delta * 0.95)

                if len(hist_df_full) > 1:
                    if local_since_timestamp >= self.end_timestamp: break

            except Exception as e:
                print(f"Error on history fragments loop, {e}")

        hist_df_final = self.history_clean(hist_df_full, pair=self.pair)

        if self.save_load:
            self.save_data(hist_df_final)

        self.return_dataframe(hist_df_final)
