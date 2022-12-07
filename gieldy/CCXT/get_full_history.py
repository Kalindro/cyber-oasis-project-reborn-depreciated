import time
import os
import datetime as dt
import pandas as pd
from random import randint
from pathlib import Path
from pandas import DataFrame as df

from gieldy.general.utils import date_string_to_datetime, datetime_to_timestamp_ms, \
    timestamp_ms_to_datetime, timeframes_to_timestamp_ms

current_path = os.path.dirname(os.path.abspath(__file__))
project_path = Path(current_path).parent.parent


class GetFullHistory:
    """Get full history of pair between desired periods or last n candles"""
    def __init__(self, pair, timeframe, save_load, API, last_n_candles=None, since=None, end=None):
        self.day_in_timestamp_ms = 86_400_000
        self.API = API
        self.save_load = save_load
        self.pair = pair
        self.pair_for_data = self.pair.replace("/", "-")
        self.timeframe = timeframe.lower()
        self.timeframe_in_timestamp = timeframes_to_timestamp_ms(self.timeframe)
        self.name = self.API["name"]
        self.exchange = self.exchange_name_check()
        self.data_location = f"{project_path}/history_data/{self.exchange}/{self.timeframe}"

        if bool(last_n_candles) == bool(since):
            raise ValueError("Please provide either starting date or number of last n candles to provide")
        if last_n_candles is True and end is True:
            raise ValueError("You cannot provide end date together with last n candles parameter")

        if last_n_candles:
            self.save_load = False
            self.since_timestamp = datetime_to_timestamp_ms(dt.datetime.now()) - (
                    last_n_candles * self.timeframe_in_timestamp)
            self.since_datetime = timestamp_ms_to_datetime(self.since_timestamp)
        if end:
            self.end_datetime = date_string_to_datetime(end)
        else:
            self.end_datetime = dt.datetime.now()
            self.end_timestamp = datetime_to_timestamp_ms(self.end_datetime)
        if since:
            self.since_datetime = date_string_to_datetime(since)
            self.since_timestamp = datetime_to_timestamp_ms(self.since_datetime)

    def exchange_name_check(self):
        """Check name of exchange to save data to correct folder"""

        if "kucoin" in self.name.lower():
            return "kucoin"

        elif "binance futures" in self.name.lower():
            return "binance futures"

        elif "binance" in self.name.lower():
            return "binance"

        else:
            raise ValueError("Unrecognized exchange name: " + str(self.name.lower()))

    def load_dataframe_from_pickle(self):
        """Check for pickled data and load, if no folder for data - create"""
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

    def save_dataframe_to_pickle(self, df_to_save):
        """Pickle the data"""
        df_to_save.to_pickle(f"{self.data_location}/{self.pair_for_data}_{self.timeframe}.pickle")
        print("Saved history dataframe as pickle")

    def cut_exact_df_dates_for_return(self, final_dataframe):
        """Cut the dataframe to exactly match the desired since/to"""
        final_dataframe = final_dataframe.loc[
                          self.since_datetime:min(final_dataframe.iloc[-1].name, self.end_datetime)]

        return final_dataframe

    @staticmethod
    def history_df_cleaning(hist_dataframe, pair):
        """Setting index, dropping duplicates, cleaning dataframe"""
        if len(hist_dataframe) > 1:
            hist_dataframe.set_index("date", inplace=True)
            hist_dataframe.sort_index(inplace=True)
            hist_dataframe.index = pd.to_datetime(hist_dataframe.index, unit="ms")
            hist_dataframe.dropna(inplace=True)
            hist_dataframe = hist_dataframe[~hist_dataframe.index.duplicated(keep="last")]
            hist_dataframe.insert(len(hist_dataframe.columns), "pair", pair)
            hist_dataframe.insert(len(hist_dataframe.columns), "symbol",
                                  pair[:-5] if pair.endswith("/USDT") else pair[:-4])
        else:
            print(f"{pair} is broken or too short, returning 0 len DF")
            return df()

        return hist_dataframe

    @staticmethod
    def get_history_fragment_for_func(pair, timeframe, since, API):
        """Get fragment of history"""
        general_client = API["general_client"]
        candle_limit = 800
        candles_list = general_client.fetchOHLCV(symbol=pair, timeframe=timeframe,
                                                 since=since, limit=candle_limit)

        columns_ordered = ["date", "open", "high", "low", "close", "volume"]
        history_dataframe_new = df(candles_list, columns=columns_ordered)

        return history_dataframe_new

    def main(self):
        """Main function to get/load/save the history"""
        print(f"Getting {self.pair} history")

        if self.save_load:
            hist_df_full = self.load_dataframe_from_pickle()
            if hist_df_full is not None and len(hist_df_full) > 1:
                if (hist_df_full.iloc[-1].name > self.end_datetime) and (
                        hist_df_full.iloc[0].name < self.since_datetime):
                    print("Saved data is sufficient, returning")
                    hist_df_final_cut = self.cut_exact_df_dates_for_return(hist_df_full)
                    return hist_df_final_cut
                else:
                    hist_df_full = df()
                    print("Saved data found, not sufficient data range, getting fresh")
        else:
            hist_df_full = df()

        local_since_timestamp = self.since_timestamp - (self.timeframe_in_timestamp * 12)
        stable_loop_timestamp_delta = 0
        while True:
            try:
                time.sleep(randint(2, 5) / 10)
                hist_df_fresh = self.get_history_fragment_for_func(pair=self.pair,
                                                                   timeframe=self.timeframe,
                                                                   since=local_since_timestamp,
                                                                   API=self.API)
                hist_df_full = pd.concat([hist_df_full, hist_df_fresh])

                loop_timestamp_delta = int(hist_df_fresh.iloc[-1].date - hist_df_fresh.iloc[0].date)
                stable_loop_timestamp_delta = max(stable_loop_timestamp_delta, loop_timestamp_delta)
                local_since_timestamp += int(stable_loop_timestamp_delta * 0.95)

                if len(hist_df_full) > 1:
                    if local_since_timestamp >= (self.end_timestamp + (self.timeframe_in_timestamp * 12)):
                        break

            except Exception as e:
                print(f"Error on history fragments loop, {e}")

        hist_df_final = self.history_df_cleaning(hist_df_full, pair=self.pair)

        if self.save_load:
            self.save_dataframe_to_pickle(hist_df_final)

        hist_df_final_cut = self.cut_exact_df_dates_for_return(hist_df_final)

        return hist_df_final_cut
