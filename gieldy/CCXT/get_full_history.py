import os
import time
import inspect
import pandas as pd
import datetime as dt
from random import randint
from pathlib import Path
from typing import Optional
from pandas import DataFrame as df

from gieldy.general.utils import (
    date_string_to_datetime,
    datetime_to_timestamp_ms,
    timeframe_to_timestamp_ms,
    timestamp_ms_to_datetime,
)


class GetFullHistory:
    """Get full history of pair between desired periods or last n candles"""

    DAY_IN_TIMESTAMP_MS = 86_400_000

    def __init__(self, pair: str, timeframe: str, save_load_history: bool, API: dict,
                 number_of_last_candles: Optional[int] = None,
                 since: Optional[str] = None, end: Optional[str] = None):
        self.API = API
        self.save_load_history = save_load_history
        self.pair = pair
        self.timeframe = timeframe.lower()
        self.timeframe_in_timestamp = timeframe_to_timestamp_ms(self.timeframe)
        self.safety_buffer = self.timeframe_in_timestamp * 12

        # Validate input parameters
        if not (number_of_last_candles or since):
            raise ValueError("Please provide either starting date or number of last n candles to provide")
        if number_of_last_candles and (since or end):
            raise ValueError("You cannot provide since/end date together with last n candles parameter")

        # Parse and convert since and end dates to timestamps
        if number_of_last_candles:
            self.save_load = False
            self.since_timestamp = datetime_to_timestamp_ms(dt.datetime.now()) - (
                    number_of_last_candles * self.timeframe_in_timestamp)
            self.since_datetime = timestamp_ms_to_datetime(self.since_timestamp)
        if since:
            self.since_datetime = date_string_to_datetime(since)
            self.since_timestamp = datetime_to_timestamp_ms(self.since_datetime)
        if end:
            self.end_datetime = date_string_to_datetime(end)
            self.end_timestamp = datetime_to_timestamp_ms(self.end_datetime)
        else:
            self.end_datetime = dt.datetime.now()
            self.end_timestamp = datetime_to_timestamp_ms(self.end_datetime)

    @property
    def exchange(self) -> str:
        return self.API["exchange"]

    @property
    def pair_for_data(self) -> str:
        return self.pair.replace("/", "-")

    @property
    def exchange_history_location(self) -> str:
        """Return the path where the history for this timeframe exchange should be saved"""
        current_path = os.path.dirname(inspect.getfile(inspect.currentframe()))
        project_path = Path(current_path).parent.parent
        return f"{project_path}/history_data/{self.exchange}"

    def cut_exact_df_dates_for_return(self, final_dataframe: pd.DataFrame) -> pd.DataFrame:
        """Cut the dataframe to exactly match the desired since/end"""
        final_dataframe = final_dataframe.loc[
                          self.since_datetime:min(final_dataframe.iloc[-1].name, self.end_datetime)]
        return final_dataframe

    @staticmethod
    def history_df_cleaning(hist_dataframe: pd.DataFrame, pair: str) -> pd.DataFrame:
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
            print(f"{pair} is broken or too short, returning empty DF")
            return df()

        return hist_dataframe

    @staticmethod
    def get_history_fragment_for_func(pair: str, timeframe: str, since: str, API: dict) -> pd.DataFrame:
        """Get fragment of history"""
        general_client = API["general_client"]
        candle_limit = 800
        candles_list = general_client.fetchOHLCV(symbol=pair, timeframe=timeframe,
                                                 since=since, limit=candle_limit)
        columns_ordered = ["date", "open", "high", "low", "close", "volume"]
        history_dataframe_new = df(candles_list, columns=columns_ordered)
        return history_dataframe_new

    def parse_dataframe_from_pickle(self) -> pd.DataFrame:
        """Check for pickled data and load, if no folder for data - create"""
        try:
            if not os.path.exists(self.exchange_history_location):
                os.makedirs(self.exchange_history_location)
            history_df_saved = pd.read_pickle(
                f"{self.exchange_history_location}/{self.timeframe}/{self.pair_for_data}_{self.timeframe}.pickle")
            print("Saved history pickle found")
            return history_df_saved

        except FileNotFoundError as err:
            print(f"No saved history for {self.pair}, {err}")
            return df()

    def load_dataframe_and_pre_check(self) -> pd.DataFrame:
        hist_df_full = self.parse_dataframe_from_pickle()
        if hist_df_full is not None and len(hist_df_full) > 1:
            if (hist_df_full.iloc[-1].name > self.end_datetime) and (
                    hist_df_full.iloc[0].name < self.since_datetime):
                print("Saved data is sufficient, returning")
                hist_df_final_cut = self.cut_exact_df_dates_for_return(hist_df_full)
                return hist_df_final_cut
            else:
                print("Saved data found, not sufficient data range, getting whole fresh")
                return df()

    def save_dataframe_to_pickle(self, df_to_save) -> None:
        """Pickle the data"""
        df_to_save.to_pickle(
            f"{self.exchange_history_location}/{self.timeframe}/{self.pair_for_data}_{self.timeframe}.pickle")
        print("Saved history dataframe as pickle")

    def main(self) -> pd.DataFrame:
        """Main function to get/load/save the history"""
        print(f"Getting {self.pair} history")
        if self.save_load_history:
            hist_df_full = self.load_dataframe_and_pre_check()
        else:
            hist_df_full = df()
        local_since_timestamp = self.since_timestamp - self.safety_buffer
        while True:
            try:
                time.sleep(randint(2, 5) / 10)
                hist_df_fresh = self.get_history_fragment_for_func(
                    self.pair, self.timeframe, local_since_timestamp, self.API
                )
                hist_df_full = pd.concat([hist_df_full, hist_df_fresh])
                local_since_timestamp = hist_df_fresh.iloc[-1].date - self.safety_buffer
                if local_since_timestamp >= (self.end_timestamp + self.safety_buffer):
                    break
            except Exception as e:
                print(f"Error on history fragments loop, {e}")

        hist_df_final = self.history_df_cleaning(hist_df_full, pair=self.pair)
        if self.save_load_history:
            self.save_dataframe_to_pickle(hist_df_final)
        hist_df_final_cut = self.cut_exact_df_dates_for_return(hist_df_final)
        return hist_df_final_cut
