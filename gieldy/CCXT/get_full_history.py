import os
import time
import inspect
import pandas as pd
import datetime as dt
from random import randint
from pathlib import Path
from typing import Optional, Union
from pandas import DataFrame as df

from gieldy.general.utils import (
    date_string_to_datetime,
    datetime_to_timestamp_ms,
    timeframe_to_timestamp_ms,
    timestamp_ms_to_datetime,
    dataframe_is_not_none_and_has_elements
)


class BaseInfoClassWithValidation:
    """Class to handle all the parameters used by other classes"""

    def __init__(self, pair: str, timeframe: str, save_load_history: bool, API: dict,
                 number_of_last_candles: Optional[int] = None,
                 since: Optional[str] = None, end: Optional[str] = None):
        self.API = API
        self.save_load_history = save_load_history
        self.pair = pair
        self.timeframe = timeframe.lower()
        self.timeframe_in_timestamp = timeframe_to_timestamp_ms(self.timeframe)
        self.number_of_last_candles = number_of_last_candles
        self.since = since
        self.since_datetime = None
        self.since_timestamp = None
        self.end = end
        self.end_datetime = None
        self.end_timestamp = None
        self.safety_buffer = self.timeframe_in_timestamp * 12
        self.exchange = self.API["exchange"]
        self.pair_for_data = self.pair.replace("/", "-")
        self.validate_inputs()

    def validate_inputs(self):
        # Validate input parameters
        if not (self.number_of_last_candles or self.since):
            raise ValueError("Please provide either starting date or number of last n candles to provide")
        if self.number_of_last_candles and (self.since or self.end):
            raise ValueError("You cannot provide since/end date together with last n candles parameter")

        # Parse and convert since and end dates to timestamps
        if self.number_of_last_candles:
            self.save_load_history = False
            self.since_timestamp = datetime_to_timestamp_ms(dt.datetime.now()) - (
                    self.number_of_last_candles * self.timeframe_in_timestamp)
            self.since_datetime = timestamp_ms_to_datetime(self.since_timestamp)
        if self.since:
            self.since_datetime = date_string_to_datetime(self.since)
            self.since_timestamp = datetime_to_timestamp_ms(self.since_datetime)
        if self.end:
            self.end_datetime = date_string_to_datetime(self.end)
            self.end_timestamp = datetime_to_timestamp_ms(self.end_datetime)
        else:
            self.end_datetime = dt.datetime.now()
            self.end_timestamp = datetime_to_timestamp_ms(self.end_datetime)


class DFCleanAndCut:
    """Class for cleaning and cutting the dataframe to desired dates"""

    @staticmethod
    def history_df_cleaning(hist_dataframe: pd.DataFrame, pair: str) -> Union[pd.DataFrame, None]:
        """Setting index, dropping duplicates, cleaning dataframe"""
        if dataframe_is_not_none_and_has_elements(hist_dataframe):
            hist_dataframe.set_index("date", inplace=True)
            hist_dataframe.sort_index(inplace=True)
            hist_dataframe.index = pd.to_datetime(hist_dataframe.index, unit="ms")
            hist_dataframe.dropna(inplace=True)
            hist_dataframe = hist_dataframe[~hist_dataframe.index.duplicated(keep="last")]
            hist_dataframe.insert(len(hist_dataframe.columns), "pair", pair)
            hist_dataframe.insert(len(hist_dataframe.columns), "symbol",
                                  pair[:-5] if pair.endswith("/USDT") else pair[:-4])
        else:
            print(f"{pair} is broken or too short")
            return None

        return hist_dataframe

    def cut_exact_df_dates_for_return(self, final_dataframe: pd.DataFrame) -> pd.DataFrame:
        """Cut the dataframe to exactly match the desired since/end, small quirk here as end_datetime can be precise
         to the second while the timeframe may be 1D - it would never return correctly, mainly when end is now"""
        final_dataframe = final_dataframe.loc[
                          self.since_datetime:min(final_dataframe.iloc[-1].name, self.end_datetime)]
        return final_dataframe


class DataStoring:
    """Class for handling the history loading and saving to pickle"""

    @property
    def exchange_history_location(self) -> str:
        """Return the path where the history for this exchange should be saved"""
        current_path = os.path.dirname(inspect.getfile(inspect.currentframe()))
        project_path = Path(current_path).parent.parent
        return f"{project_path}/history_data/{self.exchange}"

    def parse_dataframe_from_pickle(self) -> Union[pd.DataFrame, None]:
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
            return None

    def load_dataframe_and_pre_check(self) -> Union[pd.DataFrame, None]:
        """Check if loaded data range is sufficient for current request, if not - get new"""
        hist_df_full = self.parse_dataframe_from_pickle()
        if dataframe_is_not_none_and_has_elements(hist_df_full):
            if (hist_df_full.iloc[-1].name > self.end_datetime) and (
                    hist_df_full.iloc[0].name < self.since_datetime):
                print("Saved data is sufficient, returning")
                hist_df_final_cut = self.cut_exact_df_dates_for_return(hist_df_full)
                return hist_df_final_cut
            else:
                print("Saved data found, not sufficient data range, getting whole fresh")
                return None

    def save_dataframe_to_pickle(self, df_to_save: pd.DataFrame) -> None:
        """Pickle the data and save"""
        df_to_save.to_pickle(
            f"{self.exchange_history_location}/{self.timeframe}/{self.pair_for_data}_{self.timeframe}.pickle")
        print("Saved history dataframe as pickle")


class GetFullHistory:
    """Get full history of pair between desired periods or last n candles"""

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

    def main(self) -> pd.DataFrame:
        """Main logic function to loop the history acquisition"""
        print(f"Getting {self.pair} history")
        df_clean_cut = DFCleanAndCut(pair=self.pair, API=self.API, save_load_history=self.save_load_history,
                                     timeframe=self.timeframe)
        data_storing = DataStoring(pair=self.pair, API=self.API, save_load_history=self.save_load_history,
                                   timeframe=self.timeframe)

        if self.save_load_history:
            hist_df_full = load_dataframe_and_pre_check()
            if dataframe_is_not_none_and_has_elements(hist_df_full):
                return hist_df_full
        else:
            hist_df_full = df()
        local_since_timestamp = self.since_timestamp
        hist_df_fresh = self.get_history_fragment_for_func(self.pair, self.timeframe, local_since_timestamp, self.API)
        delta = hist_df_fresh.iloc[-1].date - hist_df_fresh.iloc[0].date - self.safety_buffer
        while True:
            try:
                time.sleep(randint(2, 5) / 10)
                hist_df_fresh = self.get_history_fragment_for_func(self.pair, self.timeframe, local_since_timestamp,
                                                                   self.API)
                hist_df_full = pd.concat([hist_df_full, hist_df_fresh])
                local_since_timestamp += delta
                if local_since_timestamp >= (self.end_timestamp + self.safety_buffer):
                    break
            except Exception as e:
                print(f"Error on history fragments loop, {e}")

        hist_df_final = self.history_df_cleaning(hist_df_full, pair=self.pair)
        if self.save_load_history:
            self.save_dataframe_to_pickle(hist_df_final)
        hist_df_final_cut = self.cut_exact_df_dates_for_return(hist_df_final)
        return hist_df_final_cut


GetFullHistory().main()
