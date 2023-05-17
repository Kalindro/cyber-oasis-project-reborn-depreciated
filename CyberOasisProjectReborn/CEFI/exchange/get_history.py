import datetime as dt
import os
import pickle
import time
import traceback
import typing as tp
import warnings
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import numpy as np
import pandas as pd
import vectorbtpro as vbt
from loguru import logger

from CyberOasisProjectReborn.utils.dir_paths import ROOT_DIR
from CyberOasisProjectReborn.utils.utility import (timeframe_to_timedelta, dataframe_is_not_none_and_not_empty, cut_exact_df_dates,
                                                   date_string_to_UTC_datetime, datetime_now_in_UTC)

WORKERS = 2
SLEEP = 0.25
BASE_TIMEFRAME = "15min"


class GetFullHistoryDF:
    """Main logic class to receive desired range of clean, usable history dataframe"""

    def __init__(self,
                 pairs_list: list[str],
                 timeframe: str,
                 API: dict,
                 min_data_length: int = 10,
                 vol_quantile_drop: float = None,
                 save_load_history: bool = False,
                 number_of_last_candles: tp.Optional[int] = None,
                 start: tp.Optional[str] = None,
                 end: tp.Optional[str] = None):
        self.pairs_list = pairs_list
        self.timeframe = timeframe
        self.API = API
        self.min_data_length = min_data_length
        self.vol_quantile_drop = vol_quantile_drop
        self.save_load_history = save_load_history
        self.number_of_last_candles = number_of_last_candles
        self.start = start
        self.end = end
        self._validate_dates()

    def get_full_history(self) -> dict[str: pd.DataFrame]:
        """Main function to get the desired history"""

        try:
            logger.info("Getting history of all the coins on provided pairs list...")
            vbt_history_partial = partial(self._get_vbt_one_pair_desired_history, timeframe=self.timeframe,
                                          start=self.start, end=self.end, API=self.API,
                                          save_load_history=self.save_load_history)

            with ThreadPoolExecutor(max_workers=WORKERS) as executor:
                histories_dict = dict(zip(self.pairs_list, executor.map(vbt_history_partial, self.pairs_list)))

            histories_dict = {pair: history_df for pair, history_df in histories_dict.items() if
                              dataframe_is_not_none_and_not_empty(history_df)}

            if self.min_data_length:
                histories_dict = self._drop_too_short_history(histories_dict=histories_dict)

            if self.vol_quantile_drop:
                histories_dict = self._drop_bottom_quantile_vol(histories_dict=histories_dict)

            return vbt.Data.from_data(histories_dict)

        except Exception as err:
            logger.error(f"Error on main full history, {err}")
            print(traceback.format_exc())

    def _get_vbt_one_pair_desired_history(self,
                                          pair: str,
                                          timeframe: str,
                                          API: dict,
                                          start: tp.Optional[dt.datetime] = None,
                                          end: tp.Optional[dt.datetime] = None,
                                          save_load_history: tp.Optional[bool] = False
                                          ) -> pd.DataFrame:
        """Get desired history of one pair"""
        logger.info(f"Valuating {pair} history")

        if not save_load_history:
            one_pair_df = self._history_fetch(pair=pair, timeframe=timeframe, start=start, end=end, API=API)
        else:
            one_pair_df = self._evaluate_loaded_data(pair=pair, start=start, end=end, API=API)

        if dataframe_is_not_none_and_not_empty(one_pair_df):
            one_pair_df = cut_exact_df_dates(one_pair_df, start, end)
            one_pair_df = one_pair_df.resample(timeframe.lower()).bfill()

        return one_pair_df

    def _evaluate_loaded_data(self,
                              pair: str,
                              start: dt.datetime,
                              end: dt.datetime,
                              API: dict
                              ) -> pd.DataFrame:
        """Check if loaded data range is sufficient for current request, if not, get new or update existing"""
        timeframe = BASE_TIMEFRAME
        data_storing = _DataStoring(pair=pair, timeframe=timeframe, API=API)
        one_pair_dict = data_storing.load_pickle()
        one_pair_df = one_pair_dict.get("data")
        first_valid_datetime = one_pair_dict.get("first_datetime")

        # Get first valid timestamp if not already available
        if first_valid_datetime is None:
            first_valid_datetime = vbt.CCXTData.find_earliest_date(symbol=pair, timeframe=timeframe,
                                                                   exchange=API["client"])
            one_pair_dict["first_datetime"] = first_valid_datetime
            data_storing.save_pickle(one_pair_dict)

        # Check if coin even exists in this data range
        if first_valid_datetime > end:
            logger.info(f"{pair} starts after desired end, returning None")
            return None

        if dataframe_is_not_none_and_not_empty(one_pair_df):
            # Check if desired range in the data
            if (one_pair_df.iloc[0].name <= start) and (one_pair_df.iloc[-1].name >= end):
                logger.info(f"Saved data for {pair} is sufficient")

            # Check if the coin first available datetime was later than the desired start but still before end
            elif (one_pair_df.iloc[-1].name >= end) and (one_pair_df.iloc[0].name <= first_valid_datetime) and (
                    first_valid_datetime > start):
                logger.info(
                    f"Saved data for {pair} is sufficient (history starts later than expected)")

            # There is history but not enough, update existing accordingly
            else:
                logger.info(f"Saved data for {pair} found, not enough, updating accordingly")
                available_start = one_pair_df.iloc[0].name
                available_end = one_pair_df.iloc[-1].name
                if available_start > start:
                    new_start = start
                    new_end = available_start
                    before_df = self._history_fetch(pair=pair, timeframe=timeframe, start=new_start, end=new_end,
                                                    API=API)
                    one_pair_df = pd.concat([before_df, one_pair_df]).loc[
                        ~pd.concat([before_df, one_pair_df]).index.duplicated(keep="last")]

                if available_end < end:
                    new_start = available_end
                    new_end = end
                    after_df = self._history_fetch(pair=pair, timeframe=timeframe, start=new_start, end=new_end,
                                                   API=API)
                    one_pair_df = pd.concat([one_pair_df, after_df]).loc[
                        ~pd.concat([one_pair_df, after_df]).index.duplicated(keep="last")]

                one_pair_dict["data"] = one_pair_df
                data_storing.save_pickle(one_pair_dict)

            return one_pair_df

        # No history saved, get fresh
        else:
            logger.info(f"No saved history for {pair}, getting fresh")
            one_pair_df = self._history_fetch(pair=pair, timeframe=timeframe, start=start, end=end, API=API)
            one_pair_dict["data"] = one_pair_df
            data_storing.save_pickle(one_pair_dict)

            return one_pair_df

    @staticmethod
    def _history_fetch(pair: str,
                       API: dict,
                       timeframe: str,
                       start: tp.Optional[dt.datetime] = None,
                       end: tp.Optional[dt.datetime] = None
                       ) -> pd.DataFrame:
        """Core history query wrapper"""
        delta = timeframe_to_timedelta(timeframe)
        time.sleep(SLEEP)

        try:
            logger.info(f"Getting {pair} data")
            _, one_pair_df = vbt.CCXTData.fetch(symbols=pair, timeframe=timeframe, start=start - delta * 6,
                                                end=end + delta * 6, exchange=API["client"],
                                                show_progress=False, silence_warnings=False).data.popitem()
            one_pair_df["Volume"] = one_pair_df["Volume"] * one_pair_df["Close"]

            if dataframe_is_not_none_and_not_empty(one_pair_df):
                one_pair_df = one_pair_df.resample(timeframe.lower()).bfill()

            return one_pair_df

        except Exception as err:
            if "No symbols could be fetched" in str(err):
                return None
            else:
                raise err

    def _drop_too_short_history(self, histories_dict: dict[str: pd.DataFrame]) -> dict[str: pd.DataFrame]:
        drop_len_pairs = [pair for pair, history_df in histories_dict.items() if
                          len(history_df) <= self.min_data_length]
        for pair in drop_len_pairs:
            del histories_dict[pair]
        logger.success(f"Pairs dropped due to length: {drop_len_pairs}")

        return histories_dict

    def _drop_bottom_quantile_vol(self, histories_dict: dict[str: pd.DataFrame]) -> dict[str: pd.DataFrame]:
        mean_volumes = {pair: history_df["Volume"].mean() for pair, history_df in histories_dict.items()}
        threshold = np.quantile(list(mean_volumes.values()), self.vol_quantile_drop)
        drop_vol_pairs = [pair for pair, history_df in histories_dict.items() if mean_volumes[pair] < threshold]
        for pair in drop_vol_pairs:
            del histories_dict[pair]
        logger.success(f"Pairs dropped due to being in bottom {self.vol_quantile_drop * 100}% volume: {drop_vol_pairs}")

        return histories_dict

    def _validate_dates(self) -> None:
        """Validate if correct arguments are passed"""
        timeframe_in_timedelta = timeframe_to_timedelta(self.timeframe)
        if self.number_of_last_candles and self.save_load_history:
            self.ave_load_history = False
            warnings.warn("ERROR, You cannot provide last_n_candles and save_load together, saving/loading turned off")
        if not (self.number_of_last_candles or self.start):
            raise ValueError("Please provide either starting date or number of last n candles to provide")
        if self.number_of_last_candles and (self.start or self.end):
            raise ValueError("You cannot provide start/end date together with last n candles parameter")
        if self.start:
            self.start = date_string_to_UTC_datetime(self.start)
        if self.end:
            self.end = date_string_to_UTC_datetime(self.end)
        if self.number_of_last_candles:
            self.start = datetime_now_in_UTC() - (timeframe_in_timedelta * self.number_of_last_candles)
            self.end = datetime_now_in_UTC()


class _DataStoring:
    """Class for handling the history loading and saving to pickle"""

    def __init__(self, pair: str, timeframe: str, API: dict):
        self.pair = pair
        self.pair_for_data = pair.replace("/", "-")
        self.timeframe = timeframe
        self.API = API
        self.exchange_name = self.API["exchange"]

    @property
    def _pair_pickle_location(self) -> str:
        """Return the path where this pair should be saved"""
        return f"{self._history_data_folder_location}\\{self.pair_for_data}_{self.timeframe}.pickle"

    @property
    def _history_data_folder_location(self) -> str:
        """Return the path where the history for this exchange should be saved"""
        return os.path.join(ROOT_DIR, "history_data", self.exchange_name, self.timeframe)

    def load_pickle(self) -> dict:
        """Check for pickled data and load, if no folder for data - create"""

        if not os.path.exists(self._history_data_folder_location):
            try:
                os.makedirs(self._history_data_folder_location)
            except:
                pass
        else:
            try:
                with open(self._pair_pickle_location, "rb") as f:
                    one_pair_dict = pickle.load(f)
                return one_pair_dict

            except FileNotFoundError as err:
                if "No such file or directory" in str(err):
                    logger.info(f"No saved history for {self.pair}")
                else:
                    logger.error(f"Error on loading file, {err}")

        return {}

    def save_pickle(self, dict_to_save: dict[dt.datetime: pd.DataFrame]) -> None:
        """Pickle the data and save"""
        with open(self._pair_pickle_location, "wb") as f:
            pickle.dump(dict_to_save, f)
        logger.info(f"Saved {self.pair} history dict as pickle")
