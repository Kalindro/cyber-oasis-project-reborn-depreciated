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

from utils.root_dir import ROOT_DIR
from utils.utils import (timeframe_to_timedelta, dataframe_is_not_none_and_not_empty, date_string_to_UTC_datetime,
                         cut_exact_df_dates, datetime_now_in_UTC)

WORKERS = 2
SLEEP = 0.25


class GetFullHistoryDF:
    """Main logic class to receive desired range of clean, usable history dataframe"""

    def get_full_history(self,
                         pairs_list: list[str],
                         timeframe: str,
                         API: dict,
                         min_data_length: int = 10,
                         vol_quantile_drop: float = None,
                         save_load_history: bool = False,
                         number_of_last_candles: tp.Optional[int] = None,
                         start: tp.Optional[str] = None,
                         end: tp.Optional[str] = None) -> dict[str: pd.DataFrame]:
        """Main function to get the desired history"""

        try:
            logger.info("Getting history of all the coins on provided pairs list...")
            start, end, save_load_history = self._validate_dates(timeframe=timeframe,
                                                                 number_of_last_candles=number_of_last_candles,
                                                                 save_load_history=save_load_history,
                                                                 start=start, end=end)

            vbt_history_partial = partial(self._get_vbt_one_pair_desired_history, timeframe=timeframe, start=start,
                                          end=end, API=API, save_load_history=save_load_history)

            with ThreadPoolExecutor(max_workers=WORKERS) as executor:
                histories_dict = dict(zip(pairs_list, executor.map(vbt_history_partial, pairs_list)))

            histories_dict = {pair: history_df for pair, history_df in histories_dict.items() if
                              dataframe_is_not_none_and_not_empty(history_df)}

            if min_data_length:
                histories_dict = self._drop_too_short_history(histories_dict=histories_dict,
                                                              min_data_length=min_data_length)

            if vol_quantile_drop:
                histories_dict = self._drop_bottom_quantile_vol(histories_dict=histories_dict,
                                                                quantile=vol_quantile_drop)

            vbt_full_history = vbt.Data.from_data(histories_dict)

            return vbt_full_history

        except Exception as err:
            logger.error(f"Error on main full history, {err}")
            print(traceback.format_exc())

    @staticmethod
    def _get_vbt_one_pair_desired_history(pair: str,
                                          timeframe: str,
                                          API: dict,
                                          start: tp.Optional[dt.datetime] = None,
                                          end: tp.Optional[dt.datetime] = None,
                                          save_load_history: tp.Optional[bool] = False) -> pd.DataFrame:
        """Get desired history of one pair"""
        logger.info(f"Getting {pair} history")

        if save_load_history:
            delegate_data_storing = _DataStoring(pair=pair, timeframe=timeframe, start=start, end=end, API=API)
            one_pair_df = delegate_data_storing.load_pickle_and_pre_check()
            if dataframe_is_not_none_and_not_empty(one_pair_df):
                return one_pair_df

        delta = timeframe_to_timedelta(timeframe)
        time.sleep(SLEEP)

        try:
            _, one_pair_df = vbt.CCXTData.fetch(symbols=pair, timeframe=timeframe, start=start - delta * 6,
                                                end=end + delta * 6, exchange=API["client"],
                                                show_progress=False, silence_warnings=True).data.popitem()
            one_pair_df["Volume"] = one_pair_df["Volume"] * one_pair_df["Close"]

        except Exception as err:
            if "No symbols could be fetched" in str(err):
                return None
            else:
                raise err

        one_pair_dict = {"data": one_pair_df, "first_datetime": None}

        if save_load_history:
            delegate_data_storing.save_to_pickle(one_pair_dict)

        return cut_exact_df_dates(one_pair_dict["data"], start, end)

    @staticmethod
    def _validate_dates(timeframe: str,
                        number_of_last_candles: tp.Optional[int],
                        save_load_history: tp.Optional[bool],
                        start: tp.Optional[str],
                        end: tp.Optional[str]) -> tp.Tuple[dt.datetime, tp.Union[None, dt.datetime], bool]:
        """Validate if correct arguments are passed"""
        timeframe_in_timedelta = timeframe_to_timedelta(timeframe)
        if number_of_last_candles and save_load_history:
            save_load_history = False
            warnings.warn("ERROR, You cannot provide last_n_candles and save_load together, saving/loading turned off")
        if not (number_of_last_candles or start):
            raise ValueError("Please provide either starting date or number of last n candles to provide")
        if number_of_last_candles and (start or end):
            raise ValueError("You cannot provide start/end date together with last n candles parameter")
        if start:
            start = date_string_to_UTC_datetime(start)
        if end:
            end = date_string_to_UTC_datetime(end)
        if number_of_last_candles:
            start = datetime_now_in_UTC() - (timeframe_in_timedelta * number_of_last_candles)
            end = datetime_now_in_UTC()

        return start, end, save_load_history

    @staticmethod
    def _drop_too_short_history(histories_dict: dict[str: pd.DataFrame],
                                min_data_length: int) -> dict[str: pd.DataFrame]:
        drop_len_pairs = [pair for pair, history_df in histories_dict.items() if len(history_df) <= min_data_length]
        for pair in drop_len_pairs:
            del histories_dict[pair]
        logger.success(f"Pairs dropped due to length: {drop_len_pairs}")

        return histories_dict

    @staticmethod
    def _drop_bottom_quantile_vol(histories_dict: dict[str: pd.DataFrame],
                                  quantile: float) -> dict[str: pd.DataFrame]:
        mean_volumes = {pair: history_df["Volume"].mean() for pair, history_df in histories_dict.items()}
        threshold = np.quantile(list(mean_volumes.values()), quantile)
        drop_vol_pairs = [pair for pair, history_df in histories_dict.items() if mean_volumes[pair] < threshold]
        for pair in drop_vol_pairs:
            del histories_dict[pair]
        logger.success(f"Pairs dropped due to being in bottom {quantile * 100}% volume: {drop_vol_pairs}")

        return histories_dict


class _DataStoring:
    """Class for handling the history loading and saving to pickle"""

    def __init__(self, pair: str, timeframe: str, start: dt.datetime, end: dt.datetime, API: dict):
        self.pair = pair
        self.pair_for_data = pair.replace("/", "-")
        self.timeframe = timeframe
        self.start = start
        self.end = end
        self.API = API
        self.exchange_name = self.API["exchange"]

    def load_pickle_and_pre_check(self) -> tp.Union[pd.DataFrame, None]:
        """Check if loaded data range is sufficient for current request, if not - get new"""
        one_pair_dict = self._load_pickle_only()
        if one_pair_dict:
            one_pair_df = one_pair_dict["data"]
        else:
            one_pair_df = None

        # Desired range if in the data
        if dataframe_is_not_none_and_not_empty(one_pair_df):
            if (one_pair_df.iloc[0].name <= self.start) and (one_pair_df.iloc[-1].name >= self.end):
                logger.info(f"Saved data for {self.pair} is sufficient, returning")
                hist_df_final_cut = cut_exact_df_dates(one_pair_df, self.start, self.end)
                return hist_df_final_cut

            else:
                if one_pair_dict["first_datetime"]:
                    first_valid_datetime = one_pair_dict["first_datetime"]

                else:
                    first_valid_datetime = vbt.CCXTData.find_earliest_date(symbol=self.pair, timeframe=self.timeframe,
                                                                           exchange=self.API["client"])
                    one_pair_dict["first_datetime"] = first_valid_datetime
                    self.save_to_pickle(one_pair_dict)

                # Check if the coin first available timestamp was later than the desired start
                if (one_pair_df.iloc[-1].name >= self.end) and (one_pair_df.iloc[0].name <= first_valid_datetime) and (
                        first_valid_datetime > self.start):
                    logger.info(
                        f"Saved data for {self.pair} is sufficient (history starts later than expected), returning")
                    hist_df_final_cut = cut_exact_df_dates(one_pair_df, self.start, self.end)
                    return hist_df_final_cut

            logger.info(f"Saved data for {self.pair} found, not sufficient data range, getting whole fresh")

        return None

    def save_to_pickle(self, dict_to_save: dict[dt.datetime, pd.DataFrame]) -> None:
        """Pickle the data and save"""
        with open(self._pair_pickle_location, "wb") as f:
            pickle.dump(dict_to_save, f)
        logger.info(f"Saved {self.pair} history dict as pickle")

    @property
    def _pair_pickle_location(self) -> str:
        """Return the path where this pair should be saved"""
        return f"{self._history_data_folder_location}\\{self.pair_for_data}_{self.timeframe}.pickle"

    @property
    def _history_data_folder_location(self) -> str:
        """Return the path where the history for this exchange should be saved"""
        return os.path.join(ROOT_DIR, "history_data", self.exchange_name, self.timeframe)

    def _load_pickle_only(self) -> tp.Union[pd.DataFrame, None]:
        """Check for pickled data and load, if no folder for data - create"""
        try:
            if not os.path.exists(self._history_data_folder_location):
                try:
                    os.makedirs(self._history_data_folder_location)
                except:
                    pass
            with open(self._pair_pickle_location, "rb") as f:
                one_pair_dict = pickle.load(f)

            return one_pair_dict

        except FileNotFoundError as err:
            if "No such file or directory" in str(err):
                logger.info(f"No saved history for {self.pair}")
            else:
                logger.error(f"Location error, {err}")

            return None
