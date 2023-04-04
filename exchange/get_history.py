import datetime as dt
import inspect
import os
import time
import traceback
import typing as tp
import warnings
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path

import numpy as np
import pandas as pd
import vectorbtpro as vbt
from loguru import logger

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
                         min_data_length: int = None,
                         vol_quantile_drop: float = None,
                         save_load_history: bool = False,
                         number_of_last_candles: tp.Optional[int] = None,
                         start: tp.Optional[str] = None,
                         end: tp.Optional[str] = None) -> dict[str: pd.DataFrame]:
        """Get history of all pairs on list, cut and checked if needed"""
        try:
            logger.info("Getting history of all the coins on provided pairs list...")

            if number_of_last_candles and save_load_history:
                save_load_history = False
                warnings.warn("ERROR, You cannot provide last_n_candles and save_load together,"
                              " saving/loading turned off")

            vbt_full_history = self._get_vbt_all_pairs_desired_history(pairs_list=pairs_list, timeframe=timeframe,
                                                                       API=API, save_load_history=save_load_history,
                                                                       number_of_last_candles=number_of_last_candles,
                                                                       start=start, end=end)

            # if min_data_length:
            #     pairs_history_df_dict = {pair: history_df for pair, history_df in pairs_history_df_dict.items()
            #                              if len(history_df) > min_data_length}
            # if vol_quantile_drop:
            #     pairs_history_df_dict = self._drop_bottom_quantile_vol(pairs_history_df_dict=pairs_history_df_dict,
            #                                                            quantile=vol_quantile_drop)

            logger.success("History of all the coins completed, returning")

            return vbt_full_history

        except Exception as err:
            logger.error(f"Error on main full history, {err}")
            print(traceback.format_exc())

    def _get_vbt_all_pairs_desired_history(self,
                                           pairs_list: list[str],
                                           timeframe: str,
                                           API: dict,
                                           save_load_history: bool = False,
                                           number_of_last_candles: tp.Optional[int] = None,
                                           start: tp.Optional[str] = None,
                                           end: tp.Optional[str] = None) -> vbt.Data:
        """Main function to get the desired history"""
        start, end = self._validate_dates(timeframe=timeframe, number_of_last_candles=number_of_last_candles,
                                          start=start, end=end)

        vbt_history_partial = partial(self._get_vbt_one_pair_desired_history, timeframe=timeframe, start=start,
                                      end=end, API=API, save_load_history=save_load_history)

        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            histories_dict = dict(zip(pairs_list, executor.map(vbt_history_partial, pairs_list)))

        vbt_full_history = vbt.Data.from_data(histories_dict)

        return vbt_full_history

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

        _, one_pair_df = vbt.CCXTData.fetch(symbols=pair, timeframe=timeframe, start=start - delta * 6,
                                            end=end + delta * 6, exchange=API["client"], skip_on_error=True,
                                            show_progress=False).data.popitem()

        if save_load_history:
            delegate_data_storing.save_to_pickle(one_pair_df)

        return cut_exact_df_dates(one_pair_df, start, end)

    @staticmethod
    def _validate_dates(timeframe: str,
                        number_of_last_candles: tp.Optional[int],
                        start: tp.Optional[str],
                        end: tp.Optional[str]) -> tp.Tuple[dt.datetime, tp.Union[None, dt.datetime]]:
        """Validate if correct arguments are passed"""
        timeframe_in_timedelta = timeframe_to_timedelta(timeframe)
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

        return start, end

    @staticmethod
    def _drop_bottom_quantile_vol(pairs_history_df_dict: dict[str: pd.DataFrame], quantile: float) -> \
            dict[str: pd.DataFrame]:
        mean_volumes = {pair: history_df["volume"].mean() for pair, history_df in pairs_history_df_dict.items()}
        threshold = np.quantile(list(mean_volumes.values()), 0.25)
        pairs_history_dict_quantiled = {pair: history_df for pair, history_df in pairs_history_df_dict.items() if
                                        mean_volumes[pair] > threshold}
        logger.success(f"Dropped bottom {quantile * 100}% volume coins")

        return pairs_history_dict_quantiled


class _DataStoring:
    """Class for handling the history loading and saving to pickle"""

    def __init__(self, pair: str, timeframe: str, start: dt.datetime, end: dt.datetime, API: dict):
        self.pair = pair
        self.pair_for_data = pair.replace("/", "-")
        self.timeframe = timeframe
        self.start = start
        self.end = end
        self.exchange = API["exchange"]

    def load_pickle_and_pre_check(self) -> tp.Union[pd.DataFrame, None]:
        """Check if loaded data range is sufficient for current request, if not - get new"""
        hist_df_full = self._load_pickle_only()

        if dataframe_is_not_none_and_not_empty(hist_df_full):
            if (hist_df_full.iloc[0].name <= self.start) and (hist_df_full.iloc[-1].name >= self.end):
                logger.info(f"Saved data for {self.pair} is sufficient, returning")
                hist_df_final_cut = cut_exact_df_dates(hist_df_full, self.start, self.end)
                return hist_df_final_cut
            # else:
            #     delegate_get_history = _QueryHistory()
            #     test_data = delegate_get_history.get_test_data(pair=self.pair, timeframe=self.timeframe, API=API)
            #     first_valid_datetime = timestamp_ms_to_datetime(test_data[0][0])
            #     if (hist_df_full.iloc[-1].name >= self.end) and (
            #             hist_df_full.iloc[0].name <= first_valid_datetime) and (first_valid_datetime > self.start):
            #         logger.info(
            #             f"Saved data for {self.pair} is sufficient (history starts later than expected), returning")
            #         hist_df_final_cut = cut_delegate.cut_exact_df_dates_for_return(hist_df_full, self.start, self.end)
            #         return hist_df_final_cut

            logger.info(f"Saved data for {self.pair} found, not sufficient data range, getting whole fresh")

        return None

    def save_to_pickle(self, df_to_save: pd.DataFrame) -> None:
        """Pickle the data and save"""
        df_to_save.to_pickle(f"{self._pair_history_location}\\{self.pair_for_data}_{self.timeframe}.pickle")
        logger.info(f"Saved {self.pair} history dataframe as pickle")

    @property
    def _pair_history_location(self) -> str:
        """Return the path where the history for this exchange should be saved"""
        current_path = os.path.dirname(inspect.getfile(inspect.currentframe()))
        project_path = Path(current_path).parent

        return f"{project_path}\\history_data\\{self.exchange}\\{self.timeframe}"

    def _load_pickle_only(self) -> tp.Union[pd.DataFrame, None]:
        """Check for pickled data and load, if no folder for data - create"""
        try:
            if not os.path.exists(self._pair_history_location):
                os.makedirs(self._pair_history_location)
            history_df_saved = pd.read_pickle(
                f"{self._pair_history_location}\\{self.pair_for_data}_{self.timeframe}.pickle")
            return history_df_saved

        except FileNotFoundError as err:
            if "No such file or directory" in str(err):
                logger.info(f"No saved history for {self.pair}")
            else:
                logger.error(f"Location error, {err}")
            return None
