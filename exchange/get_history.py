import concurrent.futures
import datetime as dt
import inspect
import os
import traceback
import typing as tp
from functools import partial
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger

from utils.utils import (date_string_to_datetime, datetime_to_timestamp_ms, timeframe_to_timestamp_ms,
                         timestamp_ms_to_datetime, dataframe_is_not_none_and_not_empty)

print("CHANGES")
class GetFullHistoryDF:
    """Main logic class to receive desired range of clean, usable history dataframe"""

    def get_full_history(self,
                         pairs_list: list[str],
                         timeframe: str,
                         API: dict,
                         min_data_length: int = None,
                         vol_quantile_drop: float = None,
                         **kwargs) -> dict[str: pd.DataFrame]:
        """Get history of all pairs on list
        kwargs:
            save_load_history
            number_of_last_n_candles
            since
            end
        """
        WORKERS = 2

        logger.info("Getting history of all the coins on provided pairs list...")
        delegate_history_partial = partial(self._get_desired_history_for_pair, timeframe=timeframe,
                                           API=API, **kwargs)

        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
            pairs_history_df_dict = dict(zip(pairs_list, executor.map(delegate_history_partial, pairs_list)))

        pairs_history_df_dict = {pair: history_df for pair, history_df in pairs_history_df_dict.items() if
                                 dataframe_is_not_none_and_not_empty(history_df)}

        if min_data_length:
            pairs_history_df_dict = {pair: history_df for pair, history_df in pairs_history_df_dict.items()
                                     if len(history_df) > min_data_length}
        if vol_quantile_drop:
            pairs_history_df_dict = self._drop_bottom_quantile_vol(pairs_history_df_dict=pairs_history_df_dict,
                                                                   quantile=vol_quantile_drop)

        logger.success("History of all the coins completed, returning")

        return pairs_history_df_dict

    @staticmethod
    def _drop_bottom_quantile_vol(pairs_history_df_dict: dict[str: pd.DataFrame], quantile: float) -> \
            dict[str: pd.DataFrame]:
        mean_volumes = {pair: history_df["volume"].mean() for pair, history_df in pairs_history_df_dict.items()}
        threshold = np.quantile(list(mean_volumes.values()), 0.25)
        pairs_history_dict_quantiled = {pair: history_df for pair, history_df in pairs_history_df_dict.items() if
                                        mean_volumes[pair] > threshold}
        logger.success(f"Dropped bottom {quantile * 100}% volume coins")

        return pairs_history_dict_quantiled

    def _get_desired_history_for_pair(self,
                                      pair: str,
                                      timeframe: str,
                                      API: dict,
                                      save_load_history: bool = False,
                                      number_of_last_candles: tp.Optional[int] = None,
                                      since: tp.Optional[str] = None,
                                      end: tp.Optional[str] = None) -> tp.Union[pd.DataFrame, None]:
        """Main function to get the desired history
        kwargs:
            save_load_history
            number_of_last_n_candles
            since
            end
        """

        timeframe = timeframe.lower()
        since_timestamp, end_timestamp, since_datetime, end_datetime = self._validate_dates(timeframe,
                                                                                            number_of_last_candles,
                                                                                            since, end)
        try:
            logger.info(f"Getting {pair} history")
            exchange = API["exchange"]
            delegate_query_history = _QueryHistory()
            delegate_df_clean_cut = _DFCleanAndCut()

            if save_load_history:
                delegate_data_storing = _DataStoring(pair=pair, timeframe=timeframe, exchange=exchange,
                                                     end_datetime=end_datetime, since_datetime=since_datetime)
                hist_df_full = delegate_data_storing.load_dataframe_and_pre_check(API=API)
                if dataframe_is_not_none_and_not_empty(hist_df_full):
                    return hist_df_full

            hist_df_full = delegate_query_history.get_history_range(pair=pair, timeframe=timeframe,
                                                                    since_timestamp=since_timestamp,
                                                                    end_timestamp=end_timestamp,
                                                                    API=API)
            hist_df_final = delegate_df_clean_cut.history_df_cleaning(hist_df_full, pair)

            if not dataframe_is_not_none_and_not_empty(hist_df_final):
                logger.info(f"Skipping {pair}, broken or too short")
                return None

            if save_load_history:
                delegate_data_storing.save_dataframe_to_pickle(hist_df_final)

            hist_df_final_cut = delegate_df_clean_cut.cut_exact_df_dates_for_return(hist_df_final, since_datetime,
                                                                                    end_datetime)
            return hist_df_final_cut

        except Exception as err:
            logger.error(f"Error on main full history, {err}")
            print(traceback.format_exc())

    @staticmethod
    def _validate_dates(timeframe: str, number_of_last_candles: tp.Optional[int], since: tp.Optional[str],
                        end: tp.Optional[str]) -> tp.Tuple[int, int, dt.datetime, dt.datetime]:
        """Validate if correct arguments are passed"""
        timeframe_in_timestamp = timeframe_to_timestamp_ms(timeframe.lower())
        if not (number_of_last_candles or since):
            raise ValueError("Please provide either starting date or number of last n candles to provide")
        if number_of_last_candles and (since or end):
            raise ValueError("You cannot provide since/end date together with last n candles parameter")

        if number_of_last_candles:
            since_timestamp = datetime_to_timestamp_ms(dt.datetime.now()) - (
                    number_of_last_candles * timeframe_in_timestamp)
            since_datetime = timestamp_ms_to_datetime(since_timestamp)
        if since:
            since_datetime = date_string_to_datetime(since)
            since_timestamp = datetime_to_timestamp_ms(since_datetime)
        if end:
            end_datetime = date_string_to_datetime(end)
            end_timestamp = datetime_to_timestamp_ms(end_datetime)
        else:
            end_datetime = dt.datetime.now()
            end_timestamp = datetime_to_timestamp_ms(end_datetime)

        return since_timestamp, end_timestamp, since_datetime, end_datetime


class _QueryHistory:
    """Class handling data query"""

    def get_history_range(self,
                          pair: str,
                          timeframe: str,
                          since_timestamp: int,
                          end_timestamp: int,
                          API: dict,
                          candle_limit: int = 10000) -> tp.Union[pd.DataFrame, None]:
        """Get desired range of history"""

        # Establish valid ts and delta to iter
        safety_buffer = int(timeframe_to_timestamp_ms(timeframe) * 12)
        test_data = self.get_test_data(pair=pair, timeframe=timeframe, API=API)
        delta = int(test_data[-1][0] - test_data[0][0] - safety_buffer)
        if delta <= 0:
            return None

        first_valid_timestamp = test_data[0][0]
        local_since_timestamp = max(since_timestamp, first_valid_timestamp)

        # Iteratively collect the data
        data: tp.List[list] = []
        while True:
            try:
                fresh_data = self._get_history_one_fragment(pair=pair, timeframe=timeframe,
                                                            since=local_since_timestamp, API=API,
                                                            candle_limit=candle_limit)
                data += fresh_data
                local_since_timestamp += delta

                if local_since_timestamp >= (end_timestamp + safety_buffer):
                    break

            except Exception as error:
                logger.error(f"Error on history fragments loop, {error}")
                print(traceback.format_exc())

        # Convert data to a DataFrame
        hist_df_full = pd.DataFrame(data, columns=[
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume"
        ])
        hist_df_full.index = pd.to_datetime(hist_df_full["date"], unit="ms")
        del hist_df_full["date"]
        hist_df_full["open"] = hist_df_full["open"].astype(float)
        hist_df_full["high"] = hist_df_full["high"].astype(float)
        hist_df_full["low"] = hist_df_full["low"].astype(float)
        hist_df_full["close"] = hist_df_full["close"].astype(float)
        hist_df_full["volume"] = hist_df_full["volume"].astype(float)

        return hist_df_full

    def get_test_data(self, pair: str, timeframe: str, API: dict) -> pd.DataFrame:
        """Get test sample of data, mainly to determine first available timestamp and one query length"""
        test_data = self._get_history_one_fragment(pair=pair, timeframe=timeframe, since=0, API=API,
                                                   candle_limit=10000)
        return test_data

    @staticmethod
    def _get_history_one_fragment(pair: str, timeframe: str, since: int, API: dict,
                                  candle_limit: int) -> pd.DataFrame:
        """Get history fragment"""
        exchange_client = API["client"]
        candles_list = exchange_client.fetchOHLCV(symbol=pair, timeframe=timeframe, since=since, limit=candle_limit)
        return candles_list


class _DFCleanAndCut:
    """Class for cleaning and cutting the dataframe to desired dates"""

    @staticmethod
    def history_df_cleaning(hist_dataframe: pd.DataFrame, pair: str) -> tp.Union[pd.DataFrame, None]:
        """Setting index, dropping duplicates, cleaning dataframe"""
        if dataframe_is_not_none_and_not_empty(hist_dataframe):
            hist_dataframe.dropna(inplace=True)
            hist_dataframe = hist_dataframe[~hist_dataframe.index.duplicated(keep="last")]
            hist_dataframe.insert(len(hist_dataframe.columns), "pair",
                                  pair.split(':')[0] if ":" in pair else pair)
            hist_dataframe.insert(len(hist_dataframe.columns), "symbol", pair.split('/')[0])
            return hist_dataframe
        else:
            return None

    @staticmethod
    def cut_exact_df_dates_for_return(final_dataframe: pd.DataFrame, since_datetime: dt.datetime,
                                      end_datetime: dt.datetime) -> pd.DataFrame:
        """Cut the dataframe to exactly match the desired since/end, small quirk here as end_datetime can be precise to
         the second while the TIMEFRAME may be 1D - it would never return correctly, mainly when the end is now"""
        final_dataframe = final_dataframe.loc[since_datetime:min(final_dataframe.iloc[-1].name, end_datetime)]

        return final_dataframe


class _DataStoring:
    """Class for handling the history loading and saving to pickle"""

    def __init__(self, pair: str, timeframe: str, exchange: str, end_datetime: dt.datetime,
                 since_datetime: dt.datetime):
        self.exchange = exchange
        self.timeframe = timeframe
        self.pair = pair
        self.pair_for_data = pair.replace("/", "-")
        self.end_datetime = end_datetime
        self.since_datetime = since_datetime

    @property
    def pair_history_location(self) -> str:
        """Return the path where the history for this exchange should be saved"""
        current_path = os.path.dirname(inspect.getfile(inspect.currentframe()))
        project_path = Path(current_path).parent.parent
        return f"{project_path}\\history_data\\{self.exchange}\\{self.timeframe}"

    def parse_dataframe_from_pickle(self) -> tp.Union[pd.DataFrame, None]:
        """Check for pickled data and load, if no folder for data - create"""
        try:
            if not os.path.exists(self.pair_history_location):
                os.makedirs(self.pair_history_location)
            history_df_saved = pd.read_pickle(
                f"{self.pair_history_location}\\{self.pair_for_data}_{self.timeframe}.pickle")
            return history_df_saved

        except FileNotFoundError as err:
            logger.info(f"No saved history for {self.pair_for_data}, {err}")
            return None

    def load_dataframe_and_pre_check(self, API) -> tp.Union[pd.DataFrame, None]:
        """Check if loaded data range is sufficient for current request, if not - get new"""
        cut_delegate = _DFCleanAndCut()
        hist_df_full = self.parse_dataframe_from_pickle()

        if dataframe_is_not_none_and_not_empty(hist_df_full):
            if (hist_df_full.iloc[-1].name >= self.end_datetime) and (
                    hist_df_full.iloc[0].name <= self.since_datetime):
                logger.info(f"Saved data for {self.pair} is sufficient, returning")
                hist_df_final_cut = cut_delegate.cut_exact_df_dates_for_return(hist_df_full, self.since_datetime,
                                                                               self.end_datetime)
                return hist_df_final_cut
            else:
                delegate_get_history = _QueryHistory()
                test_data = delegate_get_history.get_test_data(pair=self.pair, timeframe=self.timeframe, API=API)
                first_valid_datetime = timestamp_ms_to_datetime(test_data[0][0])
                if (hist_df_full.iloc[-1].name >= self.end_datetime) and (
                        hist_df_full.iloc[0].name <= first_valid_datetime) and (
                        first_valid_datetime > self.since_datetime):
                    logger.info(
                        f"Saved data for {self.pair} is sufficient (history starts later than expected), returning")
                    hist_df_final_cut = cut_delegate.cut_exact_df_dates_for_return(hist_df_full, self.since_datetime,
                                                                                   self.end_datetime)
                    return hist_df_final_cut

        logger.info(f"Saved data for {self.pair} found, not sufficient data range, getting whole fresh")
        return None

    def save_dataframe_to_pickle(self, df_to_save: pd.DataFrame) -> None:
        """Pickle the data and save"""
        df_to_save.to_pickle(f"{self.pair_history_location}\\{self.pair_for_data}_{self.timeframe}.pickle")
        logger.info(f"Saved {self.pair} history dataframe as pickle")
