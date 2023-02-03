import datetime as dt
import inspect
import os
import traceback
import typing as tp
from pathlib import Path

import pandas as pd
from loguru import logger

from gieldy.general.utils import (date_string_to_datetime, datetime_to_timestamp_ms, timeframe_to_timestamp_ms,
                                  timestamp_ms_to_datetime, dataframe_is_not_none_and_has_elements)


class _BaseInfoClassWithValidation:
    """Class to handle all the parameters used by other classes"""

    def __init__(self, timeframe: str, save_load_history: bool, number_of_last_candles: tp.Optional[int] = None,
                 since: tp.Optional[str] = None, end: tp.Optional[str] = None):
        self.save_load_history = save_load_history
        self.timeframe = timeframe.lower()
        self.timeframe_in_timestamp = timeframe_to_timestamp_ms(timeframe)
        self.number_of_last_candles = number_of_last_candles
        self.since = since
        self.since_datetime = None
        self.since_timestamp = None
        self.end = end
        self.end_datetime = None
        self.end_timestamp = None
        self.validate_inputs()

    def validate_inputs(self) -> None:
        """Validate input parameters"""
        if not (self.number_of_last_candles or self.since):
            raise ValueError("Please provide either starting date or number of last n candles to provide")
        if self.number_of_last_candles and (self.since or self.end):
            raise ValueError("You cannot provide since/end date together with last n candles parameter")

        # Parse and convert since and end dates to timestamps
        if self.number_of_last_candles:
            logger.info("N last candles requested, saving/loading history disabled")
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


class GetFullHistoryDF(_BaseInfoClassWithValidation):
    """Main logic function to receive desired range of clean, usable history dataframe"""

    def __init__(self,
                 timeframe: str,
                 save_load_history: bool,
                 API: dict,
                 number_of_last_candles: tp.Optional[int] = None,
                 since: tp.Optional[str] = None,
                 end: tp.Optional[str] = None):
        super().__init__(timeframe, save_load_history, number_of_last_candles, since, end)
        self.API = API

    def main(self, pair: str) -> tp.Union[pd.DataFrame, None]:
        """Main logic function to loop the history acquisition"""
        try:
            logger.info(f"Getting {pair} history")
            exchange = self.API["exchange"]
            delegate_data_storing = _DataStoring(exchange, self.timeframe, pair, self.end_datetime,
                                                 self.since_datetime)
            delegate_query_history = _QueryHistory()
            delegate_df_clean_cut = _DFCleanAndCut()

            if self.save_load_history:
                hist_df_full = delegate_data_storing.load_dataframe_and_pre_check()
                if dataframe_is_not_none_and_has_elements(hist_df_full):
                    return hist_df_full
            hist_df_full = delegate_query_history.get_history_range(pair, self.timeframe, self.since_timestamp,
                                                                    self.end_timestamp, self.API)
            hist_df_final = delegate_df_clean_cut.history_df_cleaning(hist_df_full, pair)
            if not dataframe_is_not_none_and_has_elements(hist_df_full):
                logger.info(f"{pair} is broken or too short")
                return None
            if self.save_load_history:
                delegate_data_storing.save_dataframe_to_pickle(hist_df_final)

            hist_df_final_cut = delegate_df_clean_cut.cut_exact_df_dates_for_return(hist_df_final, self.since_datetime,
                                                                                    self.end_datetime)
            return hist_df_final_cut
        except Exception as err:
            logger.error(f"Error on main full history, {err}")
            print(traceback.format_exc())


class _QueryHistory:
    def get_history_range(self,
                          pair: str,
                          timeframe: str,
                          since_timestamp: int,
                          end_timestamp: int,
                          API: dict,
                          candle_limit: int = 1000) -> tp.Union[pd.DataFrame, None]:

        # Establish valid ts and delta to iter
        SAFETY_BUFFER = int(timeframe_to_timestamp_ms(timeframe) * 12)
        test_data = self._get_history_one_fragment(pair=pair,
                                                   timeframe=timeframe,
                                                   since=0,
                                                   API=API,
                                                   candle_limit=candle_limit)
        if not len(test_data):
            return None
        first_valid_timestamp = test_data[0][0]
        local_since_timestamp = max(since_timestamp, first_valid_timestamp)
        delta = int(test_data[-1][0] - test_data[0][0] - SAFETY_BUFFER)

        # Iteratively collect the data
        data: tp.List[list] = []
        while True:
            try:
                fresh_data = self._get_history_one_fragment(pair=pair,
                                                            timeframe=timeframe,
                                                            since=local_since_timestamp,
                                                            API=API,
                                                            candle_limit=candle_limit)
                data += fresh_data
                local_since_timestamp += delta

                if local_since_timestamp >= (end_timestamp + SAFETY_BUFFER):
                    break

            except Exception as error:
                logger.error(f"Error on history fragments loop, {error}")
                print(traceback.format_exc())

        # Convert data to a DataFrame
        hist_df_full = pd.DataFrame(data, columns=[
            'date',
            'open',
            'high',
            'low',
            'close',
            'volume'
        ])
        hist_df_full.index = pd.to_datetime(hist_df_full['date'], unit='ms')
        del hist_df_full['date']
        hist_df_full['open'] = hist_df_full['open'].astype(float)
        hist_df_full['high'] = hist_df_full['high'].astype(float)
        hist_df_full['low'] = hist_df_full['low'].astype(float)
        hist_df_full['close'] = hist_df_full['close'].astype(float)
        hist_df_full['volume'] = hist_df_full['volume'].astype(float)

        return hist_df_full

    def _get_history_one_fragment(self, pair: str, timeframe: str, since: int, API: dict,
                                  candle_limit) -> pd.DataFrame:
        exchange_client = API["client"]
        candles_list = exchange_client.fetchOHLCV(symbol=pair, timeframe=timeframe, since=since,
                                                  limit=candle_limit)
        return candles_list


class _DFCleanAndCut:
    """Class for cleaning and cutting the dataframe to desired dates"""

    @staticmethod
    def history_df_cleaning(hist_dataframe: pd.DataFrame, pair: str) -> tp.Union[pd.DataFrame, None]:
        """Setting index, dropping duplicates, cleaning dataframe"""
        if dataframe_is_not_none_and_has_elements(hist_dataframe):
            hist_dataframe.dropna(inplace=True)
            hist_dataframe = hist_dataframe[~hist_dataframe.index.duplicated(keep="last")]
            hist_dataframe.insert(len(hist_dataframe.columns), "pair", pair)
            hist_dataframe.insert(len(hist_dataframe.columns), "symbol",
                                  pair[:-5] if pair.endswith("/USDT") else pair[:-4])
            return hist_dataframe
        else:
            return None

    @staticmethod
    def cut_exact_df_dates_for_return(final_dataframe: pd.DataFrame, since_datetime: dt.datetime,
                                      end_datetime: dt.datetime) -> pd.DataFrame:
        """Cut the dataframe to exactly match the desired since/end, small quirk here as
         end_datetime can be precise to the second while the timeframe may be 1D - it
          would never return correctly, mainly when end is now"""
        final_dataframe = final_dataframe.loc[since_datetime:min(final_dataframe.iloc[-1].name, end_datetime)]
        return final_dataframe


class _DataStoring:
    """Class for handling the history loading and saving to pickle"""

    def __init__(self, exchange: str, timeframe: str, pair: str, end_datetime: dt.datetime,
                 since_datetime: dt.datetime):
        self.exchange = exchange
        self.timeframe = timeframe
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

    def load_dataframe_and_pre_check(self) -> tp.Union[pd.DataFrame, None]:
        """Check if loaded data range is sufficient for current request, if not - get new"""
        cut_delegate = _DFCleanAndCut()
        hist_df_full = self.parse_dataframe_from_pickle()
        if dataframe_is_not_none_and_has_elements(hist_df_full):
            if (hist_df_full.iloc[-1].name > self.end_datetime) and (hist_df_full.iloc[0].name < self.since_datetime):
                logger.info("Saved data is sufficient, returning")
                hist_df_final_cut = cut_delegate.cut_exact_df_dates_for_return(hist_df_full, self.since_datetime,
                                                                               self.end_datetime)
                return hist_df_final_cut
        logger.info("Saved data found, not sufficient data range, getting whole fresh")
        return None

    def save_dataframe_to_pickle(self, df_to_save: pd.DataFrame) -> None:
        """Pickle the data and save"""
        df_to_save.to_pickle(f"{self.pair_history_location}\\{self.pair_for_data}_{self.timeframe}.pickle")
        logger.info("Saved history dataframe as pickle")
