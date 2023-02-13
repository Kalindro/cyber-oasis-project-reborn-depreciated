import statistics
import traceback
from functools import partial
from typing import Union

import pandas as pd
from pandas import DataFrame as df
from pandas_ta.volatility import natr as NATR

from CCXT.CCXT_functions_builtin import get_pairs_prices
from CCXT.CCXT_functions_mine import get_history_df_of_pairs_on_list, select_exchange_mode, \
    select_pairs_list_mode
from general.log_config import ConfigureLoguru
from general.utils import excel_save_formatted

logger = ConfigureLoguru().info_level()


class _BaseSettings:

    def __init__(self):
        """
        Modes available:
        :EXCHANGE_MODE: 1 - Binance Spot; 2 - Binance Futures; 3 - Kucoin Spot
        :PAIRS_MODE: 1 - Test single; 2 - Test multi; 3 - BTC; 4 - USDT
        """
        self.EXCHANGE_MODE = 1
        self.PAIRS_MODE = 4
        self.TIMEFRAME = "1h"  # Don't change
        self.NUMBER_OF_LAST_CANDLES = 1000  # Don't change
        self.MIN_VOL_USD = 300_000
        self.QUANTILE = 0.75
        self.CORES_USED = 6

        self.API = select_exchange_mode(self.EXCHANGE_MODE)
        self.pairs_list = select_pairs_list_mode(self.PAIRS_MODE, self.API)
        self.BTC_price = get_pairs_prices(self.API).loc["BTC/USDT"]["price"]
        self.min_vol_BTC = self.MIN_VOL_USD / self.BTC_price


class MomentumRank(_BaseSettings):
    def main(self) -> None:
        try:
            pairs_history_df_list = get_history_df_of_pairs_on_list(pairs_list=self.pairs_list,
                                                                    timeframe=self.TIMEFRAME,
                                                                    number_of_last_candles=self.NUMBER_OF_LAST_CANDLES,
                                                                    API=self.API)

            delegate_momentum = _MomentumCalculations()
            partial_performance_calculations = partial(delegate_momentum.performance_calculations,
                                                       min_vol_USD=self.MIN_VOL_USD, min_vol_BTC=self.min_vol_BTC)
            logger.info("Calculating performance for all the coins...")
            performance_calculation_map_results = map(partial_performance_calculations, pairs_history_df_list)
            global_performance_dataframe = df()
            for pair_results in performance_calculation_map_results:
                global_performance_dataframe = pd.concat([df(pair_results), global_performance_dataframe],
                                                         ignore_index=True)

            fast_history = global_performance_dataframe['avg_vol_fast']
            slow_history = global_performance_dataframe['avg_vol_slow']
            fast_history_quantile = fast_history.quantile(self.QUANTILE)
            slow_history_quantile = slow_history.quantile(self.QUANTILE)
            global_performance_dataframe = global_performance_dataframe[
                (fast_history >= fast_history_quantile) & (slow_history >= slow_history_quantile)]
            logger.info(f"Dropped bottom {self.QUANTILE} volume coins")

            if self.PAIRS_MODE == 4:
                market_median_performance = global_performance_dataframe["median_performance"].median()
                BTC_median_performance = global_performance_dataframe.loc[
                    global_performance_dataframe["symbol"] == "BTC", "median_performance"].iloc[-1]
                ETH_median_performance = global_performance_dataframe.loc[
                    global_performance_dataframe["symbol"] == "ETH", "median_performance"].iloc[-1]
                print(f"\033[93mMarket median performance: {market_median_performance:.2%}\033[0m")
                print(f"\033[93mBTC median performance: {BTC_median_performance:.2%}\033[0m")
                print(f"\033[93mETH median performance: {ETH_median_performance:.2%}\033[0m")

            excel_save_formatted(global_performance_dataframe, filename="performance.xlsx", global_cols_size=13,
                                 cash_cols="E:F", cash_cols_size=17, rounded_cols="D:D", rounded_cols_size=None,
                                 perc_cols="G:N", perc_cols_size=16)
            logger.success("Saved excel, all done")

        except Exception as err:
            logger.error(f"Error on main market performance, {err}")
            print(traceback.format_exc())


class _MomentumCalculations:

    @classmethod
    def _calculate_price_change(cls, cut_history_dataframe: pd.DataFrame) -> float:
        """Function counting price change in %"""
        performance = (cut_history_dataframe.iloc[-1]["close"] - cut_history_dataframe.iloc[0]["close"]) / \
                      cut_history_dataframe.iloc[0]["close"]

        return performance

    def performance_calculations(self, coin_history_df: pd.DataFrame, min_vol_USD: int, min_vol_BTC: int) -> Union[
        dict, None]:
        """Calculation all the needed performance metrics for the pair"""
        pair = str(coin_history_df.iloc[-1].pair)
        symbol = str(coin_history_df.iloc[-1].symbol)
        price = coin_history_df.iloc[-1]["close"]
        days_list = [1, 2, 3, 7, 14, 31]
        days_history_dict = {f"{days}d_hourly_history": coin_history_df.tail(24 * days) for days in
                             days_list}
        fast_history = days_history_dict["2d_hourly_history"]
        slow_history = days_history_dict["31d_hourly_history"]
        avg_vol_fast = (fast_history["volume"].sum() / int(len(fast_history) / 24)) * price
        avg_vol_slow = (slow_history["volume"].sum() / int(len(slow_history) / 24)) * price
        vol_increase = avg_vol_fast / avg_vol_slow

        if pair.endswith(("/BTC", ":BTC")):
            min_vol = min_vol_BTC
        elif pair.endswith(("/USDT", ":USDT")):
            min_vol = min_vol_USD
        else:
            raise ValueError("Invalid pair quote currency: " + pair)
        if avg_vol_slow < min_vol or len(coin_history_df) < max(days_list * 24):
            logger.info(f"Skipping {pair}, not enough volume or too short")
            return

        coin_NATR = NATR(close=fast_history["close"], high=fast_history["high"], low=fast_history["low"],
                         period=len(fast_history))[-1]

        core_dict = {"pair": [pair], "symbol": [symbol], "natr": [coin_NATR], "avg_vol_fast": [avg_vol_fast],
                     "avg_vol_slow": [avg_vol_slow], "vol_increase": [vol_increase]}
        performance_dict = {
            f"{days}d_performance": self._calculate_price_change(days_history_dict[f"{days}d_hourly_history"])
            for days in days_list}
        performance_dict["median_performance"] = statistics.median(performance_dict.values())
        core_dict.update(performance_dict)

        return core_dict


if __name__ == "__main__":
    MomentumRank().main()
