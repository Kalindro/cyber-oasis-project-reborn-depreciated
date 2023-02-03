import traceback
from functools import partial
from typing import Union

import numpy as np
import pandas as pd
from pandas import DataFrame as df
from scipy.stats import linregress
from talib import NATR

from CCXT.CCXT_functions_builtin import get_pairs_prices
from CCXT.CCXT_functions_mine import get_history_of_all_pairs_on_list, select_exchange_mode, \
    select_pairs_list_mode
from general.log_config import ConfigureLoguru
from general.utils import excel_save_formatted

pd.set_option('display.max_rows', 0)
pd.set_option('display.max_columns', 0)
pd.set_option('display.width', 0)

logger = ConfigureLoguru().info_level()


class _BaseSettings:

    def __init__(self):
        """
        Modes available:
        :EXCHANGE_MODE: 1 - Binance Spot; 2 - Binance Futures; 3 - Kucoin Spot
        :PAIRS_MODE: 1 - Test single; 2 - Test multi; 3 - BTC; 4 - USDT
        """
        self.EXCHANGE_MODE = 1
        self.PAIRS_MODE = 1
        self.save_load_history = False
        self.timeframe = "1h"
        self.number_of_last_candles = 2000
        self.min_vol_USD = 150_000
        self.CORES_USED = 6

        self.API = select_exchange_mode(self.EXCHANGE_MODE)
        self.pairs_list = select_pairs_list_mode(self.PAIRS_MODE, self.API)
        self.BTC_price = get_pairs_prices(self.API).loc["BTC/USDT"]["price"]
        self.min_vol_BTC = self.min_vol_USD / self.BTC_price


class _MomentumCalculations:

    @classmethod
    def calculate_price_change(cls, cut_history_dataframe: pd.DataFrame) -> float:
        """Function counting price change in %"""
        performance = (cut_history_dataframe.iloc[-1]["close"] - cut_history_dataframe.iloc[0]["close"]) / \
                      cut_history_dataframe.iloc[0]["close"]
        return performance

    @classmethod
    def calculate_momentum(cls, pair_history_dataframe: pd.DataFrame) -> float:
        """Momentum calculation"""
        closes = pair_history_dataframe["close"]
        returns = np.log(closes)
        x = np.arange(len(returns))
        slope, _, rvalue, _, _ = linregress(x, returns)
        momentum = slope * 100
        return momentum * (rvalue ** 2)

    def performance_calculations(self, coin_history_df: pd.DataFrame, min_vol_USD: int, min_vol_BTC: int) -> Union[
        dict, None]:
        """Calculation all the needed performance metrics for the pair"""
        last_24h_hourly_history = coin_history_df.tail(24)
        last_2d_hourly_history = coin_history_df.tail(48)
        last_3d_hourly_history = coin_history_df.tail(72)
        last_7d_hourly_history = coin_history_df.tail(168)
        last_14d_hourly_history = coin_history_df.tail(336)
        avg_daily_volume_base = last_7d_hourly_history["volume"].sum() / 7
        avg_24h_vol_usd = avg_daily_volume_base * last_7d_hourly_history.iloc[-1]["close"]
        pair = str(coin_history_df.iloc[-1].pair)
        if pair.endswith("/BTC"):
            min_vol = min_vol_BTC
        elif pair.endswith("/USDT"):
            min_vol = min_vol_USD
        else:
            raise ValueError("Invalid pair quote currency: " + pair)
        if avg_24h_vol_usd < min_vol or len(coin_history_df) < 770:
            return
        last_24h_performance = self.calculate_price_change(last_24h_hourly_history)
        last_24h_momentum = self.calculate_momentum(last_24h_hourly_history)
        last_2d_performance = self.calculate_price_change(last_2d_hourly_history)
        last_2d_momentum = self.calculate_momentum(last_2d_hourly_history)
        last_3d_performance = self.calculate_price_change(last_3d_hourly_history)
        last_3d_momentum = self.calculate_momentum(last_3d_hourly_history)
        last_7d_performance = self.calculate_price_change(last_7d_hourly_history)
        last_7d_momentum = self.calculate_momentum(last_7d_hourly_history)
        last_14d_performance = self.calculate_price_change(last_14d_hourly_history)
        last_14d_momentum = self.calculate_momentum(last_14d_hourly_history)
        coin_NATR = NATR(last_14d_hourly_history["high"], last_14d_hourly_history["low"],
                         last_14d_hourly_history["close"], timeperiod=len(last_14d_hourly_history) - 4)
        median_momentum = np.median([last_24h_momentum, last_3d_momentum, last_7d_momentum, last_14d_performance])
        median_momentum_weighted = median_momentum / coin_NATR[-1]
        performance_dict = {"pair": [pair], "symbol": [last_24h_hourly_history.iloc[-1]["symbol"]],
                            "avg_24h_vol_usd": [avg_24h_vol_usd], "NATR": [coin_NATR[-1]],
                            "24h performance": [last_24h_performance], "24h momentum": [last_24h_momentum],
                            "2d performance": [last_2d_performance], "2d momentum": [last_2d_momentum],
                            "3d performance": [last_3d_performance], "3d momentum": [last_3d_momentum],
                            "7d performance": [last_7d_performance], "7d momentum": [last_7d_momentum],
                            "14d performance": [last_14d_performance], "14d momentum": [last_14d_momentum],
                            "median momentum": [median_momentum], "mom weighted": [median_momentum_weighted]}
        return performance_dict


class MomentumRank(_BaseSettings):
    def main(self) -> None:
        try:
            all_pairs_history_list = get_history_of_all_pairs_on_list(pairs_list=self.pairs_list,
                                                                      timeframe=self.timeframe,
                                                                      save_load_history=self.save_load_history,
                                                                      number_of_last_candles=self.number_of_last_candles,
                                                                      API=self.API)
            delegate_momentum = _MomentumCalculations()
            partial_performance_calculations = partial(delegate_momentum.performance_calculations,
                                                       min_vol_USD=self.min_vol_USD, min_vol_BTC=self.min_vol_BTC)
            logger.info("Calculating performance for all the coins...")
            performance_calculation_map_results = map(partial_performance_calculations, all_pairs_history_list)
            global_performance_dataframe = df()
            for pair_results in performance_calculation_map_results:
                global_performance_dataframe = pd.concat([df(pair_results), global_performance_dataframe],
                                                         ignore_index=True)
            if self.PAIRS_MODE != 1:
                market_median_momentum = global_performance_dataframe["median momentum"].median()
                BTC_median_momentum = global_performance_dataframe.loc[
                    global_performance_dataframe["symbol"] == "BTC", "median momentum"].iloc[-1]
                ETH_median_momentum = global_performance_dataframe.loc[
                    global_performance_dataframe["symbol"] == "ETH", "median momentum"].iloc[-1]
                print(f"\033[93mMarket median momentum: {market_median_momentum:.2%}\033[0m")
                print(f"\033[93mBTC median momentum: {BTC_median_momentum:.2%}\033[0m")
                print(f"\033[93mETH median momentum: {ETH_median_momentum:.2%}\033[0m")
            excel_save_formatted(global_performance_dataframe, column_size=15, cash_cols="D:D", rounded_cols="E:E",
                                 perc_cols="F:Q")
            logger.success("Saved excel, all done")
        except Exception as err:
            logger.error(f"Error on main market performance, {err}")
            print(traceback.format_exc())


if __name__ == "__main__":
    MomentumRank().main()
