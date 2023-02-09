import statistics
import traceback
from functools import partial
from typing import Union

import pandas as pd
from pandas import DataFrame as df

from CCXT.CCXT_functions_builtin import get_pairs_prices
from CCXT.CCXT_functions_mine import get_history_df_of_pairs_on_list, select_exchange_mode, \
    select_pairs_list_mode
from general.log_config import ConfigureLoguru
from general.utils import excel_save_formatted

logger = ConfigureLoguru().info_level()


class _MomentumCalculations:

    @classmethod
    def calculate_price_change(cls, cut_history_dataframe: pd.DataFrame) -> float:
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

        days_list = [1, 3, 7, 14, 31]
        days_history_dict = {f"{days}d_hourly_history": coin_history_df.tail(24 * days) for days in
                             days_list}

        avg_daily_vol_1d = days_history_dict["1d_hourly_history"]["volume"].sum() * price
        avg_daily_vol_31d = (days_history_dict["31d_hourly_history"]["volume"].sum() / 31) * price
        vol_increase = avg_daily_vol_31d / avg_daily_vol_1d

        if pair.endswith(("/BTC", ":BTC")):
            min_vol = min_vol_BTC
        elif pair.endswith(("/USDT", ":USDT")):
            min_vol = min_vol_USD
        else:
            raise ValueError("Invalid pair quote currency: " + pair)
        if avg_daily_vol_31d < min_vol or len(coin_history_df) < max(days_list * 24):
            logger.info(f"Skipping {pair}, not enough volume or too short")
            return

        core_dict = {"pair": [pair], "symbol": [symbol], "avg_24h_vol": [avg_daily_vol_31d],
                     "vol_increase": [vol_increase]}
        performance_dict = {
            f"{days}d_performance": self.calculate_price_change(days_history_dict[f"{days}d_hourly_history"])
            for days in days_list}
        performance_dict["median_performance"] = statistics.median(performance_dict.values())
        core_dict.update(performance_dict)

        return core_dict


class _BaseSettings:

    def __init__(self):
        """
        Modes available:
        :EXCHANGE_MODE: 1 - Binance Spot; 2 - Binance Futures; 3 - Kucoin Spot
        :PAIRS_MODE: 1 - Test single; 2 - Test multi; 3 - BTC; 4 - USDT
        """
        self.EXCHANGE_MODE = 1
        self.PAIRS_MODE = 4
        self.TIMEFRAME = "1h"
        self.NUMBER_OF_LAST_CANDLES = 1000
        self.MIN_VOL_USD = 150_000
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

            if self.PAIRS_MODE == 4:
                market_median_performance = global_performance_dataframe["median_performance"].median()
                BTC_median_performance = global_performance_dataframe.loc[
                    global_performance_dataframe["symbol"] == "BTC", "median_performance"].iloc[-1]
                ETH_median_performance = global_performance_dataframe.loc[
                    global_performance_dataframe["symbol"] == "ETH", "median_performance"].iloc[-1]
                print(f"\033[93mMarket median performance: {market_median_performance:.2%}\033[0m")
                print(f"\033[93mBTC median performance: {BTC_median_performance:.2%}\033[0m")
                print(f"\033[93mETH median performance: {ETH_median_performance:.2%}\033[0m")

            excel_save_formatted(global_performance_dataframe, global_cols_size=11, cash_cols="D:D",
                                 cash_cols_size=None, rounded_cols=None, rounded_cols_size=None, perc_cols="E:K",
                                 perc_cols_size=17)
            logger.success("Saved excel, all done")

        except Exception as err:
            logger.error(f"Error on main market performance, {err}")
            print(traceback.format_exc())


if __name__ == "__main__":
    MomentumRank().main()
