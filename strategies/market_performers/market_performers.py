import statistics
import traceback
import typing as tp
from dataclasses import dataclass, field
from functools import partial

import pandas as pd
from pandas import DataFrame as df
from pandas_ta.volatility import natr as NATR

from CCXT.base_functions import get_pairs_prices
from generic.funcs_for_pairs_lists import get_full_history_for_pairs_list
from generic.select_mode import select_exchange_mode, select_pairs_list_mode
from utils.log_config import ConfigureLoguru
from utils.utils import excel_save_formatted

logger = ConfigureLoguru().info_level()


# TODO Wyjebac okna dni po których nie bede filtrował z koncowego dict, dodać sektory do coinów (chuj wie jak),
#  dodac medium vol,

@dataclass
class _BaseSettings:
    """
    Modes available:
    :EXCHANGE_MODE: 1 - Binance Spot; 2 - Binance Futures; 3 - Kucoin Spot
    :PAIRS_MODE: 1 - Test single; 2 - Test multi; 3 - BTC; 4 - USDT
    """
    EXCHANGE_MODE: int = 1
    PAIRS_MODE: int = 4
    MIN_VOL_USD: float = 300_000
    QUANTILE_DROP: float = 0.60
    DAYS_WINDOWS: list[int] = field(default_factory=lambda: [1, 2, 3, 7, 14, 31])

    # Don't change
    TIMEFRAME: str = "1h"
    NUMBER_OF_LAST_CANDLES: int = 1000

    def __post_init__(self):
        self.API = select_exchange_mode(self.EXCHANGE_MODE)
        self.pairs_list = select_pairs_list_mode(self.PAIRS_MODE, self.API)
        self.BTC_price = get_pairs_prices(self.API).loc["BTC/USDT"]["price"]
        self.MIN_VOL_BTC = self.MIN_VOL_USD / self.BTC_price
        self.min_data_length = max(self.DAYS_WINDOWS) * 24


class PerformanceRankAnalysis(_BaseSettings):
    """Main analysis class"""

    def main(self) -> None:
        """Main function runin the analysis"""
        try:
            performance_calculation_results = self._calculate_performances_on_list()
            full_performance_df = self._performances_list_to_clean_df(performance_calculation_results)

            if self.PAIRS_MODE == 4:
                market_median_performance = full_performance_df["median_performance"].median()
                BTC_median_performance = full_performance_df.loc[
                    full_performance_df["pair"] == "BTC/USDT", "median_performance"].iloc[-1]
                ETH_median_performance = full_performance_df.loc[
                    full_performance_df["pair"] == "ETH/USDT", "median_performance"].iloc[-1]
                print(f"\033[93mMarket median performance: {market_median_performance:.2%}\033[0m")
                print(f"\033[93mBTC median performance: {BTC_median_performance:.2%}\033[0m")
                print(f"\033[93mETH median performance: {ETH_median_performance:.2%}\033[0m")

            excel_save_formatted(full_performance_df, filename="performance.xlsx", global_cols_size=13,
                                 cash_cols="E:F", cash_cols_size=17, rounded_cols="D:D", perc_cols="G:N",
                                 perc_cols_size=16)
            logger.success("Saved excel, all done")

        except Exception as err:
            logger.error(f"Error on main market performance, {err}")
            print(traceback.format_exc())

    def _calculate_performances_on_list(self) -> list[dict]:
        """Calculate performance on all pairs on provided list"""
        pairs_history_df_list = get_full_history_for_pairs_list(pairs_list=self.pairs_list, timeframe=self.TIMEFRAME,
                                                                number_of_last_candles=self.NUMBER_OF_LAST_CANDLES,
                                                                API=self.API, min_data_length=self.min_data_length)

        logger.info("Calculating performance for all the coins...")
        partial_performance_calculations = partial(_PerformanceCalculation().performance_calculations,
                                                   days_windows=self.DAYS_WINDOWS, min_vol_usd=self.MIN_VOL_USD,
                                                   min_vol_btc=self.MIN_VOL_BTC)
        performances_calculation_results = [partial_performance_calculations(pair_history) for pair_history in
                                            pairs_history_df_list]

        return performances_calculation_results

    def _performances_list_to_clean_df(self, performance_calculation_results: list[dict]) -> pd.DataFrame:
        """Process the list of performances and output in formatted dataframe"""
        full_performance_df = df()
        for pair_results in performance_calculation_results:
            full_performance_df = pd.concat([df(pair_results), full_performance_df],
                                            ignore_index=True)

        fast_history = full_performance_df["avg_vol_fast"]
        slow_history = full_performance_df["avg_vol_slow"]
        fast_history_quantile = fast_history.quantile(self.QUANTILE_DROP)
        slow_history_quantile = slow_history.quantile(self.QUANTILE_DROP)
        full_performance_df = full_performance_df[
            (fast_history >= fast_history_quantile) & (slow_history >= slow_history_quantile)]
        logger.success(f"Dropped bottom {self.QUANTILE_DROP * 100}% volume coins")
        full_performance_df.sort_values(by="vol_increase", ascending=False, inplace=True)

        return full_performance_df


class _PerformanceCalculation:
    """CLass containing calculations"""

    def performance_calculations(self, coin_history_df: pd.DataFrame, days_windows: list[int], min_vol_usd: int,
                                 min_vol_btc: int) -> tp.Union[dict, None]:
        """Calculation all the needed performance metrics for the pair"""
        pair = str(coin_history_df.iloc[-1].pair)
        symbol = str(coin_history_df.iloc[-1].symbol)
        price = coin_history_df.iloc[-1]["close"]
        days_history_dict = {f"{days}d_hourly_history": coin_history_df.tail(24 * days) for days in days_windows}
        fast_history = days_history_dict["3d_hourly_history"]
        slow_history = days_history_dict["31d_hourly_history"]
        avg_vol_fast = (fast_history["volume"].sum() / int(len(fast_history) / 24)) * price
        avg_vol_slow = (slow_history["volume"].sum() / int(len(slow_history) / 24)) * price
        vol_increase = avg_vol_fast / avg_vol_slow

        if pair.endswith(("/USDT", ":USDT")):
            min_vol = min_vol_usd
        elif pair.endswith(("/BTC", ":BTC")):
            min_vol = min_vol_btc
        else:
            raise ValueError("Invalid pair quote currency: " + pair)
        if avg_vol_slow < min_vol:
            logger.info(f"Skipping {pair}, not enough volume")
            return

        coin_NATR = NATR(close=fast_history["close"], high=fast_history["high"], low=fast_history["low"],
                         period=len(fast_history))[-1]

        full_performance_dict = {"pair": [pair], "symbol": [symbol], "natr": [coin_NATR],
                                 "avg_vol_fast": [avg_vol_fast], "avg_vol_slow": [avg_vol_slow],
                                 "vol_increase": [vol_increase]}
        price_change_dict = {
            f"{days}d_performance": self._calculate_price_change(days_history_dict[f"{days}d_hourly_history"])
            for days in days_windows}
        price_change_dict["median_performance"] = statistics.median(price_change_dict.values())
        full_performance_dict.update(price_change_dict)

        return full_performance_dict

    @staticmethod
    def _calculate_price_change(cut_history_dataframe: pd.DataFrame) -> float:
        """Function counting price change in %"""
        performance = (cut_history_dataframe.iloc[-1]["close"] - cut_history_dataframe.iloc[0]["close"]) / \
                      cut_history_dataframe.iloc[0]["close"]

        return performance


if __name__ == "__main__":
    PerformanceRankAnalysis().main()
