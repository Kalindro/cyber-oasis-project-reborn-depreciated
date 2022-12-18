import multiprocessing as mp
import pandas as pd
import numpy as np
from functools import partial
from scipy.stats import linregress
from pandas import DataFrame as df
from talib import NATR

from gieldy.API.API_exchange_initiator import ExchangeAPISelect
from gieldy.CCXT.CCXT_functions_builtin import get_pairs_prices
from gieldy.CCXT.CCXT_functions_mine import get_pairs_list_USDT, get_pairs_list_BTC
from gieldy.general.utils import excel_save_formatted
from gieldy.CCXT.get_full_history import GetFullCleanHistoryDataframe

pd.set_option('display.max_rows', 0)
pd.set_option('display.max_columns', 0)
pd.set_option('display.width', 0)


class _MarketSettings:
    """
    Script for performance of all coins in a table. Pairs modes:
    - test (1)
    - futures_USDT (2)
    - spot_BTC (3)
    - spot_USDT (4)
    """

    def __init__(self):
        self.PAIRS_MODE = 2
        self.CORES_USED = 6
        self.min_vol_USD = 150_000
        self.timeframe = "1h"
        self.number_of_last_candles = 775
        self.API_spot = ExchangeAPISelect().binance_spot_read_only()
        self.API_fut = ExchangeAPISelect().binance_futures_read_only()
        self.BTC_price = get_pairs_prices(self.API_spot).loc["BTC/USDT"]["price"]
        self.min_vol_BTC = self.min_vol_USD / self.BTC_price

    def pairs_list_futures_USDT(self):
        """Only pairs on Binance futures USDT"""
        pairs_list = get_pairs_list_USDT(self.API_fut)
        return pairs_list

    def pairs_list_spot_USDT(self):
        """Only pairs on Binance spot BTC"""
        pairs_list = get_pairs_list_USDT(self.API_spot)
        return pairs_list

    def pairs_list_spot_BTC(self):
        """Only pairs on Binance spot BTC"""
        pairs_list = get_pairs_list_BTC(self.API_spot)
        return pairs_list

    def select_pairs_list_and_API(self):
        """Depending on the PAIRS_MODE, return correct paris list and API"""
        pairs_list_and_api = {1: (["BTC/USDT", "ETH/USDT"], self.API_fut),
                              2: (self.pairs_list_futures_USDT(), self.API_fut),
                              3: (self.pairs_list_spot_USDT(), self.API_spot),
                              4: (self.pairs_list_spot_BTC(), self.API_spot)}
        pairs_list_and_api = pairs_list_and_api.get(self.PAIRS_MODE)
        if pairs_list_and_api is None:
            raise ValueError("Invalid mode: " + str(self.PAIRS_MODE))
        return pairs_list_and_api


class _MomentumCalculations:

    @classmethod
    def calculate_price_change(cls, cut_history_dataframe):
        """Function counting price change in %"""
        performance = (cut_history_dataframe.iloc[-1]["close"] - cut_history_dataframe.iloc[0]["close"]) / \
                      cut_history_dataframe.iloc[0]["close"]
        return performance

    @classmethod
    def calculate_momentum(cls, pair_history_dataframe):
        """Momentum calculation"""
        closes = pair_history_dataframe["close"]
        returns = np.log(closes)
        x = np.arange(len(returns))
        slope, _, rvalue, _, _ = linregress(x, returns)
        momentum = slope * 100
        return momentum * (rvalue ** 2)

    def performance_calculations(self, pair, timeframe, number_of_last_candles, API):
        """Calculation all the needed performance metrics for the pair"""
        long_history = GetFullCleanHistoryDataframe(timeframe, False, API, number_of_last_candles).main(pair)
        last_24h_hourly_history = long_history.tail(24)
        last_2d_hourly_history = long_history.tail(48)
        last_3d_hourly_history = long_history.tail(72)
        last_7d_hourly_history = long_history.tail(168)
        last_14d_hourly_history = long_history.tail(336)
        avg_daily_volume_base = last_7d_hourly_history["volume"].sum() / 7
        avg_24h_vol_usd = avg_daily_volume_base * last_7d_hourly_history.iloc[-1]["close"]
        last_pair = str(long_history.iloc[-1].pair)
        if last_pair.endswith("/BTC"):
            min_vol = _MarketSettings().min_vol_BTC
        elif last_pair.endswith("/USDT"):
            min_vol = _MarketSettings().min_vol_USD
        else:
            raise ValueError("Invalid pair quote currency: " + last_pair)
        if avg_24h_vol_usd < min_vol or len(long_history) < 770:
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


class MomentumRank(_MarketSettings):
    def main(self):
        """Main function to run"""
        print("Starting...")
        pairs_list, API = self.select_pairs_list_and_API()
        delegate_momentum = _MomentumCalculations()
        partial_momentum = partial(delegate_momentum.performance_calculations, timeframe=self.timeframe,
                                   number_of_last_candles=self.number_of_last_candles, API=API)
        with mp.Pool(self.CORES_USED) as pool:
            performance_calculation_map_results = pool.map(partial_momentum, pairs_list)

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
            print(f"\033[92mMarket median momentum: {market_median_momentum:.2%}\033[0m")
            print(f"\033[92mBTC median momentum: {BTC_median_momentum:.2%}\033[0m")
            print(f"\033[92mETH median momentum: {ETH_median_momentum:.2%}\033[0m")
        excel_save_formatted(global_performance_dataframe, column_size=15, cash_cols="D:D", rounded_cols="E:E",
                             perc_cols="F:Q")
        print("Saved excel, done")


if __name__ == "__main__":
    MomentumRank().main()
