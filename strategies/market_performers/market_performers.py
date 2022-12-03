from pandas import DataFrame as df
from talib import NATR
from scipy.stats import linregress

import multiprocessing as mp
import pandas as pd
import numpy as np

from gieldy.APIs.API_exchange_initiator import ExchangeAPI
from gieldy.CCXT.CCXT_functions import get_pairs_prices, get_pairs_precisions_status
from gieldy.general.utils import excel_save_formatted
from gieldy.CCXT.get_full_history import GetFullHistory

pd.set_option('display.max_rows', 0)
pd.set_option('display.max_columns', 0)
pd.set_option('display.width', 0)


class MarketPerformers:
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
        self.API_fut = ExchangeAPI().binance_futures_read()
        self.API_spot = ExchangeAPI().binance_spot_read()
        self.BTC_PRICE = get_pairs_prices(self.API_spot).loc["BTC/USDT"]["price"]
        self.MIN_VOL_USD = 150_000
        self.MIN_VOL_BTC = self.MIN_VOL_USD / self.BTC_PRICE

    def futures_USDT_pairs_list(self):
        """Only pairs on Binance futures USDT"""
        pairs_precisions_status = get_pairs_precisions_status(self.API_fut)
        pairs_precisions_status = pairs_precisions_status[pairs_precisions_status["active"] == "True"]
        pairs_list_original = list(pairs_precisions_status.index)
        pairs_list = [str(pair) for pair in pairs_list_original if str(pair).endswith("/USDT")]

        return pairs_list

    def spot_BTC_pairs_list(self):
        """Only pairs on Binance spot BTC"""
        pairs_precisions_status = get_pairs_precisions_status(self.API_spot)
        pairs_precisions_status = pairs_precisions_status[pairs_precisions_status["active"] == "True"]
        pairs_list_original = list(pairs_precisions_status.index)
        pairs_list = [str(pair) for pair in pairs_list_original if str(pair).endswith("/BTC")]

        return pairs_list

    def spot_USDT_pairs_list(self):
        """Only pairs on Binance spot USDT"""
        pairs_precisions_status = get_pairs_precisions_status(self.API_spot)
        pairs_precisions_status = pairs_precisions_status[pairs_precisions_status["active"] == "True"]
        pairs_list_original = list(pairs_precisions_status.index)
        pairs_list = [str(pair) for pair in pairs_list_original if str(pair).endswith("/USDT")]

        return pairs_list

    def select_pairs_list_and_API(self):
        """Depending on the PAIRS_MODE, return correct paris list and API"""
        fut_API = ExchangeAPI().binance_futures_read()
        spot_API = ExchangeAPI().binance_spot_read()

        if self.PAIRS_MODE == "test" or self.PAIRS_MODE == 1:
            pairs_list = ["BTC/USDT", "ETH/USDT"]
            API = fut_API
        elif self.PAIRS_MODE == "futures_USDT" or self.PAIRS_MODE == 2:
            pairs_list = self.futures_USDT_pairs_list()
            API = fut_API
        elif self.PAIRS_MODE == "spot_BTC" or self.PAIRS_MODE == 3:
            pairs_list = self.spot_BTC_pairs_list()
            API = spot_API
        elif self.PAIRS_MODE == "spot_USDT" or self.PAIRS_MODE == 4:
            pairs_list = self.spot_USDT_pairs_list()
            API = spot_API
        else:
            raise ValueError("Invalid mode: " + str(self.PAIRS_MODE))

        return pairs_list, API

    def get_history(self, API, pair, timeframe, last_n_candles):
        """Function for last n candles of history"""
        pair_history = GetFullHistory(API=API, pair=pair, save_load=False, timeframe=timeframe,
                                      last_n_candles=last_n_candles).main()

        return pair_history

    @staticmethod
    def calculate_price_change(cut_history_dataframe):
        """Function counting price change in %"""
        performance = (cut_history_dataframe.iloc[-1]["close"] - cut_history_dataframe.iloc[0]["close"]) / \
                      cut_history_dataframe.iloc[0]["close"]

        return performance

    @staticmethod
    def calculate_momentum(history):
        """Momentum calculation"""
        closes = history["close"]
        returns = np.log(closes)
        x = np.arange(len(returns))
        slope, _, rvalue, _, _ = linregress(x, returns)
        momentum = (np.power(np.exp(slope), 30) - 1) * 100

        return momentum * (rvalue ** 2)

    def performance_calculations(self, pair):
        """Calculation all the needed performance metrics for the pair"""
        _, API = self.select_pairs_list_and_API()
        long_history = self.get_history(pair=pair, timeframe="1h", last_n_candles=775, API=API)
        last_24h_hourly_history = long_history.tail(24)
        last_2d_hourly_history = long_history.tail(48)
        last_3d_hourly_history = long_history.tail(72)
        last_7d_hourly_history = long_history.tail(168)
        last_14d_hourly_history = long_history.tail(336)
        avg_daily_volume_base = last_7d_hourly_history["volume"].sum() / 7
        avg_24h_vol_usd = avg_daily_volume_base * last_7d_hourly_history.iloc[-1]["close"]
        last_pair = str(long_history.iloc[-1].pair)
        if last_pair.endswith("/BTC"):
            MIN_VOL = self.MIN_VOL_BTC
        elif last_pair.endswith("/USDT"):
            MIN_VOL = self.MIN_VOL_USD
        else:
            raise ValueError("Invalid pair quote currenct: " + last_pair)
        if avg_24h_vol_usd < MIN_VOL or len(long_history) < 770:
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
        coin_NATR = NATR(last_7d_hourly_history["high"], last_7d_hourly_history["low"], last_7d_hourly_history["close"],
                         timeperiod=len(last_7d_hourly_history) - 4)
        median_momentum = np.median([last_24h_momentum, last_3d_momentum, last_7d_momentum, last_14d_performance])
        performance_dict = {
            "pair": [pair],
            "avg_24h_vol_usd": [avg_24h_vol_usd],
            "NATR": [coin_NATR[-1]],
            "24h performance": [last_24h_performance],
            "24h momentum": [last_24h_momentum],
            "2d performance": [last_2d_performance],
            "2d momentum": [last_2d_momentum],
            "3d performance": [last_3d_performance],
            "3d momentum": [last_3d_momentum],
            "7d performance": [last_7d_performance],
            "7d momentum": [last_7d_momentum],
            "14d performance": [last_14d_performance],
            "14d momentum": [last_14d_momentum],
            "median momentum": [median_momentum],
        }

        return performance_dict

    def main(self):
        """Main function to run"""

        pairs_list, _ = self.select_pairs_list_and_API()

        with mp.Pool(self.CORES_USED) as pool:
            performance_calculation_map_results = pool.map(self.performance_calculations, pairs_list)

        global_performance_dataframe = df()
        for pair_results in performance_calculation_map_results:
            global_performance_dataframe = pd.concat([df(pair_results), global_performance_dataframe],
                                                     ignore_index=True)

        excel_save_formatted(global_performance_dataframe, column_size=15, cash_cols="C:C", rounded_cols="D:D",
                             perc_cols="E:O")

        print("Saved excel, done")


if __name__ == "__main__":
    MarketPerformers().main()
