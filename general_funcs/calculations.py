from functools import partial

import numpy as np
import pandas as pd
from pandas_ta.volatility import natr as NATR
from scipy.stats import linregress

from CCXT.functions_mine import get_full_history_for_pairs_list
from CCXT.get_full_history import GetFullHistoryDF


def calculate_momentum(pair_history_dataframe: pd.DataFrame) -> float:
    closes = pair_history_dataframe["close"]
    returns = np.log(closes)
    x = np.arange(len(returns))
    slope, _, rvalue, _, _ = linregress(x, returns)
    momentum = slope * 100

    return momentum * (rvalue ** 2)


def calc_beta_neutral_allocation_for_two_pairs(self):
    """Calculate beta neutral allocation for two pairs"""
    pairs = {"long_pair": self.PAIR_LONG, "short_pair": self.PAIR_SHORT, "benchmark": self.PAIR_BENCHMARK}

    hist_func_partial = partial(GetFullHistoryDF.main, timeframe=self.TIMEFRAME, API=self.API,
                                number_of_last_candles=self.NUMBER_OF_LAST_CANDLES)
    histories = {pair_side: hist_func_partial(pair=pair_name) for pair_side, pair_name in pairs.items()}

    for pair_side, history_df in histories.items():
        history_df["returns"] = np.log(history_df["close"])
    for pair_side, history_df in histories.items():
        history_df["slope"] = linregress(x=histories["benchmark"]["returns"], y=history_df["returns"])[0]

    beta_long_pair = histories["long_pair"]["slope"][-1]
    beta_short_pair = histories["short_pair"]["slope"][-1]
    total_beta = beta_long_pair + beta_short_pair
    allocation_long_pair = (total_beta - beta_long_pair) / total_beta * self.INVESTMENT
    allocation_short_pair = (total_beta - beta_short_pair) / total_beta * -self.INVESTMENT

    print(f"Allocation to {self.PAIR_LONG}: {allocation_long_pair}")
    print(f"Allocation to {self.PAIR_SHORT}: {allocation_short_pair}")


def portfolio_parity(pairs_list: list[str], investment: int, timeframe: str, number_of_last_candles: int, period: int,
                     API: dict):
    pairs_history_df_list = get_full_history_for_pairs_list(pairs_list=pairs_list, timeframe=timeframe,
                                                            number_of_last_candles=number_of_last_candles, API=API)
    inv_vola_calculation = lambda history_df: 1 / NATR(high=history_df["high"], low=history_df["low"],
                                                       close=history_df["close"], length=period).tail(1)
    inv_vola_list = [inv_vola_calculation(pair_history) for pair_history in pairs_history_df_list]
