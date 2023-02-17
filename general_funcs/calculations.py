from functools import partial

import numpy as np
import pandas as pd
from pandas_ta.volatility import natr as NATR
from scipy.stats import linregress

from CCXT.functions_mine import get_full_history_for_pairs_list
from CCXT.get_full_history import GetFullHistoryDF


def calculate_momentum(pair_history_dataframe: pd.DataFrame) -> float:
    """Calculating momentum from close"""
    closes = pair_history_dataframe["close"]
    returns = np.log(closes)
    x = np.arange(len(returns))
    slope, _, rvalue, _, _ = linregress(x, returns)
    momentum = slope * 100

    return momentum * (rvalue ** 2)


def calc_beta_neutral_allocation_for_two_pairs(pair_long: str, pair_short: str, investment: int, timeframe: str,
                                               number_of_last_candles: int, API: dict) -> None:
    """Calculate beta neutral allocation for two pairs"""
    pairs = {"pair_long": pair_long, "pair_short": pair_short, "benchmark": "BTC/USDT"}

    hist_func_partial = partial(GetFullHistoryDF.main, timeframe=timeframe, API=API,
                                number_of_last_candles=number_of_last_candles)
    histories = {pair_side: hist_func_partial(pair=pair_name) for pair_side, pair_name in pairs.items()}

    for pair_side, history_df in histories.items():
        history_df["returns"] = np.log(history_df["close"])
    for pair_side, history_df in histories.items():
        history_df["slope"] = linregress(x=histories["benchmark"]["returns"], y=history_df["returns"])[0]

    beta_long_pair = histories["pair_long"]["slope"][-1]
    beta_short_pair = histories["pair_short"]["slope"][-1]
    total_beta = beta_long_pair + beta_short_pair
    allocation_long_pair = (total_beta - beta_long_pair) / total_beta * investment
    allocation_short_pair = (total_beta - beta_short_pair) / total_beta * investment

    print(f"Allocation to {pair_long}: {allocation_long_pair}")
    print(f"Allocation to {pair_short}: {allocation_short_pair}")


def calc_portfolio_parity(pairs_list: list[str], investment: int, period: int, timeframe: str,
                          number_of_last_candles: int, API: dict) -> None:
    """Calculate parity allocation for list/portfolio"""
    pairs_history_df_list = get_full_history_for_pairs_list(pairs_list=pairs_list, timeframe=timeframe,
                                                            number_of_last_candles=number_of_last_candles, API=API)
    inv_vola_calculation = lambda history_df: 1 / NATR(high=history_df["high"], low=history_df["low"],
                                                       close=history_df["close"], length=period).tail(1)
    inv_vola_list = [inv_vola_calculation(pair_history) for pair_history in pairs_history_df_list]
