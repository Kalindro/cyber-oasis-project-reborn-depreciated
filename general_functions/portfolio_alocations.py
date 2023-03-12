import numpy as np
import pandas as pd
from loguru import logger
from pandas_ta import natr as NATR
from scipy.stats import linregress

from exchange.get_full_history import get_full_history_for_pairs_list


def calc_portfolio_parity(pairs_history_df_list: list[pd.DataFrame], NATR_period: int, investment: int = 1000,
                          winsor_trim: bool = False) -> list[pd.DataFrame]:
    """Calculate parity allocation for list of history dataframes"""
    logger.info("Calculating portfolio parity for pairs histories")

    TRIM = 0.1
    total_inv_vola = 0
    for pair_df in pairs_history_df_list:
        pair_df["natr"] = NATR(close=pair_df["close"], high=pair_df["high"], low=pair_df["low"], window=NATR_period)
        inv_vola = 1 / pair_df["natr"]
        pair_df["inv_vola"] = inv_vola
        total_inv_vola += inv_vola

    for pair_df in pairs_history_df_list:
        pair_df["weight"] = round(pair_df["inv_vola"] / total_inv_vola, 4)
        pair_df["weight_ccy"] = round(pair_df["weight"] * investment, 0)
        pair_df.drop(columns=["natr", "inv_vola"])

    if winsor_trim:
        natr_values = [df["natr"].iloc[-1] for df in pairs_history_df_list]
        lower = pd.Series(natr_values).quantile(TRIM)
        upper = pd.Series(natr_values).quantile(1 - TRIM)
        pairs_history_df_list = [df for df in pairs_history_df_list if
                                 df["natr"].iloc[-1] > lower or df["natr"].iloc[-1] < upper]

    pairs_history_df_list_portfolio_parity = pairs_history_df_list

    return pairs_history_df_list_portfolio_parity


def calc_beta_neutral_allocation_for_two_pairs(pair_long: str, pair_short: str, timeframe: str,
                                               number_of_last_candles: int, API: dict, beta_period: int,
                                               investment: int = 1000,
                                               **kwargs) -> list[pd.DataFrame]:
    """Calculate beta neutral allocation for two pairs"""
    benchmark = "BTC/USDT"
    pairs = [pair_long, pair_short, benchmark]
    pairs_history_df_list = get_full_history_for_pairs_list(pairs_list=list(pairs), timeframe=timeframe, API=API,
                                                            number_of_last_candles=number_of_last_candles, **kwargs)

    for pair_df in pairs_history_df_list:
        pair_df["returns"] = np.log(pair_df["close"])

    benchmark_history_df = pairs_history_df_list[2]
    pairs_history_df_list = pairs_history_df_list[0:2]

    total_beta = 0
    for pair_df in pairs_history_df_list:
        asset_rolling_returns = pair_df["returns"].rolling(beta_period)
        beta = asset_rolling_returns.apply(
            lambda x: linregress(x, benchmark_history_df.loc[x.index][-beta_period:]["returns"])[0])
        pair_df["beta"] = beta
        total_beta += beta

    for pair_df in pairs_history_df_list:
        pair_df["allocation"] = round((total_beta - pair_df["beta"]) / total_beta, 4)
        pair_df["allocation_ccy"] = round((total_beta - pair_df["beta"]) / total_beta * investment, 0)
        pair_df.drop(columns=["returns"])

    pairs_history_df_list_beta_neutral = pairs_history_df_list

    return pairs_history_df_list_beta_neutral
