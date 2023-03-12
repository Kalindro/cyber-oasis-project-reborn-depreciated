import numpy as np
import pandas as pd
from loguru import logger
from pandas_ta import natr as NATR
from scipy.stats import linregress

from exchange.get_history import GetFullHistoryDF


def calc_portfolio_parity(pairs_history_df_dict: dict[str, pd.DataFrame], NATR_period: int, investment: int = 1000,
                          winsor_trim: bool = False) -> dict[str, pd.DataFrame]:
    """Calculate parity allocation for list of history dataframes"""
    logger.info("Calculating portfolio parity for pairs histories")

    TRIM = 0.1
    total_inv_vola = 0
    for pair, pair_df in pairs_history_df_dict.items():
        pair_df["natr"] = NATR(close=pair_df["close"], high=pair_df["high"], low=pair_df["low"], window=NATR_period)
        inv_vola = 1 / pair_df["natr"]
        pair_df["inv_vola"] = inv_vola
        total_inv_vola += inv_vola

    for pair, pair_df in pairs_history_df_dict.items():
        pair_df["weight"] = round(pair_df["inv_vola"] / total_inv_vola, 4)
        pair_df["weight_ccy"] = round(pair_df["weight"] * investment, 0)
        pair_df.drop(columns=["natr", "inv_vola"])

    if winsor_trim:
        natr_values = [history_df["natr"].iloc[-1] for history_df in pairs_history_df_dict.values()]
        lower = pd.Series(natr_values).quantile(TRIM)
        upper = pd.Series(natr_values).quantile(1 - TRIM)
        pairs_history_df_dict = {pair: history_df for pair, history_df in pairs_history_df_dict.items() if
                                 (history_df["natr"].iloc[-1] > lower) or (history_df["natr"].iloc[-1] < upper)}

    pairs_history_df_dict_portfolio_parity = pairs_history_df_dict

    return pairs_history_df_dict_portfolio_parity


def calc_beta_neutral_allocation_for_two_pairs(pair_long: str, pair_short: str, timeframe: str,
                                               number_of_last_candles: int, API: dict, beta_period: int,
                                               investment: int = 1000,
                                               **kwargs) -> dict[str, pd.DataFrame]:
    """Calculate beta neutral allocation for two pairs"""
    benchmark = "BTC/USDT"
    pairs = [pair_long, pair_short]
    all_pairs = pairs + [benchmark]
    pairs_history_df_dict = GetFullHistoryDF.get_full_history(pairs_list=all_pairs, timeframe=timeframe, API=API,
                                                              number_of_last_candles=number_of_last_candles, **kwargs)

    for pair_df in pairs_history_df_dict.values():
        pair_df["returns"] = np.log(pair_df["close"])

    benchmark_history_df = pairs_history_df_dict[benchmark]
    pairs_history_df_dict = {pair: pairs_history_df_dict[pair] for pair in pairs}

    total_beta = 0
    for pair_df in pairs_history_df_dict.values():
        asset_rolling_returns = pair_df["returns"].rolling(beta_period)
        beta = asset_rolling_returns.apply(
            lambda x: linregress(x, benchmark_history_df.loc[x.index][-beta_period:]["returns"])[0])
        pair_df["beta"] = beta
        total_beta += beta

    for pair_df in pairs_history_df_dict.values():
        pair_df["allocation"] = round((total_beta - pair_df["beta"]) / total_beta, 4)
        pair_df["allocation_ccy"] = round((total_beta - pair_df["beta"]) / total_beta * investment, 0)
        pair_df.drop(columns=["returns"])

    pairs_history_df_list_beta_neutral = pairs_history_df_dict

    return pairs_history_df_list_beta_neutral
