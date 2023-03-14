import numpy as np
import pandas as pd
from loguru import logger
from pandas_ta import natr as NATR
from scipy.stats import linregress


def momentum_ranking_with_parity(pairs_history_df_dict: dict[str: pd.DataFrame], momentum_period: int,
                                 NATR_period: int, top_decimal: float = None, top_number: int = None,
                                 winsor_trim: bool = False) -> pd.DataFrame:
    pairs_history_df_dict_momentum = _momentum_calc_for_pairs_histories(pairs_history_df_dict=pairs_history_df_dict,
                                                                        momentum_period=momentum_period,
                                                                        top_decimal=top_decimal, top_number=top_number)
    for pair_df in pairs_history_df_dict_momentum.values():
        natr = NATR(close=pair_df["close"], high=pair_df["high"], low=pair_df["low"], window=NATR_period)
        pair_df["inv_vola"] = 1 / natr.fillna(0)

    # Step 2: Rank the pairs based on momentum
    pair_names = pairs_history_df_dict_momentum.keys()
    momentum_df = pd.concat([df["momentum"] for df in pairs_history_df_dict_momentum.values()], axis=1,
                            keys=pair_names)
    ranked_df = momentum_df.rank(axis=1, method="max", ascending=False)

    # Step 3: Calculate the weighted average of inverse volatility for the top 2 ranked pairs
    inv_vola_df = pd.concat([df["inv_vola"] for df in pairs_history_df_dict_momentum.values()], axis=1,
                            keys=pair_names)
    top_pairs = ranked_df <= 2
    sum_inv_vola_top_pairs = inv_vola_df.where(top_pairs, other=0).sum(axis=1)
    inv_vola_top_pairs = inv_vola_df.where(top_pairs, other=np.nan)
    inv_vola_top_pairs = inv_vola_top_pairs.div(sum_inv_vola_top_pairs, axis=0)
    inv_vola_top_pairs = inv_vola_top_pairs.fillna(0)

    # Step 4: Normalize the weighted average of inverse volatility
    inv_vola_df = inv_vola_top_pairs.divide(inv_vola_top_pairs.sum(axis=1), axis=0)
    inv_vola_df = inv_vola_df.fillna(0)

    return inv_vola_df


def _momentum_calc_for_pairs_histories(pairs_history_df_dict: dict[str: pd.DataFrame], momentum_period: int,
                                       top_decimal: float = None, top_number: int = None) -> dict[str: pd.DataFrame]:
    """Calculate momentum ranking for list of history dataframes"""
    if top_number and top_decimal:
        raise AssertionError("You can only provide either top decimal or top number")

    logger.info("Calculating momentum ranking for pairs histories")
    for pair_df in pairs_history_df_dict.values():
        pair_df["momentum"] = pair_df["close"].rolling(momentum_period).apply(_momentum_calculate)

    pairs_history_df_dict_momentum = pairs_history_df_dict

    return pairs_history_df_dict_momentum


def _momentum_calculate(price_closes: pd.DataFrame) -> float:
    """Calculating momentum from close"""
    returns = np.log(price_closes)
    x = np.arange(len(returns))
    slope, _, rvalue, _, _ = linregress(x, returns)
    momentum = slope * 100

    return momentum * (rvalue ** 2)
    # return (((np.exp(slope) ** 252) - 1) * 100) * (rvalue**2)
