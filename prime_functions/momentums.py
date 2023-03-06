import numpy as np
import pandas as pd
from loguru import logger
from scipy.stats import linregress
from pandas_ta import natr as NATR

def momentum_ranking_with_parity(pairs_history_df_list: list[pd.DataFrame], momentum_period: int, NATR_period: int,
                                 top_decimal: float = None, top_number: int = None, winsor_trim: bool = False):
    df_list = momentum_calculation_for_pairs_histories(pairs_history_df_list=pairs_history_df_list,
                                                       momentum_period=momentum_period, top_decimal=top_decimal,
                                                       top_number=top_number)

    for pair_df in df_list:
        pair_df["natr"] = NATR(close=pair_df["close"], high=pair_df["high"], low=pair_df["low"], window=NATR_period)
        pair_df["inv_vola"] = 1 / pair_df["natr"]

    momentum_cols = [df["momentum"] for df in df_list]
    momentum_df = pd.concat(momentum_cols, axis=1, keys=[df["pair"].iloc[0] for df in df_list])
    ranked_df = momentum_df.rank(axis=1, method='max', ascending=False)

    for pair_df in df_list:
        pair = pair_df["pair"].iloc[0]
        pair_df["rank"] = ranked_df[pair]

    inv_vola_columns = [df["inv_vola"] for df in df_list]
    inv_vola_df = pd.concat(inv_vola_columns, axis=1, keys=[df["pair"].iloc[0] for df in df_list])

    allocation_df = inv_vola_df.apply(lambda x: x/x.sum(), axis=1)

    print(allocation_df)


def momentum_calculation_for_pairs_histories(pairs_history_df_list: list[pd.DataFrame], momentum_period: int,
                                             top_decimal: float = None, top_number: int = None):
    """Calculate momentum ranking for list of history dataframes"""
    if top_number and top_decimal:
        raise AssertionError("You can only provide either top decimal or top number")

    logger.info("Calculating momentum ranking for pairs histories")
    for pair_df in pairs_history_df_list:
        pair_df["momentum"] = pair_df["close"].rolling(momentum_period).apply(_calculate_momentum)

    pairs_history_df_list_momentum = pairs_history_df_list

    return pairs_history_df_list_momentum


def _calculate_momentum(price_closes: pd.DataFrame) -> float:
    """Calculating momentum from close"""
    returns = np.log(price_closes)
    x = np.arange(len(returns))
    slope, _, rvalue, _, _ = linregress(x, returns)
    momentum = slope * 100

    return momentum * (rvalue ** 2)
    # return (((np.exp(slope) ** 252) - 1) * 100) * (rvalue**2)
