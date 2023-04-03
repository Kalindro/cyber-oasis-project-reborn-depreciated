import numpy as np
import pandas as pd
import vectorbtpro as vbt
from loguru import logger
from scipy.stats import linregress


def allocation_momentum_ranking(vbt_data: vbt.Data, momentum_period: int, NATR_period: int, top_decimal: float = None,
                                top_number: int = None) -> pd.DataFrame:
    if not (top_decimal or top_number):
        raise ValueError("Please provide either top decimal or top number")
    if not top_number:
        top_number = int(len(vbt_data.data) * top_decimal)
    momentum_data = _momentum_calc_for_vbt_data(vbt_data=vbt_data, momentum_period=momentum_period)
    ranked_df = momentum_data.rank(axis=1, method="max", ascending=False)

    natr_data = vbt_data.run("talib_NATR", timeperiod=NATR_period).real
    inv_vol_data = 1 / natr_data

    top_pairs = ranked_df <= top_number
    top_pairs_inv_vol = inv_vol_data.where(top_pairs, other=np.nan)
    sum_top_pairs_inv_vol = top_pairs_inv_vol.sum(axis=1)
    allocations = top_pairs_inv_vol.div(sum_top_pairs_inv_vol, axis=0).fillna(0)

    return allocations


def _momentum_calc_for_vbt_data(vbt_data: vbt.Data, momentum_period: int) -> dict[str: pd.DataFrame]:
    """Calculate momentum ranking for list of history dataframes"""
    logger.info("Calculating momentum ranking for pairs histories")

    return vbt_data.close.rolling(momentum_period).apply(_momentum_calculate)


def _momentum_calculate(price_closes: pd.DataFrame) -> float:
    """Calculating momentum from close"""
    returns = np.log(price_closes)
    x = np.arange(len(returns))
    slope, _, rvalue, _, _ = linregress(x, returns)
    momentum = slope * 100

    return momentum * (rvalue ** 2)  # return (((np.exp(slope) ** 252) - 1) * 100) * (rvalue**2)
