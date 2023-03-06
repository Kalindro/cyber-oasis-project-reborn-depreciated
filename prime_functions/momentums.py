import numpy as np
import pandas as pd
from loguru import logger
from scipy.stats import linregress


def momentum_ranking_with_parity(pairs_history_df_list: list[pd.DataFrame], momentum_period: int, NATR_period: int,
                                 top_decimal: float = None, top_number: int = None, winsor_trim: bool = False):
    # df_list = momentum_calculation_for_pairs_histories(pairs_history_df_list=pairs_history_df_list,
    #                                                    momentum_period=momentum_period, top_decimal=top_decimal,
    #                                                    top_number=top_number)

    # momentum_cols = [df['momentum'] for df in df_list]
    # df = pd.concat(momentum_cols, axis=1, keys=[df["pair"].iloc[0] for df in df_list])
    #
    # def get_percentiles(row):
    #     top_20_pct = row.quantile(0.8)
    #     bottom_20_pct = row.quantile(0.2)
    #     return pd.Series([top_20_pct, bottom_20_pct])
    #
    # df[["top_pct", "bottom_pct"]] = df.apply(get_percentiles, axis=1)
    #
    # new_df = pd.DataFrame(index=df.index, columns=df.columns)
    #
    # print(df)

    momentum_dict = {}
    for pair_df in pairs_history_df_list:
        symbol = pair_df["symbol"].iloc[-1]
        pair_df["momentum"] = pair_df["close"].rolling(momentum_period).apply(_calculate_momentum)
        momentum_dict[symbol] = pair_df["momentum"].iloc[-1]

    momentum_df = pd.DataFrame.from_dict(momentum_dict, orient="index", columns=["momentum"])
    sorted_momentum = momentum_df.sort_values("momentum", ascending=False)

    if top_decimal:
        top_bottom_number = int(len(sorted_momentum) * top_decimal)
    elif top_number:
        top_bottom_number = top_number
    top_coins = sorted_momentum.index[:top_bottom_number].tolist()
    bottom_coins = sorted_momentum.index[-top_bottom_number:].tolist()
    top_coins_history_df_list = [pair_df for pair_df in pairs_history_df_list if
                                 pair_df["pair"].iloc[-1] in top_coins]
    bottom_coins_history_df_list = [pair_df for pair_df in pairs_history_df_list if
                                    pair_df["pair"].iloc[-1] in bottom_coins]
    return top_coins_history_df_list, bottom_coins_history_df_list


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
