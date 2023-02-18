import numpy as np
import pandas as pd
from pandas_ta.volatility import natr as NATR
from scipy.stats import linregress, trim_mean

from CCXT.functions_mine import get_full_history_for_pairs_list


def calculate_momentum(pair_history_dataframe: pd.DataFrame) -> float:
    """Calculating momentum from close"""
    closes = pair_history_dataframe["close"]
    returns = np.log(closes)
    x = np.arange(len(returns))
    slope, _, rvalue, _, _ = linregress(x, returns)
    momentum = slope * 100

    return momentum * (rvalue ** 2)
    # return (((np.exp(slope) ** 252) - 1) * 100) * (rvalue**2)


def calc_beta_neutral_allocation_for_two_pairs(pair_long: str, pair_short: str, timeframe: str,
                                               number_of_last_candles: int, API: dict, investment: int = 1000,
                                               **kwargs) -> pd.DataFrame:
    """Calculate beta neutral allocation for two pairs"""
    benchmark = "BTC/USDT"
    pairs = [pair_long, pair_short, benchmark]
    histories_list = get_full_history_for_pairs_list(pairs_list=list(pairs), timeframe=timeframe, API=API,
                                                     number_of_last_candles=number_of_last_candles, **kwargs)
    histories = {"pair_long": histories_list[0],
                 "pair_short": histories_list[1],
                 "benchmark": histories_list[2]}

    for pair_side, history_df in histories.items():
        history_df["returns"] = np.log(history_df["close"])
    for pair_side, history_df in histories.items():
        history_df["slope"] = linregress(x=histories["benchmark"]["returns"], y=history_df["returns"])[0]

    beta_long_pair = histories["pair_long"]["slope"][-1]
    beta_short_pair = histories["pair_short"]["slope"][-1]
    total_beta = beta_long_pair + beta_short_pair
    allocation_long_pair = round((total_beta - beta_long_pair) / total_beta, 4)
    allocation_short_pair = round((total_beta - beta_short_pair) / total_beta, 4)
    allocation_long_pair_ccy = round((total_beta - beta_long_pair) / total_beta * investment, 0)
    allocation_short_pair_ccy = round((total_beta - beta_short_pair) / total_beta * -investment, 0)

    allocation_data = {
        "pair": [pair_long, pair_short],
        "allocation": [allocation_long_pair, allocation_short_pair],
        "allocation_ccy": [allocation_long_pair_ccy, allocation_short_pair_ccy]
    }
    allocation_df = pd.DataFrame(allocation_data).set_index("pair")

    return allocation_df


def calc_portfolio_parity(pairs_list: list[str], period: int, timeframe: str, number_of_last_candles: int, API: dict,
                          investment: int = 1000, **kwargs):
    """Calculate parity allocation for list/portfolio"""
    trim = 0.1
    pairs_history_df_list = get_full_history_for_pairs_list(pairs_list=pairs_list, timeframe=timeframe,
                                                            number_of_last_candles=number_of_last_candles, API=API,
                                                            **kwargs)
    total_inv_vola = 0
    for pair_df in pairs_history_df_list:
        pair_df["natr"] = NATR(close=pair_df["close"], high=pair_df["high"], low=pair_df["low"], window=period)
        inv_vola = 1 / pair_df["natr"]
        pair_df["inv_vola"] = inv_vola
        total_inv_vola += inv_vola

    for pair_df in pairs_history_df_list:
        pair_df["inv_vola_zscore"] = (pair_df["inv_vola"] - pair_df["inv_vola"].mean()) / pair_df["inv_vola"].std()
        pair_df["inv_vola_zscore_winsor"] = trim_mean(pair_df["inv_vola_zscore"], trim)
        pair_df["weight"] = pair_df["inv_vola_zscore_winsor"].abs() / total_inv_vola
        pair_df["weight"] = pair_df["weight"] / pair_df["weight"].sum()
        pair_df["weight_ccy"] = pair_df["weight"] * investment

    dfs = []
    for pair_history_df in pairs_history_df_list:
        pair_df = pd.DataFrame({
            "pair": [pair_history_df["pair"].iloc[-1]],
            "weight": [pair_history_df["weight"].iloc[-1]],
            "weight_ccy": [pair_history_df["weight_ccy"].iloc[-1]],
        })

        dfs.append(pair_df)

    allocation_df = pd.concat(dfs).set_index("pair")
    for pair_history_df in pairs_history_df_list:
        pair_history_df.drop(columns=["open", "high", "low", "close", "volume"])
        print(pair_history_df)

    return allocation_df
