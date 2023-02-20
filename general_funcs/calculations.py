import numpy as np
import pandas as pd
from pandas_ta.volatility import natr as NATR
from scipy.stats import linregress

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


def momentum_ranking_for_pairs_list(pairs_history_df_list: list[pd.DataFrame]):
    """Calculate momentum ranking for list of history dataframes"""
    for pair_df in pairs_history_df_list:
        momentums = stocks.copy(deep=True)
        for ticker in tickers:
            momentums[ticker] = stocks[ticker].rolling(90).apply(momentum, raw=False)


def calc_portfolio_parity(pairs_history_df_list: list[pd.DataFrame], period: int, investment: int = 1000,
                          winsor_trim: bool = False) -> list[pd.DataFrame]:
    """Calculate parity allocation for list of history dataframes"""
    TRIM = 0.1

    total_inv_vola = 0
    for pair_df in pairs_history_df_list:
        pair_df["natr"] = NATR(close=pair_df["close"], high=pair_df["high"], low=pair_df["low"], window=period)
        inv_vola = 1 / pair_df["natr"]
        pair_df["inv_vola"] = inv_vola
        total_inv_vola += inv_vola

    for pair_df in pairs_history_df_list:
        pair_df["weight"] = pair_df["inv_vola"] / total_inv_vola
        pair_df["weight_ccy"] = pair_df["weight"] * investment
        pair_df.drop(columns=["natr", "inv_vola"])

    if winsor_trim:
        natr_values = [df["natr"].iloc[-1] for df in pairs_history_df_list]
        lower = pd.Series(natr_values).quantile(TRIM)
        upper = pd.Series(natr_values).quantile(1 - TRIM)
        pairs_history_df_list = [df for df in pairs_history_df_list if
                                 df["natr"].iloc[-1] < lower or df["natr"].iloc[-1] > upper]

    pairs_history_df_list_portfolio_parity = pairs_history_df_list

    # allocation_df = []
    # for pair_history_df in pairs_history_df_list:
    #     pair_df = pd.DataFrame({
    #         "pair": [pair_history_df["pair"].iloc[-1]],
    #         "weight": [pair_history_df["weight"].iloc[-1]],
    #         "weight_ccy": [pair_history_df["weight_ccy"].iloc[-1]],
    #     })
    #     allocation_df.append(pair_df)
    #
    # allocation_df = pd.concat(allocation_df).set_index("pair")

    return pairs_history_df_list_portfolio_parity


def calc_beta_neutral_allocation_for_two_pairs(pair_long: str, pair_short: str, timeframe: str,
                                               number_of_last_candles: int, API: dict, period: int,
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

    benchmark_rolling_returns = benchmark_history_df['returns'].rolling(period)

    total_beta = 0
    for pair_df in pairs_history_df_list:
        asset_rolling_returns = pair_df['returns'].rolling(period)
        beta = asset_rolling_returns.apply(
            lambda x: linregress(x, benchmark_history_df.loc[x.index][-period:]['returns'])[0])
        pair_df["beta"] = beta
        total_beta += beta

    for pair_df in pairs_history_df_list:
        pair_df["allocation"] = round((total_beta - pair_df["beta"]) / total_beta, 4)
        pair_df["allocation_ccy"] = round((total_beta - pair_df["beta"]) / total_beta * investment, 0)

    pairs_history_df_list_beta_neutral = pairs_history_df_list

    return pairs_history_df_list_beta_neutral


def calc_beta_neutral_allocation_for_two_pairs2(pair_long: str, pair_short: str, timeframe: str,
                                                number_of_last_candles: int, API: dict, period: int,
                                                investment: int = 1000,
                                                **kwargs) -> pd.DataFrame:
    """Calculate beta neutral allocation for two pairs"""
    benchmark = "BTC/USDT"
    pairs = [pair_long, pair_short, benchmark]
    pairs_history_df_list = get_full_history_for_pairs_list(pairs_list=list(pairs), timeframe=timeframe, API=API,
                                                            number_of_last_candles=number_of_last_candles, **kwargs)

    for pair_df in pairs_history_df_list:
        pair_df["returns"] = np.log(pair_df["close"])

    benchmark_history_df = pairs_history_df_list[2]
    pairs_history_df_list = pairs_history_df_list[0:2]

    for pair_df in pairs_history_df_list:
        pair_df["beta"] = linregress(x=benchmark_history_df["returns"], y=pair_df["returns"])[0]

    total_beta = 0
    for pair_df in pairs_history_df_list:
        total_beta += pair_df["beta"]

    for pair_df in pairs_history_df_list:
        pair_df["allocation"] = round((total_beta - pair_df["beta"]) / total_beta, 4)
        pair_df["allocation_ccy"] = round((total_beta - pair_df["beta"]) / total_beta * investment, 0)

    allocation_df = []
    for history_df in pairs_history_df_list:
        pair_df = pd.DataFrame({
            "pair": [history_df["pair"].iloc[-1]],
            "allocation": [history_df["allocation"].iloc[-1]],
            "allocation_ccy": [history_df["allocation_ccy"].iloc[-1]],
        })
        allocation_df.append(pair_df)

    allocation_df = pd.concat(allocation_df).set_index("pair")

    return allocation_df
