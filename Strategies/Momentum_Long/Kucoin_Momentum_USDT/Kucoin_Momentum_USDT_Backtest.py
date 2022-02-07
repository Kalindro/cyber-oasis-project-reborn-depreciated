from Gieldy.Kucoin.API_initiation_Kucoin_AB_USDT import API_initiation

from Strategies.Momentum_Long.Backtest_Momentum_L_beta import backtest_momentum_L


if __name__ == "__main__":

    API = API_initiation()

    backtest_momentum_L(API, base="USDT")
