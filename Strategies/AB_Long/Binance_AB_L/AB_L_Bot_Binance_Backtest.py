from Gieldy.Binance.API_initiation_Binance import API_initiation

from Strategies.AB_Long.Backtest_AB_L import backtest_AB_l

if __name__ == "__main__":
    API = API_initiation()

    backtest_AB_l(API, base="USDT")
