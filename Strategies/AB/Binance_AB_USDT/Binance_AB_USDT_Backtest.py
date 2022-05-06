from Gieldy.Binance.Manual_initiation.API_initiation_Binance_AB_USDT import API_initiation

from Strategies.AB_Long.AB_Backtest import backtest_AB_l

if __name__ == "__main__":
    API = API_initiation()

    backtest_AB_l(API, base="USDT")
