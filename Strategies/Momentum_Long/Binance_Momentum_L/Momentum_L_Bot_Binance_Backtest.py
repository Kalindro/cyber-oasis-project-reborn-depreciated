from Gieldy.Binance.API_initiation_Binance import API_initiation
from Gieldy.FTX.API_initiation_FTX import API_initiation as FTX_API

from Strategies.Momentum_Long_Bot.Backtest_Momentum_L import RunBacktest


if __name__ == "__main__":
    API = API_initiation()
    API["FTX"] = FTX_API()

    RunBacktest().results_controller(API=API, base="USDT")
