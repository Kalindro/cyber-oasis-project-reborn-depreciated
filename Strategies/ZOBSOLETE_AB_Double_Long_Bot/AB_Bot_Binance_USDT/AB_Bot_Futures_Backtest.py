from AB_Bot.AB_Bot_Binance_USDT.API_initiation import API_initiation

from LS_Bot.Backtest_LS import backtest_ab



if __name__ == "__main__":
    API = API_initiation()

    backtest_ab(API)
