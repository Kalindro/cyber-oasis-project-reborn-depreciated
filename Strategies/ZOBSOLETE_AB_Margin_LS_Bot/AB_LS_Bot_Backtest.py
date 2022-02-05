from AB_LS_Bot.API_initiation import API_initiation

from AB_LS_Bot.AB_LS import backtest_ab


if __name__ == "__main__":
    API = API_initiation()

    backtest_ab(API, Base = "USDT")
