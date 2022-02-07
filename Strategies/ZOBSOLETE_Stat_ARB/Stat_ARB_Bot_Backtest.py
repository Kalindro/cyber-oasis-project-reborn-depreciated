from Stat_ARB_Bot.API_initiation import API_initiation

from Stat_ARB_Bot.Stat_ARB import backtest_ab


if __name__ == "__main__":
    API = API_initiation()

    backtest_ab(API, Base = "USDT")
