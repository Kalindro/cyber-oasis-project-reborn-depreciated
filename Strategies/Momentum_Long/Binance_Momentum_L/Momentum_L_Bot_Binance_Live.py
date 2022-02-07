from Gieldy.Binance.API_initiation_Binance import API_initiation

from Strategies.Momentum_Long_Bot.Live_Momentum_L import live_momentum_L


if __name__ == "__main__":
    API = API_initiation()

    live_momentum_L(API)
