from Gieldy.Binance.API_initiation_Binance_AB_USDT import API_initiation

from Strategies.Momentum_Long.Momentum_Live import live_momentum_L


if __name__ == "__main__":
    API = API_initiation()

    live_momentum_L(API)
