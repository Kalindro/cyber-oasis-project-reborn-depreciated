from Gieldy.Binance.API_initiation_Binance_AB_USDT import API_initiation

from Strategies.Portfolio_clean.Portfolio_clean import portfolio_clean

API = API_initiation()

portfolio_clean(API, base="USDT")
