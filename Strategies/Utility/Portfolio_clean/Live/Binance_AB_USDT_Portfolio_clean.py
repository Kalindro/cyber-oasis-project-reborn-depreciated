from Gieldy.Binance.Manual_initiation.API_initiation_Binance_AB_USDT import API_initiation

from Strategies.Utility.Portfolio_clean.Portfolio_clean import portfolio_clean

API = API_initiation()

portfolio_clean(API, base="USDT")
