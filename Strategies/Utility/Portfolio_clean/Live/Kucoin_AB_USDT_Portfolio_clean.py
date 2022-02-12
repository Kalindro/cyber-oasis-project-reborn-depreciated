from Gieldy.Kucoin.API_initiation_Kucoin_AB_USDT import API_initiation

from Strategies.Utility.Portfolio_clean.Portfolio_clean import portfolio_clean

API = API_initiation()

portfolio_clean(API, base="USDT")
