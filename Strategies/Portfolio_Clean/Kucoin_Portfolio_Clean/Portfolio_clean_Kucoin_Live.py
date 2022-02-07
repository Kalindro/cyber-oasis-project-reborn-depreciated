from Gieldy.Kucoin.API_initiation_Kucoin import API_initiation

from Strategies.Portfolio_Clean.Portfolio_clean import portfolio_clean

API = API_initiation()

portfolio_clean(API, base="USDT")
