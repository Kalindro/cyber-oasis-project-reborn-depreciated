from Strategies.Portfolio_Clean.AB_Bot_Kucoin_USDT.API_initiation import API_initiation

from Strategies.Portfolio_Clean.Portfolio_clean import portfolio_clean


API = API_initiation()

portfolio_clean(API, Base = "USDT")
