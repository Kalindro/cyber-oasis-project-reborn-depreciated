from AB_Bot.AB_Bot_Binance_USDT.API_initiation import API_initiation

from AB_Bot.Period_clean import period_clean


API = API_initiation()

period_clean(API, Base = "USDT")
