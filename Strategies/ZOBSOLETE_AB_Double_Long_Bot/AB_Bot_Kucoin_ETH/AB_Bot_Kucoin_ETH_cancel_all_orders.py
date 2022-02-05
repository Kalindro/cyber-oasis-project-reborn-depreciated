from AB_Bot.AB_Bot_Kucoin_ETH.API_initiation import API_initiation
from Gieldy.Refractor_general.Main_refracting import cancel_all_orders

API = API_initiation()

cancel_all_orders(API)

