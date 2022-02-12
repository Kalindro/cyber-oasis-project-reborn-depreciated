from Gieldy.Binance.API_initiation_Binance_AB_USDT import API_initiation as API_Binance_AB_USDT
from Gieldy.Kucoin.API_initiation_Kucoin_AB_USDT import API_initiation as API_Kucoin_AB_USDT
from Gieldy.Gateio.API_initiation_Gateio_AB_USDT import API_initiation as API_Gateio_AB_USDT
from Gieldy.Okx.API_initiation_Okx_AB_USDT import API_initiation as API_Okex_AB_USDT

from Gieldy.Refractor_general.Main_refracting import *


def global_cancel():
    print("Cancelling API_Binance_AB_USDT")
    cancel_all_orders(API_Binance_AB_USDT())
    print("Cancelling API_Kucoin_AB_USDT")
    cancel_all_orders(API_Kucoin_AB_USDT())
    print("Cancelling API_Gateio_AB_USDT")
    cancel_all_orders(API_Gateio_AB_USDT())
    print("Cancelling API_Okex_AB_USDT")
    cancel_all_orders(API_Okex_AB_USDT())

print("Emergency cancellation of all orders on all AB exchanges")

global_cancel()
