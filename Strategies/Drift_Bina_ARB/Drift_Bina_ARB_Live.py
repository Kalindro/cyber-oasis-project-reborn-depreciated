import asyncio
import pandas as pd
import time

from Gieldy.Binance.Binance_utils import *
from Gieldy.Drift.Drift_utils import *

from Gieldy.Binance.API_initiation_Binance_Futures_USDT import API_initiation as API_1
from Gieldy.Drift.API_initiation_Drift_USDC import API_initiation as API_2


asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


async def main():
    API_Binance = API_1()
    API_Drift = API_2()

    drift_prices = await drift_get_pair_prices_rates(API_Drift)
    bina_prices = binance_get_futures_pair_prices_rates(API_Binance)
    arb_df = df()
    arb_df["drift_price"] = drift_prices["mark_price"]
    arb_df["bina_price"] = bina_prices["mark_price"]
    arb_df["gap_percent"] = abs((arb_df["bina_price"] - arb_df["drift_price"]) / arb_df["bina_price"] * 100)
    arb_df.sort_values(by=["gap_percent"], ascending=False, inplace=True)
    print(arb_df)
    print()


while True:
    asyncio.run(main())
