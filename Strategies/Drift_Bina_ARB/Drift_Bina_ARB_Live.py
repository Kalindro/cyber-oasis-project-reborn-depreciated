import asyncio
import pandas as pd
import time
import datetime as dt
from datetime import timedelta
import os
from Gieldy.Binance.Binance_utils import *
from Gieldy.Drift.Drift_utils import *
from Gieldy.Refractor_general.General_utils import *

from Gieldy.Binance.API_initiation_Binance_Futures_USDT import API_initiation as API_1
from Gieldy.Drift.API_initiation_Drift_USDC import API_initiation as API_2


asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

current_path = os.path.dirname(os.path.abspath(__file__))
project_path = Path(current_path).parent.parent


def zscore(x, window):
    r = x.rolling(window=window)
    m = r.mean().shift(1)
    s = r.std(ddof=0).shift(1)
    z = (x-m)/s
    return z


async def main():
    try:
        historical_arb_df = pd.read_csv(f"{project_path}/History_data/Drift/5S/Price_gaps_5S.csv", index_col=0, parse_dates=True)
    except:
        historical_arb_df = df()

    i = 0
    API_Binance = API_1()
    API_Drift = await API_2()

    while True:
        try:
            start_time = time.time()

            drift_prices = await drift_get_pair_prices_rates(API_Drift)
            bina_prices = binance_get_futures_pair_prices_rates(API_Binance)

            arb_df = df()
            zscore_period = 100

            arb_df["drift_price"] = drift_prices["mark_price"]
            arb_df["bina_price"] = bina_prices["mark_price"]
            arb_df["gap_percent"] = abs((arb_df["bina_price"] - arb_df["drift_price"]) / arb_df["bina_price"] * 100)
            arb_df["timestamp"] = round_time(dt=dt.datetime.now(), date_delta=datetime.timedelta(seconds=5))
            arb_df.reset_index(inplace=True)
            arb_df.set_index("timestamp", inplace=True)
            arb_df.sort_values(by=["gap_percent"], ascending=False, inplace=True)

            historical_arb_df = historical_arb_df.append(arb_df)
            historical_arb_df.reset_index(inplace=True)
            historical_arb_df.drop_duplicates(subset=["timestamp", "symbol"], keep="last", inplace=True)
            historical_arb_df.set_index("timestamp", inplace=True)

            historical_arb_df = historical_arb_df[historical_arb_df.index > (dt.datetime.now() - timedelta(hours=6))]
            historical_arb_df["average_gap"] = historical_arb_df.groupby("symbol")["gap_percent"].transform(lambda x: x.rolling(zscore_period, zscore_period).mean())
            historical_arb_df["zscore"] = historical_arb_df.groupby("symbol")["gap_percent"].transform(lambda x: zscore(x, zscore_period))
            # historical_arb_df["stdev"] = historical_arb_df.groupby("symbol")["gap_percent"].transform(lambda x: x.rolling(zscore_period, zscore_period).std())

            historical_arb_df.sort_values(["timestamp", "average_gap"], ascending=[True, False], inplace=True)
            print(historical_arb_df)
            if i > 125:
                print(historical_arb_df)
                historical_arb_df.to_csv(f"{project_path}/History_data/Drift/5S/Price_gaps_5S.csv")
                print("Saving CSV")
                i = 0

            binance_balances = binance_get_futures_balance(API_Binance)
            binance_positions = binance_futures_positions(API_Binance)
            i += 1
            print("--- %s seconds ---" % (time.time() - start_time).__round__(2))

        except:
            raise


asyncio.run(main())

