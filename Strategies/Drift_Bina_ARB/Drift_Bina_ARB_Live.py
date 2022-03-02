import asyncio
import pandas as pd
import time
import os
import datetime as dt

from datetime import timedelta

from pathlib import Path
from Gieldy.Binance.Binance_utils import *
from Gieldy.Drift.Drift_utils import *
from Gieldy.Refractor_general.General_utils import round_time, zscore

from Gieldy.Binance.API_initiation_Binance_Futures_USDT import API_initiation as API_binance
from Gieldy.Drift.API_initiation_Drift_USDC import API_initiation as API_drift


asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

current_path = os.path.dirname(os.path.abspath(__file__))
project_path = Path(current_path).parent.parent


class DriftBinaARBLive:

    def __init__(self):
        self.zscore_period = 100

    @staticmethod
    def initiate_binance():
        return API_binance()

    @staticmethod
    async def initiate_drift():
        return await API_drift()

    @staticmethod
    def read_historical_dataframe():
        try:
            historical_arb_df = pd.read_csv(f"{project_path}/History_data/Drift/5S/Price_gaps_5S.csv", index_col=0, parse_dates=True)
        except:
            print("No saved DF")
            historical_arb_df = df()

        return historical_arb_df

    async def update_dataframe(self, historical_arb_df, API_drift, API_binance):
        try:
            arb_df = df()

            drift_prices = await drift_get_pair_prices_rates(API_drift)
            bina_prices = binance_get_futures_pair_prices_rates(API_binance)

            arb_df["drift_price"] = drift_prices["mark_price"]
            arb_df["bina_price"] = bina_prices["mark_price"]
            arb_df["gap_percent"] = abs((arb_df["bina_price"] - arb_df["drift_price"]) / arb_df["bina_price"] * 100)
            arb_df["timestamp"] = round_time(dt=dt.datetime.now(), date_delta=timedelta(seconds=5))
            arb_df.reset_index(inplace=True)
            arb_df.set_index("timestamp", inplace=True)
            arb_df.sort_values(by=["gap_percent"], ascending=False, inplace=True)

            historical_arb_df = historical_arb_df.append(arb_df)
            historical_arb_df.reset_index(inplace=True)
            historical_arb_df.drop_duplicates(subset=["timestamp", "symbol"], keep="last", inplace=True)

            historical_arb_df = historical_arb_df[historical_arb_df["timestamp"] > (dt.datetime.now() - timedelta(hours=6))]
            historical_arb_df["average_gap"] = historical_arb_df.groupby("symbol")["gap_percent"].transform(
                lambda x: x.rolling(self.zscore_period, self.zscore_period).mean())
            historical_arb_df["zscore"] = historical_arb_df.groupby("symbol")["gap_percent"].transform(lambda x: zscore(x, self.zscore_period))
            # historical_arb_df["stdev"] = historical_arb_df.groupby("symbol")["gap_percent"].transform(lambda x: x.rolling(zscore_period, zscore_period).std())

            historical_arb_df.sort_values(["timestamp", "average_gap"], inplace=True)
            historical_arb_df.set_index("timestamp", inplace=True)

            print(historical_arb_df)

            # binance_balances = binance_get_futures_balance(API_binance)
            # binance_positions = binance_futures_positions(API_binance)
            # drift_positions = await drift_load_position_table(API_drift)

            return historical_arb_df

        except:
            raise

    async def run_constant_update(self):
        API_1 = await self.initiate_drift()
        API_2 = self.initiate_binance()
        historical_arb_df = self.read_historical_dataframe()

        i = 0
        while True:
            start_time = time.time()

            historical_arb_df = await self.update_dataframe(historical_arb_df=historical_arb_df, API_drift=API_1, API_binance=API_2)
            if i > 250:
                print(historical_arb_df)
                historical_arb_df.to_csv(f"{project_path}/History_data/Drift/5S/Price_gaps_5S.csv")
                print("Saving CSV")
                i = 0
            i += 1
            time.sleep(0.5)
            print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__":
    asyncio.run(DriftBinaARBLive().run_constant_update())


