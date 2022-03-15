import asyncio
import pandas as pd
import time
import os
import datetime as dt
import numpy as np

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
        self.zscore_period = 720
        self.min_gap = 0.32

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

    def conditions(self, row):
        if (row["gap_perc"] or row["avg_gap"]) > self.min_gap:
            return True
        elif (row["gap_perc"] or row["avg_gap"]) < -self.min_gap:
            return True
        else:
            return False

    async def update_dataframe(self, historical_arb_df, API_drift, API_binance):
        try:
            arb_df = df()

            drift_prices = await drift_get_pair_prices_rates(API_drift)
            bina_prices = binance_get_futures_pair_prices_rates(API_binance)

            arb_df["drift_price"] = drift_prices["mark_price"]
            arb_df["bina_price"] = bina_prices["mark_price"]
            arb_df["gap_perc"] = (arb_df["bina_price"] - arb_df["drift_price"]) / arb_df["bina_price"] * 100
            arb_df["timestamp"] = round_time(dt=dt.datetime.now(), date_delta=timedelta(seconds=5))
            arb_df.reset_index(inplace=True)
            arb_df.set_index("timestamp", inplace=True)

            historical_arb_df = historical_arb_df.append(arb_df)
            historical_arb_df.reset_index(inplace=True)
            historical_arb_df.drop_duplicates(subset=["timestamp", "symbol"], keep="last", inplace=True)
            # historical_arb_df = historical_arb_df[historical_arb_df["timestamp"] > (dt.datetime.now() - timedelta(hours=6))]
            historical_arb_df.set_index("timestamp", inplace=True)

            playable_coins = historical_arb_df.symbol.unique()
            coin_dataframes_dict = {elem: pd.DataFrame for elem in playable_coins}
            for key in coin_dataframes_dict.keys():
                coin_dataframes_dict[key] = historical_arb_df[:][historical_arb_df.symbol == key]

            fresh_data = df()
            for frame in coin_dataframes_dict.values():
                frame["avg_gap"] = frame["gap_perc"].rolling(self.zscore_period, self.zscore_period).mean()
                frame["top_avg_gaps"] = frame["gap_perc"].rolling(self.zscore_period, self.zscore_period).apply(
                    lambda x: np.median(sorted(x, reverse=True)[:int(0.15*self.zscore_period)]))
                frame["bottom_avg_gaps"] = frame["gap_perc"].rolling(self.zscore_period, self.zscore_period).apply(
                    lambda x: np.median(sorted(x, reverse=False)[:int(0.15*self.zscore_period)]))
                frame["gap_stdv"] = frame["gap_perc"].rolling(self.zscore_period, self.zscore_period).std()
                frame["zscore"] = (frame["gap_perc"] - frame["avg_gap"]) / frame["gap_stdv"]
                frame["upper_zsc"] = 4.00
                frame["lower_zsc"] = -4.00
                frame["in_play"] = frame.apply(lambda row: self.conditions(row), axis=1)
                fresh_data = fresh_data.append(frame.iloc[-1])

            fresh_data.sort_values(by=["avg_gap"], ascending=False, inplace=True)
            print(fresh_data)

            binance_balances = binance_get_futures_balance(API_binance)
            binance_positions = binance_futures_positions(API_binance)
            binance_positions = binance_positions[binance_positions.index.isin(playable_coins)]

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

            historical_arb_df = self.update_dataframe(historical_arb_df=historical_arb_df, API_drift=API_1, API_binance=API_2)
            if i > 250:
                historical_arb_df.to_csv(f"{project_path}/History_data/Drift/5S/Price_gaps_5S.csv")
                print("Saved CSV")
                i = 0
            i += 1
            time.sleep(1)
            print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__":
    asyncio.run(DriftBinaARBLive().run_constant_update())


