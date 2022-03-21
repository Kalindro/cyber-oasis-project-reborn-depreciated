import traceback
import os
import datetime as dt

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
        self.min_gap = 0.30
        self.leverage = 3

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

    @staticmethod
    def conditions_inplay(row):
        if (row["binance_pos"] or row["drift_pos"]) > 0:
            return True
        else:
            return False

    def conds_open_long_drift(self, row):
        if ((row["gap_perc"] > self.min_gap) or (row["avg_gap"] > self.min_gap)) and (row["gap_perc"] > row["top_avg_gaps"]):
            return True
        else:
            return False

    def conds_open_short_drift(self, row):
        if ((row["gap_perc"] < -self.min_gap) or (row["avg_gap"] < -self.min_gap)) and (row["gap_perc"] < row["bottom_avg_gaps"]):
            return True
        else:
            return False

    @staticmethod
    def conds_close_long_drift(row):
        if row["gap_perc"] < row["bottom_avg_gaps"]:
            return True
        else:
            return False

    @staticmethod
    def conds_close_short_drift(row):
        if row["gap_perc"] > row["top_avg_gaps"]:
            return True
        else:
            return False

    def fresh_data_aggregator(self, coin_dataframes_dict):
        fresh_data = df()
        for frame in coin_dataframes_dict.values():
            frame["avg_gap"] = frame["gap_perc"].rolling(self.zscore_period, self.zscore_period).mean()
            frame["top_avg_gaps"] = frame["gap_perc"].rolling(self.zscore_period, self.zscore_period).apply(
                lambda x: np.median(sorted(x, reverse=True)[:int(0.10*self.zscore_period)]))
            frame["bottom_avg_gaps"] = frame["gap_perc"].rolling(self.zscore_period, self.zscore_period).apply(
                lambda x: np.median(sorted(x, reverse=False)[:int(0.10*self.zscore_period)]))
            frame["open_l_drift"] = frame.apply(lambda row: self.conds_open_long_drift(row), axis=1)
            frame["open_s_drift"] = frame.apply(lambda row: self.conds_open_short_drift(row), axis=1)
            frame["close_l_drift"] = frame.apply(lambda row: self.conds_close_long_drift(row), axis=1)
            frame["close_s_drift"] = frame.apply(lambda row: self.conds_close_short_drift(row), axis=1)
            fresh_data = fresh_data.append(frame.iloc[-1])
        fresh_data.sort_values(by=["avg_gap"], ascending=False, inplace=True)

        return fresh_data

    def binance_futures_margin_leverage_check(self, API_binance, binance_positions):
        if (~binance_positions.leverage.isin([str(self.leverage)])).any():
            for _, row in binance_positions.iterrows():
                if row["leverage"] != self.leverage:
                    print("Changing leverage")
                    binance_futures_change_leverage(API_binance, pair=row["pair"], leverage=self.leverage)

        if (~binance_positions.isolated).any():
            for _, row in binance_positions.iterrows():
                if not row["isolated"]:
                    print("Changing  margin type")
                    binance_futures_change_marin_type(API_binance, pair=row["pair"], type="ISOLATED")

    async def update_dataframe(self, historical_arb_df, API_drift, API_binance):
        arb_df = df()

        drift_prices = await drift_get_pair_prices_rates(API_drift)
        bina_prices = binance_get_futures_pair_prices_rates(API_binance)
        arb_df["drift_index"] = drift_prices["market_index"]
        arb_df["drift_price"] = drift_prices["mark_price"]
        arb_df["bina_price"] = bina_prices["mark_price"]
        arb_df["gap_perc"] = (arb_df["bina_price"] - arb_df["drift_price"]) / arb_df["bina_price"] * 100
        arb_df["timestamp"] = round_time(dt=dt.datetime.now(), date_delta=dt.timedelta(seconds=5))
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

        fresh_data = self.fresh_data_aggregator(coin_dataframes_dict=coin_dataframes_dict)
        print(fresh_data)

        binance_positions = binance_futures_positions(API_binance)
        drift_positions = await drift_load_positions(API_drift)
        binance_positions = binance_positions[binance_positions.index.isin(playable_coins)]
        self.binance_futures_margin_leverage_check(API_binance=API_binance, binance_positions=binance_positions)

        balances_dataframe = df()
        balances_dataframe.index = binance_positions.index
        balances_dataframe["binance_pos"] = binance_positions["positionAmt"].astype(float)
        balances_dataframe["drift_pos"] = drift_positions["base_asset_amount"].astype(float)
        balances_dataframe.fillna(0, inplace=True)
        balances_dataframe["binance_pair"] = binance_positions["pair"]
        balances_dataframe["drift_pair"] = drift_prices["market_index"]
        balances_dataframe["in_play"] = balances_dataframe.apply(lambda row: self.conditions_inplay(row), axis=1)
        print(balances_dataframe)

        best_coin_row = fresh_data.iloc[-1]
        best_symbol = best_coin_row["symbol"]

        if not balances_dataframe.loc[best_symbol, "in_play"]:
            if best_coin_row["open_l_drift"]:
                print("Longing Drift, shorting Binance")
            elif best_coin_row["open_s_drift"]:
                print("Shorting Drift, longing Binance")
        else:
            if best_coin_row["close_l_drift"]:
                print("Closing Drift long, closing Binance short")
            elif best_coin_row["close_s_drift"]:
                print("Closing Drift short, closing Binance long")

        return historical_arb_df

    async def run_constant_update(self):
        while True:
            try:
                API_1 = await self.initiate_drift()
                API_2 = self.initiate_binance()
                historical_arb_df = self.read_historical_dataframe()

                i = 0
                while True:
                    start_time = time.time()

                    historical_arb_df = await self.update_dataframe(historical_arb_df=historical_arb_df, API_drift=API_1, API_binance=API_2)

                    if i > 250:
                        historical_arb_df.to_csv(f"{project_path}/History_data/Drift/5S/Price_gaps_5S.csv")
                        print("Saved CSV")
                        i = 0
                    i += 1
                    print("--- %s seconds ---" % (time.time() - start_time))

            except Exception as err:
                trace = traceback.format_exc()
                print(f"Error: {err}")
                print(trace)
                time.sleep(5)


if __name__ == "__main__":
    asyncio.run(DriftBinaARBLive().run_constant_update())


