import traceback
import os
import datetime as dt

from pathlib import Path
from Gieldy.Binance.Binance_utils import *
from Gieldy.Drift.Drift_utils import *
from Gieldy.Refractor_general.General_utils import round_time

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
        self.zscore_period = 180
        self.min_gap = 0.30
        self.leverage = 3
        self.drift_big_N = 1_000_000

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
    def conds_inplay(row):
        if (abs(row["binance_pos"]) > 0) and (abs(row["drift_pos"]) > 0):
            return True
        else:
            return False

    @staticmethod
    def conds_noplay(row):
        if (abs(row["binance_pos"]) == 0) and (abs(row["drift_pos"]) == 0):
            return True
        else:
            return False

    @staticmethod
    def conds_binance_inplay(row):
        if abs(row["binance_pos"]) > 0:
            return True
        else:
            return False

    @staticmethod
    def conds_drift_inplay(row):
        if abs(row["drift_pos"]) > 0:
            return True
        else:
            return False

    @staticmethod
    def conds_imbalance(row):
        if (row["binance_inplay"] and not row["drift_inplay"]) or (row["drift_inplay"] and not row["binance_inplay"]):
            return True
        else:
            return False

    def conds_open_long_drift(self, row):
        if (((row["gap_perc"] > self.min_gap) or (row["avg_gap"] > self.min_gap)) or (
                (row["gap_range"]) > self.min_gap)) and (row["gap_perc"] > row["top_avg_gaps"]):
            return True
        else:
            return False

    def conds_open_short_drift(self, row):
        if (((row["gap_perc"] < -self.min_gap) or (row["avg_gap"] < -self.min_gap)) or (
                (row["gap_range"]) > self.min_gap)) and (row["gap_perc"] < row["bottom_avg_gaps"]):
            return True
        else:
            return False

    @staticmethod
    def conds_close_long_drift(row):
        if row["gap_perc"] < min(row["bottom_avg_gaps"], 0):
            return True
        else:
            return False

    @staticmethod
    def conds_close_short_drift(row):
        if row["gap_perc"] > max(row["top_avg_gaps"], 0):
            return True
        else:
            return False

    async def update_history_dataframe(self, historical_arb_df, API_drift, API_binance):
        arb_df = df()

        drift_prices = await drift_get_pair_prices_rates(API_drift)
        bina_prices = binance_get_futures_pair_prices_rates(API_binance)
        arb_df["drift_pair"] = drift_prices["market_index"]
        arb_df["binance_pair"] = bina_prices["pair"]
        arb_df = arb_df[["binance_pair", "drift_pair"]]
        arb_df["bina_price"] = bina_prices["mark_price"]
        arb_df["drift_price"] = drift_prices["mark_price"]
        arb_df["gap_perc"] = (arb_df["bina_price"] - arb_df["drift_price"]) / arb_df["bina_price"] * 100
        arb_df["timestamp"] = round_time(dt=dt.datetime.now(), date_delta=dt.timedelta(seconds=5))
        arb_df.reset_index(inplace=True)
        arb_df.set_index("timestamp", inplace=True)

        historical_arb_df = historical_arb_df.append(arb_df)
        historical_arb_df.reset_index(inplace=True)
        historical_arb_df.drop_duplicates(subset=["timestamp", "symbol"], keep="last", inplace=True)
        historical_arb_df = historical_arb_df[historical_arb_df["timestamp"] > (dt.datetime.now() - timedelta(seconds=(self.zscore_period*1.25)*5))]
        historical_arb_df.set_index("timestamp", inplace=True)
        historical_arb_df = historical_arb_df[historical_arb_df["bina_price"].notna()]

        return historical_arb_df

    def fresh_data_aggregator(self, historical_arb_df):
        playable_coins = historical_arb_df.symbol.unique()
        coin_dataframes_dict = {elem: pd.DataFrame for elem in playable_coins}
        for key in coin_dataframes_dict.keys():
            coin_dataframes_dict[key] = historical_arb_df[:][historical_arb_df.symbol == key]

        fresh_data = df()
        for frame in coin_dataframes_dict.values():
            frame["avg_gap"] = frame["gap_perc"].rolling(self.zscore_period, self.zscore_period).mean()
            frame["avg_gap_abs"] = abs(frame["avg_gap"])
            frame["top_avg_gaps"] = frame["gap_perc"].rolling(self.zscore_period, self.zscore_period).apply(
                lambda x: np.median(sorted(x, reverse=True)[:int(0.10*self.zscore_period)]))
            frame["bottom_avg_gaps"] = frame["gap_perc"].rolling(self.zscore_period, self.zscore_period).apply(
                lambda x: np.median(sorted(x, reverse=False)[:int(0.10*self.zscore_period)]))
            frame["gap_range"] = abs(frame["top_avg_gaps"] - frame["bottom_avg_gaps"])
            frame["open_l_drift"] = frame.apply(lambda row: self.conds_open_long_drift(row), axis=1)
            frame["open_s_drift"] = frame.apply(lambda row: self.conds_open_short_drift(row), axis=1)
            frame["close_l_drift"] = frame.apply(lambda row: self.conds_close_long_drift(row), axis=1)
            frame["close_s_drift"] = frame.apply(lambda row: self.conds_close_short_drift(row), axis=1)
            fresh_data = fresh_data.append(frame.iloc[-1])

        fresh_data.sort_values(by=["gap_range"], inplace=True)

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
                    print("Changing margin type")
                    binance_futures_change_marin_type(API_binance, pair=row["pair"], type="ISOLATED")

    async def get_positions_summary(self, historical_arb_df, API_binance, API_drift):
        playable_coins = historical_arb_df.symbol.unique()
        binance_positions = binance_futures_positions(API_binance)
        binance_positions = binance_positions[binance_positions.index.isin(playable_coins)]
        drift_positions = await drift_load_positions(API_drift)
        self.binance_futures_margin_leverage_check(API_binance=API_binance, binance_positions=binance_positions)

        positions_dataframe = df()
        positions_dataframe.index = binance_positions.index
        positions_dataframe["binance_pos"] = binance_positions["positionAmt"].astype(float)
        positions_dataframe["drift_pos"] = drift_positions["base_asset_amount"].astype(float)
        positions_dataframe.fillna(0, inplace=True)
        positions_dataframe["binance_pair"] = binance_positions["pair"]
        positions_dataframe["drift_pair"] = (historical_arb_df.drop_duplicates(subset="symbol").set_index("symbol").loc[positions_dataframe.index, "drift_pair"]).astype(int)
        positions_dataframe["inplay"] = positions_dataframe.apply(lambda row: self.conds_inplay(row), axis=1)
        positions_dataframe["noplay"] = positions_dataframe.apply(lambda row: self.conds_noplay(row), axis=1)
        positions_dataframe["binance_inplay"] = positions_dataframe.apply(lambda row: self.conds_binance_inplay(row), axis=1)
        positions_dataframe["drift_inplay"] = positions_dataframe.apply(lambda row: self.conds_drift_inplay(row), axis=1)
        positions_dataframe["imbalance"] = positions_dataframe.apply(lambda row: self.conds_imbalance(row), axis=1)
        print(positions_dataframe)
        time.sleep(1.5)

        return positions_dataframe

    @staticmethod
    async def get_balances_summary(API_binance, API_drift):
        binance_balances = binance_futures_get_balance(API=API_binance).loc["USDT"]
        drift_balances = await drift_get_margin_account_info(API=API_drift)
        balances_dict = {"binance": float(binance_balances['total']), "drift": float(drift_balances['total_collateral']),
                "binance_play_value": float(binance_balances['total']) * 0.75,
                "drift_play_value": float(drift_balances['total_collateral']) * 0.75,
                "coin_target_value": 10}
                # "coin_target_value": float(binance_balances['total']) * 0.5}
        time.sleep(1.5)

        return balances_dict

    async def run_constant_update(self):
        API_drift = await self.initiate_drift()
        API_binance = self.initiate_binance()
        historical_arb_df = self.read_historical_dataframe()
        historical_arb_df = await self.update_history_dataframe(historical_arb_df=historical_arb_df, API_drift=API_drift, API_binance=API_binance)
        positions_dataframe = await self.get_positions_summary(historical_arb_df=historical_arb_df, API_drift=API_drift, API_binance=API_binance)
        i = 0
        while True:
            start_time = time.time()

            historical_arb_df = await self.update_history_dataframe(historical_arb_df=historical_arb_df, API_drift=API_drift, API_binance=API_binance)
            fresh_data = self.fresh_data_aggregator(historical_arb_df=historical_arb_df)

            best_coin_row = fresh_data.iloc[-1]
            best_coin_symbol = best_coin_row["symbol"]
            play_symbols_binance_list = [coin for coin in positions_dataframe.loc[positions_dataframe["binance_inplay"]].index]
            play_symbols_drift_list = [coin for coin in positions_dataframe.loc[positions_dataframe["drift_inplay"]].index]
            play_symbols_list = play_symbols_binance_list + play_symbols_drift_list
            play_symbols_list.append(best_coin_symbol)
            play_symbols_list = list(set(play_symbols_list))
            play_coins_dataframe = fresh_data[fresh_data.symbol.isin(play_symbols_list)]

            if not np.isnan(best_coin_row["avg_gap"]):
                for index, coin_row in play_coins_dataframe.iterrows():
                    coin_symbol = coin_row["symbol"]
                    coin_pair = coin_row["binance_pair"]
                    coin_price = coin_row["bina_price"]

                    if not positions_dataframe.loc[coin_symbol, "inplay"]:
                        if coin_row["open_l_drift"]:
                            precisions_dataframe = binance_futures_get_pairs_precisions_status(API_binance)
                            balances_dict = await self.get_balances_summary(API_binance=API_binance, API_drift=API_drift)
                            coin_target_value = balances_dict["coin_target_value"]
                            bina_open_amount = round(coin_target_value / coin_price, precisions_dataframe.loc[coin_pair, "amount_precision"])
                            if bina_open_amount > (precisions_dataframe.loc[coin_pair, "min_order_amount"] * 1.05):
                                print(f"{coin_symbol} Longing Drift: {coin_target_value}, shorting Binance: {bina_open_amount}")
                                print(fresh_data)
                                i = 1
                                while not positions_dataframe.loc[coin_symbol, "inplay"]:
                                    print(f"Try number: {i}")
                                    try:
                                        orders_time = time.time()
                                        if not positions_dataframe.loc[coin_symbol, "drift_inplay"]:
                                            long_drift = await drift_open_market_long(API=API_drift, amount=coin_target_value*self.drift_big_N, drift_index=coin_row["drift_pair"])
                                            print("--- Orders time %s seconds ---" % (round(time.time() - orders_time, 2)))
                                        if not positions_dataframe.loc[coin_symbol, "binance_inplay"]:
                                            short_binance = binance_futures_open_market_short(API=API_binance, pair=coin_row["binance_pair"], amount=bina_open_amount)
                                        positions_dataframe = await self.get_positions_summary(historical_arb_df=historical_arb_df, API_drift=API_drift, API_binance=API_binance)
                                        i += 1
                                    except Exception as err:
                                        trace = traceback.format_exc()
                                        print(f"Error, retrying buys: {err}\n{trace}")
                                        positions_dataframe = await self.get_positions_summary(historical_arb_df=historical_arb_df, API_drift=API_drift, API_binance=API_binance)
                        elif coin_row["open_s_drift"]:
                            precisions_dataframe = binance_futures_get_pairs_precisions_status(API_binance)
                            balances_dict = await self.get_balances_summary(API_binance=API_binance, API_drift=API_drift)
                            coin_target_value = balances_dict["coin_target_value"]
                            bina_open_amount = round((coin_target_value / coin_price), precisions_dataframe.loc[coin_pair, "amount_precision"])
                            if bina_open_amount > (precisions_dataframe.loc[coin_pair, "min_order_amount"] * 1.05):
                                print(f"{coin_symbol} Shorting Drift: {coin_target_value}, longing Binance: {bina_open_amount}")
                                print(fresh_data)
                                i = 1
                                while not positions_dataframe.loc[coin_symbol, "inplay"]:
                                    print(f"Try number: {i}")
                                    try:
                                        orders_time = time.time()
                                        if not positions_dataframe.loc[coin_symbol, "drift_inplay"]:
                                            short_drift = await drift_open_market_short(API=API_drift, amount=coin_target_value*self.drift_big_N, drift_index=coin_row["drift_pair"])
                                            print("--- Orders time %s seconds ---" % (round(time.time() - orders_time, 2)))
                                        if not positions_dataframe.loc[coin_symbol, "binance_inplay"]:
                                            long_binance = binance_futures_open_market_long(API=API_binance, pair=coin_row["binance_pair"], amount=bina_open_amount)
                                        positions_dataframe = await self.get_positions_summary(historical_arb_df=historical_arb_df, API_drift=API_drift, API_binance=API_binance)
                                        i += 1
                                    except Exception as err:
                                        trace = traceback.format_exc()
                                        print(f"Error, retrying buys: {err}\n{trace}")
                                        positions_dataframe = await self.get_positions_summary(historical_arb_df=historical_arb_df, API_drift=API_drift, API_binance=API_binance)

                    else:
                        precisions_dataframe = binance_futures_get_pairs_precisions_status(API_binance)
                        bina_close_amount = round(abs(positions_dataframe.loc[coin_symbol, "binance_pos"] * 5), precisions_dataframe.loc[coin_pair, "amount_precision"])
                        if coin_row["close_l_drift"] and positions_dataframe.loc[coin_symbol, "drift_pos"] > 0:
                            if bina_close_amount > (precisions_dataframe.loc[coin_pair, "min_order_amount"] * 1.05):
                                print(f"{coin_symbol} Closing Drift long: All, closing Binance short: {bina_close_amount}")
                                print(fresh_data)
                                i = 1
                                while not positions_dataframe.loc[coin_symbol, "noplay"]:
                                    print(f"Try number: {i}")
                                    try:
                                        orders_time = time.time()
                                        if positions_dataframe.loc[coin_symbol, "drift_inplay"]:
                                            close_drift_long = await drift_close_order(API=API_drift, drift_index=coin_row["drift_pair"])
                                            print("--- Orders time %s seconds ---" % (round(time.time() - orders_time, 2)))
                                        if positions_dataframe.loc[coin_symbol, "binance_inplay"]:
                                            close_binance_short = binance_futures_close_market_short(API=API_binance, pair=coin_row["binance_pair"], amount=bina_close_amount)
                                        positions_dataframe = await self.get_positions_summary(historical_arb_df=historical_arb_df, API_drift=API_drift, API_binance=API_binance)
                                        i += 1
                                    except Exception as err:
                                        trace = traceback.format_exc()
                                        print(f"Error, retrying sells: {err}\n{trace}")
                                        positions_dataframe = await self.get_positions_summary(historical_arb_df=historical_arb_df, API_drift=API_drift, API_binance=API_binance)
                        elif coin_row["close_s_drift"] and positions_dataframe.loc[coin_symbol, "drift_pos"] < 0:
                            if bina_close_amount > (precisions_dataframe.loc[coin_pair, "min_order_amount"] * 1.05):
                                print(f"{coin_symbol} Closing Drift short: All, closing Binance long: {bina_close_amount}")

                                print(fresh_data)
                                i = 1
                                while not positions_dataframe.loc[coin_symbol, "noplay"]:
                                    print(f"Try number: {i}")
                                    try:
                                        orders_time = time.time()
                                        if positions_dataframe.loc[coin_symbol, "drift_inplay"]:
                                            close_drift_short = await drift_close_order(API=API_drift, drift_index=coin_row["drift_pair"])
                                            print("--- Orders time %s seconds ---" % (round(time.time() - orders_time, 2)))
                                        if positions_dataframe.loc[coin_symbol, "binance_inplay"]:
                                            close_binance_long = binance_futures_close_market_long(API=API_binance, pair=coin_row["binance_pair"], amount=bina_close_amount)
                                        positions_dataframe = await self.get_positions_summary(historical_arb_df=historical_arb_df, API_drift=API_drift, API_binance=API_binance)
                                        i += 1
                                    except Exception as err:
                                        trace = traceback.format_exc()
                                        print(f"Error, retrying sells: {err}\n{trace}")
                                        positions_dataframe = await self.get_positions_summary(historical_arb_df=historical_arb_df, API_drift=API_drift, API_binance=API_binance)

            if i > 125:
                historical_arb_df.to_csv(f"{project_path}/History_data/Drift/5S/Price_gaps_5S.csv")
                print("Saved CSV")
                i = 0

            elapsed = time.time() - start_time
            if elapsed < 1.5:
                time.sleep(1.5 - elapsed)

            i += 1
            print("--- Loop %s seconds ---" % (round(time.time() - start_time, 2)))

    async def main(self):
        while True:
            try:
                await self.run_constant_update()

            except Exception as err:
                trace = traceback.format_exc()
                print(f"Error: {err}\n{trace}")
                time.sleep(5)


if __name__ == "__main__":
    asyncio.run(DriftBinaARBLive().main())


