import traceback
import os
import datetime as dt

import httpcore
import httpx
import requests.exceptions
import solana
import sys
import shutil

from multiprocessing import Process
from colorama import Fore, Style
from pathlib import Path
from Gieldy.Binance.Binance_utils import *
from Gieldy.Drift.Drift_utils import *
from Gieldy.Refractor_general.General_utils import round_time

from Gieldy.Binance.API_initiation_Binance_Spot_Futures_USDT import API_initiation as API_binance_1
from Gieldy.Drift.API_initiation_Drift_USDC import API_initiation as API_drift_2


if sys.platform == "win32" and sys.version_info.minor >= 8:
    asyncio.set_event_loop_policy(
        asyncio.WindowsSelectorEventLoopPolicy()
    )

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

current_path = os.path.dirname(os.path.abspath(__file__))
project_path = Path(current_path).parent.parent


class Initialize:

    def __init__(self):
        self.LIMIT_DATA = True
        self.ZSCORE_PERIOD = int(1 * 3600 / 5)  # Edit first number, hours of period (hours * minute in seconds / 5s data frequency)
        self.FAST_AVG = 18
        self.QUARTILE = 0.15
        self.MIN_REGULAR_GAP = 0.44
        self.MIN_CLOSING_GAP = 0.00
        self.LEVERAGE = 4
        self.COINS_AT_ONCE = 5
        self.DRIFT_BIG_N = 1_000_000
        self.DRIFT_USDC_PRECISION = 4

    @staticmethod
    def read_historical_dataframe():
        i = 0
        while True:
            try:
                source = f"{project_path}/History_data/Drift/5S/Price_gaps_5S_LIVE_WRITE.pickle"
                destination = f"{project_path}/History_data/Drift/5S/Price_gaps_5S_COPY_READ.pickle"
                shutil.copyfile(source, destination)
                time.sleep(0.3)
                historical_arb_df = pd.read_pickle(f"{project_path}/History_data/Drift/5S/Price_gaps_5S_COPY_READ.pickle")
                if (len(historical_arb_df) < 2) or np.isnan(historical_arb_df.iloc[-1]["bina_price"]) or np.isnan(historical_arb_df.iloc[-1]["drift_price"]) or np.isnan(historical_arb_df.iloc[-1]["gap_perc"]):
                    _ = 5/0  # Exception force
                else:
                    return historical_arb_df

            except Exception as err:
                i += 1
                if i > 10:
                    print(i)
                    print(f"Reading historical DF Fail: {err}")
                    time.sleep(0.2)
                if i > 20:
                    print("Something wrong with historical read, creating fresh")
                    historical_arb_df = df()
                    return historical_arb_df

    @staticmethod
    def initiate_binance():
        return API_binance_1()

    @staticmethod
    async def initiate_drift():
        return await API_drift_2()


class DataHandle(Initialize):

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
        arb_df["gap_abs"] = abs(arb_df["gap_perc"])
        arb_df["timestamp"] = round_time(dt=dt.datetime.now(), date_delta=dt.timedelta(seconds=5))
        arb_df.reset_index(inplace=True)
        arb_df.set_index("timestamp", inplace=True)

        historical_arb_df = historical_arb_df.append(arb_df)
        historical_arb_df.reset_index(inplace=True)
        historical_arb_df.drop_duplicates(subset=["timestamp", "symbol"], keep="last", inplace=True)
        historical_arb_df.set_index("timestamp", inplace=True)
        historical_arb_df = historical_arb_df[historical_arb_df["bina_price"].notna()]

        if self.LIMIT_DATA:
            historical_arb_df = historical_arb_df[historical_arb_df.index > (dt.datetime.now() - timedelta(seconds=(self.ZSCORE_PERIOD * 1.25) * 5))]

        return historical_arb_df

    async def run_constant_parallel_fresh_data_update(self):
        print("Running data side...")

        API_drift = await self.initiate_drift()
        API_binance = self.initiate_binance()
        historical_arb_df = await self.update_history_dataframe(historical_arb_df=self.read_historical_dataframe(), API_drift=API_drift, API_binance=API_binance)

        x = 0
        while True:
            try:
                data_start_time = time.perf_counter()
                historical_arb_df = await self.update_history_dataframe(historical_arb_df=historical_arb_df, API_drift=API_drift, API_binance=API_binance)
                if len(historical_arb_df) > 2:
                    historical_arb_df.to_pickle(f"{project_path}/History_data/Drift/5S/Price_gaps_5S_LIVE_WRITE.pickle")
                else:
                    print("Probowal zapisac pusta kurwe ock czemu")

                elapsed = time.perf_counter() - data_start_time
                expected = 2.5
                if elapsed < expected:
                    time.sleep(expected - elapsed)
                elif elapsed > 3:
                    print(f"{round_time(dt=dt.datetime.now(), date_delta=dt.timedelta(seconds=5))} --- Data loop %s seconds ---" % (round(time.perf_counter() - data_start_time, 2)))

                if x > 500:
                    historical_arb_df.to_csv(f"{project_path}/History_data/Drift/5S/Price_gaps_5S.csv")
                    API_drift = await self.initiate_drift()
                    API_binance = self.initiate_binance()
                    x = 0

            except (httpcore.ReadTimeout, httpcore.ConnectError, httpcore.RemoteProtocolError, httpx.ReadTimeout,
                    requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout, httpx.ConnectError) as err:
                print(f"Read timeout/connection error: {err}")
                time.sleep(1)

            except Exception as err:
                trace = traceback.format_exc()
                print(f"Err: {err}")
                print(f"Err type: {type(err)}")
                print(f"Err cause: {err.__cause__}")
                print(f"Err cause type: {type(err.__cause__)}")
                print(f"Err context: {err.__context__}")
                print(f"Err context type: {type(err.__context__)}")
                print(f"Error on data: {err}\n{trace}")
                time.sleep(1)

    def main(self):
        asyncio.run(self.run_constant_parallel_fresh_data_update())


class LogicConds(Initialize):

    @staticmethod
    def conds_inplay(row):
        if (abs(row["binance_pos"]) > 0) and (abs(row["drift_pos"]) > 0):
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
    def conds_open_somewhere(row):
        if row["open_l_drift"] or row["open_s_drift"]:
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
        conds1 = row["gap_perc"] > self.MIN_REGULAR_GAP
        conds2 = row["gap_perc"] > row["top_avg_gaps"]
        conds3 = row["fast_avg_gap"] > self.MIN_REGULAR_GAP
        conds4 = row["gap_perc"] > row["fast_avg_gap"]

        if conds1 and conds2 and conds3 and conds4:
            return True
        else:
            return False

    def conds_open_short_drift(self, row):
        conds1 = row["gap_perc"] < -self.MIN_REGULAR_GAP
        conds2 = row["gap_perc"] < row["bottom_avg_gaps"]
        conds3 = row["fast_avg_gap"] < -self.MIN_REGULAR_GAP
        conds4 = row["gap_perc"] < row["fast_avg_gap"]

        if conds1 and conds2 and conds3 and conds4:
            return True
        else:
            return False

    def conds_close_long_drift(self, row):
        conds1 = row["gap_perc"] < -self.MIN_CLOSING_GAP
        conds2 = row["gap_perc"] < row["bottom_avg_gaps"]
        conds3 = row["fast_avg_gap"] < -self.MIN_CLOSING_GAP
        conds4 = row["gap_perc"] < row["fast_avg_gap"]

        if conds1 and conds2 and conds3 and conds4:
            return True
        else:
            return False

    def conds_close_short_drift(self, row):
        conds1 = row["gap_perc"] > self.MIN_CLOSING_GAP
        conds2 = row["gap_perc"] > row["top_avg_gaps"]
        conds3 = row["fast_avg_gap"] > self.MIN_CLOSING_GAP
        conds4 = row["gap_perc"] > row["fast_avg_gap"]

        if conds1 and conds2 and conds3 and conds4:
            return True
        else:
            return False


class LogicHandle(Initialize):

    async def imbalance_checker(self, fresh_data, coin_symbol, API_drift, API_binance):
        time.sleep(15)
        positions_dataframe = await self.get_positions_summary(fresh_data, API_drift, API_binance)
        if positions_dataframe.loc[coin_symbol, "imbalance"]:
            print("Imbalance on first check")
            return True, positions_dataframe

        time.sleep(15)
        positions_dataframe = await self.get_positions_summary(fresh_data, API_drift, API_binance)
        if positions_dataframe.loc[coin_symbol, "imbalance"]:
            print("Imbalance on second check")
            return True, positions_dataframe

        time.sleep(15)
        positions_dataframe = await self.get_positions_summary(fresh_data, API_drift, API_binance)
        if positions_dataframe.loc[coin_symbol, "imbalance"]:
            print("Imbalance on third check")
            return True, positions_dataframe
        else:
            print("No imbalance after third check")
            return False, positions_dataframe

    def fresh_data_aggregator(self):
        fresh_data = df()

        historical_arb_df = self.read_historical_dataframe()
        playable_coins = historical_arb_df.symbol.unique()
        coin_dataframes_dict = {elem: pd.DataFrame for elem in playable_coins}
        for key in coin_dataframes_dict.keys():
            coin_dataframes_dict[key] = historical_arb_df[:][historical_arb_df.symbol == key]

        data_calc_start_time = time.perf_counter()
        for frame in coin_dataframes_dict.values():
            frame["fast_avg_gap"] = frame["gap_perc"].rolling(self.FAST_AVG, self.FAST_AVG).median()
            frame["top_avg_gaps"] = frame["gap_perc"].rolling(self.ZSCORE_PERIOD, self.ZSCORE_PERIOD).apply(
                lambda x: np.median(sorted(x, reverse=True)[:int(self.QUARTILE * self.ZSCORE_PERIOD)]))
            frame["bottom_avg_gaps"] = frame["gap_perc"].rolling(self.ZSCORE_PERIOD, self.ZSCORE_PERIOD).apply(
                lambda x: np.median(sorted(x, reverse=False)[:int(self.QUARTILE * self.ZSCORE_PERIOD)]))
            # frame["gap_range"] = abs(frame["top_avg_gaps"] - frame["bottom_avg_gaps"])
            frame["open_l_drift"] = frame.apply(lambda row: LogicConds().conds_open_long_drift(row), axis=1)
            frame["open_s_drift"] = frame.apply(lambda row: LogicConds().conds_open_short_drift(row), axis=1)
            frame["open_somewhere"] = frame.apply(lambda row: LogicConds().conds_open_somewhere(row), axis=1)
            frame["close_l_drift"] = frame.apply(lambda row: LogicConds().conds_close_long_drift(row), axis=1)
            frame["close_s_drift"] = frame.apply(lambda row: LogicConds().conds_close_short_drift(row), axis=1)
            fresh_data = fresh_data.append(frame.iloc[-1])

        fresh_data.sort_values(by=["gap_abs"], inplace=True)
        fresh_data.reset_index(inplace=True)
        fresh_data.rename(columns={"index": "timestamp"}, inplace=True)
        fresh_data.set_index("symbol", inplace=True)
        print(f"Data calc: {round(time.perf_counter() - data_calc_start_time, 2)}")

        return fresh_data

    def binance_futures_margin_leverage_check(self, API_binance, binance_positions):
        if (~binance_positions.leverage.isin([str(self.LEVERAGE)])).any():
            for _, row in binance_positions.iterrows():
                if row["leverage"] != self.LEVERAGE:
                    print("Changing leverage")
                    binance_futures_change_leverage(API_binance, pair=row["pair"], leverage=self.LEVERAGE)

        if binance_positions.isolated.any():
            for _, row in binance_positions.iterrows():
                if row["isolated"]:
                    print("Changing margin type")
                    binance_futures_change_marin_type(API_binance, pair=row["pair"], type="CROSSED")

    async def get_positions_summary(self, fresh_data, API_drift, API_binance, printing=True, sleeping=True):
        playable_coins_list = fresh_data.index.unique()
        binance_positions = binance_futures_positions(API_binance)
        binance_positions = binance_positions[binance_positions.index.isin(playable_coins_list)]
        drift_positions = await drift_load_positions(API_drift)
        self.binance_futures_margin_leverage_check(API_binance, binance_positions)

        positions_dataframe = df()
        positions_dataframe.index = binance_positions.index
        positions_dataframe["binance_pos"] = binance_positions["positionAmt"].astype(float)
        positions_dataframe["drift_pos"] = drift_positions["base_asset_amount"].astype(float)
        positions_dataframe.fillna(0, inplace=True)
        positions_dataframe["binance_pair"] = binance_positions["pair"]
        positions_dataframe["drift_pair"] = fresh_data.loc[positions_dataframe.index, "drift_pair"].astype(int)
        positions_dataframe["inplay"] = positions_dataframe.apply(lambda row: LogicConds().conds_inplay(row), axis=1)
        positions_dataframe["binance_inplay"] = positions_dataframe.apply(lambda row: LogicConds().conds_binance_inplay(row), axis=1)
        positions_dataframe["drift_inplay"] = positions_dataframe.apply(lambda row: LogicConds().conds_drift_inplay(row), axis=1)
        positions_dataframe["imbalance"] = positions_dataframe.apply(lambda row: LogicConds().conds_imbalance(row), axis=1)
        if printing:
            print(positions_dataframe)
        if sleeping:
            time.sleep(1.5)

        return positions_dataframe

    async def get_balances_summary(self, API_drift, API_binance, printing=True):
        binance_balances = binance_futures_get_balance(API_binance).loc["USDT"]
        drift_balances = await drift_get_margin_account_info(API_drift)
        balances_dict = {"binance": float(binance_balances['total']),
                         "drift": float(drift_balances['collateral']),
                         "sum": float(binance_balances['total']) + float(drift_balances['collateral']),
                         "binance_play_value": float(binance_balances['total']) * 0.80,
                         "drift_play_value": float(drift_balances['collateral']) * 0.80,
                         "coin_target_value": float(binance_balances['total']) * 0.80 * self.LEVERAGE / self.COINS_AT_ONCE}

        if printing:
            print(Fore.GREEN + f"{round_time(dt=dt.datetime.now(), date_delta=dt.timedelta(seconds=5))}: Account value sum: {balances_dict['sum']:.2f}, Bina: {balances_dict['binance']:.2f} Drift: {balances_dict['drift']:.2f}" + Style.RESET_ALL)
        time.sleep(1.5)

        return balances_dict

    async def run_constant_parallel_logic(self):
        print("Running logic side...")
        API_drift = await self.initiate_drift()
        API_binance = self.initiate_binance()
        precisions_dataframe = binance_futures_get_pairs_precisions_status(API_binance)
        fresh_data = self.fresh_data_aggregator()
        balances_dict = await self.get_balances_summary(API_drift, API_binance)
        positions_dataframe = await self.get_positions_summary(fresh_data, API_drift, API_binance)
        print(fresh_data)

        x = 0
        while True:
            try:
                logic_start_time = time.perf_counter()
                fresh_data = self.fresh_data_aggregator()
                play_dataframe = fresh_data[fresh_data["open_somewhere"]]
                best_coins_open = [coin for coin in play_dataframe.index]
                best_coins_open.reverse()
                play_symbols_list = [coin for coin in positions_dataframe.loc[positions_dataframe["inplay"]].index]
                play_symbols_list_pre = play_symbols_list + best_coins_open
                play_symbols_list_final = []
                [play_symbols_list_final.append(symbol) for symbol in play_symbols_list_pre if symbol not in play_symbols_list_final]

                print(f"Logic only list: {round(time.perf_counter() - logic_start_time, 2)}")

                if np.isnan(fresh_data.iloc[-1]["top_avg_gaps"]):
                    print("Not enough data or wrong load, logic sleeping...")
                    time.sleep(30)
                    continue

                for coin in play_symbols_list_final:
                    test_time = time.perf_counter()
                    fresh_data = self.fresh_data_aggregator()
                    coin_row = fresh_data.loc[coin]
                    coin_symbol = coin
                    coin_pair = coin_row["binance_pair"]
                    coin_bina_price = coin_row["bina_price"]
                    coin_drift_price = coin_row["drift_price"]
                    print(f"Test time only selecting coin: {round(time.perf_counter() - test_time, 2)}")

                    if (not positions_dataframe.loc[coin_symbol, "inplay"]) and (positions_dataframe["inplay"].sum() < self.COINS_AT_ONCE):
                        if coin_row["open_l_drift"]:
                            coin_target_value = balances_dict["coin_target_value"]
                            bina_open_amount = round(coin_target_value / coin_bina_price, precisions_dataframe.loc[coin_pair, "amount_precision"])
                            drift_open_value = round(bina_open_amount * coin_drift_price, self.DRIFT_USDC_PRECISION)
                            if bina_open_amount > (precisions_dataframe.loc[coin_pair, "min_order_amount"] * 1.05):
                                print(Fore.YELLOW + f"{round_time(dt=dt.datetime.now(), date_delta=dt.timedelta(seconds=5))} {coin_symbol} Longing Drift: {drift_open_value}, shorting Binance: {bina_open_amount}" + Style.RESET_ALL)
                                print(fresh_data)
                                i = 1
                                while True:
                                    try:
                                        print(f"Try number: {i}")
                                        if not positions_dataframe.loc[coin_symbol, "drift_inplay"]:
                                            drift_orders = time.perf_counter()
                                            long_drift = await drift_open_market_long(API_drift, amount=drift_open_value*self.DRIFT_BIG_N, drift_index=coin_row["drift_pair"])
                                            print("--- Drift orders %s seconds ---" % (round(time.perf_counter() - drift_orders, 2)))
                                        if not positions_dataframe.loc[coin_symbol, "binance_inplay"]:
                                            bina_orders = time.perf_counter()
                                            short_binance = binance_futures_open_market_short(API_binance, pair=coin_row["binance_pair"], amount=bina_open_amount)
                                            print("--- Bina orders %s seconds ---" % (round(time.perf_counter() - bina_orders, 2)))
                                        time.sleep(3)
                                        positions_dataframe = await self.get_positions_summary(fresh_data, API_drift, API_binance)
                                        time.sleep(2)
                                        balances_dict = await self.get_balances_summary(API_drift, API_binance)
                                        break
                                    except Exception as err:
                                        if type(err) == solana.rpc.core.UnconfirmedTxError:
                                            print(f"Unconfirmed TX Error on open positions: {err}")
                                        else:
                                            trace = traceback.format_exc()
                                            print(f"Error on open positions: {err}\n{trace}")
                                        imbalance_status, positions_dataframe = await self.imbalance_checker(fresh_data, coin_symbol, API_drift, API_binance)
                                        if imbalance_status:
                                            continue
                                        else:
                                            break

                        elif coin_row["open_s_drift"]:
                            coin_target_value = balances_dict["coin_target_value"]
                            bina_open_amount = round(coin_target_value / coin_bina_price, precisions_dataframe.loc[coin_pair, "amount_precision"])
                            drift_open_value = round(bina_open_amount * coin_drift_price, self.DRIFT_USDC_PRECISION)
                            if bina_open_amount > (precisions_dataframe.loc[coin_pair, "min_order_amount"] * 1.05):
                                print(Fore.YELLOW + f"{round_time(dt=dt.datetime.now(), date_delta=dt.timedelta(seconds=5))} {coin_symbol} Shorting Drift: {drift_open_value}, longing Binance: {bina_open_amount}" + Style.RESET_ALL)
                                print(fresh_data)
                                i = 1
                                while True:
                                    try:
                                        print(f"Try number: {i}")
                                        if not positions_dataframe.loc[coin_symbol, "drift_inplay"]:
                                            drift_orders = time.perf_counter()
                                            short_drift = await drift_open_market_short(API_drift, amount=drift_open_value*self.DRIFT_BIG_N, drift_index=coin_row["drift_pair"])
                                            print("--- Drift orders %s seconds ---" % (round(time.perf_counter() - drift_orders, 2)))
                                        if not positions_dataframe.loc[coin_symbol, "binance_inplay"]:
                                            bina_orders = time.perf_counter()
                                            long_binance = binance_futures_open_market_long(API_binance, pair=coin_row["binance_pair"], amount=bina_open_amount)
                                            print("--- Bina orders %s seconds ---" % (round(time.perf_counter() - bina_orders, 2)))
                                        time.sleep(3)
                                        positions_dataframe = await self.get_positions_summary(fresh_data, API_drift, API_binance)
                                        time.sleep(2)
                                        balances_dict = await self.get_balances_summary(API_drift, API_binance)
                                        break
                                    except Exception as err:
                                        if type(err) == solana.rpc.core.UnconfirmedTxError:
                                            print(f"Unconfirmed TX Error on open positions: {err}")
                                        else:
                                            trace = traceback.format_exc()
                                            print(f"Error on open positions: {err}\n{trace}")
                                        imbalance_status, positions_dataframe = await self.imbalance_checker(fresh_data, coin_symbol, API_drift, API_binance)
                                        if imbalance_status:
                                            continue
                                        else:
                                            break

                    else:
                        bina_close_amount = round(abs(positions_dataframe.loc[coin_symbol, "binance_pos"] * 1.1), precisions_dataframe.loc[coin_pair, "amount_precision"])
                        if coin_row["close_l_drift"] and (positions_dataframe.loc[coin_symbol, "drift_pos"] > 0):
                            if bina_close_amount > (precisions_dataframe.loc[coin_pair, "min_order_amount"] * 1.05):
                                print(Fore.YELLOW + f"{round_time(dt=dt.datetime.now(), date_delta=dt.timedelta(seconds=5))} {coin_symbol} Closing Drift long: All, closing Binance short: {bina_close_amount}" + Style.RESET_ALL)
                                print(fresh_data)
                                i = 1
                                while True:
                                    try:
                                        print(f"Try number: {i}")
                                        if positions_dataframe.loc[coin_symbol, "drift_inplay"]:
                                            drift_orders = time.perf_counter()
                                            close_drift_long = await drift_close_order(API_drift, drift_index=coin_row["drift_pair"])
                                            print("--- Drift orders %s seconds ---" % (round(time.perf_counter() - drift_orders, 2)))
                                        if positions_dataframe.loc[coin_symbol, "binance_inplay"]:
                                            bina_orders = time.perf_counter()
                                            close_binance_short = binance_futures_close_market_short(API_binance, pair=coin_row["binance_pair"], amount=bina_close_amount)
                                            print("--- Bina orders %s seconds ---" % (round(time.perf_counter() - bina_orders, 2)))
                                        time.sleep(3)
                                        positions_dataframe = await self.get_positions_summary(fresh_data, API_drift, API_binance)
                                        time.sleep(2)
                                        balances_dict = await self.get_balances_summary(API_drift, API_binance)
                                        break
                                    except Exception as err:
                                        if type(err) == solana.rpc.core.UnconfirmedTxError:
                                            print(f"Unconfirmed TX Error on closing positions: {err}")
                                        else:
                                            trace = traceback.format_exc()
                                            print(f"Error on closing positions: {err}\n{trace}")
                                        imbalance_status, positions_dataframe = await self.imbalance_checker(fresh_data, coin_symbol, API_drift, API_binance)
                                        if imbalance_status:
                                            continue
                                        else:
                                            break

                        elif coin_row["close_s_drift"] and (positions_dataframe.loc[coin_symbol, "drift_pos"] < 0):
                            if bina_close_amount > (precisions_dataframe.loc[coin_pair, "min_order_amount"] * 1.05):
                                print(Fore.YELLOW + f"{round_time(dt=dt.datetime.now(), date_delta=dt.timedelta(seconds=5))} {coin_symbol} Closing Drift short: All, closing Binance long: {bina_close_amount}" + Style.RESET_ALL)
                                print(fresh_data)
                                i = 1
                                while True:
                                    try:
                                        print(f"Try number: {i}")
                                        if positions_dataframe.loc[coin_symbol, "drift_inplay"]:
                                            drift_orders = time.perf_counter()
                                            close_drift_short = await drift_close_order(API_drift, drift_index=coin_row["drift_pair"])
                                            print("--- Drift orders %s seconds ---" % (round(time.perf_counter() - drift_orders, 2)))
                                        if positions_dataframe.loc[coin_symbol, "binance_inplay"]:
                                            bina_orders = time.perf_counter()
                                            close_binance_long = binance_futures_close_market_long(API_binance, pair=coin_row["binance_pair"], amount=bina_close_amount)
                                            print("--- Bina orders %s seconds ---" % (round(time.perf_counter() - bina_orders, 2)))
                                        time.sleep(3)
                                        positions_dataframe = await self.get_positions_summary(fresh_data, API_drift, API_binance)
                                        time.sleep(2)
                                        balances_dict = await self.get_balances_summary(API_drift, API_binance)
                                        break
                                    except Exception as err:
                                        if type(err) == solana.rpc.core.UnconfirmedTxError:
                                            print(f"Unconfirmed TX Error on closing positions: {err}")
                                        else:
                                            trace = traceback.format_exc()
                                            print(f"Error on closing positions: {err}\n{trace}")
                                        imbalance_status, positions_dataframe = await self.imbalance_checker(fresh_data, coin_symbol, API_drift, API_binance)
                                        if imbalance_status:
                                            continue
                                        else:
                                            break

                elapsed = time.perf_counter() - logic_start_time
                expected = 2.5
                if elapsed < expected:
                    time.sleep(expected - elapsed)
                elif elapsed > 5:
                    pass
                    print(f"{round_time(dt=dt.datetime.now(), date_delta=dt.timedelta(seconds=5))} --- Logic loop %s seconds ---" % (round(time.perf_counter() - logic_start_time, 2)))

                if x > 50:
                    balances_dict = await self.get_balances_summary(API_drift, API_binance)
                    API_drift = await self.initiate_drift()
                    API_binance = self.initiate_binance()
                    x = 0

                x += 1

            except (httpcore.ReadTimeout, httpcore.ConnectError, httpcore.RemoteProtocolError, httpx.ReadTimeout,
                    requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout, httpx.ConnectError) as err:
                print(f"Read timeout/connection error: {err}")
                time.sleep(1)

            except Exception as err:
                trace = traceback.format_exc()
                print(f"Err: {err}")
                print(f"Err type: {type(err)}")
                print(f"Err cause: {err.__cause__}")
                print(f"Err cause type: {type(err.__cause__)}")
                print(f"Err context: {err.__context__}")
                print(f"Err context type: {type(err.__context__)}")
                print(f"Error on logic: {err}\n{trace}")
                time.sleep(1)

    def main(self):
        asyncio.run(self.run_constant_parallel_logic())


if __name__ == "__main__":
    try:
        p1 = Process(target=DataHandle().main)
        p1.start()
        time.sleep(10)
        p2 = Process(target=LogicHandle().main)
        p2.start()

    except (httpcore.ReadTimeout, httpcore.ConnectError, httpcore.RemoteProtocolError, httpx.ReadTimeout, requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout, httpx.ConnectError) as err:
        print(f"Read timeout/connection error: {err}")
        time.sleep(1)

    except Exception as err:
        trace = traceback.format_exc()
        print(f"Err: {err}")
        print(f"Err type: {type(err)}")
        print(f"Err cause: {err.__cause__}")
        print(f"Err cause type: {type(err.__cause__)}")
        print(f"Err context: {err.__context__}")
        print(f"Err context type: {type(err.__context__)}")
        print(f"Error on main: {err}\n{trace}")
        time.sleep(1)
