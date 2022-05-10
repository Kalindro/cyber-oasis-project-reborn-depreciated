import traceback
import os
import datetime as dt
import asyncio
import multiprocessing
import httpcore
import httpx
import requests.exceptions
import solana
import sys
import shutil
import time
import schedule

from multiprocessing import Process
from colorama import Fore, Style
from pathlib import Path
from Gieldy.Kucoin.Kucoin_futures_utils import *
from Gieldy.Drift.Drift_utils import *
from Gieldy.Refractor_general.General_utils import round_time

from Gieldy.Kucoin.API_initiation_Kucoin_futures_Drift_ARB_Layer_1 import API_initiation as API_kucoin
from Gieldy.Drift.API_initiation_Drift_ARB_Layer_1 import API_initiation as API_drift


if sys.platform == "win32" and sys.version_info.minor >= 8:
    asyncio.set_event_loop_policy(
        asyncio.WindowsSelectorEventLoopPolicy()
    )

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

current_path = os.path.dirname(os.path.abspath(__file__))
project_path = Path(current_path).parent.parent

timeout_errors = (httpcore.ReadTimeout, httpcore.ConnectError, httpcore.RemoteProtocolError, httpx.ReadTimeout,
                  requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout, httpx.ConnectError, httpcore.RemoteProtocolError,
                  httpx.RemoteProtocolError, httpx.HTTPStatusError)


def exception_handler(error_input_handle, counter=1):
    if (type(error_input_handle) in timeout_errors) or (type(error_input_handle.__context__) in timeout_errors):
        print(f"Read timeout/connection error: {error_input_handle}")
        time.sleep(1)
    elif ("too many requests for" in str(error_input_handle).lower()) or ("too many requests for" in str(error_input_handle.__context__).lower()):
        print(f"Too many requests: {error_input_handle}")
        time.sleep(5)
    else:
        trace = traceback.format_exc()
        print(f"Err: {error_input_handle}")
        print(f"Err type: {type(error_input_handle)}")
        print(f"Err cause: {error_input_handle.__cause__}")
        print(f"Err cause type: {type(error_input_handle.__cause__)}")
        print(f"Err context: {error_input_handle.__context__}")
        print(f"Err context type: {type(error_input_handle.__context__)}")
        print(f"Full error and trace: {error_input_handle}\n{trace}")
        time.sleep(5)
    if counter > 100:
        print("Major error, requests burn, sleeping big time")
        time.sleep(108_000)
        counter = 0


class Initialize:

    def __init__(self):
        self.LIMIT_DATA = True
        self.ZSCORE_PERIOD = int(1 * 3600 / 5)  # Edit first number, hours of period (hours * minute in seconds / 5s data frequency)
        self.FAST_AVG = 28
        self.QUARTILE = 0.20
        self.QUARTILE_PERIOD = int(0.20 * self.ZSCORE_PERIOD)
        self.MIN_REGULAR_GAP = 0.45
        self.MIN_CLOSING_GAP = 0.02
        self.LEVERAGE = 4
        self.COINS_AT_ONCE = 5
        self.DRIFT_BIG_N = 1_000_000
        self.DRIFT_USDC_PRECISION = 4
        self.CLOSE_ONLY_MODE = False

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
                if (len(historical_arb_df) < 2) or np.isnan(historical_arb_df.iloc[-1]["kucoin_price"]) or np.isnan(historical_arb_df.iloc[-1]["drift_price"]) or np.isnan(historical_arb_df.iloc[-1]["gap_perc"]):
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
    def initiate_kucoin():
        return API_kucoin()

    @staticmethod
    async def initiate_drift_private():
        return await API_drift(private=True)

    @staticmethod
    async def initiate_drift_public():
        return await API_drift(private=False)


class DataHandle(Initialize):

    async def update_history_dataframe(self, historical_arb_df, API_drift, API_kucoin):
        arb_df = df()

        drift_prices = await drift_get_pair_prices_rates(API_drift)
        kucoin_prices = kucoin_futures_get_pair_prices(API_kucoin)
        arb_df["drift_pair"] = drift_prices["market_index"]
        arb_df["kucoin_pair"] = kucoin_prices["pair"]
        arb_df = arb_df[["kucoin_pair", "drift_pair"]]
        arb_df["kucoin_price"] = kucoin_prices["mark_price"]
        arb_df["drift_price"] = drift_prices["mark_price"]
        arb_df["gap_perc"] = (arb_df["kucoin_price"] - arb_df["drift_price"]) / arb_df["kucoin_price"] * 100
        arb_df["gap_abs"] = abs(arb_df["gap_perc"])
        arb_df["timestamp"] = round_time(dt=dt.datetime.now(), date_delta=dt.timedelta(seconds=5))
        arb_df.reset_index(inplace=True)
        arb_df.set_index("timestamp", inplace=True)

        historical_arb_df = historical_arb_df.append(arb_df)
        historical_arb_df.reset_index(inplace=True)
        historical_arb_df.drop_duplicates(subset=["timestamp", "symbol"], keep="last", inplace=True)
        historical_arb_df.set_index("timestamp", inplace=True)
        historical_arb_df = historical_arb_df[historical_arb_df["kucoin_price"].notna()]

        if self.LIMIT_DATA:
            historical_arb_df = historical_arb_df[historical_arb_df.index > (dt.datetime.now() - timedelta(seconds=(self.ZSCORE_PERIOD * 1.25) * 5))]

        return historical_arb_df

    async def run_constant_parallel_fresh_data_update(self):
        print("Running data side...")

        API_drift = await self.initiate_drift_public()
        API_kucoin = self.initiate_kucoin()
        historical_arb_df = await self.update_history_dataframe(self.read_historical_dataframe(), API_drift, API_kucoin)

        save_counter_data = 0
        while True:
            try:
                data_start_time = time.perf_counter()
                historical_arb_df = await self.update_history_dataframe(historical_arb_df, API_drift, API_kucoin)
                if len(historical_arb_df) > 2:
                    historical_arb_df.to_pickle(f"{project_path}/History_data/Drift/5S/Price_gaps_5S_LIVE_WRITE.pickle")
                else:
                    print("Probowal zapisac pusta kurwe ock czemu")

                elapsed = time.perf_counter() - data_start_time
                expected = 2.5
                if elapsed < expected:
                    time.sleep(expected - elapsed)
                elif elapsed > 4:
                    print(f"{round_time(dt=dt.datetime.now(), date_delta=dt.timedelta(seconds=5))} --- Data loop %s seconds ---" % (round(time.perf_counter() - data_start_time, 2)))

                if save_counter_data > 500:
                    historical_arb_df.to_csv(f"{project_path}/History_data/Drift/5S/Price_gaps_5S.csv")
                    API_drift = await self.initiate_drift_public()
                    API_kucoin = self.initiate_kucoin()
                    save_counter_data = 0

            except Exception as err:
                print("Error on data below")
                exception_handler(err)
            finally:
                save_counter_data += 1

    def main(self):
        asyncio.run(self.run_constant_parallel_fresh_data_update())


class LogicConds(Initialize):

    @staticmethod
    def conds_inplay(row):
        if (abs(row["kucoin_pos"]) > 0) and (abs(row["drift_pos"]) > 0):
            return True
        else:
            return False

    @staticmethod
    def conds_kucoin_inplay(row):
        if abs(row["kucoin_pos"]) > 0:
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
        if (row["kucoin_inplay"] and not row["drift_inplay"]) or (row["drift_inplay"] and not row["kucoin_inplay"]):
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

    async def imbalance_checker(self, fresh_data, coin_symbol, API_drift, API_kucoin):
        positions_dataframe = await self.get_positions_summary(fresh_data, API_drift, API_kucoin, printing=False)
        if positions_dataframe.loc[coin_symbol, "imbalance"]:
            print("Imbalance on initial check")
            return True, positions_dataframe

        time.sleep(15)
        positions_dataframe = await self.get_positions_summary(fresh_data, API_drift, API_kucoin, printing=False)
        if positions_dataframe.loc[coin_symbol, "imbalance"]:
            print("Imbalance on first check")
            return True, positions_dataframe

        time.sleep(15)
        positions_dataframe = await self.get_positions_summary(fresh_data, API_drift, API_kucoin, printing=False)
        if positions_dataframe.loc[coin_symbol, "imbalance"]:
            print("Imbalance on second check")
            return True, positions_dataframe

        time.sleep(15)
        positions_dataframe = await self.get_positions_summary(fresh_data, API_drift, API_kucoin, printing=False)
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

        for frame in coin_dataframes_dict.values():
            fresh_row = frame.iloc[-1]
            fresh_row["fast_avg_gap"] = frame["gap_perc"].tail(self.FAST_AVG).median() * 0.95
            fresh_row["top_avg_gaps"] = frame["gap_perc"].tail(self.ZSCORE_PERIOD).nlargest(self.QUARTILE_PERIOD).median() if\
                len(frame) > self.ZSCORE_PERIOD else np.nan
            fresh_row.loc["bottom_avg_gaps"] = frame["gap_perc"].tail(self.ZSCORE_PERIOD).nsmallest(self.QUARTILE_PERIOD).median() if\
                len(frame) > self.ZSCORE_PERIOD else np.nan
            fresh_row["open_l_drift"] = LogicConds().conds_open_long_drift(fresh_row)
            fresh_row["open_s_drift"] = LogicConds().conds_open_short_drift(fresh_row)
            fresh_row["close_l_drift"] = LogicConds().conds_close_long_drift(fresh_row)
            fresh_row["close_s_drift"] = LogicConds().conds_close_short_drift(fresh_row)
            fresh_data = fresh_data.append(fresh_row)

        fresh_data.sort_values(by=["gap_abs"], inplace=True)
        fresh_data.reset_index(inplace=True)
        fresh_data.rename(columns={"index": "timestamp"}, inplace=True)
        fresh_data.set_index("symbol", inplace=True)

        return fresh_data

    async def get_positions_summary(self, fresh_data, API_drift, API_kucoin, printing=True):
        playable_coins_list = fresh_data.index.unique()
        kucoin_positions = kucoin_futures_positions(API_kucoin)
        kucoin_positions = kucoin_positions[kucoin_positions.index.isin(playable_coins_list)]
        drift_positions = await drift_load_positions(API_drift)
        kucoin_futures_margin_check(API_kucoin, self.LEVERAGE)

        positions_dataframe = df()
        positions_dataframe.index = kucoin_positions.index
        positions_dataframe["kucoin_pos"] = kucoin_positions["amount"]
        positions_dataframe["drift_pos"] = drift_positions["base_asset_amount"]
        positions_dataframe.fillna(0, inplace=True)
        positions_dataframe["kucoin_pair"] = kucoin_positions["pair"]
        positions_dataframe["drift_pair"] = fresh_data.loc[positions_dataframe.index, "drift_pair"].astype(int)
        positions_dataframe["inplay"] = positions_dataframe.apply(lambda row: LogicConds().conds_inplay(row), axis=1)
        positions_dataframe["kucoin_inplay"] = positions_dataframe.apply(lambda row: LogicConds().conds_kucoin_inplay(row), axis=1)
        positions_dataframe["drift_inplay"] = positions_dataframe.apply(lambda row: LogicConds().conds_drift_inplay(row), axis=1)
        positions_dataframe["imbalance"] = positions_dataframe.apply(lambda row: LogicConds().conds_imbalance(row), axis=1)
        if printing:
            print(positions_dataframe)

        return positions_dataframe

    async def get_balances_summary(self, API_drift, API_kucoin, printing=True):
        kucoin_balances = kucoin_futures_get_balance(API_kucoin)
        drift_balances = await drift_get_margin_account_info(API_drift)
        balances_dict = {"kucoin": float(kucoin_balances['total']),
                         "drift": float(drift_balances['collateral']),
                         "sum": float(kucoin_balances['total']) + float(drift_balances['collateral']),
                         "kucoin_play_value": float(kucoin_balances['total']) * 0.80,
                         "drift_play_value": float(drift_balances['collateral']) * 0.80,
                         "coin_target_value": float(kucoin_balances['total']) * 0.80 * self.LEVERAGE / self.COINS_AT_ONCE}

        if printing:
            print(Fore.GREEN + f"{round_time(dt=dt.datetime.now(), date_delta=dt.timedelta(seconds=5))}: Account value sum: {balances_dict['sum']:.0f},"
                               f" Kucoin: {balances_dict['kucoin']:.0f} Drift: {balances_dict['drift']:.0f}" + Style.RESET_ALL)

        return balances_dict

    async def run_constant_parallel_logic(self):
        print("Running logic side...")
        API_drift = await self.initiate_drift_private()
        API_kucoin = self.initiate_kucoin()
        precisions_dataframe = kucoin_futures_get_pairs_precisions_status(API_kucoin)
        fresh_data = self.fresh_data_aggregator()
        balances_dict = await self.get_balances_summary(API_drift, API_kucoin)
        positions_dataframe = await self.get_positions_summary(fresh_data, API_drift, API_kucoin)
        print(fresh_data)

        loop_counter_logic = 0
        while True:
            try:
                logic_start_time = time.perf_counter()
                fresh_data = self.fresh_data_aggregator()
                not_inplay_list = [coin for coin in positions_dataframe.loc[~positions_dataframe["inplay"]].index]
                not_inplay_data = fresh_data[fresh_data.index.isin(not_inplay_list)]
                top_to_be_open_coins = [coin for coin in not_inplay_data.tail(2).index]
                top_to_be_open_coins.reverse()
                inplay_coins = [coin for coin in positions_dataframe.loc[positions_dataframe["inplay"]].index]
                play_sombols_list = inplay_coins + top_to_be_open_coins
                play_symbols_list_final = []
                [play_symbols_list_final.append(symbol) for symbol in play_sombols_list if symbol not in play_symbols_list_final]

                if np.isnan(fresh_data.iloc[-4]["top_avg_gaps"]):
                    print("Not enough data or wrong load, logic sleeping...")
                    time.sleep(30)
                    continue

                for coin in play_symbols_list_final:
                    fresh_data = self.fresh_data_aggregator()
                    coin_row = fresh_data.loc[coin]
                    coin_symbol = coin
                    coin_kucoin_price = coin_row["kucoin_price"]
                    coin_drift_price = coin_row["drift_price"]

                    if (not positions_dataframe.loc[coin_symbol, "inplay"]) and (positions_dataframe["inplay"].sum() < self.COINS_AT_ONCE) and (not self.CLOSE_ONLY_MODE):
                        if coin_row["open_l_drift"]:
                            coin_target_value = balances_dict["coin_target_value"]
                            kucoin_open_amount = round(coin_target_value / coin_kucoin_price, precisions_dataframe.loc[coin_symbol, "amount_precision"])
                            drift_open_value = round(kucoin_open_amount * coin_drift_price, self.DRIFT_USDC_PRECISION)
                            if kucoin_open_amount > (precisions_dataframe.loc[coin_symbol, "min_order_amount"] * 1.05):
                                print(Fore.YELLOW + f"{round_time(dt=dt.datetime.now(), date_delta=dt.timedelta(seconds=5))} {coin_symbol} Longing Drift: {drift_open_value}, shorting Kucoin: {kucoin_open_amount}" + Style.RESET_ALL)
                                print(fresh_data)
                                err_counter_orders = 1
                                while True:
                                    try:
                                        print(f"Try number: {err_counter_orders}")
                                        if not positions_dataframe.loc[coin_symbol, "drift_inplay"]:
                                            drift_orders = time.perf_counter()
                                            long_drift = await drift_open_market_long(API_drift, amount=drift_open_value*self.DRIFT_BIG_N, drift_index=coin_row["drift_pair"])
                                            print("--- Drift orders %s seconds ---" % (round(time.perf_counter() - drift_orders, 2)))
                                        if not positions_dataframe.loc[coin_symbol, "kucoin_inplay"]:
                                            kucoin_orders = time.perf_counter()
                                            short_kucoin = kucoin_futures_open_market_short(API_kucoin, pair=coin_row["kucoin_pair"], amount=kucoin_open_amount, leverage=self.LEVERAGE)
                                            print("--- Kucoin orders %s seconds ---" % (round(time.perf_counter() - kucoin_orders, 2)))
                                        time.sleep(4)
                                        positions_dataframe = await self.get_positions_summary(fresh_data, API_drift, API_kucoin)
                                        break
                                    except solana.rpc.core.UnconfirmedTxError as err:
                                        print(f"Unconfirmed TX Error on opening positions: {err}")
                                        imbalance_status, positions_dataframe = await self.imbalance_checker(fresh_data, coin_symbol, API_drift, API_kucoin)
                                        if imbalance_status:
                                            continue
                                        else:
                                            break
                                    except Exception as err:
                                        print("Error on data below")
                                        exception_handler(err)
                                    finally:
                                        err_counter_orders += 1

                        elif coin_row["open_s_drift"]:
                            coin_target_value = balances_dict["coin_target_value"]
                            kucoin_open_amount = round(coin_target_value / coin_kucoin_price, precisions_dataframe.loc[coin_symbol, "amount_precision"])
                            drift_open_value = round(kucoin_open_amount * coin_drift_price, self.DRIFT_USDC_PRECISION)
                            if kucoin_open_amount > (precisions_dataframe.loc[coin_symbol, "min_order_amount"] * 1.05):
                                print(Fore.YELLOW + f"{round_time(dt=dt.datetime.now(), date_delta=dt.timedelta(seconds=5))} {coin_symbol} Shorting Drift: {drift_open_value}, longing Kucoin: {kucoin_open_amount}" + Style.RESET_ALL)
                                print(fresh_data)
                                err_counter_orders = 1
                                while True:
                                    try:
                                        print(f"Try number: {err_counter_orders}")
                                        if not positions_dataframe.loc[coin_symbol, "drift_inplay"]:
                                            drift_orders = time.perf_counter()
                                            short_drift = await drift_open_market_short(API_drift, amount=drift_open_value*self.DRIFT_BIG_N, drift_index=coin_row["drift_pair"])
                                            print("--- Drift orders %s seconds ---" % (round(time.perf_counter() - drift_orders, 2)))
                                        if not positions_dataframe.loc[coin_symbol, "kucoin_inplay"]:
                                            kucoin_orders = time.perf_counter()
                                            long_kucoin = kucoin_futures_open_market_long(API_kucoin, pair=coin_row["kucoin_pair"], amount=kucoin_open_amount, leverage=self.LEVERAGE)
                                            print("--- Kucoin orders %s seconds ---" % (round(time.perf_counter() - kucoin_orders, 2)))
                                        time.sleep(4)
                                        positions_dataframe = await self.get_positions_summary(fresh_data, API_drift, API_kucoin)
                                        break
                                    except solana.rpc.core.UnconfirmedTxError as err:
                                        print(f"Unconfirmed TX Error on opening positions: {err}")
                                        imbalance_status, positions_dataframe = await self.imbalance_checker(fresh_data, coin_symbol, API_drift, API_kucoin)
                                        if imbalance_status:
                                            continue
                                        else:
                                            break
                                    except Exception as err:
                                        print("Error on data below")
                                        exception_handler(err)
                                    finally:
                                        err_counter_orders += 1

                    else:
                        kucoin_close_amount = round(abs(positions_dataframe.loc[coin_symbol, "kucoin_pos"] * 1.1), precisions_dataframe.loc[coin_symbol, "amount_precision"])
                        if coin_row["close_l_drift"] and (positions_dataframe.loc[coin_symbol, "drift_pos"] > 0):
                            if kucoin_close_amount > (precisions_dataframe.loc[coin_symbol, "min_order_amount"] * 1.05):
                                print(Fore.YELLOW + f"{round_time(dt=dt.datetime.now(), date_delta=dt.timedelta(seconds=5))} {coin_symbol} Closing Drift long: All, closing Kucoin short: {kucoin_close_amount}" + Style.RESET_ALL)
                                print(fresh_data)
                                err_counter_orders = 1
                                while True:
                                    try:
                                        print(f"Try number: {err_counter_orders}")
                                        if positions_dataframe.loc[coin_symbol, "drift_inplay"]:
                                            drift_orders = time.perf_counter()
                                            close_drift_long = await drift_close_order(API_drift, drift_index=coin_row["drift_pair"])
                                            print("--- Drift orders %s seconds ---" % (round(time.perf_counter() - drift_orders, 2)))
                                        if positions_dataframe.loc[coin_symbol, "kucoin_inplay"]:
                                            kucoin_orders = time.perf_counter()
                                            close_kucoin_short = kucoin_futures_close_market_short(API_kucoin, pair=coin_row["kucoin_pair"], leverage=self.LEVERAGE)
                                            print("--- Kucoin orders %s seconds ---" % (round(time.perf_counter() - kucoin_orders, 2)))
                                        time.sleep(4)
                                        positions_dataframe = await self.get_positions_summary(fresh_data, API_drift, API_kucoin)
                                        break
                                    except solana.rpc.core.UnconfirmedTxError as err:
                                        print(f"Unconfirmed TX Error on closing positions: {err}")
                                        imbalance_status, positions_dataframe = await self.imbalance_checker(fresh_data, coin_symbol, API_drift, API_kucoin)
                                        if imbalance_status:
                                            continue
                                        else:
                                            break
                                    except Exception as err:
                                        print("Error on data below")
                                        exception_handler(err)
                                    finally:
                                        err_counter_orders += 1

                        elif coin_row["close_s_drift"] and (positions_dataframe.loc[coin_symbol, "drift_pos"] < 0):
                            if kucoin_close_amount > (precisions_dataframe.loc[coin_symbol, "min_order_amount"] * 1.05):
                                print(Fore.YELLOW + f"{round_time(dt=dt.datetime.now(), date_delta=dt.timedelta(seconds=5))} {coin_symbol} Closing Drift short: All, closing Kucoin long: {kucoin_close_amount}" + Style.RESET_ALL)
                                print(fresh_data)
                                err_counter_orders = 1
                                while True:
                                    try:
                                        print(f"Try number: {err_counter_orders}")
                                        if positions_dataframe.loc[coin_symbol, "drift_inplay"]:
                                            drift_orders = time.perf_counter()
                                            close_drift_short = await drift_close_order(API_drift, drift_index=coin_row["drift_pair"])
                                            print("--- Drift orders %s seconds ---" % (round(time.perf_counter() - drift_orders, 2)))
                                        if positions_dataframe.loc[coin_symbol, "kucoin_inplay"]:
                                            kucoin_orders = time.perf_counter()
                                            close_kucoin_long = kucoin_futures_close_market_long(API_kucoin, pair=coin_row["kucoin_pair"], leverage=self.LEVERAGE)
                                            print("--- Kucoin orders %s seconds ---" % (round(time.perf_counter() - kucoin_orders, 2)))
                                        time.sleep(4)
                                        positions_dataframe = await self.get_positions_summary(fresh_data, API_drift, API_kucoin)
                                        break
                                    except solana.rpc.core.UnconfirmedTxError as err:
                                        print(f"Unconfirmed TX Error on closing positions: {err}")
                                        imbalance_status, positions_dataframe = await self.imbalance_checker(fresh_data, coin_symbol, API_drift, API_kucoin)
                                        if imbalance_status:
                                            continue
                                        else:
                                            break
                                    except Exception as err:
                                        print("Error on data below")
                                        exception_handler(err)
                                    finally:
                                        err_counter_orders += 1

                elapsed = time.perf_counter() - logic_start_time
                expected = 5
                if elapsed < expected:
                    time.sleep(expected - elapsed)
                elif elapsed > 10:
                    print(f"{round_time(dt=dt.datetime.now(), date_delta=dt.timedelta(seconds=5))} --- Logic loop %s seconds ---" % (round(time.perf_counter() - logic_start_time, 2)))

                if loop_counter_logic > 35:
                    balances_dict = await self.get_balances_summary(API_drift, API_kucoin)
                    API_drift = await self.initiate_drift_private()
                    API_kucoin = self.initiate_kucoin()
                    loop_counter_logic = 0

                loop_counter_logic += 1

            except Exception as err:
                print("Error on logic below")
                exception_handler(err)

    def main(self):
        asyncio.run(self.run_constant_parallel_logic())


if __name__ == "__main__":

    def start_processes():
        p1 = Process(target=DataHandle().main)
        p1.start()
        time.sleep(10)
        p2 = Process(target=LogicHandle().main)
        p2.start()

    def restart_processes():
        print("Restarting processes commencing!")

        print("Terminating processes")
        for p in multiprocessing.active_children():
            p.terminate()
            time.sleep(3)

        print("Starting new processes")
        start_processes()

    try:
        start_processes()
        schedule.every(12).hours.do(restart_processes)

        while True:
            schedule.run_pending()
            time.sleep(5)

    except Exception as err:
        print("Error on main below")
        exception_handler(err)
