import traceback
import os
import datetime as dt
import sys
import solana


from colorama import Fore, Back, Style
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
        self.zscore_period = 240
        self.quartile = 0.15
        self.min_regular_gap = 0.30
        self.min_range_gap = 0.32
        self.leverage = 3
        self.drift_big_N = 1_000_000

    @staticmethod
    def initiate_binance():
        return API_binance()

    @staticmethod
    async def initiate_drift():
        return await API_drift()

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
        if (((row["gap_perc"] > self.min_regular_gap) or (row["avg_gap"] > self.min_regular_gap)) or (
                (row["gap_range"]) > self.min_range_gap)) and (row["gap_perc"] > max(row["top_avg_gaps"], 0.12)):
            return True
        else:
            return False

    def conds_open_short_drift(self, row):
        if (((row["gap_perc"] < -self.min_regular_gap) or (row["avg_gap"] < -self.min_regular_gap)) or (
                (row["gap_range"]) > self.min_range_gap)) and (row["gap_perc"] < min(row["bottom_avg_gaps"], -0.12)):
            return True
        else:
            return False

    @staticmethod
    def conds_close_long_drift(row):
        if row["gap_perc"] < min(row["bottom_avg_gaps"], -0.05):
            return True
        else:
            return False

    @staticmethod
    def conds_close_short_drift(row):
        if row["gap_perc"] > max(row["top_avg_gaps"], 0.05):
            return True
        else:
            return False

    @staticmethod
    def read_historical_dataframe():
        try:
            historical_arb_df = pd.read_csv(f"{project_path}/History_data/Drift/5S/Price_gaps_5S.csv", index_col=0, parse_dates=True)
        except:
            print("No saved DF")
            historical_arb_df = df()

        return historical_arb_df

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
                lambda x: np.median(sorted(x, reverse=True)[:int(self.quartile*self.zscore_period)]))
            frame["bottom_avg_gaps"] = frame["gap_perc"].rolling(self.zscore_period, self.zscore_period).apply(
                lambda x: np.median(sorted(x, reverse=False)[:int(self.quartile*self.zscore_period)]))
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

    async def get_positions_summary(self, fresh_data, API_binance, API_drift, printing=True):
        playable_coins_list = fresh_data.symbol.unique()
        binance_positions = binance_futures_positions(API_binance)
        binance_positions = binance_positions[binance_positions.index.isin(playable_coins_list)]
        drift_positions = await drift_load_positions(API_drift)
        self.binance_futures_margin_leverage_check(API_binance=API_binance, binance_positions=binance_positions)

        positions_dataframe = df()
        positions_dataframe.index = binance_positions.index
        positions_dataframe["binance_pos"] = binance_positions["positionAmt"].astype(float)
        positions_dataframe["drift_pos"] = drift_positions["base_asset_amount"].astype(float)
        positions_dataframe.fillna(0, inplace=True)
        positions_dataframe["binance_pair"] = binance_positions["pair"]
        positions_dataframe["drift_pair"] = (fresh_data.set_index("symbol").loc[positions_dataframe.index, "drift_pair"]).astype(int)
        positions_dataframe["inplay"] = positions_dataframe.apply(lambda row: self.conds_inplay(row), axis=1)
        positions_dataframe["noplay"] = positions_dataframe.apply(lambda row: self.conds_noplay(row), axis=1)
        positions_dataframe["binance_inplay"] = positions_dataframe.apply(lambda row: self.conds_binance_inplay(row), axis=1)
        positions_dataframe["drift_inplay"] = positions_dataframe.apply(lambda row: self.conds_drift_inplay(row), axis=1)
        positions_dataframe["imbalance"] = positions_dataframe.apply(lambda row: self.conds_imbalance(row), axis=1)
        if printing:
            print(positions_dataframe)
        time.sleep(1)

        return positions_dataframe

    async def get_balances_summary(self, API_binance, API_drift, printing=True):
        binance_balances = binance_futures_get_balance(API=API_binance).loc["USDT"]
        drift_balances = await drift_get_margin_account_info(API=API_drift)
        balances_dict = {"binance": float(binance_balances['total']),
                         "drift": float(drift_balances['total_collateral']),
                         "sum": float(binance_balances['total']) + float(drift_balances['total_collateral']),
                         "binance_play_value": float(binance_balances['total']) * 0.75,
                         "drift_play_value": float(drift_balances['total_collateral']) * 0.75,
                         "coin_target_value": 15 * self.leverage}
        # "coin_target_value": float(binance_balances['total']) * 0.5}
        if printing:
            print(f"Account value sum: {balances_dict['sum']:.2f}")
        time.sleep(1)

        return balances_dict

    async def constant_parallel_fresh_data_update(self):
        historical_arb_df = await self.update_history_dataframe(historical_arb_df=self.read_historical_dataframe(), API_drift=API_drift, API_binance=API_binance)

        while True:
            historical_arb_df = await self.update_history_dataframe(historical_arb_df=historical_arb_df, API_drift=API_drift, API_binance=API_binance)
            historical_arb_df.to_csv(f"{project_path}/History_data/Drift/5S/Price_gaps_5S.csv")
            fresh_data = self.fresh_data_aggregator(historical_arb_df=historical_arb_df)
            return fresh_data

    async def run_constant_logic(self):
        print("Running...")
        API_drift = await self.initiate_drift()
        API_binance = self.initiate_binance()
        historical_arb_df = self.read_historical_dataframe()
        fresh_data = self.fresh_data_aggregator(historical_arb_df=historical_arb_df)
        positions_dataframe = await self.get_positions_summary(fresh_data=fresh_data, API_drift=API_drift, API_binance=API_binance)
        balances_dict = await self.get_balances_summary(API_binance=API_binance, API_drift=API_drift)

        x = 0
        while True:
            start_time = time.time()
            historical_arb_df = self.read_historical_dataframe()
            fresh_data = self.fresh_data_aggregator(historical_arb_df=historical_arb_df)
            best_coins_open_l = [coin for coin in fresh_data.loc[fresh_data["open_l_drift"], "symbol"]]
            best_coins_open_s = [coin for coin in fresh_data.loc[fresh_data["open_s_drift"], "symbol"]]
            play_symbols_binance_list = [coin for coin in positions_dataframe.loc[positions_dataframe["binance_inplay"]].index]
            play_symbols_drift_list = [coin for coin in positions_dataframe.loc[positions_dataframe["drift_inplay"]].index]
            play_symbols_list = play_symbols_binance_list + play_symbols_drift_list + best_coins_open_s + best_coins_open_l
            play_symbols_list = list(set(play_symbols_list))

            if np.isnan(fresh_data.iloc[-1]["avg_gap"]): continue
            for coin in play_symbols_list:
                coin_row = fresh_data.loc[fresh_data["symbol"] == coin].iloc[-1]
                coin_symbol = coin_row["symbol"]
                coin_pair = coin_row["binance_pair"]
                coin_bina_price = coin_row["bina_price"]
                coin_drift_price = coin_row["drift_price"]

                if (not positions_dataframe.loc[coin_symbol, "inplay"]) and (positions_dataframe["inplay"].sum() < 2):
                    if coin_row["open_l_drift"]:
                        precisions_dataframe = binance_futures_get_pairs_precisions_status(API_binance)
                        balances_dict = await self.get_balances_summary(API_binance=API_binance, API_drift=API_drift)
                        coin_target_value = balances_dict["coin_target_value"]
                        try:
                            bina_open_amount = round(coin_target_value / coin_bina_price, precisions_dataframe.loc[coin_pair, "amount_precision"])
                            drift_open_value = bina_open_amount / coin_drift_price
                        except:
                            print(f"Target val: {coin_target_value}, price: {coin_bina_price}, precision: {precisions_dataframe.loc[coin_pair, 'amount_precision']}")
                        if bina_open_amount > (precisions_dataframe.loc[coin_pair, "min_order_amount"] * 1.05):
                            print(Fore.YELLOW + f"{coin_symbol} Longing Drift: {drift_open_value}, shorting Binance: {bina_open_amount}" + Style.RESET_ALL)
                            print(fresh_data)
                            i = 1
                            while True:
                                try:
                                    print(f"Try number: {i}")
                                    if not positions_dataframe.loc[coin_symbol, "drift_inplay"]:
                                        drift_orders = time.time()
                                        long_drift = await drift_open_market_long(API=API_drift, amount=drift_open_value*self.drift_big_N, drift_index=coin_row["drift_pair"])
                                        print("--- Drift orders %s seconds ---" % (round(time.time() - drift_orders, 2)))
                                    if not positions_dataframe.loc[coin_symbol, "binance_inplay"]:
                                        bina_orders = time.time()
                                        short_binance = binance_futures_open_market_short(API=API_binance, pair=coin_row["binance_pair"], amount=bina_open_amount)
                                        print("--- Bina orders %s seconds ---" % (round(time.time() - bina_orders, 2)))
                                    positions_dataframe = await self.get_positions_summary(fresh_data=fresh_data, API_drift=API_drift, API_binance=API_binance)
                                    break
                                except solana.rpc.core.UnconfirmedTxError as err:
                                    print(f"Unconfirmed TX Error on buys: {err}")
                                    time.sleep(0.5)
                                except Exception as err:
                                    trace = traceback.format_exc()
                                    print(f"Error on buys: {err}\n{trace}")
                                    positions_dataframe = await self.get_positions_summary(fresh_data=fresh_data,
                                                                                           API_drift=API_drift, API_binance=API_binance, printing=False)
                                    if positions_dataframe.loc[coin_symbol, "imbalance"]:
                                        pass
                                    else:
                                        break
                                finally:
                                    i += 1

                    elif coin_row["open_s_drift"]:
                        precisions_dataframe = binance_futures_get_pairs_precisions_status(API_binance)
                        balances_dict = await self.get_balances_summary(API_binance=API_binance, API_drift=API_drift)
                        coin_target_value = balances_dict["coin_target_value"]
                        try:
                            bina_open_amount = round(coin_target_value / coin_bina_price, precisions_dataframe.loc[coin_pair, "amount_precision"])
                            drift_open_value = bina_open_amount / coin_drift_price
                        except:
                            print(f"Target val: {coin_target_value}, price: {coin_bina_price}, precision: {precisions_dataframe.loc[coin_pair, 'amount_precision']}")
                        if bina_open_amount > (precisions_dataframe.loc[coin_pair, "min_order_amount"] * 1.05):
                            print(Fore.YELLOW + f"{coin_symbol} Shorting Drift: {drift_open_value}, longing Binance: {bina_open_amount}" + Style.RESET_ALL)
                            print(fresh_data)
                            i = 1
                            while True:
                                try:
                                    print(f"Try number: {i}")
                                    if not positions_dataframe.loc[coin_symbol, "drift_inplay"]:
                                        drift_orders = time.time()
                                        short_drift = await drift_open_market_short(API=API_drift, amount=drift_open_value*self.drift_big_N, drift_index=coin_row["drift_pair"])
                                        print("--- Drift orders %s seconds ---" % (round(time.time() - drift_orders, 2)))
                                    if not positions_dataframe.loc[coin_symbol, "binance_inplay"]:
                                        bina_orders = time.time()
                                        long_binance = binance_futures_open_market_long(API=API_binance, pair=coin_row["binance_pair"], amount=bina_open_amount)
                                        print("--- Bina orders %s seconds ---" % (round(time.time() - bina_orders, 2)))
                                    positions_dataframe = await self.get_positions_summary(fresh_data=fresh_data, API_drift=API_drift, API_binance=API_binance)
                                    break
                                except solana.rpc.core.UnconfirmedTxError as err:
                                    print(f"Unconfirmed TX Error on buys: {err}")
                                    time.sleep(0.5)
                                except Exception as err:
                                    trace = traceback.format_exc()
                                    print(f"Error on buys: {err}\n{trace}")
                                    positions_dataframe = await self.get_positions_summary(fresh_data=fresh_data,
                                                                                           API_drift=API_drift, API_binance=API_binance, printing=False)
                                    if positions_dataframe.loc[coin_symbol, "imbalance"]:
                                        pass
                                    else:
                                        break
                                finally:
                                    i += 1

                else:
                    precisions_dataframe = binance_futures_get_pairs_precisions_status(API_binance)
                    bina_close_amount = round(abs(positions_dataframe.loc[coin_symbol, "binance_pos"] * 1.1), precisions_dataframe.loc[coin_pair, "amount_precision"])
                    if coin_row["close_l_drift"] and (positions_dataframe.loc[coin_symbol, "drift_pos"] > 0):
                        if bina_close_amount > (precisions_dataframe.loc[coin_pair, "min_order_amount"] * 1.05):
                            print(Fore.YELLOW + f"{coin_symbol} Closing Drift long: All, closing Binance short: {bina_close_amount}" + Style.RESET_ALL)
                            print(fresh_data)
                            i = 1
                            while True:
                                try:
                                    print(f"Try number: {i}")
                                    if positions_dataframe.loc[coin_symbol, "drift_inplay"]:
                                        drift_orders = time.time()
                                        close_drift_long = await drift_close_order(API=API_drift, drift_index=coin_row["drift_pair"])
                                        print("--- Drift orders %s seconds ---" % (round(time.time() - drift_orders, 2)))
                                    if positions_dataframe.loc[coin_symbol, "binance_inplay"]:
                                        bina_orders = time.time()
                                        close_binance_short = binance_futures_close_market_short(API=API_binance, pair=coin_row["binance_pair"], amount=bina_close_amount)
                                        print("--- Bina orders %s seconds ---" % (round(time.time() - bina_orders, 2)))
                                    positions_dataframe = await self.get_positions_summary(fresh_data=fresh_data,
                                                                                           API_drift=API_drift, API_binance=API_binance, printing=False)
                                    break
                                except solana.rpc.core.UnconfirmedTxError as err:
                                    print(f"Unconfirmed TX Error on sells: {err}")
                                    time.sleep(0.5)
                                except Exception as err:
                                    trace = traceback.format_exc()
                                    print(f"Error on sells: {err}\n{trace}")
                                    positions_dataframe = await self.get_positions_summary(fresh_data=fresh_data,
                                                                                           API_drift=API_drift, API_binance=API_binance, printing=False)
                                    if positions_dataframe.loc[coin_symbol, "imbalance"]:
                                        pass
                                    else:
                                        break
                                finally:
                                    i += 1
                    elif coin_row["close_s_drift"] and (positions_dataframe.loc[coin_symbol, "drift_pos"] < 0):
                        if bina_close_amount > (precisions_dataframe.loc[coin_pair, "min_order_amount"] * 1.05):
                            print(Fore.YELLOW + f"{coin_symbol} Closing Drift short: All, closing Binance long: {bina_close_amount}" + Style.RESET_ALL)
                            print(fresh_data)
                            i = 1
                            while True:
                                try:
                                    print(f"Try number: {i}")
                                    if positions_dataframe.loc[coin_symbol, "drift_inplay"]:
                                        drift_orders = time.time()
                                        close_drift_short = await drift_close_order(API=API_drift, drift_index=coin_row["drift_pair"])
                                        print("--- Drift orders %s seconds ---" % (round(time.time() - drift_orders, 2)))
                                    if positions_dataframe.loc[coin_symbol, "binance_inplay"]:
                                        bina_orders = time.time()
                                        close_binance_long = binance_futures_close_market_long(API=API_binance, pair=coin_row["binance_pair"], amount=bina_close_amount)
                                        print("--- Bina orders %s seconds ---" % (round(time.time() - bina_orders, 2)))
                                    positions_dataframe = await self.get_positions_summary(fresh_data=fresh_data, API_drift=API_drift, API_binance=API_binance)
                                    break
                                except solana.rpc.core.UnconfirmedTxError as err:
                                    print(f"Unconfirmed TX Error on sells: {err}")
                                    time.sleep(0.5)
                                except Exception as err:
                                    trace = traceback.format_exc()
                                    print(f"Error on sells: {err}\n{trace}")
                                    positions_dataframe = await self.get_positions_summary(fresh_data=fresh_data,
                                                                                           API_drift=API_drift, API_binance=API_binance, printing=False)
                                    if positions_dataframe.loc[coin_symbol, "imbalance"]:
                                        pass
                                    else:
                                        break
                                finally:
                                    i += 1

            elapsed = time.time() - start_time
            if elapsed < 1.5:
                time.sleep(1.5 - elapsed)
            elif elapsed > 3:
                print("--- Whole loop %s seconds ---\n" % (round(time.time() - start_time, 2)))

            x += 1

    async def main(self):
        while True:
            try:
                await self.run_constant_logic()

            except Exception as err:
                trace = traceback.format_exc()
                print(f"Error: {err}\n{trace}")
                time.sleep(5)


if __name__ == "__main__":
    asyncio.run(DriftBinaARBLive().main())


