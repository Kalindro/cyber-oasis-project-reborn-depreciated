from datetime import timedelta, time, date

import schedule
from functools import partial
from multiprocessing import Pool
import talib

from Gieldy.Refractor_general.General_utils import *
from Gieldy.Refractor_general.Main_refracting import *
from Gieldy.Refractor_general.Average_momentum import *
from Gieldy.Refractor_general.Basic_portfolio_values import get_basic_portfolio_values
from Gieldy.Refractor_general.Get_filtered_tickers import get_filtered_tickers
from Gieldy.Refractor_general.Get_history import get_history_full
from Gieldy.Refractor_general.Mail_sender import mail_error_sender

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

# Parameter settings
MAX_ATTEMPTS = 3
CONSTANT_ERROR_SLEEP_TIME = 600
EXCHANGE_MIN_TRANSACTION_VALUE = 5
WIGGLE_ROOM = 0.995
SAFE_PERCENT = 0.025

TEST = False
REBALANCING = True

FORBID_EXCHANGE_TOKENS = False
FRESH_LIVE_TICKERS = True
BLACK_LIST_ENABLE = True

if BLACK_LIST_ENABLE:
    BLACK_LIST = ["SNX/USDT", "UTK/USDT", "COS/USDT", "CTXC/USDT", "AION/USDT", "BEL/USDT", "MTL/USDT", "EOS/USDT", "WTC/USDT", "LTC/USDT",
                  "KMD/USDT", "LRC/USDT", "ARDR/USDT", "AKRO/USDT", "BCH/USDT", "BAT/USDT", "WAN/USDT", "BTS/USDT",
                  "KEY/USDT", "GTO/USDT", "ONT/USDT"]
else:
    BLACK_LIST = []

TIMEFRAME = "4H"

NUM_ELITE_COINS = 25
MOMENTUM_PERIOD = 100
ETH_SMA_PERIOD = 160
ATR_PERIOD = 42
RVALUE_FILTER = 0.45
MOMENTUM_FILTER = 0


def momentum_l_function(API):
    attempt = 0
    while True:
        try:
            attempt = attempt + 1

            print("Starting new loop")

            time_start = time.time()

            # Clean old logs
            log_clean = open("Logi.txt", "r+")
            log_clean.seek(0)
            log_clean.truncate(0)

            print("Old logs cleaned")

            # Load all pairs and get tickers
            cancel_all_orders(API)

            all_pairs_precisions_status = get_pairs_precisions_status(API)
            all_pairs_prices = get_pairs_prices(API)

            tickers = ["FUN/USDT"] if TEST else get_filtered_tickers(fresh_live_tickers=True,
                                                                     forbid_exchange_tokens=FORBID_EXCHANGE_TOKENS, base="USDT",
                                                                     API=API)

            end = date.today() + timedelta(days=2)
            start = end - timedelta(days=30)

            eth_history = get_history_full("ETH/USDT", start=start, end=end, fresh_live_history=True, timeframe=TIMEFRAME, API=API)
            eth_history["SMA"] = talib.SMA(eth_history["close"], timeperiod=ETH_SMA_PERIOD)
            bull_market_bool = eth_history["close"][-2] > eth_history["SMA"][-2]

            shark_pool = Pool(processes=6)
            all_coins_history = shark_pool.map(
                partial(get_history_full, start=start, end=end, fresh_live_history=True, API=API, timeframe=TIMEFRAME), tickers)
            all_coins_history = dict(zip(tickers, all_coins_history))
            shark_pool.close()
            shark_pool.join()

            print("||| Got all pairs history |||")
            print(f"Bull market is {bull_market_bool} ({round(eth_history['SMA'][-2], 2)})")

            # Get balances
            portfolio_values = get_basic_portfolio_values(API, all_pairs_precisions_status=all_pairs_precisions_status)
            balances = portfolio_values["balances"]
            whole_account_value = portfolio_values["whole_account_value"]

            print("Calculating indicators for all coins")

            # Indicators calculation
            for one_coin_dataframe in all_coins_history.values():
                ATR = talib.ATR(high=one_coin_dataframe["high"], low=one_coin_dataframe["low"], close=one_coin_dataframe["close"], timeperiod=ATR_PERIOD)
                one_coin_dataframe["momentum"] = average_momentum(close=one_coin_dataframe["close"], momentum_period=MOMENTUM_PERIOD)
                one_coin_dataframe["rvalue"] = average_rvalue(close=one_coin_dataframe["close"], momentum_period=MOMENTUM_PERIOD, rvalue_filter=RVALUE_FILTER)
                one_coin_dataframe["SMA"] = talib.SMA(one_coin_dataframe["close"], timeperiod=int(ETH_SMA_PERIOD/2))
                one_coin_dataframe["ATR"] = ATR
                one_coin_dataframe["ATRR"] = 1 / (ATR / one_coin_dataframe["close"])

            print("Creating ranking")

            # Ranking creation
            def ranking():
                ranking = df(columns=["pair", "symbol", "momentum", "rvalue", "ATRR", "target_value", "current_value"])
                for key, value in all_coins_history.items():
                    pair = key
                    pair_history = value
                    symbol = pair_history["symbol"][-2]
                    pair_ATRR = pair_history["ATRR"][-2]
                    pair_momentum = pair_history["momentum"][-2]
                    pair_rvalue = pair_history["rvalue"][-2]
                    ranking_dict = {"pair": [pair], "symbol": [symbol], "momentum": [pair_momentum], "rvalue": [pair_rvalue],
                                    "ATRR": [pair_ATRR]}
                    if (pair_momentum < MOMENTUM_FILTER) or (pair_rvalue < RVALUE_FILTER) or (
                            all_coins_history[pair]["close"].iloc[-2] < all_coins_history[pair]["SMA"].iloc[-2]) or (pair in BLACK_LIST): continue
                    ziobro_ty_kurwo = pd.DataFrame.from_dict(ranking_dict)
                    ranking = ranking.append(ziobro_ty_kurwo, ignore_index=True)
                ranking.sort_values(by=["momentum"], inplace=True, ascending=False)
                ranking.reset_index(inplace=True, drop=True)
                ranking[["pair", "momentum", "rvalue", "ATRR"]].to_excel("Ranking Live.xlsx")
                return ranking

            ranking = ranking()
            ATRR_elite_sum = ranking.head(NUM_ELITE_COINS)["ATRR"].sum()

            # Selling garbage
            def remove_garbage_holdings():
                print("Removing garbage holdings")
                balances_symbols = list(balances.index)
                all_coins_history_symbols = [Symbol[:-5] for Symbol in all_coins_history.keys()]
                garbage_holdings_symbols = [Symbol for Symbol in balances_symbols if Symbol not in all_coins_history_symbols]

                for garbage_symbol in garbage_holdings_symbols:
                    garbage_pair = garbage_symbol + "/USDT"
                    garbage_amount_without_precision = float(balances.loc[garbage_symbol, "total"])
                    if garbage_amount_without_precision <= 0: continue
                    try:
                        garbage_pair_precisions_status = all_pairs_precisions_status.loc[garbage_pair]
                        garbage_price = all_pairs_prices[garbage_pair]
                    except Exception as exec:
                        print(f"{exec}, garbage jakim jest {garbage_pair} nie ma, jebać to skip")
                        continue

                    trading_enabled = trading_enabled_bool(pair_precisions_status=garbage_pair_precisions_status, API=API)
                    if not trading_enabled: continue

                    garbage_amount_precision = garbage_pair_precisions_status["amount_precision"]
                    garbage_min_amount = round_up(max(garbage_pair_precisions_status["min_order_amount"],
                                                      garbage_pair_precisions_status["min_order_value"] / garbage_price),
                                                  garbage_amount_precision)
                    garbage_total_amount = round_down(float(balances.loc[garbage_symbol, "total"]), garbage_amount_precision)
                    if garbage_total_amount > garbage_min_amount:
                        print(f"Sold garbage {garbage_pair}")
                        garbage_market_sell = create_sell_limit_order(pair=garbage_pair, size=garbage_total_amount,
                                                                      price=garbage_price * 0.95, API=API)
                        return garbage_market_sell

            remove_garbage_holdings()

            elite_length = len(ranking.head(NUM_ELITE_COINS))
            safe_amount = whole_account_value * SAFE_PERCENT
            account_value = whole_account_value - safe_amount
            playing_value = account_value * (elite_length / (NUM_ELITE_COINS + 1))

            # Sells sections
            def sells():
                print("Starting SELLS loop")
                for key, value in all_coins_history.items():
                    pair = key
                    symbol = pair[:-5]
                    print(f"Working on possible sells for {pair}")
                    pair_history = value
                    pair_precisions_status = all_pairs_precisions_status.loc[pair]
                    trading_enabled = trading_enabled_bool(pair_precisions_status=pair_precisions_status, API=API)
                    if not trading_enabled: continue

                    amount_precision = pair_precisions_status["amount_precision"]
                    price_precision = pair_precisions_status["price_precision"]

                    current_price = float(pair_history["close"].iloc[-1])
                    candle_1_close = float(pair_history["close"].iloc[-2])
                    candle_1_coin_SMA = float(pair_history["SMA"].iloc[-2])
                    candle_1_ATRR = float(pair_history["ATRR"].iloc[-2])

                    if symbol in balances.index:
                        coin_total_balance = round_down(float(balances.loc[symbol, "total"]), amount_precision)
                        coin_available_balance = round_down(float(balances.loc[symbol, "available"]), amount_precision)
                    else:
                        coin_total_balance = coin_available_balance = 0

                    if coin_total_balance > 0: print(f"Coin has balance, total balance: {coin_total_balance}, available balance: {coin_available_balance}")

                    coin_weight = candle_1_ATRR / ATRR_elite_sum
                    coin_target_value = round(playing_value * coin_weight, 2)
                    coin_current_value = round(coin_total_balance * current_price, 2)
                    in_play_bool = coin_current_value > coin_target_value * 0.5
                    value_left_to_buy = coin_target_value - coin_current_value
                    size_left_to_buy = round_down(value_left_to_buy / current_price, amount_precision)
                    whole_coin_sell_size = round_down(coin_total_balance, amount_precision)
                    sell_size_one_third = round_down(whole_coin_sell_size / 3, amount_precision)

                    sell_place1 = round(current_price * 0.9950, price_precision)
                    sell_place2 = round(current_price * 0.9925, price_precision)
                    sell_place3 = round(current_price * 0.9900, price_precision)
                    average_sell_place = sell_place2
                    exchange_min_amount_sell = round_up(
                        max(pair_precisions_status["min_order_amount"], pair_precisions_status["min_order_value"] / average_sell_place,
                            EXCHANGE_MIN_TRANSACTION_VALUE / average_sell_place), amount_precision)

                    if (pair not in ranking["pair"].head(NUM_ELITE_COINS).values) or (candle_1_close < candle_1_coin_SMA):
                        if sell_size_one_third > exchange_min_amount_sell:
                            print(f"Sell 1: {sell_size_one_third}, {sell_place1}")
                            time.sleep(0.1)
                            sell_order1 = create_sell_limit_order(pair=pair, size=sell_size_one_third, price=sell_place1, API=API)
                            print(f"Sell 2: {sell_size_one_third}, {sell_place2}")
                            time.sleep(0.1)
                            sell_order2 = create_sell_limit_order(pair=pair, size=sell_size_one_third, price=sell_place2, API=API)
                            print(f"Sell 3: {sell_size_one_third}, {sell_place3}")
                            time.sleep(0.1)
                            sell_order3 = create_sell_limit_order(pair=pair, size=sell_size_one_third, price=sell_place3, API=API)
                            continue
                        elif sell_size_one_third < exchange_min_amount_sell < whole_coin_sell_size:
                            print(f"Dust sell: {whole_coin_sell_size}, {sell_place2}")
                            time.sleep(0.1)
                            sell_dust = create_sell_limit_order(pair=pair, size=whole_coin_sell_size, price=sell_place2, API=API)
                            continue

                    # Rebalance sells
                    if REBALANCING:
                        my_min_rebalance_value = coin_target_value * 0.15

                        if (pair in ranking["pair"].head(NUM_ELITE_COINS).values) and in_play_bool:
                            print("Rebalancing...")
                            if ((-size_left_to_buy) > exchange_min_amount_sell) and ((-value_left_to_buy) > my_min_rebalance_value):
                                print(f"Sell Rebalance: {-size_left_to_buy}, {sell_place2}")
                                time.sleep(0.1)
                                sell_rebalance = create_sell_limit_order(pair=pair, size=(-size_left_to_buy), price=sell_place2, API=API)
                    else:
                        continue

            sells()

            # Buys section
            if bull_market_bool:
                def buys():
                    print("Starting BUYS loop")
                    for key, value in all_coins_history.items():
                        pair = key
                        symbol = pair[:-5]
                        print(f"Working on possible buys for {pair}")
                        pair_history = value
                        pair_precisions_status = all_pairs_precisions_status.loc[pair]
                        trading_enabled = trading_enabled_bool(pair_precisions_status=pair_precisions_status, API=API)
                        if not trading_enabled: continue

                        amount_precision = pair_precisions_status["amount_precision"]
                        price_precision = pair_precisions_status["price_precision"]

                        current_price = float(pair_history["close"].iloc[-1])
                        candle_1_close = float(pair_history["close"].iloc[-2])
                        candle_1_coin_sma = float(pair_history["SMA"].iloc[-2])
                        candle_1_ATRR = float(pair_history["ATRR"].iloc[-2])

                        if symbol in balances.index:
                            coin_total_balance = round_down(float(balances.loc[symbol, "total"]), amount_precision)
                            coin_available_balance = round_down(float(balances.loc[symbol, "available"]), amount_precision)
                        else:
                            coin_total_balance = coin_available_balance = 0

                        if coin_total_balance > 0: print(f"Coin total balance: {coin_total_balance}")
                        if coin_available_balance > 0: print(f"Coin available balance: {coin_available_balance}")

                        coin_weight = candle_1_ATRR / ATRR_elite_sum
                        coin_target_value = round(playing_value * coin_weight, 2)
                        coin_current_value = round(coin_total_balance * current_price, 2)
                        in_play_bool = coin_current_value > coin_target_value * 0.5
                        value_left = coin_target_value - coin_current_value
                        size_left_to_buy = round_down(value_left / current_price, amount_precision)
                        buy_size_one_third = round_down(size_left_to_buy * WIGGLE_ROOM / 3, amount_precision)

                        buy_place1 = round(current_price * 1.0100, price_precision)
                        buy_place2 = round(current_price * 1.0075, price_precision)
                        buy_place3 = round(current_price * 1.0050, price_precision)
                        average_buy_place = buy_place2
                        exchange_min_amount_buy = round_up(
                            max(pair_precisions_status["min_order_amount"], pair_precisions_status["min_order_value"] / average_buy_place,
                                EXCHANGE_MIN_TRANSACTION_VALUE / average_buy_place), amount_precision)

                        if (pair in ranking["pair"].head(NUM_ELITE_COINS).values) and (candle_1_close > candle_1_coin_sma):
                            if (buy_size_one_third > exchange_min_amount_buy) and (not in_play_bool):
                                print(f"Buy 1: {buy_size_one_third}, {buy_place1}")
                                time.sleep(0.1)
                                buy_order1 = create_buy_limit_order(pair=pair, size=buy_size_one_third, price=buy_place1, API=API)
                                print(f"Buy 2: {buy_size_one_third}, {buy_place2}")
                                time.sleep(0.1)
                                buy_order2 = create_buy_limit_order(pair=pair, size=buy_size_one_third, price=buy_place2, API=API)
                                print(f"Buy 3: {buy_size_one_third}, {buy_place3}")
                                time.sleep(0.1)
                                buy_order3 = create_buy_limit_order(pair=pair, size=buy_size_one_third, price=buy_place3, API=API)
                                continue
                            elif buy_size_one_third < exchange_min_amount_buy < size_left_to_buy:
                                print(f"Small buy: {size_left_to_buy}, {buy_place2}")
                                time.sleep(0.1)
                                buy_small = create_buy_limit_order(pair=pair, size=size_left_to_buy, price=buy_place2, API=API)
                                continue

                        # Rebalance buys
                        if REBALANCING:
                            my_min_rebalance_value = coin_target_value * 0.075

                            if (pair in ranking["pair"].head(NUM_ELITE_COINS).values) and in_play_bool:
                                if (size_left_to_buy > exchange_min_amount_buy) and (value_left > my_min_rebalance_value):
                                    print(f"Buy Rebalance: {size_left_to_buy}, {buy_place2}")
                                    time.sleep(0.1)
                                    buy_rebalance = create_buy_limit_order(pair=pair, size=size_left_to_buy, price=buy_place2, API=API)
                        else:
                            continue

                buys()

            portfolio_values = get_basic_portfolio_values(API, all_pairs_precisions_status=all_pairs_precisions_status)
            balances = portfolio_values["balances"]
            whole_account_value = portfolio_values["whole_account_value"]

            # Ranking updating
            def ranking_for_print():
                print("Updating ranking")
                for key, value in all_coins_history.items():
                    pair = key
                    symbol = pair[:-5]
                    pair_history = value
                    pair_precisions_status = all_pairs_precisions_status.loc[pair]
                    trading_enabled = trading_enabled_bool(pair_precisions_status=pair_precisions_status, API=API)
                    if not trading_enabled: continue

                    amount_precision = pair_precisions_status["amount_precision"]
                    price_precision = pair_precisions_status["price_precision"]

                    current_price = float(pair_history["close"].iloc[-1])
                    candle_1_close = float(pair_history["close"].iloc[-2])
                    candle_1_coin_sma = float(pair_history["SMA"].iloc[-2])
                    candle_1_ATRR = float(pair_history["ATRR"].iloc[-2])

                    if symbol in balances.index:
                        coin_total_balance = round_down(float(balances.loc[symbol, "total"]), amount_precision)
                        coin_available_balance = round_down(float(balances.loc[symbol, "available"]), amount_precision)
                    else:
                        coin_total_balance = coin_available_balance = 0

                    coin_weight = candle_1_ATRR / ATRR_elite_sum
                    coin_target_value = round(playing_value * coin_weight, 2)
                    coin_current_value = round(coin_total_balance * current_price, 2)
                    ranking.loc[ranking["pair"] == pair, ["target_value"]] = coin_target_value
                    ranking.loc[ranking["pair"] == pair, ["current_value"]] = coin_current_value

            ranking_for_print()

            time_end = time.time()
            loop_time = time_end - time_start

            print("||| LOOP END |||")
            get_basic_portfolio_values(API, all_pairs_precisions_status=all_pairs_precisions_status)
            print(f"Bull market is {bull_market_bool} ({round(eth_history['SMA'][-2], 2)})")
            print(ranking.head(NUM_ELITE_COINS))
            print(f"Bull market is {bull_market_bool} ({round(eth_history['SMA'][-2], 2)})")
            print(f"Loop took {loop_time:.2f} seconds")
            break

        # Exceptions handling outer loop

        except ConnectionError as Error:
            print(f"(Connection reset zajebane: {Error}")
            time.sleep(2.5)

        except Exception as Error:
            print(f"Outside loop error # {attempt}\n{Error}")
            time.sleep(2.5)
            if attempt >= MAX_ATTEMPTS:
                print(f"Failed {MAX_ATTEMPTS} attempts")
                mail_error_sender(error=Error, API=API)
                print(f"Sleeping for {CONSTANT_ERROR_SLEEP_TIME} seconds")
                time.sleep(CONSTANT_ERROR_SLEEP_TIME)
                attempt = 0


def live_momentum_L(API):
    attempt = 0

    REBALANCE_PERIOD = 8
    momentum_l_function(API)
    schedule.every(REBALANCE_PERIOD).hours.do(partial(momentum_l_function, API=API))

    while True:
        try:
            hours = int(schedule.idle_seconds() // 3600)
            minutes = int(schedule.idle_seconds() / 60 - hours * 60)
            print(f"{hours} hours {minutes} minutes to next run ")
            portfolio_values = get_basic_portfolio_values(API=API)
            whole_account_value = round(portfolio_values["whole_account_value"], 2)

            def jebany_performance():
                try:
                    performance_dataframe = pd.read_excel("Performance.xlsx", converters={0: str, 'Account_value': float}, index_col=0)
                except:
                    print("Unable to read performance excel")
                    performance_dataframe = df()
                pl_time = str(date.today())
                ziobro_dataframe = df(whole_account_value, columns=["Account_value"], index=[pl_time])
                performance_dataframe = performance_dataframe.append(ziobro_dataframe)
                performance_dataframe = performance_dataframe[~performance_dataframe.index.duplicated(keep='last')]
                performance_dataframe.to_excel("Performance.xlsx")

            jebany_performance()
            schedule.run_pending()
            time.sleep(1800)


        # Exceptions handling outer loop

        except ConnectionError as Error:
            print(f"(Connection reset zajebane: {Error}")
            time.sleep(2.5)

        except Exception as Error:
            print(f"Outside loop error # {attempt}\n{Error}")
            time.sleep(2.5)
            if attempt >= MAX_ATTEMPTS:
                print(f"Failed {MAX_ATTEMPTS} attempts")
                mail_error_sender(error=Error, API=API)
                print(f"Sleeping for {CONSTANT_ERROR_SLEEP_TIME} seconds")
                time.sleep(CONSTANT_ERROR_SLEEP_TIME)
                attempt = 0
