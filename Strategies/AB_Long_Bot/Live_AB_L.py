from datetime import timedelta, datetime, date
import schedule
from huobi.exception.huobi_api_exception import HuobiApiException
from requests.exceptions import ChunkedEncodingError, ConnectionError

from Gieldy.Refractor_general.Envelopes import history_dataframe_envelope
from Gieldy.Refractor_general.Main_refracting import *
from Gieldy.Refractor_general.ZOBSOLETE_PORTFOLIO_VALUES import get_advanced_portfolio_values
from Gieldy.Refractor_general.General_utils import *
from Gieldy.Refractor_general.Mail_sender import mail_error_sender


def main_AB(API):

    # --------------------------------------------
    # Notepad API Import

    Name = API["Name"]

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)

    # --------------------------------------------
    # Declarations

    Max_attempts = 5
    Loop_count = 0
    Constant_error_sleep_time = 30
    Days_of_history = 31
    Wiggle_room = 0.995
    Timeframe = "1h"

    # --------------------------------------------
    # Options

    Planned_time = 240
    Safe_percent = 0.9
    Hours = 32
    ENV_period, ATRP_period = (5, 88)
    ATRP_fix_global, ATRP_fix_upper, ATRP_fix_lower = (3.7, 1.0, 1.0)
    Grid_step = 0.83
    First_safety_layer_on_value, First_safety_layer_off_value, First_layer_fix_lower, First_layer_fix_upper = (0.225, 0.20, 1.10, 1.00)
    Second_safety_layer_on_value, Second_safety_layer_off_value, Second_layer_fix_lower, Second_layer_fix_upper = (0.30, 0.275, 1.40, 1.00)
    Third_safety_layer_on_value, Third_safety_layer_off_value, Third_layer_fix_lower, Third_layer_fix_upper = (0.375, 0.35, 1.70, 0.90)
    Max_deep_halt_on_value, Max_deep_halt_off_value, Max_deep_fix_upper = (0.45, 0.425, 0.80)

    Normal_SL = True
    Slow_SL = True
    Spajker = False
    One_time_only = False

    # --------------------------------------------
    # Infinite loop

    Attempt = 0
    while True:
        try:
            Attempt = Attempt + 1

            print("Starting new loop")

            # --------------------------------------------
            # Read pairs settings

            Time_start = time.time()

            Pair_settings_dataframe = read_pairs()
            Tickers_dataframe = get_pairs_prices(API)

            # --------------------------------------------
            # Clean old logs

            Log_clean = open("Logi.txt","r+")
            Log_clean.seek(0)
            Log_clean.truncate(0)

            print("Old logs cleaned")

            # --------------------------------------------
            # Load all pairs

            All_market_pairs = get_pairs_precisions_status(API)

            # --------------------------------------------
            # Start loop through all coins

            Number_of_coins = len(Pair_settings_dataframe)
            print(f"Number of coins: {Number_of_coins}")

            for Pair_number, Row in Pair_settings_dataframe.iterrows():
                Attempt = 0
                while True:
                    try:
                        Attempt = Attempt + 1

                        print("||| COIN START |||")

                        print(f"Coin number: {Pair_number + 1}")

                        # --------------------------------------------
                        # Pair settings

                        Symbol = Row["Symbol"]
                        Pair = Row["Pair"]

                        print(f"Symbol: {Symbol}")
                        print(f"Working on {Pair}")

                        # --------------------------------------------
                        # Pair data

                        Pair_dataframe = All_market_pairs.loc[Pair]

                        if len(Pair_dataframe) <= 0:
                            print(f"Broken pair data for {Pair}, skipping")
                            break

                        # --------------------------------------------
                        # Check if coin is tradable

                        Trading_enabled = trading_enabled_bool(API, Pair_dataframe)
                        if not Trading_enabled:
                            print(f"Trading disabled for {Pair}, skipping")
                            break

                        # --------------------------------------------
                        # Portfolio setup

                        Portfolio_values = get_advanced_portfolio_values(API, All_market_pairs)
                        Balances = Portfolio_values["Balances"]

                        USDT_total_balance = Portfolio_values["USDT_total_balance"]
                        USDT_available_balance = Portfolio_values["USDT_available_balance"]
                        USDT_inplay_coins_value = Portfolio_values["USDT_inplay_coins_value"]
                        USDT_with_inplay_coins = Portfolio_values["USDT_with_inplay_coins"]
                        USDT_with_inplay_coins_value = Portfolio_values["USDT_with_inplay_coins_value"]
                        Number_of_USDT_pairs = Portfolio_values["Number_of_USDT_pairs"]

                        BTC_total_balance = Portfolio_values["BTC_total_balance"]
                        BTC_available_balance = Portfolio_values["BTC_available_balance"]
                        BTC_inplay_coins_value = Portfolio_values["BTC_inplay_coins_value"]
                        BTC_with_inplay_coins = Portfolio_values["BTC_with_inplay_coins"]
                        BTC_with_inplay_coins_value = Portfolio_values["BTC_with_inplay_coins_value"]
                        Number_of_BTC_pairs = Portfolio_values["Number_of_BTC_pairs"]

                        ETH_total_balance = Portfolio_values["ETH_total_balance"]
                        ETH_available_balance = Portfolio_values["ETH_available_balance"]
                        ETH_inplay_coins_value = Portfolio_values["ETH_inplay_coins_value"]
                        ETH_with_inplay_coins = Portfolio_values["ETH_with_inplay_coins"]
                        ETH_with_inplay_coins_value = Portfolio_values["ETH_with_inplay_coins_value"]
                        Number_of_ETH_pairs = Portfolio_values["Number_of_ETH_pairs"]

                        # --------------------------------------------
                        # Current pair check

                        if Pair.endswith("/USDT"):
                            Base_coin_total_balance = USDT_total_balance
                            Base_coin_available_balance = USDT_available_balance
                            Number_of_this_base_coins = Number_of_USDT_pairs
                            Base_coin_with_inplay_coins_before_safe = USDT_with_inplay_coins
                            Base_price = 1
                            Base_inplay_coins_value = USDT_inplay_coins_value
                            In_play_total_percent = USDT_inplay_coins_value / USDT_with_inplay_coins_value

                        if Pair.endswith("/BTC"):
                            Base_coin_total_balance = BTC_total_balance
                            Base_coin_available_balance = BTC_available_balance
                            Number_of_this_base_coins = Number_of_BTC_pairs
                            Base_coin_with_inplay_coins_before_safe = BTC_with_inplay_coins
                            Alts_inplay_coins_value = BTC_inplay_coins_value + ETH_inplay_coins_value
                            Alts_with_inplay_coins_value = BTC_with_inplay_coins_value + ETH_with_inplay_coins_value
                            Base_price = float(Tickers_dataframe.loc["BTC/USDT", "Price"])
                            Base_inplay_coins_value = Alts_inplay_coins_value
                            In_play_total_percent = Alts_inplay_coins_value / Alts_with_inplay_coins_value

                        if Pair.endswith("/ETH"):
                            Base_coin_total_balance = ETH_total_balance
                            Base_coin_available_balance = ETH_available_balance
                            Number_of_this_base_coins = Number_of_ETH_pairs
                            Base_coin_with_inplay_coins_before_safe = ETH_with_inplay_coins
                            Alts_inplay_coins_value = BTC_inplay_coins_value + ETH_inplay_coins_value
                            Alts_with_inplay_coins_value = BTC_with_inplay_coins_value + ETH_with_inplay_coins_value
                            Base_price = float(Tickers_dataframe.loc["ETH/USDT", "Price"])
                            Base_inplay_coins_value = Alts_inplay_coins_value
                            In_play_total_percent = Alts_inplay_coins_value / Alts_with_inplay_coins_value

                        # --------------------------------------------
                        # Performance

                        def Jebany_performance():
                            print("Updating performance sheet")
                            Column_list = pd.read_excel("Performance.xlsx").columns
                            Converters = {col: (float if col != "Date" else str) for col in Column_list}
                            Performance_dataframe = pd.read_excel("Performance.xlsx", converters = Converters, index_col = False)
                            Poland_time = date.today()
                            Exact_time_now = datetime.now()
                            print(f"Time now: {Exact_time_now.strftime('%H:%M:%S')}")
                            if len(Performance_dataframe) >= 1 and Performance_dataframe.iloc[-1]["Date"] == str(Poland_time):
                                Performance_dataframe.loc[Performance_dataframe.index[-1], ["Base_amount"]] = float(Base_coin_with_inplay_coins_before_safe)
                                Performance_dataframe["Profit"] = Performance_dataframe["Base_amount"] - Performance_dataframe["Base_amount"].shift(1)
                                Performance_dataframe["Profit_value"] = Performance_dataframe["Profit"] * Base_price
                                Performance_dataframe["ROI"] = Performance_dataframe["Profit"] / Performance_dataframe["Base_amount"].shift(1)
                                Performance_dataframe["In_play"] = In_play_total_percent
                                Performance_dataframe.loc[Performance_dataframe.index[-1], ["Week_profit"]] = Performance_dataframe["Base_amount"][-7:].iloc[-1] - Performance_dataframe["Base_amount"][-7:].iloc[0]
                            else:
                                Performance_dataframe = Performance_dataframe.append(
                                    {"Date": str(Poland_time), "Base_amount": Base_coin_with_inplay_coins_before_safe}, ignore_index = True)
                            Performance_dataframe.to_excel("Performance.xlsx", index = False)

                        if not One_time_only:
                            schedule.every(5).minutes.do(Jebany_performance)
                            One_time_only = True
                        schedule.run_pending()

                        # --------------------------------------------
                        # Print colored inplay percent

                        if In_play_total_percent <= 0.175:
                            print(f"\033[92m{In_play_total_percent:.2%} of account in play\033[0m")
                        if In_play_total_percent > 0.175 and In_play_total_percent < 0.35:
                            print(f"\033[93m{In_play_total_percent:.2%} of account in play\033[0m")
                        if In_play_total_percent >= 0.35:
                            print(f"\033[91m{In_play_total_percent:.2%} of account in play\033[0m")

                        # --------------------------------------------
                        # In play calculation

                        First_safety_layer_enable = In_play_total_percent > First_safety_layer_on_value
                        First_safety_layer_disable = In_play_total_percent < First_safety_layer_off_value
                        Second_safety_layer_enable = In_play_total_percent > Second_safety_layer_on_value
                        Second_safety_layer_disable = In_play_total_percent < Second_safety_layer_off_value
                        Third_safety_layer_enable = In_play_total_percent > Third_safety_layer_on_value
                        Third_safety_layer_disable = In_play_total_percent < Third_safety_layer_off_value
                        Max_deep_halt_enable = In_play_total_percent > Max_deep_halt_on_value
                        Max_deep_halt_disable = In_play_total_percent < Max_deep_halt_off_value

                        # --------------------------------------------
                        # Empty cells fill

                        if str(Pair_settings_dataframe["First_safety_layer_canceled"].iloc[0]) != ("Canceled" or "Not canceled"):
                            Pair_settings_dataframe["First_safety_layer_canceled"].iloc[0] = "Not canceled"

                        if str(Pair_settings_dataframe["Second_safety_layer_canceled"].iloc[0]) != ("Canceled" or "Not canceled"):
                            Pair_settings_dataframe["Second_safety_layer_canceled"].iloc[0] = "Not canceled"

                        if str(Pair_settings_dataframe["Third_safety_layer_canceled"].iloc[0]) != ("Canceled" or "Not canceled"):
                            Pair_settings_dataframe["Third_safety_layer_canceled"].iloc[0] = "Not canceled"

                        if str(Pair_settings_dataframe["Max_deep_halt_canceled"].iloc[0]) != ("Canceled" or "Not canceled"):
                            Pair_settings_dataframe["Max_deep_halt_canceled"].iloc[0] = "Not canceled"

                        # --------------------------------------------
                        # Safety layers

                        First_safety_layer_canceled = Pair_settings_dataframe["First_safety_layer_canceled"].iloc[0]
                        Second_safety_layer_canceled = Pair_settings_dataframe["Second_safety_layer_canceled"].iloc[0]
                        Third_safety_layer_canceled = Pair_settings_dataframe["Third_safety_layer_canceled"].iloc[0]
                        Max_deep_halt_canceled = Pair_settings_dataframe["Max_deep_halt_canceled"].iloc[0]

                        # --------------------------------------------
                        # Canceling if layer is hit

                        if First_safety_layer_enable and (First_safety_layer_canceled == "Not canceled"):
                            print("First safety layer hit, canceling all orders")
                            cancel_all_orders(API)
                            Pair_settings_dataframe["First_safety_layer_canceled"].iloc[0] = "Canceled"
                            print("First safety layer canceled all orders")

                        if Second_safety_layer_enable and (Second_safety_layer_canceled == "Not canceled"):
                            print("Second safety layer hit, canceling all orders")
                            cancel_all_orders(API)
                            Pair_settings_dataframe["Second_safety_layer_canceled"].iloc[0] = "Canceled"
                            print("Second safety layer canceled all orders")

                        if Third_safety_layer_enable and (Third_safety_layer_canceled == "Not canceled"):
                            print("Third safety layer hit, canceling all orders")
                            cancel_all_orders(API)
                            Pair_settings_dataframe["Third_safety_layer_canceled"].iloc[0] = "Canceled"
                            print("Third safety layer canceled all orders")

                        if Max_deep_halt_enable and (Max_deep_halt_canceled == "Not canceled"):
                            print("Max deep halt hit, canceling all orders")
                            cancel_all_orders(API)
                            Pair_settings_dataframe["Max_deep_halt_canceled"].iloc[0] = "Canceled"
                            print("Max deep halt canceled all orders")

                        # --------------------------------------------
                        # If layer turned off

                        if Max_deep_halt_disable:
                            Pair_settings_dataframe["Max_deep_halt_canceled"].iloc[0] = "Not canceled"

                        if Third_safety_layer_disable:
                            Pair_settings_dataframe["Third_safety_layer_canceled"].iloc[0] = "Not canceled"

                        if Second_safety_layer_disable:
                            Pair_settings_dataframe["Second_safety_layer_canceled"].iloc[0] = "Not canceled"

                        if First_safety_layer_disable:
                            Pair_settings_dataframe["First_safety_layer_canceled"].iloc[0] = "Not canceled"

                        print("First safety cancel status:", Pair_settings_dataframe["First_safety_layer_canceled"].iloc[0])
                        print("Second safety cancel status:", Pair_settings_dataframe["Second_safety_layer_canceled"].iloc[0])
                        print("Third safety cancel status:", Pair_settings_dataframe["Third_safety_layer_canceled"].iloc[0])
                        print("Max deep cancel status:", Pair_settings_dataframe["Max_deep_halt_canceled"].iloc[0])

                        # --------------------------------------------
                        # Max deep info

                        if not Max_deep_halt_enable:
                            print(f"{(Max_deep_halt_on_value - In_play_total_percent):.2%} max left to put in")
                        else:
                            print(f"{(In_play_total_percent - Max_deep_halt_off_value):.2%} too much, not buying more")

                        # --------------------------------------------
                        # Safety ATRP

                        if Pair_settings_dataframe["First_safety_layer_canceled"].iloc[0] == "Canceled" and Pair_settings_dataframe["Second_safety_layer_canceled"].iloc[0] != "Canceled":
                            ATRP_fix_lower = First_layer_fix_lower
                            ATRP_fix_upper = First_layer_fix_upper
                            print(f"First safety layer hit, buy ATRP is multiplied by {ATRP_fix_lower}, sell ATRP by {ATRP_fix_upper}")

                        if Pair_settings_dataframe["Second_safety_layer_canceled"].iloc[0] == "Canceled" and Pair_settings_dataframe["Third_safety_layer_canceled"].iloc[0] != "Canceled":
                            ATRP_fix_lower = Second_layer_fix_lower
                            ATRP_fix_upper = Second_layer_fix_upper
                            print(f"Second safety layer hit, buy ATRP is multiplied by {ATRP_fix_lower}, sell ATRP by {ATRP_fix_upper}")

                        if Pair_settings_dataframe["Third_safety_layer_canceled"].iloc[0] == "Canceled" and Pair_settings_dataframe["Max_deep_halt_canceled"].iloc[0] != "Canceled":
                            ATRP_fix_lower = Third_layer_fix_lower
                            ATRP_fix_upper = Third_layer_fix_upper
                            print(f"Third safety layer hit, buy ATRP is multiplied by {ATRP_fix_lower}, sell ATRP by {ATRP_fix_upper}")

                        if Pair_settings_dataframe["Max_deep_halt_canceled"].iloc[0] == "Canceled":
                            ATRP_fix_upper = Max_deep_fix_upper
                            print(f"Max deep safety layer hit, not buying more, sell ATRP is multiplied by {ATRP_fix_upper}")

                        # --------------------------------------------
                        # Get history with envelopes

                        History_dataframe = history_dataframe_envelope(API, Pair, Timeframe, Days_of_history, ENV_period, ATRP_period, ATRP_fix_global)

                        if len(History_dataframe) <= Days_of_history:
                            print(f"History dataframe is fucked for {Pair}, skipping")
                            break

                        ATRP = History_dataframe["ATRP"].iloc[-2]

                        if not ATRP or ATRP <= 0 or pd.isna(ATRP):
                            print(f"ATRP is fucked for {Pair}, skipping")
                            break

                        Perc = ATRP / 2
                        print(f"ATRP is: {ATRP:.2%}")
                        print(f"Perc is: {Perc:.2%}")
                        # --------------------------------------------
                        # Declarations in loop

                        Buy_size1 = Buy_size2 = Buy_size3 = Buy_size4 = Buy_size5 = Entry_time = 0
                        Termination_normal = Termination_slow = SL_execute = False

                        # --------------------------------------------
                        # Candles

                        Candle_0_close = float(History_dataframe["Close"].iloc[-1])
                        Current_price = Candle_0_close
                        Candle_1_open = float(History_dataframe["Open"].iloc[-2])
                        Candle_1_close = float(History_dataframe["Close"].iloc[-2])
                        Candle_1_low = float(History_dataframe["Low"].iloc[-2])

                        # --------------------------------------------
                        # Get pair precision

                        Amount_precision = Pair_dataframe["Amount_precision"]
                        Price_precision = Pair_dataframe["Price_precision"]

                        # --------------------------------------------
                        # Sells placements

                        Sell_place5 = round_down(History_dataframe["Envelope_upper"].iloc[-2] * ATRP_fix_upper * 1.0050, Price_precision)
                        Sell_place4 = round_down(History_dataframe["Envelope_upper"].iloc[-2] * ATRP_fix_upper * 1.0025, Price_precision)
                        Sell_place3 = round_down(History_dataframe["Envelope_upper"].iloc[-2] * ATRP_fix_upper * 1.0000, Price_precision)
                        Sell_place2 = round_down(History_dataframe["Envelope_upper"].iloc[-2] * ATRP_fix_upper * 0.9975, Price_precision)
                        Sell_place1 = round_down(History_dataframe["Envelope_upper"].iloc[-2] * ATRP_fix_upper * 0.9950, Price_precision)
                        Average_sell_place = Sell_place3

                        # --------------------------------------------
                        # Buy placements

                        Buy_place5 = round_up(History_dataframe["Envelope_lower"].iloc[-2] * ATRP_fix_lower * (1 - (Perc * Grid_step * 4)), Price_precision)
                        Buy_place4 = round_up(History_dataframe["Envelope_lower"].iloc[-2] * ATRP_fix_lower * (1 - (Perc * Grid_step * 3)), Price_precision)
                        Buy_place3 = round_up(History_dataframe["Envelope_lower"].iloc[-2] * ATRP_fix_lower * (1 - (Perc * Grid_step * 2)), Price_precision)
                        Buy_place2 = round_up(History_dataframe["Envelope_lower"].iloc[-2] * ATRP_fix_lower * (1 - (Perc * Grid_step * 1)), Price_precision)
                        Buy_place1 = round_up(History_dataframe["Envelope_lower"].iloc[-2] * ATRP_fix_lower * (1 - (Perc * Grid_step * 0)), Price_precision)

                        Average_buy_place = Buy_place3

                        Envelope_middle = float(round_up(History_dataframe["Envelope_middle"].iloc[-2], Price_precision))

                        SMA1 = round_down(Envelope_middle * 1.001, Price_precision)
                        SMA2 = round_up(Envelope_middle * 0.999, Price_precision)

                        # --------------------------------------------
                        # Min amount calculation

                        My_min_value = 5 / Base_price
                        Exchange_min_amount_buy = round_up(
                            max(Pair_dataframe["Min_order_amount"], Pair_dataframe["Min_order_value"] / Average_buy_place * 1.05,
                                My_min_value / Average_buy_place * 1.05), Amount_precision)
                        Exchange_min_amount_sell = round_up(
                            max(Pair_dataframe["Min_order_amount"], Pair_dataframe["Min_order_value"] / Average_sell_place * 1.05,
                                My_min_value / Average_sell_place * 1.05), Amount_precision)

                        print(f"Amount precision: {Amount_precision}")
                        print(f"Price precision: {Price_precision}")
                        print(f"Exchange min buy amount: {Exchange_min_amount_buy}")
                        print(f"Exchange min sell amount: {Exchange_min_amount_sell}")

                        # --------------------------------------------
                        # Filled order 1 (average) price and entry

                        if Row["Buy_order1_ID"] is not None:
                            try:
                                Order_ID = Row["Buy_order1_ID"]
                                Order1 = fetch_order(API, Order_ID, Pair)
                                Buy_order1_price = round_up(Order1["Price"], Price_precision)
                                Entry_time_timestamp = Order1["Timestamp"]
                                Average_bought_price = Buy_order1_price
                                print(f"Average bought price is {Average_bought_price}")

                            except Exception as EEE:
                                print(f"{EEE}, No history for order #1 (average order)")
                                Average_bought_price = Entry_time_timestamp = 0
                        else:
                            print("No history for order #1 (average order)")
                            Average_bought_price = Entry_time_timestamp = 0

                        # --------------------------------------------
                        # Target value and safe amount

                        Target_value_before_safe = Base_coin_with_inplay_coins_before_safe / Number_of_this_base_coins
                        Safe_amount = Base_coin_with_inplay_coins_before_safe * Safe_percent
                        Base_coin_with_inplay_coins = Base_coin_with_inplay_coins_before_safe - Safe_amount
                        Target_value = Base_coin_with_inplay_coins / Number_of_this_base_coins

                        # --------------------------------------------
                        # Get pair balances

                        if Symbol in Balances.index:
                            Coin_total_balance = round_down(float(Balances.loc[Symbol, "Total"]), Amount_precision)
                            Coin_available_balance = round_down(float(Balances.loc[Symbol, "Available"]), Amount_precision)
                        else:
                            Coin_total_balance = Coin_available_balance = 0

                        # --------------------------------------------
                        # Order sizing

                        Coin_current_value = round_down(Coin_total_balance * Current_price, 16)
                        Target_size = round_down(Target_value / Average_buy_place, Amount_precision)
                        Size_left = round_down((Target_value - Coin_current_value) / Average_buy_place, Amount_precision)
                        Coin_buy_target = round_down(Target_size / 5, Amount_precision)

                        # --------------------------------------------
                        # Minimal amount required check

                        My_min_inplay = Target_size * 0.15

                        # --------------------------------------------
                        # Buy orders size

                        Coin_buy_size1 = Coin_buy_size2 = Coin_buy_size3 = Coin_buy_size4 = Coin_buy_size5 = round_down(Coin_buy_target * Wiggle_room, Amount_precision)

                        # --------------------------------------------
                        # Simulate partial fills

                        if Size_left >= 0 * Coin_buy_target and Size_left < 1 * Coin_buy_target:
                            Coin_buy_size5 = round_down(Size_left * Wiggle_room, Amount_precision)
                        if Size_left >= 1 * Coin_buy_target and Size_left < 2 * Coin_buy_target:
                            Coin_buy_size4 = round_down((Size_left - 1 * Coin_buy_target) * Wiggle_room, Amount_precision)
                        if Size_left >= 2 * Coin_buy_target and Size_left < 3 * Coin_buy_target:
                            Coin_buy_size3 = round_down((Size_left - 2 * Coin_buy_target) * Wiggle_room, Amount_precision)
                        if Size_left >= 3 * Coin_buy_target and Size_left < 4 * Coin_buy_target:
                            Coin_buy_size2 = round_down((Size_left - 3 * Coin_buy_target) * Wiggle_room, Amount_precision)
                        if Size_left >= 4 * Coin_buy_target and Size_left < 5 * Coin_buy_target:
                            Coin_buy_size1 = round_down((Size_left - 4 * Coin_buy_target) * Wiggle_room, Amount_precision)

                        # --------------------------------------------
                        # Printing amounts for debugging

                        print(f"Number of this base coins: {Number_of_this_base_coins}")
                        print(f"Base coin total balance: {Base_coin_total_balance}")
                        print(f"Base inplay coins value: {Base_inplay_coins_value}")
                        print(f"Base coin with inplay before safe: {Base_coin_with_inplay_coins_before_safe}")
                        print(f"Safe amount is: {Safe_amount}")
                        print(f"Base coin with inplay: {Base_coin_with_inplay_coins}")
                        print(f"Base coin available balance: {Base_coin_available_balance}")
                        print(f"Coin total balance: {Coin_total_balance}")
                        print(f"Coin available balance: {Coin_available_balance}")
                        print(f"Coin target size: {Target_size}")
                        print(f"Coin target value (every coin): {Target_value}")
                        print(f"Coin size left: {Size_left}")
                        print(f"Coin current value: {Coin_current_value}")
                        print(f"Average buy place is: {Average_buy_place}")
                        print(f"Average sell place is: {Average_sell_place}")

                        # --------------------------------------------
                        # Orders cancel

                        Canceled_pair_orders_amount = cancel_pair_orders(API, Pair)

                        print(f"Canceled all pair orders: {Canceled_pair_orders_amount}")

                        # --------------------------------------------
                        # Buys

                        print("Buys:")

                        if (Size_left >= My_min_inplay) and Max_deep_halt_disable and Average_buy_place > 0:
                            print("Creating buys...")
                            if (Coin_buy_size1 >= Exchange_min_amount_buy) and (Size_left >= 4 * Coin_buy_target):
                                print(f"Buy 1: {Coin_buy_size1}, {Buy_place1}")
                                time.sleep(0.1)
                                Buy_order1 = create_buy_limit_order(API, Pair, size= Coin_buy_size1, price= Buy_place1)
                                Buy_order1_ID = Buy_order1["ID"]
                                Buy_size1 = Coin_buy_size1
                                Pair_settings_dataframe.loc[Pair_number, "Buy_order1_ID"] = Buy_order1_ID
                            else:
                                Buy_size1 = 0
                            if (Coin_buy_size2 >= Exchange_min_amount_buy) and (Size_left >= 3 * Coin_buy_target):
                                print(f"Buy 2: {Coin_buy_size2}, {Buy_place2}")
                                time.sleep(0.1)
                                Buy_order2 = create_buy_limit_order(API, Pair, size= Coin_buy_size2, price= Buy_place2)
                                Buy_order2_ID = Buy_order2["ID"]
                                Buy_size2 = Coin_buy_size2
                                Pair_settings_dataframe.loc[Pair_number, "Buy_order2_ID"] = Buy_order2_ID
                            else:
                                Buy_size2 = 0
                            if (Coin_buy_size3 >= Exchange_min_amount_buy) and (Size_left >= 2 * Coin_buy_target):
                                print(f"Buy 3: {Coin_buy_size3}, {Buy_place3}")
                                time.sleep(0.1)
                                Buy_order3 = create_buy_limit_order(API, Pair, size= Coin_buy_size3, price= Buy_place3)
                                Buy_order3_ID = Buy_order3["ID"]
                                Buy_size3 = Coin_buy_size3
                                Pair_settings_dataframe.loc[Pair_number, "Buy_order3_ID"] = Buy_order3_ID
                            else:
                                Buy_size3 = 0
                            if (Coin_buy_size4 >= Exchange_min_amount_buy) and (Size_left >= 1 * Coin_buy_target):
                                print(f"Buy 4: {Coin_buy_size4}, {Buy_place4}")
                                time.sleep(0.1)
                                Buy_order4 = create_buy_limit_order(API, Pair, size= Coin_buy_size4, price= Buy_place4)
                                Buy_order4_ID = Buy_order4["ID"]
                                Buy_size4 = Coin_buy_size4
                                Pair_settings_dataframe.loc[Pair_number, "Buy_order4_ID"] = Buy_order4_ID
                            else:
                                Buy_size4 = 0
                            if (Coin_buy_size5 >= Exchange_min_amount_buy) and (Size_left >= 0 * Coin_buy_target):
                                print(f"Buy 5: {Coin_buy_size5}, {Buy_place5}")
                                Buy_order5 = create_buy_limit_order(API, Pair, size= Coin_buy_size5, price= Buy_place5)
                                time.sleep(0.1)
                                Buy_order5_ID = Buy_order5["ID"]
                                Buy_size5 = Coin_buy_size5
                                Pair_settings_dataframe.loc[Pair_number, "Buy_order5_ID"] = Buy_order5_ID
                            else:
                                Buy_size5 = 0
                        print("Buys created")

                        # --------------------------------------------
                        # Stop losses logic

                        if Entry_time_timestamp > 0:
                            Pair_settings_dataframe.at[Pair_number, "Entry_time"] = datetime.fromtimestamp(Entry_time_timestamp / 1000)
                            Entry_time = Row["Entry_time"]
                            Pair_settings_dataframe.at[Pair_number, "Termination_time_normal"] = Entry_time + timedelta(hours = Hours)
                            Termination_time_normal = Row["Termination_time_normal"]
                            Pair_settings_dataframe.at[Pair_number, "Termination_time_slow"] = Entry_time + timedelta(hours = Hours * 2)
                            Termination_time_slow = Row["Termination_time_slow"]
                            Time_now = History_dataframe.index[-1] + timedelta(minutes = 30)
                            print(f"Entry time is {Entry_time}")
                            print(f"Time now is {Time_now}")
                            Termination_normal = Time_now >= Termination_time_normal
                            print(f"Termination normal time is {Termination_normal}, {Termination_time_normal}")
                            Termination_slow = Time_now >= Termination_time_slow
                            print(f"Termination slow time is {Termination_slow}, {Termination_time_slow}")

                        # --------------------------------------------
                        # Balance sizes

                        Whole_coin_sell_size = round_down(Coin_total_balance * Wiggle_room, Amount_precision)
                        Half_size = round_down(Whole_coin_sell_size / 2, Amount_precision)
                        Buy_orders_value = Buy_size1 * Buy_place1 + Buy_size2 * Buy_place2 + Buy_size3 * Buy_place3 + Buy_size4 * Buy_place4 + Buy_size5 * Buy_place5

                        print(f"Buy orders value: {Buy_orders_value}")
                        print(f"Total planned value: {Coin_current_value + Buy_orders_value}")

                        # --------------------------------------------
                        # 4 SL's

                        print("Sells:")

                        def SL_sell():
                            if Half_size < Exchange_min_amount_sell:
                                print(f"SL size: {Whole_coin_sell_size}")
                                SL = create_sell_limit_order(API, Pair, size= Whole_coin_sell_size, price= SMA1)
                            else:
                                print(f"Half size: {Half_size}")
                                SL1 = create_sell_limit_order(API, Pair, size= Half_size, price= SMA1)
                                SL2 = create_sell_limit_order(API, Pair, size= Half_size, price= SMA2)

                        if Spajker:
                            Spajk_condition = ((min(Candle_1_close, Candle_1_open) / Candle_1_low) - 1) > (ATRP / 2)
                            if Spajk_condition and not SL_execute and (Whole_coin_sell_size >= Exchange_min_amount_sell):
                                print("Creating spajk sell")
                                SL_sell()
                                SL_execute = True
                                print("Spajk SL savior")

                        if Normal_SL:
                            Normal_termination_conditions = (Termination_normal == True and Current_price < Average_bought_price)
                            if Normal_termination_conditions and (Whole_coin_sell_size >= Exchange_min_amount_sell) and not SL_execute:
                                print("Creating normal termination SL")
                                SL_sell()
                                SL_execute = True
                                print("Normal termination SL savior")

                        if Slow_SL:
                            Slow_termination_conditions = Termination_slow == True
                            if Slow_termination_conditions and (Whole_coin_sell_size >= Exchange_min_amount_sell) and not SL_execute:
                                print("Creating slow termination SL")
                                SL_sell()
                                SL_execute = True
                                print("Slow termination SL savior")

                        # --------------------------------------------
                        # Sell orders size

                        Coin_sell_size1 = Coin_sell_size2 = Coin_sell_size3 = Coin_sell_size4 = Coin_sell_size5 = round_down(Whole_coin_sell_size / 5, Amount_precision)

                        # --------------------------------------------
                        # Logic when close to minimal amount size

                        if (Whole_coin_sell_size >= 1 * Exchange_min_amount_sell) and (Whole_coin_sell_size < 2 * Exchange_min_amount_sell):
                            Coin_sell_size5 = round_down(Whole_coin_sell_size, Amount_precision)
                            Coin_sell_size4 = 0
                            Coin_sell_size3 = 0
                            Coin_sell_size2 = 0
                            Coin_sell_size1 = 0
                        if (Whole_coin_sell_size >= 2 * Exchange_min_amount_sell) and (Whole_coin_sell_size < 3 * Exchange_min_amount_sell):
                            Coin_sell_size5 = Exchange_min_amount_sell
                            Coin_sell_size4 = round_down(Whole_coin_sell_size - Exchange_min_amount_sell, Amount_precision)
                            Coin_sell_size3 = 0
                            Coin_sell_size2 = 0
                            Coin_sell_size1 = 0
                        if (Whole_coin_sell_size >= 3 * Exchange_min_amount_sell) and (Whole_coin_sell_size < 4 * Exchange_min_amount_sell):
                            Coin_sell_size5 = Exchange_min_amount_sell
                            Coin_sell_size4 = Exchange_min_amount_sell
                            Coin_sell_size3 = round_down(Whole_coin_sell_size - 2 * Exchange_min_amount_sell, Amount_precision)
                            Coin_sell_size2 = 0
                            Coin_sell_size1 = 0
                        if (Whole_coin_sell_size >= 4 * Exchange_min_amount_sell) and (Whole_coin_sell_size < 5 * Exchange_min_amount_sell):
                            Coin_sell_size5 = Exchange_min_amount_sell
                            Coin_sell_size4 = Exchange_min_amount_sell
                            Coin_sell_size3 = Exchange_min_amount_sell
                            Coin_sell_size2 = round_down(Whole_coin_sell_size - 3 * Exchange_min_amount_sell, Amount_precision)
                            Coin_sell_size1 = 0

                        # --------------------------------------------
                        # Regular sells execution

                        if not SL_execute:
                            print("Creating sells...")

                            if Coin_sell_size1 >= Exchange_min_amount_sell:
                                print(f"Sell 1: {Coin_sell_size1}, {Sell_place1}")
                                time.sleep(0.1)
                                Sell_order1 = create_sell_limit_order(API, Pair, size= Coin_sell_size1, price= Sell_place1)
                            if Coin_sell_size2 >= Exchange_min_amount_sell:
                                print(f"Sell 2: {Coin_sell_size2}, {Sell_place2}")
                                time.sleep(0.1)
                                Sell_order2 = create_sell_limit_order(API, Pair, size= Coin_sell_size2, price= Sell_place2)
                            if Coin_sell_size3 >= Exchange_min_amount_sell:
                                print(f"Sell 3: {Coin_sell_size3}, {Sell_place3}")
                                time.sleep(0.1)
                                Sell_order3 = create_sell_limit_order(API, Pair, size= Coin_sell_size3, price= Sell_place3)
                            if Coin_sell_size4 >= Exchange_min_amount_sell:
                                print(f"Sell 4: {Coin_sell_size4}, {Sell_place4}")
                                time.sleep(0.1)
                                Sell_order4 = create_sell_limit_order(API, Pair, size= Coin_sell_size4, price= Sell_place4)
                            if Coin_sell_size5 >= Exchange_min_amount_sell:
                                print(f"Sell 5: {Coin_sell_size5}, {Sell_place5}")
                                time.sleep(0.1)
                                Sell_order5 = create_sell_limit_order(API, Pair, size= Coin_sell_size5, price= Sell_place5)

                        print("Sells created")

                        # --------------------------------------------
                        # Prepare next pair

                        print(Pair, "done")

                        print("||| COIN END |||")

                        Coins_left = Number_of_coins - (Pair_number + 1)
                        print(f"{Coins_left} coins left")
                    # --------------------------------------------
                    # Exceptions handling inner loop

                    except ChunkedEncodingError as Error:
                        print(f"(Connection reset zajebane: {Error}")
                        time.sleep(2.5)

                    except ConnectionError as Error:
                        print(f"(Connection reset zajebane: {Error}")
                        time.sleep(2.5)

                    except HuobiApiException as Error:
                        print(f"(Huobi error: {Error}")
                        time.sleep(2.5)
                        break

                    except Exception as Error:
                        print(f"Inside loop error # {Attempt}\n{Error}")
                        time.sleep(5)
                        if Attempt >= Max_attempts:
                            print(f"Failed {Max_attempts} attempts")
                            mail_error_sender(error= Error, API = API)
                            print(f"Sleeping for {Constant_error_sleep_time} seconds")
                            time.sleep(Constant_error_sleep_time)
                            Attempt = 0
                            break

                    else:
                        break

            # --------------------------------------------
            # Loop count & sleep

            Pair_settings_dataframe.to_excel("Pairs.xlsx", index = False)
            Loop_count = Loop_count + 1
            print(f"Loop count: {Loop_count}")
            Time_end = time.time()
            Loop_time = Time_end - Time_start
            Time_left = Planned_time - Loop_time

            print(f"Loop took {Loop_time:.2f} seconds")
            if Time_left > 0:
                print(f"Sleeping for {Time_left:.2f} seconds")
                time.sleep(Time_left)
            else:
                print(f"This is {(Loop_time - Planned_time):.2f} seconds too long")

            # --------------------------------------------
            # Reset attempts to zero

            Attempt = 0

        # --------------------------------------------
        # Exceptions handling outer loop

        except ChunkedEncodingError as Error:
            print(f"(Connection reset zajebane: {Error}")
            time.sleep(2.5)

        except ConnectionError as Error:
            print(f"(Connection reset zajebane: {Error}")
            time.sleep(2.5)

        except HuobiApiException as Error:
            print(f"(Huobi error: {Error}")
            time.sleep(2.5)

        except Exception as Error:
            print(f"Outside loop error # {Attempt}\n{Error}")
            time.sleep(2.5)
            if Attempt >= Max_attempts:
                print(f"Failed {Max_attempts} attempts")
                mail_error_sender(error= Error, API = API)
                print(f"Sleeping for {Constant_error_sleep_time} seconds")
                time.sleep(Constant_error_sleep_time)
                Attempt = 0
