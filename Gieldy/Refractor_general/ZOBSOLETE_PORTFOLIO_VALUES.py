from Gieldy.Refractor_general.Main_refracting import *
from Gieldy.Refractor_general.General_utils import *


def get_advanced_portfolio_values(All_market_pairs_precisions_status, API):

    print("Getting portfolio values")

    # Gettings balances and data for tickers

    Balances = get_balances(API)
    All_market_pairs_prices = get_pairs_prices(API)
    All_markets_precisions_status = All_market_pairs_precisions_status

    BTC_price = float(All_market_pairs_prices.loc["BTC/USDT", "Price"])
    ETH_price = float(All_market_pairs_prices.loc["ETH/USDT", "Price"])
    USDT_total_balance = Balances.loc["USDT", "Total"] if "USDT" in Balances.index else 0
    USDT_available_balance = Balances.loc["USDT", "Available"] if "USDT" in Balances.index else 0
    BTC_total_balance = Balances.loc["BTC", "Total"] if "BTC" in Balances.index else 0
    BTC_available_balance = Balances.loc["BTC", "Available"] if "BTC" in Balances.index else 0
    ETH_total_balance = Balances.loc["ETH", "Total"] if "ETH" in Balances.index else 0
    ETH_available_balance = Balances.loc["ETH", "Available"] if "ETH" in Balances.index else 0

    # Declarations

    USDT_inplay_coins, USDT_with_inplay_coins = (0, 0)
    BTC_inplay_coins, BTC_with_inplay_coins = (0, 0)
    ETH_inplay_coins, ETH_with_inplay_coins = (0, 0)
    Number_of_USDT_pairs, Number_of_BTC_pairs, Number_of_ETH_pairs = (0, 0, 0)

    # Starting loop over coins
    for Symbol in Balances.index:
        Pair = Symbol + f"/USDT"
        if Pair in All_markets_precisions_status.index:
            Pair_precisions_status = All_markets_precisions_status.loc[Pair]
        else:
            continue
    for Pair_number, Row in Pair_settings_dataframe.iterrows():
        Pair = Row["Pair"]
        Pair_dataframe = Markets_data.loc[Pair]
        Coin_amount_precision = Pair_dataframe["Amount_precision"]

        # If base is USDT

        Trading_enabled = trading_enabled_bool(Pair_dataframe=Pair_dataframe, API=API)

        Ticker_price = Tickers_dataframe.loc[Pair, "Price"] if Trading_enabled else 0

        if Pair.endswith("/USDT"):
            Number_of_USDT_pairs = Number_of_USDT_pairs + 1
            Symbol = Row["Symbol"]
            if Symbol in Balances.index:
                Coin_total_balance = round_down(Balances.loc[Symbol, "Total"], Coin_amount_precision)
            else:
                Coin_total_balance = 0
            USDT_inplay_coins = USDT_inplay_coins + (float(Coin_total_balance) * float(Ticker_price))
            USDT_with_inplay_coins = USDT_total_balance + USDT_inplay_coins
            continue

        # If base is BTC

        if Pair.endswith("/BTC"):
            Number_of_BTC_pairs = Number_of_BTC_pairs + 1
            Symbol = Row["Symbol"]
            if Symbol in Balances.index:
                Coin_total_balance = round_down(float(Balances.loc[Symbol, "Total"]), Coin_amount_precision)
            else:
                Coin_total_balance = 0
            BTC_inplay_coins = BTC_inplay_coins + (Coin_total_balance * Ticker_price)
            BTC_with_inplay_coins = BTC_total_balance + BTC_inplay_coins
            continue

        # If base is ETH

        if Pair.endswith("/ETH"):
            Number_of_ETH_pairs = Number_of_ETH_pairs + 1
            Symbol = Row["Symbol"]
            if Symbol in Balances.index:
                Coin_total_balance = round_down(float(Balances.loc[Symbol, "Total"]), Coin_amount_precision)
            else:
                Coin_total_balance = 0
            ETH_inplay_coins = ETH_inplay_coins + (Coin_total_balance * Ticker_price)
            ETH_with_inplay_coins = ETH_total_balance + ETH_inplay_coins
            continue

    USDT_inplay_coins_value = USDT_inplay_coins * 1
    USDT_with_inplay_coins_value = USDT_with_inplay_coins * 1
    BTC_inplay_coins_value = BTC_inplay_coins * BTC_price
    BTC_with_inplay_coins_value = BTC_with_inplay_coins * BTC_price
    ETH_inplay_coins_value = ETH_inplay_coins * ETH_price
    ETH_with_inplay_coins_value = ETH_with_inplay_coins * ETH_price
    Whole_portfolio_value = BTC_with_inplay_coins_value + ETH_with_inplay_coins_value + USDT_with_inplay_coins_value

    print(f"Whole portfolio value is: ${Whole_portfolio_value:.2f}")

    return {"USDT_inplay_coins": USDT_inplay_coins,
            "USDT_inplay_coins_value": USDT_inplay_coins_value,
            "USDT_with_inplay_coins": USDT_with_inplay_coins,
            "USDT_with_inplay_coins_value": USDT_with_inplay_coins_value,
            "USDT_total_balance": USDT_total_balance,
            "USDT_available_balance": USDT_available_balance,
            "Number_of_USDT_pairs": Number_of_USDT_pairs,
            "BTC_inplay_coins": BTC_inplay_coins,
            "BTC_inplay_coins_value": BTC_inplay_coins_value,
            "BTC_with_inplay_coins": BTC_with_inplay_coins,
            "BTC_with_inplay_coins_value": BTC_with_inplay_coins_value,
            "BTC_total_balance": BTC_total_balance,
            "BTC_available_balance": BTC_available_balance,
            "Number_of_BTC_pairs": Number_of_BTC_pairs,
            "ETH_inplay_coins": ETH_inplay_coins,
            "ETH_inplay_coins_value": ETH_inplay_coins_value,
            "ETH_with_inplay_coins": ETH_with_inplay_coins,
            "ETH_with_inplay_coins_value": ETH_with_inplay_coins_value,
            "ETH_total_balance": ETH_total_balance,
            "ETH_available_balance": ETH_available_balance,
            "Number_of_ETH_pairs": Number_of_ETH_pairs,
            "Whole_portfolio_value": Whole_portfolio_value,
            "Balances": Balances}
