from Gieldy.Refractor_general.Main_refracting import *
from Gieldy.Refractor_general.General_utils import *


def get_basic_portfolio_values(API, all_pairs_precisions_status=None):
    print("Getting portfolio values")

    balances = get_balances(API)
    all_market_pairs_prices = get_pairs_prices(API)
    if all_pairs_precisions_status is None: all_pairs_precisions_status = get_pairs_precisions_status(API)
    USDT_total_balance = balances.loc["USDT", "total"] if "USDT" in balances.index else 0
    USDT_available_balance = balances.loc["USDT", "available"] if "USDT" in balances.index else 0

    USDT_inplay_coins_value = 0
    USDT_with_inplay_coins_value = USDT_total_balance

    for Symbol in balances.index:
        pair = Symbol + "/USDT"
        if pair in all_pairs_precisions_status.index:
            pair_precisions_status = all_pairs_precisions_status.loc[pair]
        else:
            continue

        amount_precision = pair_precisions_status["amount_precision"]
        price_precision = pair_precisions_status["price_precision"]
        trading_enabled = trading_enabled_bool(pair_precisions_status=pair_precisions_status, API=API)
        ticker_price = round(all_market_pairs_prices.loc[pair, "price"], price_precision) if trading_enabled else 0
        coin_total_balance = round(balances.loc[Symbol, "total"], amount_precision)
        USDT_inplay_coins_value = USDT_inplay_coins_value + (float(coin_total_balance) * float(ticker_price))
        USDT_with_inplay_coins_value = USDT_total_balance + USDT_inplay_coins_value
    whole_account_value = USDT_with_inplay_coins_value

    print(f"In play value: ${USDT_inplay_coins_value:,.2f}")
    print(f"USDT amount: ${USDT_total_balance:,.2f}")
    print(f"\033[92mWhole account value: ${whole_account_value:,.2f}\033[0m")

    return {"USDT_inplay_coins_value": USDT_inplay_coins_value,
            "USDT_total_balance": USDT_total_balance,
            "USDT_with_inplay_coins_value": USDT_with_inplay_coins_value,
            "USDT_available_balance": USDT_available_balance,
            "whole_account_value": whole_account_value,
            "balances": balances}
