from Gieldy.Refractor_general.Main_refracting import *
from Gieldy.Refractor_general.General_utils import *
from Gieldy.Refractor_general.Basic_portfolio_values import get_basic_portfolio_values


def portfolio_clean(API, base):
    balances = get_balances(API)
    all_pairs_precisions_status = get_pairs_precisions_status(API)
    all_pairs_prices = get_pairs_prices(API)

    try:
        cancel_all_orders(API)

        for Symbol in balances.index:
            if Symbol != "BTC" and Symbol != "ETH" and Symbol != "USDT" and (balances.loc[Symbol, "total"] > 0):
                if base.upper() == "USDT":
                    pair = Symbol + "/USDT"
                elif base.upper() == "BTC":
                    pair = Symbol + "/BTC"
                elif base.upper() == "ETH":
                    pair = Symbol + "/ETH"
                try:
                    pair_price = all_pairs_prices.loc[pair, "price"]
                except Exception as err:
                    print(f"{err}, Error, skipping {pair}")
                    continue
                pair_precisions_status = all_pairs_precisions_status.loc[pair]
                amount_precision = pair_precisions_status["amount_precision"]
                price_precision = pair_precisions_status["price_precision"]
                exchange_min_amount = round_up(max(pair_precisions_status["min_order_amount"],
                                                   pair_precisions_status["min_order_value"] / pair_price * 1.05), amount_precision)
                size = round_down(float(balances.loc[Symbol, "total"]), amount_precision)
                sell_price = round_down(pair_price * 0.95, price_precision)
                if size > exchange_min_amount:
                    create_sell_limit_order(pair=pair, size=size, price=sell_price, API=API)
                    print(f"Sold {pair}")
                else:
                    print(f"{pair} too small amount")

        get_basic_portfolio_values(all_pairs_precisions_status=all_pairs_precisions_status, API=API)

    except Exception as Error:
        print(f"Pipa na portfolio clean: {Error}")
