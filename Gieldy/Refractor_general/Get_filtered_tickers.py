import functools
import multiprocessing
from datetime import date

from Gieldy.Refractor_general.Main_refracting import *
from Gieldy.Refractor_general.Get_history import get_history_full
from Gieldy.Refractor_general.General_utils import get_project_root


# Get /BTC, /ETH, /USDT tickers

def ticker_filter(ticker, all_tickers_dataframe, all_pairs_precisions_status, forbid_exchange_tokens, API):
    approved_list = dict()
    MIN_VOLUME = 30_000
    MIN_PRICE_USDT = 0.000_000_4
    MIN_PRICE_BTC = 0.000_000_01
    MIN_PRICE_ETH = 0.000_000_4
    BLOCKY_PERCENT = 0.005
    BTC_price = all_tickers_dataframe.loc["BTC/USDT", "price"]
    ETH_price = all_tickers_dataframe.loc["ETH/USDT", "price"]

    # Removing unwanted symbols

    symbol = ticker[:-5] if ticker.endswith("/USDT") else ticker[:-4]

    forbidden_symbols_equals = ["USDT", "DAI", "BRL", "IDRT", "BVND", "BIDR", "NGN", "UAH", "AUD", "EUR", "USD", "GBP", "JPY", "TRY", "NZD",
                                "RUB", "R", "FIL6"]
    if forbid_exchange_tokens: forbidden_symbols_equals.extend(["BTC", "ETH", "BNB", "HT", "KCS", "GT"])
    forbidden_symbols_containing = ["USD", "PAX"]
    forbidden_symbols_ending = ["UP", "DOWN", "BEAR", "BULL", "CROSS", "HEDGE", "HALF", "1L", "1S", "2L", "2S", "3L", "3S", "4L", "4S",
                                "5L", "5S", "6L", "6S"]
    a = symbol in forbidden_symbols_equals
    b = any(forbidden in symbol for forbidden in forbidden_symbols_containing)
    c = symbol.endswith(tuple(forbidden_symbols_ending))

    if a or b or c:
        print(f"Removing {ticker}, leveraged, unwanted, weird or stable coin symbol")
        approved_list["unwanted"] = ticker
        return approved_list

    try:
        ticker_history = get_history_full(pair=ticker, timeframe="1D", start=date.fromisoformat("2018-01-01"), end=date.today(),
                                          fresh_live_history_no_save_read=True, API=API)
    except Exception as err:
        print(f"Error on history for {ticker}, {err}")
        approved_list["error"] = ticker
        return approved_list

    # volume and min price check

    try:
        price = all_tickers_dataframe.loc[ticker, "price"]
        price_precision = all_pairs_precisions_status.loc[ticker, "price_precision"]
        price_tick = 1 / 10 ** price_precision if price_precision > 0 else 0
        if price <= 0:
            x = 5 / 0  # Exception force
    except Exception as err:
        print(f"Error on price {ticker}, {err}")
        approved_list["error"] = ticker
        return approved_list

    # History length check

    if len(ticker_history) > 60:
        volume_history = ticker_history.tail(14)
        days_amount = len(volume_history)
        volume_nominal = volume_history["volume"].sum()
    else:
        print(f"Removing {ticker} from tickers, too short history")
        approved_list["short"] = ticker
        return approved_list

    volume = volume_nominal / days_amount

    if ((ticker_history["high"][30:].pct_change() * 100) > 500).any():
        approved_list["pumper"] = ticker
        return approved_list

    if (((price + price_tick) / price) - 1) > BLOCKY_PERCENT:
        approved_list["blocky"] = ticker
        return approved_list

    if ticker.endswith("/BTC") and (((volume * BTC_price) < MIN_VOLUME) or (price < MIN_PRICE_BTC)):
        approved_list["low_vol"] = ticker
        return approved_list

    if ticker.endswith("/ETH") and (((volume * ETH_price) < MIN_VOLUME) or (price < MIN_PRICE_ETH)):
        approved_list["low_vol"] = ticker
        return approved_list

    if ticker.endswith("/USDT") and ((volume < MIN_VOLUME) or (price < MIN_PRICE_USDT)):
        approved_list["low_vol"] = ticker
        return approved_list

    approved_list["approved"] = ticker

    return approved_list


def get_filtered_tickers(base, fresh_live_tickers, forbid_exchange_tokens, API,):
    time_start = time.time()
    name = API["name"]

    if "huobi" in name.lower():
        exchange = "Huobi"

    if "kucoin" in name.lower():
        exchange = "Kucoin"

    if "binance" in name.lower():
        exchange = "Binance"

    if "gate" in name.lower():
        exchange = "Gateio"

    root = get_project_root()

    if not fresh_live_tickers:
        try:
            final_tickers_list = pd.read_csv(f"{root}/History_data/{exchange}/Backtesting_tickers.csv")["tickers"].tolist()
            return final_tickers_list

        except:
            print("No saved tickers list")
            pass

    print("Getting fresh tickers list")

    removed_tickers_unwanted_symbol = []
    removed_tickers_short = []
    removed_tickers_vol = []
    removed_tickers_pumper = []
    removed_tickers_blocky = []
    error_tickers = []
    final_tickers_list = []

    all_pairs_precisions_status = get_pairs_precisions_status(API)
    all_pairs_prices = get_pairs_prices(API)
    raw_tickers_list = all_pairs_prices.index.tolist()

    print(f"Base: {base}")
    print("Getting tickers...")

    if base.upper() == "USDT":
        tickers_list = [ticker for ticker in raw_tickers_list if ticker.endswith("/USDT")]

    if base.upper() == "BTC":
        tickers_list = [ticker for ticker in raw_tickers_list if ticker.endswith("/BTC")]

    if base.upper() == "ETH":
        tickers_list = [ticker for ticker in raw_tickers_list if ticker.endswith("/ETH")]

    if base.upper() == "ALL":
        tickers_list = [ticker for ticker in raw_tickers_list if
                        (ticker.endswith("/BTC") or ticker.endswith("/ETH") or ticker.endswith("/USDT"))]

    if base.upper() == "ALT":
        tickers_list = [ticker for ticker in raw_tickers_list if (ticker.endswith("/BTC") or ticker.endswith("/ETH"))]

    partial_filter = functools.partial(ticker_filter, all_tickers_dataframe=all_pairs_prices,
                                       all_pairs_precisions_status=all_pairs_precisions_status,
                                       forbid_exchange_tokens=forbid_exchange_tokens, API=API)

    shark_pool = multiprocessing.Pool(processes=6)
    approved_list = shark_pool.imap(partial_filter, tickers_list)
    shark_pool.close()
    shark_pool.join()

    for result in approved_list:
        if result.get("unwanted") is not None: removed_tickers_unwanted_symbol.append(result.get("unwanted"))
        if result.get("short") is not None: removed_tickers_short.append(result.get("short"))
        if result.get("low_vol") is not None: removed_tickers_vol.append(result.get("low_vol"))
        if result.get("pumper") is not None: removed_tickers_pumper.append(result.get("pumper"))
        if result.get("blocky") is not None: removed_tickers_blocky.append(result.get("blocky"))
        if result.get("error") is not None: error_tickers.append(result.get("error"))
        if result.get("approved") is not None: final_tickers_list.append(result.get("approved"))

    time_end = time.time()
    loop_time = time_end - time_start

    print(f"Tickers acquisition took {loop_time:.2f} seconds")
    print(f"Removed tickers in tickers acquisition because of unwanted symbol: {removed_tickers_unwanted_symbol}")
    print(f"Removed tickers in tickers acquisition because of too short history: {removed_tickers_short}")
    print(f"Removed tickers in tickers acquisition because of not enough volume or too small price: {removed_tickers_vol}")
    print(f"Removed tickers in tickers acquisition because of blocky chart: {removed_tickers_blocky}")
    print(f"Removed tickers in tickers acquisition because of pump and dump: {removed_tickers_pumper}")
    print(f"Removed tickers in tickers acquisition because of error: {error_tickers}")
    print(f"Passed tickers: {final_tickers_list}")

    if not fresh_live_tickers:
        final_tickers_list_dataframe = df(data={"tickers": final_tickers_list})
        final_tickers_list_dataframe.to_csv(f"{root}/History_data/{exchange}/Backtesting_tickers.csv", index=False)

        print("Saved tickers CSV")

    return final_tickers_list
