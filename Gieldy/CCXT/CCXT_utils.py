from Gieldy.CCXT.API_initiation.API_initiation_CCXT_Kucoin_futures_Drift_ARB_first_layer import API_initiation
from pandas import DataFrame as df
from pprint import pprint
import numpy as np

client = API_initiation()["general_client"]

markets = client.load_markets()

for k, v in markets.items():
    print(k)
    print(v)

print(df.from_dict(markets, orient="index"))

def binance_get_futures_pair_prices_rates(API):
    general_client = API["general_client"]

    prices_dataframe = df(general_client.futures_mark_price())
    prices_dataframe = prices_dataframe.apply(pd.to_numeric, errors="ignore")
    prices_dataframe.rename(columns={"symbol": "pair", "markPrice": "mark_price", "lastFundingRate": "funding_rate"}, inplace=True)
    col = prices_dataframe["pair"].str[:-4]
    prices_dataframe.insert(1, "symbol", col)
    prices_dataframe["quote"] = prices_dataframe["pair"].str[-4:]
    prices_dataframe = prices_dataframe[prices_dataframe.quote.str.contains("USDT")]
    prices_dataframe.pop("quote")
    prices_dataframe["funding_rate_APR"] = (prices_dataframe["funding_rate"] * 3 * 365.25).round(2)
    prices_dataframe.set_index("symbol", inplace=True)

    return prices_dataframe
