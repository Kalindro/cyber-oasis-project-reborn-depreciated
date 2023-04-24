from API.API_exchange_initiator import ExchangeAPISelect
from exchange.get_history import GetFullHistoryDF
from utils.utils import merge_df_dicts

API = ExchangeAPISelect().binance_spot_read_only()
pair = ["BTC/USDT"]
timeframe = "1d"
start = "01.01.2023"
end = "01.02.2023"

history = GetFullHistoryDF(pairs_list=pair, start=start, end=end, timeframe=timeframe, API=API).get_full_history()

print(history)

for data in list(history.data.values()):
    print(data.head(10))
    print(data.tail(10))

