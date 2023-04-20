from API.API_exchange_initiator import ExchangeAPISelect
from exchange.get_history import GetFullHistoryDF

API = ExchangeAPISelect().binance_spot_read_only()
pair = ["JOE/BTC"]
timeframe = "1d"
start = "01.01.2023"
end = "01.02.2023"

new_start = "01.12.2022"
new_end = "01.03.2023"

history = GetFullHistoryDF().get_full_history(pairs_list=pair, start=start, end=end, timeframe=timeframe, API=API)

for data in list(history.data.values()):
    print(data.head(10))
    print(data.tail(10))

