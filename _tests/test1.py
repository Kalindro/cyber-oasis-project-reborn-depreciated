from API.API_exchange_initiator import ExchangeAPISelect
from exchange.get_history import GetFullHistoryDF

API = ExchangeAPISelect().binance_spot_read_only()
pair = ["JOE/BTC"]
timeframe = "1d"
last_n_candles = 50

history = GetFullHistoryDF().get_full_history(pairs_list=pair, number_of_last_candles=last_n_candles,
                                              timeframe=timeframe, API=API)

[print(data) for data in list(history.data.values())]
