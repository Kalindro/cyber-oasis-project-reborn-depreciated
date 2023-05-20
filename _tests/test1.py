import ccxt.binance
import vectorbtpro

from CyberOasisProjectReborn.CEFI.API.API_exchange_initiator import ExchangeAPISelect
from CyberOasisProjectReborn.CEFI.exchange.get_history import GetFullHistoryDF

API = ExchangeAPISelect().binance_spot_read_only()
pair = ["BTC/USDT"]
timeframe = "1d"
start = "01.01.2022"
end = "01.02.2023"

history = GetFullHistoryDF(pairs_list=pair, start=start, end=end, timeframe=timeframe, API=API).get_full_history()

print(history)
