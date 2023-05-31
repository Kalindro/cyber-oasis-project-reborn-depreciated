from CyberOasisProjectReborn.CEFI.exchange.exchanges import BinanceSpotReadOnly

pairs_list = ["BTC/USDT"]
timeframe = "1h"
candles = 100

x = BinanceSpotReadOnly()
print(x.functions.get_history(pairs_list, timeframe, number_of_last_candles=100))

