from CyberOasisProjectReborn.CEFI.exchange.exchanges import BinanceSpotReadOnly


timeframe = "1h"
candles = 100
x = BinanceSpotReadOnly()

pairs_list = x.functions.get_pairs_list_USDT()
print(x.functions.get_history(pairs_list, timeframe, start="01.01.2023", end="01.03.2023", save_load_history=True))
