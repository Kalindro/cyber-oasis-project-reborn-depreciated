from CyberOasisProjectReborn.CEFI.API.exchanges import BinanceSpotReadOnly

x = BinanceSpotReadOnly()
pairs_list = x.functions.get_pairs_list_test_multi()
timeframe = "1H"
candles = 500
history = x.functions.get_history(pairs_list=pairs_list, timeframe=timeframe, number_of_last_candles=candles)
print(history)
