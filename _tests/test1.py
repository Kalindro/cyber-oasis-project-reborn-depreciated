from CyberOasisProjectReborn.CEFI.API.exchanges import BinanceSpotReadOnly

x = BinanceSpotReadOnly()
print(x.get_pairs_with_precisions_status())
timeframe = "1H"
candles = 500
history = x.functions.get_history(pairs_list=pairs_list, timeframe=timeframe, number_of_last_candles=candles)
print(history)
