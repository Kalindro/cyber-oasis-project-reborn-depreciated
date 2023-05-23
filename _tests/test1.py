from CyberOasisProjectReborn.CEFI.API.exchanges import BinanceSpotReadOnly
import vectorbtpro as vbt
x = BinanceSpotReadOnly()
print(x.functions.get_pairs_list_USDT())