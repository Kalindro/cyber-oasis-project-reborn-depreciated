from CyberOasisProjectReborn.CEFI.exchange.exchanges import BinanceSpotReadOnly

x = BinanceSpotReadOnly()
x.history.get_full_history()