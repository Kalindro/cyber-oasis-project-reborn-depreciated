from CyberOasisProjectReborn.CEFI.exchange.exchanges import BinanceSpotReadOnly

x = BinanceSpotReadOnly()
x.history()

"""
1. Osobna funkcja, i zawsze przed uzyciem passed exchagne do niej
2. Iniekcja do initu exchange funkcji, i get history osobno, by bylo pod self.funcs i self.history
3. Zostawienie jak jest w jednej wielkiej klasie
4. Exchange inheritować moze klase z funkcji, to w sumie jak zostrawienie
"""
