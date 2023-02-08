from CCXT.get_full_history import GetFullHistoryDF


class ParityBalancer:
    def __init__(self):
        self.x = 5
        self.TIMEFRAME = "1d"
        self.N_CANDLES = 2000
        self.API = "X"

    def main(self, pair1, pair2):
        pair1_history = GetFullHistoryDF().main(pair=pair1, timeframe=self.TIMEFRAME,
                                                number_of_last_candles=self.N_CANDLES, API=self.API)
