from gieldy.API.API_exchange_initiator import ExchangeAPISelect
from gieldy.CCXT.CCXT_functions_mine import get_history_of_all_pairs_on_list_mp

pairs_list = ["BTC/USDT", "ETH/USDT", "LTC/USDT", "BNB/USDT"]
timeframe = "1h"
number_of_last_candles = 500



class hitl:
    def jaruz(self, API):
        all_pairs_history = get_history_of_all_pairs_on_list_mp(pairs_list=pairs_list, timeframe=timeframe,
                                                                save_load_history=False,
                                                                number_of_last_candles=number_of_last_candles,
                                                                API=API)
        print(all_pairs_history)


if __name__ == "__main__":
    API = ExchangeAPISelect().binance_futures_read_only()
    hitl().jaruz(API)
