from pandas_ta.volatility import natr as NATR

from API.API_exchange_initiator import ExchangeAPISelect
from CCXT.CCXT_functions_mine import get_history_df_dict_pairs_list


class PortfolioAllocationParity:
    def main(self,
             pairs_list: list[str],
             investment: int,
             timeframe: str,
             number_of_last_candles: int,
             period: int,
             API: dict):
        pairs_history_df_list = get_history_df_dict_pairs_list(pairs_list=pairs_list, timeframe=timeframe,
                                                               number_of_last_candles=number_of_last_candles, API=API)
        inv_vola_calculation = lambda history_df: 1 / NATR(high=history_df["high"], low=history_df["low"],
                                                         close=history_df["close"],
                                                         length=period).tail(1)
        inv_vola_list = [inv_vola_calculation(pair_history) for pair_history in pairs_history_df_list]


if __name__ == "__main__":
    pairs_list = ["BTC/USDT", "ETH/USDT"]
    investment = 1000
    number_of_last_candles = 750
    period = 72
    API = ExchangeAPISelect().binance_spot_read_only()
    PortfolioAllocationParity().main(pairs_list=pairs_list, investment=investment, timeframe="1h", period=period,
                                     number_of_last_candles=number_of_last_candles, API=API)
