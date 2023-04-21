from API.API_exchange_initiator import ExchangeAPISelect
from exchange.get_history import GetFullHistoryDF
from utils.utils import merge_df_dicts

API = ExchangeAPISelect().binance_spot_read_only()
pair = ["JOE/BTC"]
timeframe = "1d"
start = "01.01.2023"
end = "01.02.2023"

new_start = "01.12.2022"
new_end = "01.04.2023"

# history = GetFullHistoryDF(pairs_list=pair, start=start, end=end, timeframe=timeframe, API=API).get_full_history()

# for data in list(history.data.values()):
#     print(data.head(10))
#     print(data.tail(10))


data_1 = GetFullHistoryDF(pairs_list=pair, start=new_start, end=end, timeframe=timeframe, API=API).get_full_history().data
data_2 = GetFullHistoryDF(pairs_list=pair, start=start, end=new_end, timeframe=timeframe, API=API).get_full_history().data


final_dict = merge_df_dicts(data_1, data_2)

print(data_1.values())
print(data_2.values())
print(final_dict.values())
