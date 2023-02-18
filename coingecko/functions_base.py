import requests
from pandas import DataFrame as df

from general_funcs.log_config import ConfigureLoguru

logger = ConfigureLoguru().info_level()

url = 'https://min-api.cryptocompare.com/data/all/coinlist'
response = requests.get(url)
data = response.json()

dataframe = df.from_dict(data["Data"], orient="index").set_index("Symbol")
print(dataframe)
print(dataframe.loc["BAND"]["Taxonomy"])

