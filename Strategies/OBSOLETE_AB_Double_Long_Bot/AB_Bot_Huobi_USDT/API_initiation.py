from huobi.client.trade import TradeClient
from huobi.client.account import AccountClient
from huobi.client.generic import GenericClient
from huobi.client.market import MarketClient
from configparser import ConfigParser


def API_initiation():

    Name = "Huobi AB Bot USDT"

    parser = ConfigParser()
    parser.read("Huobi_USDT.ini")

    Public_key = parser.get("Trade_keys", "Public_key")
    Secret_key = parser.get("Trade_keys", "Secret_key")

    Generic_client = GenericClient()
    Market_client = MarketClient()
    Account_client = AccountClient(api_key = Public_key, secret_key = Secret_key)
    Account_ID = Account_client.get_accounts()[0].id
    Trade_client = TradeClient(api_key = Public_key, secret_key = Secret_key)

    API = {"Name": Name,
           "Account_ID": Account_ID,
           "Trade_client": Trade_client,
           "Account_client": Account_client,
           "Generic_client": Generic_client,
           "Market_client": Market_client}

    return API
