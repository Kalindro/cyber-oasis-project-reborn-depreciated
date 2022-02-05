from huobi.client.trade import TradeClient
from huobi.client.account import AccountClient
from huobi.client.generic import GenericClient
from huobi.client.market import MarketClient
import os

from configparser import ConfigParser


def API_initiation():
    name = "Huobi USDT"

    dir_path = os.path.dirname(os.path.realpath(__file__))
    parent_path = os.path.dirname(dir_path)

    parser = ConfigParser()
    parser.read(f"{parent_path}/APIs/Huobi_USDT.ini")

    public_key = parser.get("Trade_keys", "Public_key")
    secret_key = parser.get("Trade_keys", "Secret_key")

    generic_client = GenericClient()
    market_client = MarketClient()
    account_client = AccountClient(api_key=public_key, secret_key=secret_key)
    account_id = account_client.get_accounts()[0].id
    trade_client = TradeClient(api_key=public_key, secret_key=secret_key)

    API = {"name": name,
           "account_ID": account_id,
           "trade_client": trade_client,
           "account_client": account_client,
           "generic_client": generic_client,
           "market_client": market_client}

    return API
