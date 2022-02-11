from kucoin.client import Client
import os

from configparser import ConfigParser


def API_initiation():
    name = "Kucoin AB USDT"

    dir_path = os.path.dirname(os.path.realpath(__file__))
    parent_path = os.path.dirname(dir_path)

    parser = ConfigParser()
    parser.read(f"{parent_path}\APIs\Kucoin_AB_USDT.ini")

    public_key = parser.get("Trade_keys", "Public_key")
    secret_key = parser.get("Trade_keys", "Secret_key")
    passphrase = parser.get("Trade_keys", "Passphrase")

    general_client = Client(api_key=public_key, api_secret=secret_key, passphrase=passphrase)
    # Trade_client = Trade(Public_key, Secret_key, Passphrase, is_sandbox = False)
    trade_client = general_client

    API = {"name": name,
           "general_client": general_client,
           "trade_client": trade_client
           }

    return API
