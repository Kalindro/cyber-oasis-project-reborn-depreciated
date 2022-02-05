from kucoin.client import Client
# from kucoinsdk.client import Trade

from configparser import ConfigParser


def API_initiation():

    name = "Kucoin Momentum L Bot USDT"

    parser = ConfigParser()
    parser.read("Kucoin_USDT.ini")

    PUBLIC_KEY = parser.get("Trade_keys", "Public_key")
    SECRET_KEY = parser.get("Trade_keys", "Secret_key")
    PASSPHRASE = parser.get("Trade_keys", "Passphrase")

    general_client = Client(PUBLIC_KEY, SECRET_KEY, PASSPHRASE)
    # trade_client = Trade(Public_key, Secret_key, Passphrase, is_sandbox = False)
    trade_client = general_client

    API = {"name": name,
           "general_client": general_client,
           "trade_client": trade_client
           }

    return API
