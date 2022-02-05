from kucoin.client import Client
from kucoinsdk.client import Trade

from configparser import ConfigParser


def API_initiation():

    Name = "Kucoin AB Bot USDT"

    parser = ConfigParser()
    parser.read("Kucoin_USDT.ini")

    Public_key = parser.get("Trade_keys", "Public_key")
    Secret_key = parser.get("Trade_keys", "Secret_key")
    Passphrase = parser.get("Trade_keys", "Passphrase")

    General_client = Client(Public_key, Secret_key, Passphrase)
    Trade_client = Trade(Public_key, Secret_key, Passphrase, is_sandbox = False)

    API = {"Name": Name,
           "General_client": General_client,
           "Trade_client": Trade_client
           }

    return API
