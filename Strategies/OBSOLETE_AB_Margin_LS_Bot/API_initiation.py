from binance.client import Client

from configparser import ConfigParser


def API_initiation():

    Name = "Binance AB LS Bot USDT"

    parser = ConfigParser()
    parser.read("Binance_USDT.ini")

    Public_key = parser.get("Trade_keys", "Public_key")
    Secret_key = parser.get("Trade_keys", "Secret_key")

    General_client = Client(Public_key, Secret_key)

    API = {"Name": Name,
           "General_client": General_client
           }

    return API
