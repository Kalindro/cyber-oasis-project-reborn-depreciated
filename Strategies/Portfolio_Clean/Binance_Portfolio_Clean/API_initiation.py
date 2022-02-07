from binance.client import Client

from configparser import ConfigParser


def API_initiation():

    name = "Binance Momentum L Bot USDT"

    parser = ConfigParser()
    parser.read("Binance_AB_USDT.ini")

    PUBLIC_KEY = parser.get("Trade_keys", "Public_key")
    SECRET_KEY = parser.get("Trade_keys", "Secret_key")

    general_client = Client(PUBLIC_KEY, SECRET_KEY)

    API = {"name": name,
           "general_client": general_client
           }

    return API
