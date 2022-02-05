from ftx import FtxClient
import os

from configparser import ConfigParser

def API_initiation():
    name = "FTX USDT"

    dir_path = os.path.dirname(os.path.realpath(__file__))
    parent_path = os.path.dirname(dir_path)

    parser = ConfigParser()
    parser.read(f"{parent_path}/APIs/FTX_USDT.ini")

    public_key = parser.get("Trade_keys", "Public_key")
    secret_key = parser.get("Trade_keys", "Secret_key")

    general_client = FtxClient(api_key=public_key, api_secret=secret_key)

    API = {"name": name,
           "general_client": general_client
           }

    return API
