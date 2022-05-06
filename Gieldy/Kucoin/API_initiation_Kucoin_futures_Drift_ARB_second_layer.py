from kucoin.client import Client
import os
from pathlib import Path

from configparser import ConfigParser


def API_initiation():
    name = "Kucoin futures Drift ARB second layer"

    current_path = os.path.dirname(os.path.abspath(__file__))
    project_path = Path(current_path).parent.parent

    parser = ConfigParser()
    parser.read(f"{project_path}/Gieldy/APIs/Kucoin_futures_Drift_ARB_second_layer")

    public_key = parser.get("Trade_keys", "Public_key")
    secret_key = parser.get("Trade_keys", "Secret_key")
    passphrase = parser.get("Trade_keys", "Passphrase")

    general_client = Client(api_key=public_key, api_secret=secret_key, passphrase=passphrase)

    API = {"name": name,
           "general_client": general_client
           }

    return API
