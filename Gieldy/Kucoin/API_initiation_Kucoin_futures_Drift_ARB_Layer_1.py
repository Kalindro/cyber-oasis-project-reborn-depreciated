from kucoin_futures.client import Market
from kucoin_futures.client import Trade
from kucoin_futures.client import User

import os
from pathlib import Path

from configparser import ConfigParser


def API_initiation():
    name = "Kucoin futures Drift ARB first layer"

    current_path = os.path.dirname(os.path.abspath(__file__))
    project_path = Path(current_path).parent.parent

    parser = ConfigParser()
    parser.read(f"{project_path}/Gieldy/APIs/Kucoin_futures_Drift_ARB_Layer_1.ini")

    public_key = parser.get("Trade_keys", "Public_key")
    secret_key = parser.get("Trade_keys", "Secret_key")
    passphrase = parser.get("Trade_keys", "Passphrase")

    general_client = Market(key=public_key, secret=secret_key, passphrase=passphrase)
    trade_client = Trade(key=public_key, secret=secret_key, passphrase=passphrase)
    user_client = User(key=public_key, secret=secret_key, passphrase=passphrase)

    API = {"name": name,
           "general_client": general_client,
           "trade_client": trade_client,
           "user_client": user_client,
           }

    return API
