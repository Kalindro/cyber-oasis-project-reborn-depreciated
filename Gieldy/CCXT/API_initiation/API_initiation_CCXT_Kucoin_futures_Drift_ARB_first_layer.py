import ccxt
import os
from pathlib import Path

from configparser import ConfigParser


def API_initiation():
    name = "Kucoin futures Drift ARB first layer"

    current_path = os.path.dirname(os.path.abspath(__file__))
    project_path = Path(current_path).parent.parent.parent

    parser = ConfigParser()
    parser.read(f"{project_path}/Gieldy/APIs/Kucoin_futures_Drift_ARB_first_layer.ini")

    public_key = parser.get("Trade_keys", "Public_key")
    secret_key = parser.get("Trade_keys", "Secret_key")
    passphrase = parser.get("Trade_keys", "Passphrase")

    exchange = ccxt.kucoinfutures({
        'apiKey': public_key,
        'secret': secret_key,
        'passphrase': passphrase,
    })

    API = {"name": name,
           "general_client": exchange
           }

    return API

