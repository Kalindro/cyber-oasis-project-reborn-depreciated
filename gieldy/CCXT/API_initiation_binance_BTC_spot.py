import ccxt
import os
from pathlib import Path

from configparser import ConfigParser


def API_initiation():
    name = "binance BTC AB spot"

    current_path = os.path.dirname(os.path.abspath(__file__))
    project_path = Path(current_path).parent.parent

    parser = ConfigParser()
    parser.read(f"{project_path}/Gieldy/APIs/Binance_BTC_AB_spot.ini")

    public_key = parser.get("Trade_keys", "Public_key")
    secret_key = parser.get("Trade_keys", "Secret_key")

    exchange = ccxt.binance({
        'apiKey': public_key,
        'secret': secret_key,
    })

    API = {"name": name,
           "general_client": exchange
           }

    return API

