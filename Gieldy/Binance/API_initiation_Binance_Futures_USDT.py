from binance.client import Client
import os
from pathlib import Path

from configparser import ConfigParser


def API_initiation():
    name = "Binance Futures USDT"

    current_path = os.path.dirname(os.path.abspath(__file__))
    project_path = Path(current_path).parent.parent

    parser = ConfigParser()
    parser.read(f"{project_path}\Gieldy\APIs\Binance_Futures_USDT.ini")

    public_key = parser.get("Trade_keys", "Public_key")
    secret_key = parser.get("Trade_keys", "Secret_key")

    general_client = Client(api_key=public_key, api_secret=secret_key)

    API = {"name": name,
           "general_client": general_client
           }

    return API
