from okex.Trade_api import TradeAPI
import os
from pathlib import Path

from configparser import ConfigParser


def API_initiation():
    name = "Okx AB USDT"

    current_path = os.path.dirname(os.path.abspath(__file__))
    project_path = Path(current_path).parent.parent

    parser = ConfigParser()
    parser.read(f"{project_path}\Gieldy\APIs\Okx_AB_USDT.ini")

    public_key = parser.get("Trade_keys", "Public_key")
    secret_key = parser.get("Trade_keys", "Secret_key")
    passphrase = parser.get("Trade_keys", "Passphrase")

    trade_client = TradeAPI(api_key=public_key, api_secret_key=secret_key, passphrase=passphrase)

    API = {"name": name,
           "trade_client": trade_client
           }

    return API
