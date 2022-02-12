from okex.client import Client
import os
from pathlib import Path

from configparser import ConfigParser


def API_initiation():
    name = "Okex AB USDT"

    current_path = os.getcwd()
    project_path = Path(current_path).parent.parent

    parser = ConfigParser()
    parser.read(f"{project_path}\Gieldy\APIs\Okex_AB_USDT.ini")

    public_key = parser.get("Trade_keys", "Public_key")
    secret_key = parser.get("Trade_keys", "Secret_key")
    passphrase = parser.get("Trade_keys", "Passphrase")

    general_client = Client(api_key=public_key, api_secret_key=secret_key, passphrase=passphrase)

    API = {"name": name,
           "general_client": general_client,
           }

    return API
