from ftx import FtxClient
import os
from pathlib import Path

from configparser import ConfigParser

def API_initiation():
    name = "FTX"

    current_path = os.path.dirname(os.path.abspath(__file__))
    project_path = Path(current_path).parent.parent

    parser = ConfigParser()
    parser.read(f"{project_path}/Gieldy/APIs/FTX.ini")

    public_key = parser.get("Trade_keys", "Public_key")
    secret_key = parser.get("Trade_keys", "Secret_key")

    general_client = FtxClient(api_key=public_key, api_secret=secret_key)

    API = {"name": name,
           "general_client": general_client
           }

    return API
