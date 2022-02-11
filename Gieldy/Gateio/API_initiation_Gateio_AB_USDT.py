import gate_api
import os

from configparser import ConfigParser


def API_initiation():
    name = "Gateio AB USDT"

    dir_path = os.path.dirname(os.path.realpath(__file__))
    parent_path = os.path.dirname(dir_path)

    parser = ConfigParser()
    parser.read(f"{parent_path}\APIs\Gateio_AB_USDT.ini")

    public_key = parser.get("Trade_keys", "Public_key")
    secret_key = parser.get("Trade_keys", "Secret_key")

    general_configuration = gate_api.Configuration(
        host="https://api.gateio.ws/api/v4"
    )
    trade_configuration = gate_api.Configuration(
        host="https://api.gateio.ws/api/v4",
        key=public_key,
        secret=secret_key
    )
    general_api_client = gate_api.ApiClient(general_configuration)
    general_client = gate_api.SpotApi(general_api_client)
    trade_api_client = gate_api.ApiClient(trade_configuration)
    trade_client = gate_api.SpotApi(trade_api_client)

    API = {"name": name,
           "general_client": general_client,
           "trade_client": trade_client
           }

    return API
