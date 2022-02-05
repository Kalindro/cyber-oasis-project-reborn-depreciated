import gate_api

from configparser import ConfigParser


def API_initiation():

    Name = "Gateio AB Bot USDT"

    parser = ConfigParser()
    parser.read("Gateio_USDT.ini")

    Public_key = parser.get("Trade_keys", "Public_key")
    Secret_key = parser.get("Trade_keys", "Secret_key")

    General_configuration = gate_api.Configuration(
        host = "https://api.gateio.ws/api/v4"
    )
    Trade_configuration = gate_api.Configuration(
        host = "https://api.gateio.ws/api/v4",
        key = Public_key,
        secret = Secret_key
    )
    General_api_client = gate_api.ApiClient(General_configuration)
    General_client = gate_api.SpotApi(General_api_client)
    Trade_api_client = gate_api.ApiClient(Trade_configuration)
    Trade_client = gate_api.SpotApi(Trade_api_client)

    API = {"Name": Name,
           "General_client": General_client,
           "Trade_client": Trade_client
           }

    return API
