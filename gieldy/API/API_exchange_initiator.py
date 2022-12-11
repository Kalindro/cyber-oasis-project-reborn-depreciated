import ccxt
import os
from dotenv import load_dotenv
from typing import Optional


def create_exchange_instance(CCXT_exchange_name: str, public_key: str, secret_key: str,
                             passphrase: Optional[str] = None):
    exchange_params = {
        "apiKey": public_key,
        "secret": secret_key,
    }
    if passphrase:
        exchange_params["password"] = passphrase
    return getattr(ccxt, CCXT_exchange_name)(exchange_params)


class CCXTExchangeSelect:
    """Base class for creating instances of CCXT exchanges"""

    @staticmethod
    def binance_spot(public_key: str, secret_key: str):
        return create_exchange_instance("binance", public_key, secret_key)

    @staticmethod
    def kucoin_spot(public_key: str, secret_key: str, passphrase: str):
        return create_exchange_instance("kucoin", public_key, secret_key, passphrase)

    @staticmethod
    def binance_futures(public_key: str, secret_key: str):
        return create_exchange_instance("binanceusdm", public_key, secret_key)


class ExchangeAPISelect:
    """Class that allows to select API with exchange"""
    load_dotenv()

    @staticmethod
    def binance_spot_read_only() -> dict:
        name = "Binance Spot Read Only"
        exchange = "binance_spot"
        public_key = os.getenv("BINANCE_READ_ONLY_PUBLIC_KEY")
        secret_key = os.getenv("BINANCE_READ_ONLY_PRIVATE_KEY")
        API = {"name": name,
               "exchange": exchange,
               "general_client": CCXTExchangeSelect.binance_spot(public_key, secret_key)
               }
        return API

    @staticmethod
    def kucoin_spot_read_only() -> dict:
        name = "Kucoin Spot Read Only"
        exchange = "kucoin_spot"
        public_key = os.getenv("KUCOIN_SPOT_READ_ONLY_PUBLIC_KEY")
        secret_key = os.getenv("KUCOIN_SPOT_READ_ONLY_PRIVATE_KEY")
        passphrase = os.getenv("KUCOIN_SPOT_READ_ONLY_PASSPHRASE")
        API = {"name": name,
               "exchange": exchange,
               "general_client": CCXTExchangeSelect.kucoin_spot(public_key, secret_key, passphrase)
               }
        return API

    @staticmethod
    def binance_futures_read_only() -> dict:
        name = "Binance Futures Read Only"
        exchange = "binance_futures"
        public_key = os.getenv("BINANCE_READ_ONLY_PUBLIC_KEY")
        secret_key = os.getenv("BINANCE_READ_ONLY_PRIVATE_KEY")
        API = {"name": name,
               "exchange": exchange,
               "general_client": CCXTExchangeSelect.binance_futures(public_key, secret_key)
               }
        return API
