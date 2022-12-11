import ccxt
import os
from dotenv import load_dotenv
from typing import Optional


class CCXTExchangeSelect:
    """Base class for exchanges"""

    def __init__(self, public_key: str, secret_key: str, passphrase: Optional[str] = None):
        self.public_key = public_key
        self.secret_key = secret_key
        self.passphrase = passphrase

    def binance_spot(self):
        """Binance spot exchange"""
        exchange = ccxt.binance({
            "apiKey": self.public_key,
            "secret": self.secret_key,
        })
        return exchange

    def kucoin_spot(self):
        """Kucoin spot exchange"""
        exchange = ccxt.kucoin({
            "apiKey": self.public_key,
            "secret": self.secret_key,
            "password": self.passphrase,
        })
        return exchange

    def binance_futures(self):
        """Binance futures exchange"""
        exchange = ccxt.binanceusdm({
            "apiKey": self.public_key,
            "secret": self.secret_key,
        })
        return exchange


class ExchangeAPISelect:
    """Class that allows to select API with exchange"""
    load_dotenv()

    @staticmethod
    def binance_spot_read_only() -> dict:
        name = "Binance Spot Read Only"
        public_key = os.getenv("BINANCE_READ_ONLY_PUBLIC_KEY")
        secret_key = os.getenv("BINANCE_READ_ONLY_PRIVATE_KEY")
        API = {"name": name,
               "general_client": CCXTExchangeSelect(public_key, secret_key).binance_spot()
               }
        return API

    @staticmethod
    def kucoin_spot_read_only() -> dict:
        name = "Kucoin Spot Read Only"
        public_key = os.getenv("KUCOIN_SPOT_READ_ONLY_PUBLIC_KEY")
        secret_key = os.getenv("KUCOIN_SPOT_READ_ONLY_PRIVATE_KEY")
        passphrase = os.getenv("KUCOIN_SPOT_READ_ONLY_PASSPHRASE")
        API = {"name": name,
               "general_client": CCXTExchangeSelect(public_key, secret_key, passphrase).kucoin_spot()
               }
        return API

    @staticmethod
    def binance_futures_read_only() -> dict:
        name = "Binance Futures Read Only"
        public_key = os.getenv("BINANCE_READ_ONLY_PUBLIC_KEY")
        secret_key = os.getenv("BINANCE_READ_ONLY_PRIVATE_KEY")
        API = {"name": name,
               "general_client": CCXTExchangeSelect(public_key, secret_key).binance_futures()
               }
        return API
