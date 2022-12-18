import ccxt
import os
from dotenv import load_dotenv
from typing import Optional


def _create_exchange_instance(CCXT_exchange_name: str, public_key: str, secret_key: str,
                              passphrase: Optional[str] = None) -> ccxt.Exchange:
    """Factory function for creating CCXT exchange instance"""
    exchange_params = {"apiKey": public_key, "secret": secret_key}
    if passphrase:
        exchange_params["password"] = passphrase
    return getattr(ccxt, CCXT_exchange_name)(exchange_params)


class CCXTExchangeSelect:
    """Class to create CCXT instance of selected exchange"""

    @staticmethod
    def _binance_spot(public_key: str, secret_key: str) -> ccxt.Exchange:
        return _create_exchange_instance("binance", public_key, secret_key)

    @staticmethod
    def _kucoin_spot(public_key: str, secret_key: str, passphrase: str) -> ccxt.Exchange:
        return _create_exchange_instance("kucoin", public_key, secret_key, passphrase)

    @staticmethod
    def _binance_futures(public_key: str, secret_key: str) -> ccxt.Exchange:
        return _create_exchange_instance("binanceusdm", public_key, secret_key)


class ExchangeAPISelect(CCXTExchangeSelect):
    """Class for selecting the exchange and initiate with API"""
    load_dotenv()

    def binance_spot_read_only(self) -> dict:
        name = "Binance Spot Read Only"
        exchange = "binance_spot"
        public_key = os.getenv("BINANCE_READ_ONLY_PUBLIC_KEY")
        secret_key = os.getenv("BINANCE_READ_ONLY_PRIVATE_KEY")
        API = {"full_name": name, "exchange": exchange,
               "client": self._binance_spot(public_key, secret_key)}
        return API

    def kucoin_spot_read_only(self) -> dict:
        name = "Kucoin Spot Read Only"
        exchange = "kucoin_spot"
        public_key = os.getenv("KUCOIN_SPOT_READ_ONLY_PUBLIC_KEY")
        secret_key = os.getenv("KUCOIN_SPOT_READ_ONLY_PRIVATE_KEY")
        passphrase = os.getenv("KUCOIN_SPOT_READ_ONLY_PASSPHRASE")
        API = {"name": name, "exchange": exchange,
               "client": self._kucoin_spot(public_key, secret_key, passphrase)}
        return API

    def binance_futures_read_only(self) -> dict:
        name = "Binance Futures Read Only"
        exchange = "binance_futures"
        public_key = os.getenv("BINANCE_READ_ONLY_PUBLIC_KEY")
        secret_key = os.getenv("BINANCE_READ_ONLY_PRIVATE_KEY")
        API = {"name": name, "exchange": exchange,
               "client": self._binance_futures(public_key, secret_key)}
        return API
