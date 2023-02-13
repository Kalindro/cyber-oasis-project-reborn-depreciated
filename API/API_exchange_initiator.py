import os
from typing import Optional

import ccxt
from dotenv import load_dotenv


class _CCXTExchangeSelect:
    """Class to create CCXT instance of selected exchange"""

    @classmethod
    def _create_exchange_instance(cls, CCXT_exchange_name: str, public_key: str, secret_key: str,
                                  passphrase: Optional[str] = None, options: Optional[dict] = None) -> ccxt.Exchange:
        """Factory function for creating CCXT exchange instance"""
        exchange_params = {"apiKey": public_key, "secret": secret_key}
        if passphrase:
            exchange_params["password"] = passphrase
        if options:
            exchange_params["options"] = options
        return getattr(ccxt, CCXT_exchange_name)(exchange_params)

    def _binance_spot(self, public_key: str, secret_key: str) -> ccxt.Exchange:
        return self._create_exchange_instance("binance", public_key=public_key, secret_key=secret_key,
                                              options={"defaultType": "spot", "fetchMarkets": ["spot"]})

    def _binance_futures(self, public_key: str, secret_key: str) -> ccxt.Exchange:
        return self._create_exchange_instance("binanceusdm", public_key, secret_key,
                                              options={"defaultType": "future", "fetchMarkets": ["linear"]})

    def _kucoin_spot(self, public_key: str, secret_key: str, passphrase: str) -> ccxt.Exchange:
        return self._create_exchange_instance("kucoin", public_key, secret_key, passphrase)

    def _bybit_spot_futures(self, public_key: str, secret_key: str) -> ccxt.Exchange:
        return self._create_exchange_instance("bybit", public_key, secret_key)


class ExchangeAPISelect(_CCXTExchangeSelect):
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

    def binance_futures_read_only(self) -> dict:
        name = "Binance Futures Read Only"
        exchange = "binance_futures"
        public_key = os.getenv("BINANCE_READ_ONLY_PUBLIC_KEY")
        secret_key = os.getenv("BINANCE_READ_ONLY_PRIVATE_KEY")
        API = {"name": name, "exchange": exchange,
               "client": self._binance_futures(public_key, secret_key)}
        return API

    def binance_futures_trade(self) -> dict:
        name = "Binance Futures Trade"
        exchange = "binance_futures"
        public_key = os.getenv("BINANCE_TRADE_PUBLIC_KEY")
        secret_key = os.getenv("BINANCE_TRADE_PRIVATE_KEY")
        API = {"name": name, "exchange": exchange,
               "client": self._binance_futures(public_key, secret_key)}
        return API

    def binance_spot_trade(self) -> dict:
        name = "Binance Spot Trade"
        exchange = "binance_spot"
        public_key = os.getenv("BINANCE_TRADE_PUBLIC_KEY")
        secret_key = os.getenv("BINANCE_TRADE_PRIVATE_KEY")
        API = {"name": name, "exchange": exchange,
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

    def bybit_read_only(self) -> dict:
        name = "Bybit Spot Trade"
        exchange = "bybit"
        public_key = os.getenv("BYBIT_READ_ONLY_PUBLIC_KEY")
        secret_key = os.getenv("BYBIT_READ_ONLY_PRIVATE_KEY")
        API = {"name": name, "exchange": exchange,
               "client": self._bybit_spot_futures(public_key, secret_key)}
        return API

    def bybit_trade(self) -> dict:
        name = "Bybit Trade"
        exchange = "bybit"
        public_key = os.getenv("BYBIT_TRADE_PUBLIC_KEY")
        secret_key = os.getenv("BYBIT_TRADE_PRIVATE_KEY")
        API = {"name": name, "exchange": exchange,
               "client": self._bybit_spot_futures(public_key, secret_key)}
        return API
