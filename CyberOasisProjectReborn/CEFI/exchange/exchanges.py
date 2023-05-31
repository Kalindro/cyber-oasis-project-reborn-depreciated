import os
import typing as tp
from abc import ABC, abstractmethod

import ccxt
from dotenv import load_dotenv

from CyberOasisProjectReborn.CEFI.functions.exchange_functions import ExchangeFunctions


class Exchange(ABC):
    """Base abstract class to create specific exchange client"""
    load_dotenv()

    def __init__(self):
        self.exchange_client = None
        self.exchange_name = None
        self.exchange_path_name = None
        self.reinitialize()
        self.functions = ExchangeFunctions(self)

    @abstractmethod
    def reinitialize(self):
        pass


class ExchangeConstructor:

    def binance_spot(self, public_key: str, secret_key: str) -> ccxt.Exchange:
        return self._create_exchange_instance("binance", public_key, secret_key,
                                              options={"defaultType": "spot", "fetchMarkets": ["spot"]})

    def binance_futures(self, public_key: str, secret_key: str) -> ccxt.Exchange:
        return self._create_exchange_instance("binanceusdm", public_key, secret_key,
                                              options={"defaultType": "future", "fetchMarkets": ["linear"]})

    def bybit_spot_futures(self, public_key: str, secret_key: str) -> ccxt.Exchange:
        return self._create_exchange_instance("bybit", public_key, secret_key)

    @staticmethod
    def _create_exchange_instance(CCXT_exchange_name: str,
                                  public_key: str,
                                  secret_key: str,
                                  passphrase: tp.Optional[str] = None,
                                  options: tp.Optional[dict] = None
                                  ) -> ccxt.Exchange:
        exchange_params = {"apiKey": public_key, "secret": secret_key}
        if passphrase:
            exchange_params["password"] = passphrase
        if options:
            exchange_params["options"] = options
        return getattr(ccxt, CCXT_exchange_name)(exchange_params)


class BinanceSpotReadOnly(Exchange, ExchangeConstructor):

    def reinitialize(self):
        public_key = os.getenv("BINANCE_READ_ONLY_PUBLIC_KEY")
        secret_key = os.getenv("BINANCE_READ_ONLY_PRIVATE_KEY")
        self.exchange_client = self.binance_spot(public_key, secret_key)
        self.exchange_name = "Binance Spot Read Only"
        self.exchange_path_name = "binance_spot"


class BinanceSpotTrade(Exchange, ExchangeConstructor):

    def reinitialize(self):
        public_key = os.getenv("BINANCE_TRADE_PUBLIC_KEY")
        secret_key = os.getenv("BINANCE_TRADE_PRIVATE_KEY")
        self.exchange_client = self.binance_spot(public_key, secret_key)
        self.exchange_name = "Binance Spot Trade"
        self.exchange_path_name = "binance_spot"


class BinanceFuturesReadOnly(Exchange, ExchangeConstructor):

    def reinitialize(self):
        public_key = os.getenv("BINANCE_READ_ONLY_PUBLIC_KEY")
        secret_key = os.getenv("BINANCE_READ_ONLY_PRIVATE_KEY")
        self.exchange_client = self.binance_futures(public_key, secret_key)
        self.exchange_name = "Binance Futures Read Only"
        self.exchange_path_name = "binance_futures"


class BinanceFuturesTrade(Exchange, ExchangeConstructor):

    def reinitialize(self):
        public_key = os.getenv("BINANCE_TRADE_PUBLIC_KEY")
        secret_key = os.getenv("BINANCE_TRADE_PRIVATE_KEY")
        self.exchange_client = self.binance_futures(public_key, secret_key)
        self.exchange_name = "Binance Futures Trade"
        self.exchange_path_name = "binance_futures"


class BybitReadOnly(Exchange, ExchangeConstructor):

    def reinitialize(self):
        public_key = os.getenv("BYBIT_READ_ONLY_PUBLIC_KEY")
        secret_key = os.getenv("BYBIT_READ_ONLY_PRIVATE_KEY")
        self.exchange_client = self.bybit_spot_futures(public_key, secret_key)
        self.exchange_name = "Bybit Read Only"
        self.exchange_path_name = "bybit"


class BybitTrade(Exchange, ExchangeConstructor):

    def reinitialize(self):
        public_key = os.getenv("BYBIT_TRADE_PUBLIC_KEY")
        secret_key = os.getenv("BYBIT_TRADE_PRIVATE_KEY")
        self.exchange_client = self.bybit_spot_futures(public_key, secret_key)
        self.exchange_name = "Bybit Trade"
        self.exchange_path_name = "bybit"
