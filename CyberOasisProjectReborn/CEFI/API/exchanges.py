import os
import typing as tp
from abc import ABC, abstractmethod

import ccxt
from dotenv import load_dotenv


class ExchangeClientCreation:
    """Class including base creation of CCXT exchange client"""

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


class Exchange(ABC):
    """Base abstract class to create specific exchange client"""
    load_dotenv()

    def __init__(self):
        self.creator = ExchangeClientCreation()
        self.client = None
        self.name = None
        self.path_name = None
        self.initialize()

    @abstractmethod
    def initialize(self):
        pass


class BinanceSpotReadOnly(Exchange):

    def initialize(self):
        public_key = os.getenv("BINANCE_READ_ONLY_PUBLIC_KEY")
        secret_key = os.getenv("BINANCE_READ_ONLY_PRIVATE_KEY")
        self.client = self.creator.binance_spot(public_key, secret_key)
        self.name = "Binance Spot Read Only"
        self.path_name = "binance_spot"


class BinanceSpotTrade(Exchange):

    def initialize(self):
        public_key = os.getenv("BINANCE_TRADE_PUBLIC_KEY")
        secret_key = os.getenv("BINANCE_TRADE_PRIVATE_KEY")
        self.client = self.creator.binance_spot(public_key, secret_key)
        self.name = "Binance Spot Trade"
        self.path_name = "binance_spot"


class BinanceFuturesReadOnly(Exchange):

    def initialize(self):
        public_key = os.getenv("BINANCE_READ_ONLY_PUBLIC_KEY")
        secret_key = os.getenv("BINANCE_READ_ONLY_PRIVATE_KEY")
        self.client = self.creator.binance_futures(public_key, secret_key)
        self.name = "Binance Futures Read Only"
        self.path_name = "binance_futures"


class BinanceFuturesTrade(Exchange):

    def initialize(self):
        public_key = os.getenv("BINANCE_TRADE_PUBLIC_KEY")
        secret_key = os.getenv("BINANCE_TRADE_PRIVATE_KEY")
        self.client = self.creator.binance_futures(public_key, secret_key)
        self.name = "Binance Futures Trade"
        self.path_name = "binance_futures"


class BybitReadOnly(Exchange):

    def initialize(self):
        public_key = os.getenv("BYBIT_READ_ONLY_PUBLIC_KEY")
        secret_key = os.getenv("BYBIT_READ_ONLY_PRIVATE_KEY")
        self.client = self.creator.bybit_spot_futures(public_key, secret_key)
        self.name = "Bybit Read Only"
        self.path_name = "bybit"


class BybitTrade(Exchange):

    def initialize(self):
        public_key = os.getenv("BYBIT_TRADE_PUBLIC_KEY")
        secret_key = os.getenv("BYBIT_TRADE_PRIVATE_KEY")
        self.client = self.creator.bybit_spot_futures(public_key, secret_key)
        self.name = "Bybit Trade"
        self.path_name = "bybit"
