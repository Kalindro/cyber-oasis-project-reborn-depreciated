import os
import typing as tp
from abc import ABC, abstractmethod

import ccxt
from dotenv import load_dotenv

from CyberOasisProjectReborn.CEFI.exchange.exchange_functions import ExchangeFunctions


class ExchangeClientCreation:

    def binance_spot(self, public_key: str, secret_key: str) -> ccxt.Exchange:
        return self._create_exchange_instance("binance", public_key, secret_key,
                                              options={"defaultType": "spot", "fetchMarkets": ["spot"]})

    def binance_futures(self, public_key: str, secret_key: str) -> ccxt.Exchange:
        return self._create_exchange_instance("binanceusdm", public_key, secret_key,
                                              options={"defaultType": "future", "fetchMarkets": ["linear"]})

    def bybit_spot_futures(self, public_key: str, secret_key: str) -> ccxt.Exchange:
        return self._create_exchange_instance("bybit", public_key, secret_key)

    def _create_exchange_instance(self,
                                  CCXT_name: str,
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
        return getattr(ccxt, CCXT_name)(exchange_params)


class Exchange(ABC):
    """Base abstract class to create specific exchange client"""
    load_dotenv()

    def __init__(self):
        self.client = None
        self.name = None
        self.path_name = None
        self.creator = ExchangeClientCreation()
        self.initialize()
        self.functions_wrap()

    @abstractmethod
    def initialize(self):
        pass

    def functions_wrap(self):
        return ExchangeFunctions(self)


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
