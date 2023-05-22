import os

from CyberOasisProjectReborn.CEFI.API._exchanges_initiator import Exchange


class BinanceSpotReadOnly(Exchange):

    def initialize(self):
        public_key = os.getenv("BINANCE_READ_ONLY_PUBLIC_KEY")
        secret_key = os.getenv("BINANCE_READ_ONLY_PRIVATE_KEY")
        self.client = self.binance_spot(public_key, secret_key)
        self.name = "Binance Spot Read Only"
        self.path_name = "binance_spot"


class BinanceSpotTrade(Exchange):

    def initialize(self):
        public_key = os.getenv("BINANCE_TRADE_PUBLIC_KEY")
        secret_key = os.getenv("BINANCE_TRADE_PRIVATE_KEY")
        self.client = self.binance_spot(public_key, secret_key)
        self.name = "Binance Spot Trade"
        self.path_name = "binance_spot"


class BinanceFuturesReadOnly(Exchange):

    def initialize(self):
        public_key = os.getenv("BINANCE_READ_ONLY_PUBLIC_KEY")
        secret_key = os.getenv("BINANCE_READ_ONLY_PRIVATE_KEY")
        self.client = self.binance_futures(public_key, secret_key)
        self.name = "Binance Futures Read Only"
        self.path_name = "binance_futures"


class BinanceFuturesTrade(Exchange):

    def initialize(self):
        public_key = os.getenv("BINANCE_TRADE_PUBLIC_KEY")
        secret_key = os.getenv("BINANCE_TRADE_PRIVATE_KEY")
        self.client = self.binance_futures(public_key, secret_key)
        self.name = "Binance Futures Trade"
        self.path_name = "binance_futures"


class BybitReadOnly(Exchange):

    def initialize(self):
        public_key = os.getenv("BYBIT_READ_ONLY_PUBLIC_KEY")
        secret_key = os.getenv("BYBIT_READ_ONLY_PRIVATE_KEY")
        self.client = self.bybit_spot_futures(public_key, secret_key)
        self.name = "Bybit Read Only"
        self.path_name = "bybit"


class BybitTrade(Exchange):

    def initialize(self):
        public_key = os.getenv("BYBIT_TRADE_PUBLIC_KEY")
        secret_key = os.getenv("BYBIT_TRADE_PRIVATE_KEY")
        self.client = self.bybit_spot_futures(public_key, secret_key)
        self.name = "Bybit Trade"
        self.path_name = "bybit"
