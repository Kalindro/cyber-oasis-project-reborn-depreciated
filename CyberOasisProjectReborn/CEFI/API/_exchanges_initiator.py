import typing as tp
from abc import ABC, abstractmethod

import ccxt
from dotenv import load_dotenv


class Exchange(ABC):
    load_dotenv()

    def __init__(self):
        self.client = None
        self.name = None
        self.path_name = None
        self.initialize()

    @abstractmethod
    def initialize(self):
        pass

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
        """Factory function for creating CCXT exchange instance"""
        exchange_params = {"apiKey": public_key, "secret": secret_key}
        if passphrase:
            exchange_params["password"] = passphrase
        if options:
            exchange_params["options"] = options
        return getattr(ccxt, CCXT_exchange_name)(exchange_params)
