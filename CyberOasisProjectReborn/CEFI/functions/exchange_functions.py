from __future__ import annotations

import typing as tp
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import TYPE_CHECKING

import pandas as pd
from pandas import DataFrame as df

from CyberOasisProjectReborn.CEFI.functions.get_history import GetFullHistory
from CyberOasisProjectReborn.utils.logger_custom import default_logger as logger

if TYPE_CHECKING:
    from CyberOasisProjectReborn.CEFI.exchange.exchanges import Exchange


class ExchangeFunctions:
    """Base abstract class to create specific exchange client"""

    def __init__(self, exchange: Exchange):
        self.exchange = exchange
        self.exchange_client = exchange.exchange_client
        self.exchange_name = exchange.exchange_name

    # ############# Basic ############# #

    def get_exchange_timestamp(self) -> str:
        """Get exchange time for timezone setting"""
        exchange_status = self.exchange_client.fetch_status()
        exchange_timestamp = exchange_status["updated"]
        return exchange_timestamp

    def get_pairs_with_precisions_status(self) -> pd.DataFrame:
        """Get exchange pairs with trading precisions and active status"""
        logger.info("Getting pairs with precisions and status...")
        pairs_precisions_status_df = self.exchange_client.fetch_markets()
        pairs_precisions_status_df = df(pairs_precisions_status_df,
                                        columns=["symbol", "base", "quote", "active", "precision", "limits"])
        pairs_precisions_status_df = pairs_precisions_status_df.astype({"active": str})
        pairs_precisions_status_df.set_index("symbol", inplace=True)
        logger.debug("Pairs with precisions and status completed, returning")
        return pairs_precisions_status_df

    def get_pairs_prices(self) -> pd.DataFrame:
        """Get exchange pairs with current prices"""
        logger.info("Getting pairs prices...")
        raw_pairs = self.exchange_client.fetch_tickers()
        pairs_prices_df = df.from_dict(raw_pairs, orient="index", columns=["average"])
        pairs_prices_df.rename(columns={"average": "price"}, inplace=True)
        pairs_prices_df.index = pairs_prices_df.index.to_series().apply(
            lambda x: x.split(':')[0] if ":" in x else x)
        logger.debug("Pairs prices completed, returning")
        return pairs_prices_df

    # ############# Pairs lists ############# #

    def get_pairs_list_test_single(self) -> list[str]:
        """Get single pair list"""
        return ["BTC/USDT"]

    def get_pairs_list_test_multi(self) -> list[str]:
        """Get few pairs list"""
        return ["NEO/USDT", "BTC/USDT", "ETH/USDT", "BNB/USDT", "LTC/USDT"]

    def get_pairs_list_BTC(self) -> list[str]:
        """Get BTC pairs list"""
        logger.info("Getting BTC pairs list...")
        desired_quote = ("/BTC", ":BTC")
        pairs_list = self._get_pairs_list_base(desired_quote)
        logger.debug("Pairs list completed, returning")
        return pairs_list

    def get_pairs_list_USDT(self) -> list[str]:
        """Get USDT pairs list"""
        logger.info("Getting USDT pairs list...")
        desired_quote = ("/USDT", ":USDT")
        pairs_list = self._get_pairs_list_base(desired_quote)
        logger.debug("Pairs list completed, returning")
        return pairs_list

    def get_pairs_list_ALL(self) -> list[str]:
        """Get ALL pairs list"""
        logger.info("Getting ALL pairs list...")
        desired_quote = ("/USDT", ":USDT", "/BTC", ":BTC", "/ETH", ":ETH")
        pairs_list = self._get_pairs_list_base(desired_quote)
        logger.debug("Pairs list completed, returning")
        return pairs_list

    def _get_pairs_list_base(self, desired_quote: tuple):
        """Base functions for retrieving pairs"""
        pairs_precisions_status = self.get_pairs_with_precisions_status()
        pairs_precisions_status = pairs_precisions_status[pairs_precisions_status["active"] == "True"]
        pairs_list_original = list(pairs_precisions_status.index)
        pairs_list_original = self._remove_shit_from_pairs_list(pairs_list_original)
        pairs_list = [str(pair) for pair in pairs_list_original if str(pair).endswith(desired_quote)]
        pairs_list_final = []
        [pairs_list_final.append(pair) for pair in pairs_list if pair not in pairs_list_final]
        return pairs_list_final

    @staticmethod
    def _remove_shit_from_pairs_list(pairs_list: list[str]):
        """Remove bs pairs from pairs list"""
        forbidden_symbols_lying = ("LUNA", "FTT", "DREP")
        forbidden_symbols_fiat = ("EUR", "USD", "GBP", "AUD", "NZD", "CNY", "JPY", "CAD", "CHF")
        forbidden_symbols_stables = (
            "USDC", "USDT", "USDP", "TUSD", "BUSD", "DAI", "USDO", "FRAX", "USDD", "GUSD", "LUSD", "USTC")
        forbidden_symbols = forbidden_symbols_lying + forbidden_symbols_fiat + forbidden_symbols_stables
        forbidden_symbol_ending = ("UP", "DOWN", "BEAR", "BULL")

        def get_symbol(pair): return str(pair).split("/")[0]

        pairs_list = [str(pair) for pair in pairs_list if not get_symbol(pair) in forbidden_symbols]
        pairs_list = [str(pair) for pair in pairs_list if not get_symbol(pair).endswith(forbidden_symbol_ending)]
        return pairs_list

    # ############# Leverage ############# #

    def change_leverage_and_mode_for_whole_exchange(self, leverage: int, isolated: bool):
        """Change leverage and margin mode on all exchange pairs"""
        logger.info("Changing leverage and margin mode on all pairs on exchange")
        pairs_list = self.get_pairs_list_ALL()
        self.change_leverage_and_mode_for_pairs_list(leverage, pairs_list, isolated)
        logger.success("Finished changing leverage and margin mode on all")

    def change_leverage_and_mode_for_pairs_list(self, leverage: int, pairs_list: list[str], isolated: bool):
        """Change leverage and margin mode on all pairs on list"""
        with ThreadPoolExecutor(max_workers=2) as executor:
            change_lev_partial = partial(self.change_leverage_and_mode_one_pair, leverage=leverage, isolated=isolated)
            output = dict(zip(pairs_list, executor.map(change_lev_partial, pairs_list)))

    def change_leverage_and_mode_one_pair(self, pair: str, leverage: int, isolated: bool):
        """Change leverage and margin mode for one pairs_list"""
        mmode = "ISOLATED" if isolated else "CROSS"

        logger.info(f"Changing leverage and margin for {pair}")
        if "bybit" in self.exchange_name.lower():
            try:
                self.exchange_client.set_leverage(leverage, pair)
                self.exchange_client.set_margin_mode(mmode, pair, params={"leverage": leverage})
            except Exception as err:
                if "not modified" in str(err):
                    pass
                else:
                    print(err)
        elif "okx" in self.exchange_name.lower():
            try:
                self.exchange_client.set_leverage(leverage, pair)
                self.exchange_client.set_margin_mode(mmode, pair, params={"lever": leverage})
            except Exception as err:
                if "Margin trading is not supported" in str(err) or "Leverage exceeds" in str(err):
                    pass
                else:
                    print(err)
        else:
            self.exchange_client.set_leverage(leverage, pair)
            self.exchange_client.set_margin_mode(mmode, pair)

        logger.info(f"{pair} leverage changed to {leverage}, margin mode to {mmode}")

    # ############# History ############# #

    def get_history(self,
                    pairs_list: list[str],
                    timeframe: str,
                    min_data_length: int = 10,
                    vol_quantile_drop: float = None,
                    save_load_history: bool = False,
                    number_of_last_candles: tp.Optional[int] = None,
                    start: tp.Optional[str] = None,
                    end: tp.Optional[str] = None):

        return GetFullHistory(self.exchange, pairs_list, timeframe, min_data_length, vol_quantile_drop,
                              save_load_history, number_of_last_candles, start, end).get_full_history()
