import typing as tp
from dataclasses import dataclass

import pandas as pd
from pandas import DataFrame as df

from CCXT.funcs_select_mode import select_exchange_mode
from chatGPT.ask_chat import ChatGPTDialog
from utils.log_config import ConfigureLoguru
from utils.utils import dataframe_is_not_none_and_not_empty
from webscraper.crypto_news_scraper import CryptoNewsScraper

logger = ConfigureLoguru().info_level()


@dataclass
class _BaseSettings:
    """
    Modes available:
    :EXCHANGE_MODE: 1 - Binance Spot; 2 - Binance Futures; 3 - Kucoin Spot
    :PAIRS_MODE: 1 - Test single; 2 - Test multi; 3 - BTC; 4 - USDT
    """
    EXCHANGE_MODE: int = 1
    PAIRS_MODE: int = 4
    GUIDING_QUESTION = """This is crypto market news. Tell me in one word if this will affect price 
    positive, neutral or negative"""

    # Don't change
    TIMEFRAME: str = "1h"
    NUMBER_OF_LAST_CANDLES: int = 1000

    def __init__(self):
        self.API = select_exchange_mode(self.EXCHANGE_MODE)
        self.old_rows = df()


class NewsEnjoyer(_BaseSettings):
    """Acquire and process news"""

    def main(self):
        """Main starting functon"""
        logger.info("News enjoyer started...")
        articles_dataframe_stream = CryptoNewsScraper().main()

        last_dataframe = None
        for articles_dataframe in articles_dataframe_stream:
            self._check_if_soup_works(articles_dataframe)
            new_rows = self._get_new_rows(last_dataframe=last_dataframe, new_dataframe=articles_dataframe)
            if dataframe_is_not_none_and_not_empty(new_rows):
                unseen_rows = self._get_unseen_rows(new_rows)
                unseen_rows["sentiment"] = self._process_new_news(message=unseen_rows["message"])
                self.old_rows = pd.concat([self.old_rows, unseen_rows])
            last_dataframe = articles_dataframe

    def _get_new_rows(self, last_dataframe: pd.DataFrame, new_dataframe: pd.DataFrame) -> tp.Union[None, pd.DataFrame]:
        """Return only new rows (unique rows between old and new df)"""
        if last_dataframe is None:
            self.old_rows = new_dataframe
            return None
        else:
            return new_dataframe.loc[~new_dataframe["message"].isin(last_dataframe["message"])]

    @staticmethod
    def _check_if_soup_works(articles_dataframe: pd.DataFrame) -> None:
        """Just a check if scrapper not banned"""
        if not dataframe_is_not_none_and_not_empty(articles_dataframe):
            logger.warning("Soup incident")

    def _get_unseen_rows(self, fresh_rows: pd.DataFrame) -> pd.DataFrame:
        """Return rows only if message wasn't seen before"""
        unseen_rows = fresh_rows.loc[~fresh_rows["message"].isin(self.old_rows["message"])]

        return unseen_rows

    def _process_new_news(self, message: str) -> str:
        """Steps to take when there is a new news"""
        logger.info(f"New news: {message}")
        question = f"{self.GUIDING_QUESTION}: {message}"
        chat_response = ChatGPTDialog().main(question=question)
        logger.success(f"chatGPT: {chat_response}")

        return chat_response


if __name__ == "__main__":
    NewsEnjoyer().main()
