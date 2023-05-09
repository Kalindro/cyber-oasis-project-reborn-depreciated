import typing as tp

import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from pandas import DataFrame as df

from _depreciated.chatGPT.ask_chat import ask_question_API
from exchange.fundamental_template import FundamentalTemplate
from utils.log_config import ConfigureLoguru
from utils.utils import dataframe_is_not_none_and_not_empty, excel_save_formatted_naive
from _depreciated.webscraper.crypto_news_scraper import CryptoNewsScraper

logger = ConfigureLoguru().info_level()


class _BaseTemplate(FundamentalTemplate):
    def __init__(self):
        self.EXCHANGE_MODE: int = 1
        self.PAIRS_MODE: int = 4
        super().__init__(exchange_mode=self.EXCHANGE_MODE, pairs_mode=self.PAIRS_MODE)

        self.TIMEFRAME = "1h"
        self.NUMBER_OF_LAST_CANDLES = 1000

        self.GUIDING_QUESTION = """This is crypto market news. Tell me in one word if this will affect price 
        positive, neutral or negative"""
        self.old_rows = df()


class NewsEnjoyer(_BaseTemplate):
    """Acquire and process news"""

    def main(self):
        """Main starting functon"""
        logger.info("News enjoyer started...")
        scheduler = BackgroundScheduler()
        scheduler.add_job(self._save_dataframe, "interval", minutes=5)
        scheduler.start()

        articles_dataframe_stream = CryptoNewsScraper().main()
        last_dataframe = None
        for articles_dataframe in articles_dataframe_stream:
            self._check_if_soup_works(articles_dataframe)
            new_rows = self._get_new_rows(last_dataframe=last_dataframe, new_dataframe=articles_dataframe)
            if dataframe_is_not_none_and_not_empty(new_rows):
                unseen_rows = self._get_unseen_rows(new_rows)
                if dataframe_is_not_none_and_not_empty(unseen_rows):
                    unseen_rows["sentiment"] = self._process_new_news(message=unseen_rows["message"])
                    self.old_rows = pd.concat([self.old_rows, unseen_rows])

            last_dataframe = articles_dataframe

    @staticmethod
    def _check_if_soup_works(articles_dataframe: pd.DataFrame) -> None:
        """Just a check if scrapper not banned"""
        if not dataframe_is_not_none_and_not_empty(articles_dataframe):
            logger.warning("Soup incident")

    def _get_new_rows(self, last_dataframe: pd.DataFrame, new_dataframe: pd.DataFrame) -> tp.Union[None, pd.DataFrame]:
        """Return only new rows (unique rows between old and new df)"""
        if last_dataframe is None:
            self.old_rows = new_dataframe
            self.old_rows["sentiment"] = "NA"
            return None
        else:
            return new_dataframe.loc[~new_dataframe["message"].isin(last_dataframe["message"])]

    def _get_unseen_rows(self, fresh_rows: pd.DataFrame) -> pd.DataFrame:
        """Return rows only if message wasn't seen before"""
        unseen_rows = fresh_rows.loc[~fresh_rows["message"].isin(self.old_rows["message"])]

        return unseen_rows

    def _process_new_news(self, message: str) -> str:
        """Steps to take when there is a new news"""
        logger.info(f"New news: {message}")
        question = f"{self.GUIDING_QUESTION}: {message}"
        chat_response = ask_question_API(question=question)
        logger.success(f"chatGPT: {chat_response}")

        return chat_response

    def _save_dataframe(self) -> None:
        excel_save_formatted_naive(dataframe=self.old_rows, filename="old_news.xlsx", global_cols_size=20, str_cols="B:B",
                                   str_cols_size=155)


if __name__ == "__main__":
    NewsEnjoyer().main()
