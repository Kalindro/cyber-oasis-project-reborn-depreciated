from dataclasses import dataclass

from CCXT.functions_mine import select_exchange_mode
from chatGPT.ask_chat import ChatGPTDialog
from general_funcs.log_config import ConfigureLoguru
from general_funcs.utils import dataframe_is_not_none_and_has_elements
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
    positivity neutrally or negatively"""

    # Don't change
    TIMEFRAME: str = "1h"
    NUMBER_OF_LAST_CANDLES: int = 1000

    def __post_init__(self):
        self.API = select_exchange_mode(self.EXCHANGE_MODE)
        self.seen_messages = set()


class NewsEnjoyer(_BaseSettings):
    def main(self):
        logger.info("News enjoyer started...")
        articles_dataframe_stream = CryptoNewsScraper().main()

        last_dataframe = None
        for new_articles_dataframe in articles_dataframe_stream:
            new_rows = self._get_new_rows(last_dataframe, new_articles_dataframe)
            unseen_articles = self._get_unseen_messages(new_rows)
            for article in unseen_articles:
                self._process_new_news(message=article)
            last_dataframe = new_articles_dataframe

    @staticmethod
    def _get_new_rows(last_dataframe, current_dataframe):
        if last_dataframe is None:
            return None
        else:
            return current_dataframe.loc[~current_dataframe.index.isin(last_dataframe.index)]

    def _get_unseen_messages(self, rows):
        return [row["message"] for index, row in rows.iterrows() if row["message"] not in self.seen_messages]

    def _process_new_news(self, message):
        print(f"New news: {message}")
        question = f"{self.GUIDING_QUESTION}: {message}"
        chat_response = ChatGPTDialog().main(question=question)
        print(f"chatGPT:\n {chat_response}")


if __name__ == "__main__":
    NewsEnjoyer().main()
