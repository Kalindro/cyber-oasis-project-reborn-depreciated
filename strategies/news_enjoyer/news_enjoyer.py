from dataclasses import dataclass

import pandas as pd

from CCXT.functions_mine import select_exchange_mode
from chatGPT.ask_chat import ChatGPTDialog
from general_funcs.log_config import ConfigureLoguru
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

    # Don't change
    TIMEFRAME: str = "1h"
    NUMBER_OF_LAST_CANDLES: int = 1000

    def __post_init__(self):
        self.API = select_exchange_mode(self.EXCHANGE_MODE)


class NewsEnjoyer:
    def main(self):
        logger.info("News enjoyer started...")
        articles_dataframe_stream = CryptoNewsScraper().main()
        latest_datetime = pd.to_datetime("01.01.2100")
        for articles_dataframe in articles_dataframe_stream:
            fresh_row = articles_dataframe.iloc[0]
            fresh_datetime = fresh_row["timestamp"]
            fresh_news = fresh_row["message"]
            if fresh_datetime > latest_datetime:
                print(f"New news: {fresh_news}")
                question = f"Tell me if this news is positive, neutral or negative {fresh_news}"
                chat_response = ChatGPTDialog().main(question=question)
                print(chat_response)
            latest_datetime = articles_dataframe["timestamp"].max()


if __name__ == "__main__":
    NewsEnjoyer().main()
