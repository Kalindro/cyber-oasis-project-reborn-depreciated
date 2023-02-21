from dataclasses import dataclass

from CCXT.functions_mine import select_exchange_mode
from chatGPT.ask_chat import ChatGPTDialog
from webscraper.crypto_news_scraper import CryptoNewsScraper


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
        stream = CryptoNewsScraper().main()

        for news in stream:
            for index, row in news.iterrows():
                single_news = row["message"]
                print(f"News: {single_news}")
                question = f"Tell me if this news is positive, neutral or negative {single_news}"
                chat_response = ChatGPTDialog().main(question=question)
                print(chat_response)


if __name__ == "__main__":
    NewsEnjoyer().main()
