import random
import time
from dataclasses import dataclass
from time import perf_counter

import pandas as pd
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from pandas import DataFrame as df
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from utils.log_config import ConfigureLoguru
from utils.utils import clean_string_from_spaces_and_links

logger = ConfigureLoguru().info_level()


@dataclass
class CryptoNewsScraper:
    SITE_URL: str = "https://news.treeofalpha.com/"

    def main(self) -> pd.DataFrame:
        my_driver = self._initiate_driver()

        while True:
            perf_start = perf_counter()

            soup = self._get_soup(driver=my_driver, site_url=self.SITE_URL)
            articles_dataframe = self._tree_of_alpha_scraper(soup=soup)

            perf_stop = perf_counter()
            execution_time = perf_stop - perf_start

            yield articles_dataframe

            sleep_time = random.uniform(1, 2) - execution_time
            if sleep_time > 0:
                time.sleep(sleep_time)

    @staticmethod
    def _tree_of_alpha_scraper(soup: BeautifulSoup) -> pd.DataFrame:
        articles = soup.find_all("div", class_="contentWrapper")

        articles_dataframe = df()
        for article in articles:
            message = article.find("h2", class_="contentTitle").text.strip().upper()
            if message.endswith("..."):
                message = message[:-3].rstrip()
            if message.endswith(","):
                message = message[:-1].rstrip()
            message = clean_string_from_spaces_and_links(message)

            timestamp = article.find("div", class_="originTime").text.strip().replace("CET", "")
            article_data = {"message": [message]}
            article_df = df(article_data, index=[pd.to_datetime(timestamp)])
            articles_dataframe = pd.concat([articles_dataframe, article_df], ignore_index=False)

        return articles_dataframe

    @staticmethod
    def _get_soup(site_url: str, driver: webdriver) -> BeautifulSoup:
        driver.get(site_url)

        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "contentWrapper")))

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        return soup

    @staticmethod
    def _initiate_driver() -> webdriver:
        user_agent = UserAgent().random
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument(f'user-agent={user_agent}')
        chrome_options.add_argument("--headless")
        driver = webdriver.Chrome(options=chrome_options)

        return driver


if __name__ == "__main__":
    while True:
        scrap_stream = CryptoNewsScraper().main()
        for data in scrap_stream:
            print("Dataframe:")
            print(data.to_string())
