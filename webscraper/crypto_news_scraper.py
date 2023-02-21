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


@dataclass
class CryptoNewsScraper:
    SITE_URL: str = "https://news.treeofalpha.com/"

    def main(self) -> pd.DataFrame:
        my_driver = self._initiate_driver()

        while True:
            perf_start = perf_counter()

            soup = self._get_soup(driver=my_driver, site_url=self.SITE_URL)
            articles_dataframe = self._tree_of_alpha_scraper(soup=soup)

            print(articles_dataframe)
            perf_stop = perf_counter()
            print(f"Time to execute: {perf_stop - perf_start:.2f}")

    @staticmethod
    def _tree_of_alpha_scraper(soup: BeautifulSoup) -> pd.DataFrame:
        articles = soup.find_all("div", class_="contentWrapper")

        articles_dataframe = df()
        for article in articles:
            message = article.find("h2", class_="contentTitle").text.strip()
            timestamp = article.find("div", class_="originTime").text.strip()
            article_data = {"timestamp": [pd.to_datetime(timestamp.replace("CET", ""))], "message": [message]}
            article_df = df(article_data)
            articles_dataframe = pd.concat([articles_dataframe, article_df], ignore_index=True)

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
        x = CryptoNewsScraper().main()
        print("Dataframe:")
        print(x.to_string())
