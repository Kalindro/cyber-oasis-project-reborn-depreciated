import time
from dataclasses import dataclass

import pandas as pd
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from pandas import DataFrame as df
from selenium import webdriver


@dataclass
class CryptoNewsScrapper:
    SITE_URL: str = "https://news.treeofalpha.com/"

    def main(self):
        soup = self.get_soup(site_url=self.SITE_URL)
        articles_dataframe = self.tree_of_alpha_scrapper(soup=soup)

        return articles_dataframe

    @staticmethod
    def tree_of_alpha_scrapper(soup):
        articles = soup.find_all("div", class_="contentWrapper")

        articles_dataframe = df()
        for article in articles:
            message = article.find("h2", class_="contentTitle").text.strip()
            timestamp = article.find("div", class_="originTime").text.strip()
            article_data = {"timestamp": [timestamp], "message": [message]}
            article_df = df(article_data)
            articles_dataframe = pd.concat([articles_dataframe, article_df], ignore_index=True)

        return articles_dataframe

    @staticmethod
    def get_soup(site_url: str) -> BeautifulSoup:
        user_agent = UserAgent().random
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument(f'user-agent={user_agent}')
        chrome_options.add_argument("--headless")
        driver = webdriver.Chrome(options=chrome_options)

        driver.get(site_url)
        wait_time = 0
        while True:
            if wait_time >= 10:
                break
            if driver.execute_script("return document.readyState") == "complete":
                break
            time.sleep(1)
            wait_time += 1
        html = driver.page_source
        driver.quit()
        soup = BeautifulSoup(html, "html.parser")

        return soup


if __name__ == "__main__":
    CryptoNewsScrapper().main()
