from selenium import webdriver
from bs4 import BeautifulSoup


def tree_of_alpha_scrapper():
    url = "https://news.treeofalpha.com/"

    driver = webdriver.Chrome()

    driver.get(url)
    driver.implicitly_wait(100)
    html = driver.page_source
    driver.quit()

    soup = BeautifulSoup(html, "html.parser")

    articles = soup.find_all("div", class_="contentWrapper")

    for article in articles:
        message = article.find("h2", class_="contentTitle").text.strip()
        timestamp = article.find("div", class_="originTime").text.strip()
        print(f"{timestamp}: {message}")
