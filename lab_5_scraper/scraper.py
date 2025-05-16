"""
Crawler implementation.
"""

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import datetime
import json
import pathlib
import shutil
from random import randint
from time import sleep
from typing import Pattern, Union

import requests
from bs4 import BeautifulSoup

from core_utils.article.article import Article
from core_utils.article.io import to_meta, to_raw
from core_utils.config_dto import ConfigDTO
from core_utils.constants import ASSETS_PATH, CRAWLER_CONFIG_PATH


class IncorrectSeedURLError(Exception):
    """
    Urls don't match the standard pattern
    """


class NumberOfArticlesOutOfRangeError(Exception):
    """
    Number of articles is out of range
    """


class IncorrectNumberOfArticlesError(Exception):
    """
    Number of articles is wrong
    """


class IncorrectHeadersError(Exception):
    """
    Headers are not in a dictionary
    """


class IncorrectEncodingError(Exception):
    """
    Encoding is wrong
    """


class IncorrectTimeoutError(Exception):
    """
    Timeout value incorrect
    """


class IncorrectVerifyError(Exception):
    """
    Verify certificate is wrong
    """


class Config:
    """
    Class for unpacking and validating configurations.
    """

    def __init__(self, path_to_config: pathlib.Path) -> None:
        """
        Initialize an instance of the Config class.

        Args:
            path_to_config (pathlib.Path): Path to configuration.
        """
        self.path_to_config = path_to_config
        self.config = self._extract_config_content()
        self._validate_config_content()
        self._seed_urls = self.config.seed_urls
        self._num_articles = self.config.total_articles
        self._headers = self.config.headers
        self._encoding = self.config.encoding
        self._timeout = self.config.timeout
        self._should_verify_certificate = self.config.should_verify_certificate
        self._headless_mode = self.config.headless_mode

    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        with open(self.path_to_config, "r", encoding="utf-8") as config:
            data = json.load(config)
        return ConfigDTO(**data)

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        if (
            not self.config.seed_urls
            or not isinstance(self.config.seed_urls, list)
            or not all(isinstance(url, str) for url in self.config.seed_urls)
            or not all("https://tuvapravda.ru/" in url for url in self.config.seed_urls)
        ):
            raise IncorrectSeedURLError("Something is wrong with the urls")
        if (
            not isinstance(self.config.total_articles, int)
            or self.config.total_articles < 0
            or isinstance(self.config.total_articles, bool)
        ):
            raise IncorrectNumberOfArticlesError("Not correct n of articles")
        if self.config.total_articles > 150:
            raise NumberOfArticlesOutOfRangeError("N of articles out of range")
        if not isinstance(self.config.headers, dict):
            raise IncorrectHeadersError("Headers are incorrect")
        if not isinstance(self.config.encoding, str):
            raise IncorrectEncodingError("Encoding is incorrect")
        if not isinstance(self.config.timeout, int) or not 0 < self.config.timeout <= 60:
            raise IncorrectTimeoutError("The timings are wrong")
        if not isinstance(self.config.should_verify_certificate, bool):
            raise IncorrectVerifyError("Verify is not a bool")
        if not isinstance(self.config.headless_mode, bool):
            raise IncorrectVerifyError("Headless mode is not bool")

    def get_seed_urls(self) -> list[str]:
        """
        Retrieve seed urls.

        Returns:
            list[str]: Seed urls
        """
        return self._seed_urls

    def get_num_articles(self) -> int:
        """
        Retrieve total number of articles to scrape.

        Returns:
            int: Total number of articles to scrape
        """
        return self._num_articles

    def get_headers(self) -> dict[str, str]:
        """
        Retrieve headers to use during requesting.

        Returns:
            dict[str, str]: Headers
        """
        return self._headers

    def get_encoding(self) -> str:
        """
        Retrieve encoding to use during parsing.

        Returns:
            str: Encoding
        """
        return self._encoding

    def get_timeout(self) -> int:
        """
        Retrieve number of seconds to wait for response.

        Returns:
            int: Number of seconds to wait for response
        """
        return self._timeout

    def get_verify_certificate(self) -> bool:
        """
        Retrieve whether to verify certificate.

        Returns:
            bool: Whether to verify certificate or not
        """
        return self._should_verify_certificate

    def get_headless_mode(self) -> bool:
        """
        Retrieve whether to use headless mode.

        Returns:
            bool: Whether to use headless mode or not
        """
        return self._headless_mode


def make_request(url: str, config: Config) -> requests.models.Response:
    """
    Deliver a response from a request with given configuration.

    Args:
        url (str): Site url
        config (Config): Configuration

    Returns:
        requests.models.Response: A response from a request
    """
    sleep(randint(1, 5))
    response = requests.get(
        url,
        headers=config.get_headers(),
        verify=config.get_verify_certificate(),
        timeout=config.get_timeout(),
    )
    response.encoding = config.get_encoding()
    return response


class Crawler:
    """
    Crawler implementation.
    """

    #: Url pattern
    url_pattern: Union[Pattern, str]

    def __init__(self, config: Config) -> None:
        """
        Initialize an instance of the Crawler class.

        Args:
            config (Config): Configuration
        """
        self.config = config
        self.urls = []

    def _extract_url(self, article_bs: BeautifulSoup) -> str:
        """
        Find and retrieve url from HTML.

        Args:
            article_bs (bs4.BeautifulSoup): BeautifulSoup instance

        Returns:
            str: Url from HTML
        """
        href = article_bs.get("href")
        if isinstance(href, str):
            real_link = f"https://tuvapravda.ru{href}"
            return real_link
        return ""

    def find_articles(self) -> None:
        """
        Find articles.
        """
        urls = self.get_search_urls()
        for url in urls:
            if len(self.urls) >= self.config.get_num_articles():
                break
            response = make_request(url, self.config)
            if not response.ok:
                continue
            bs = BeautifulSoup(response.text, "lxml")
            news = bs.find_all("a", class_="news-card__link-to-article")
            for article in news:
                if len(self.urls) >= self.config.get_num_articles():
                    break
                extracted_url = self._extract_url(article)
                if extracted_url and extracted_url not in self.urls:
                    self.urls.append(extracted_url)

    def get_search_urls(self) -> list:
        """
        Get seed_urls param.

        Returns:
            list: seed_urls param
        """
        return self.config.get_seed_urls()


# 10
# 4, 6, 8, 10


class HTMLParser:
    """
    HTMLParser implementation.
    """

    def __init__(self, full_url: str, article_id: int, config: Config) -> None:
        """
        Initialize an instance of the HTMLParser class.

        Args:
            full_url (str): Site url
            article_id (int): Article id
            config (Config): Configuration
        """
        self.full_url = full_url
        self.article_id = article_id
        self.config = config
        self.article = Article(url=full_url, article_id=article_id)

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        news = article_soup.find("div", class_="text-content")
        text = []
        for i in news:
            if i.get_text().strip():
                text.append(i.get_text(strip=True, separator="\n"))
        self.article.text = "\n".join(text)

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        self.article.title = article_soup.find("h1", class_="article__title").text.strip()
        date = json.loads(article_soup.find('script', type='application/ld+json')
                          .text)["@graph"][1]['datePublished']
        self.article.date = self.unify_date_format(date)
        if 'https://tuvapravda.ru/natsionalnye-proekty/' in self.article.url:
            self.article.topics = 'Национальные проекты'
        elif 'https://tuvapravda.ru/novosti/' in self.article.url:
            self.article.topics = 'Новости'
        elif 'https://tuvapravda.ru/fotofakt/' in self.article.url:
            self.article.topics = 'Фотофакт'
        else:
            self.article.topics = 'NOT FOUND'
        self.article.author = [article_soup.find("span", class_="author__name").text]

    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """
        nice_date = date_str.split("T")
        return datetime.datetime.strptime(" ".join(nice_date), "%Y-%m-%d %H:%M:%S")

    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """
        response = make_request(self.full_url, self.config)
        if response.ok:
            bs = BeautifulSoup(response.text, "lxml")
            self._fill_article_with_text(bs)
            self._fill_article_with_meta_information(bs)
        return self.article


def prepare_environment(base_path: Union[pathlib.Path, str]) -> None:
    """
    Create ASSETS_PATH folder if no created and remove existing folder.

    Args:
        base_path (Union[pathlib.Path, str]): Path where articles stores
    """
    if pathlib.Path(base_path).exists():
        shutil.rmtree(base_path)
    pathlib.Path(base_path).mkdir(parents=True)


def main() -> None:
    """
    Entrypoint for scrapper module.
    """
    config = Config(CRAWLER_CONFIG_PATH)
    prepare_environment(ASSETS_PATH)
    crawler = Crawler(config)
    crawler.find_articles()
    for article_id, url in enumerate(crawler.urls, 1):
        parser = HTMLParser(url, article_id=article_id, config=config)
        article = parser.parse()
        if not article:
            continue
        if isinstance(article, Article):
            to_raw(article)
            to_meta(article)


if __name__ == "__main__":
    main()
