"""
Crawler implementation.
"""

import datetime
import json

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import pathlib
import shutil
from random import random
from time import sleep
from typing import Pattern, Union

import requests
from bs4 import BeautifulSoup

from core_utils.article.article import Article
from core_utils.article.io import to_meta, to_raw
from core_utils.config_dto import ConfigDTO
from core_utils.constants import ASSETS_PATH, CRAWLER_CONFIG_PATH


class IncorrectEncodingError(Exception):
    """
    Raised when encoding is corrupt.
    """


class IncorrectHeadersError(Exception):
    """
    Raised when headers value is corrupt.
    """


class IncorrectNumberOfArticlesError(Exception):
    """
    Raised when number of articles to find and parse is corrupt.
    """


class IncorrectSeedURLError(Exception):
    """
    Raised when seed URL is corrupt.
    """


class IncorrectTimeoutError(Exception):
    """
    Raised when timeout value is corrupt.
    """


class IncorrectVerifyError(Exception):
    """
    Raised when verify certificate or headless mode values are corrupt.
    """


class NumberOfArticlesOutOfRangeError(Exception):
    """
    Raised when number of articles to find and parse is out of range.
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
        self._validate_config_content()
        self.config_dto = self._extract_config_content()
        self._seed_urls = self.config_dto.seed_urls
        self._num_articles = self.config_dto.total_articles
        self._headers = self.config_dto.headers
        self._encoding = self.config_dto.encoding
        self._timeout = self.config_dto.timeout
        self._should_verify_certificate = self.config_dto.should_verify_certificate
        self._headless_mode = self.config_dto.headless_mode

    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        with open(self.path_to_config, 'r', encoding='utf-8') as file:
            config = json.load(file)
        return ConfigDTO(**config)

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        config = self._extract_config_content()
        if (not config.seed_urls or not isinstance(config.seed_urls, list)
                or not all(isinstance(url, str) for url in config.seed_urls)
                or not all('https://tomsk-novosti.ru/' in url for url in config.seed_urls)):
            raise IncorrectSeedURLError('Seed URLs value is corrupt.')
        if (not isinstance(config.total_articles, int) or isinstance(config.total_articles, bool)
                or config.total_articles < 0):
            raise IncorrectNumberOfArticlesError('Number of articles value is corrupt.')
        if config.total_articles >= 150:
            raise NumberOfArticlesOutOfRangeError('Number of articles is out of range (1-150).')
        if not config.encoding or not isinstance(config.encoding, str):
            raise IncorrectEncodingError('Encoding is corrupt.')
        if not config.headers or not isinstance(config.headers, dict):
            raise IncorrectHeadersError('Headers value is corrupt.')
        if (not isinstance(config.timeout, int) or isinstance(config.timeout, bool)
                or not 0 < config.timeout <= 60):
            raise IncorrectTimeoutError('Timeout value is either corrupt or out of range (0-60).')
        if not isinstance(config.should_verify_certificate, bool):
            raise IncorrectVerifyError('Verify certificate value is corrupt.')
        if not isinstance(config.headless_mode, bool):
            raise IncorrectVerifyError('Headless mode value is corrupt.')

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
    response = requests.get(url, headers=config.get_headers(),
                            verify=config.get_verify_certificate(), timeout=config.get_timeout())
    response.encoding = config.get_encoding()
    sleep(random())
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
        self.urls = []
        self.config = config

    def _extract_url(self, article_bs: BeautifulSoup) -> str:
        """
        Find and retrieve url from HTML.

        Args:
            article_bs (bs4.BeautifulSoup): BeautifulSoup instance

        Returns:
            str: Url from HTML
        """
        post_class = article_bs.find_all('a', class_='post-thumbnail')
        for post in post_class:
            url = post['href']
            if url:
                return url
        return ''

    def find_articles(self) -> None:
        """
        Find articles.
        """
        for seed_url in self.get_search_urls():
            response = make_request(seed_url, self.config)
            if not response.ok:
                continue
            soup = BeautifulSoup(response.text, 'lxml')
            post_class = soup.find_all('a', class_='post-thumbnail')
            for post in post_class:
                url = post['href']
                if url:
                    if len(self.urls) < self.config.get_num_articles():
                        if url not in self.urls:
                            self.urls.append(url)

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
        self.article = Article(url=full_url, article_id=article_id)
        self.config = config

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        texts = article_soup.find_all('p')
        self.article.text = ' '.join([text.text for text in texts])

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        self.article.title = article_soup.find('title').text
        self.article.author = article_soup.find('a', class_='url fn n').text
        date = article_soup.find('time', class_='entry-date').attrs['datetime']
        self.article.date = self.unify_date_format(date)
        self.article.topics = article_soup.find('span', class_='cat-links').text.split(', ')

    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """
        date = date_str.split('T')
        return datetime.datetime.strptime(' '.join(date), '%Y-%m-%d %H:%M:%S%z')

    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """
        soup = BeautifulSoup(make_request(self.article.url, self.config).text, 'lxml')
        self._fill_article_with_text(soup)
        self._fill_article_with_meta_information(soup)
        return self.article


def prepare_environment(base_path: Union[pathlib.Path, str]) -> None:
    """
    Create ASSETS_PATH folder if no created and remove existing folder.

    Args:
        base_path (Union[pathlib.Path, str]): Path where articles stores
    """
    if base_path.is_dir():
        shutil.rmtree(base_path)
    base_path.mkdir(parents=True)


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
