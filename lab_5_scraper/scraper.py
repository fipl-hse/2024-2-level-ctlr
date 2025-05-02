"""
Crawler implementation.
"""

import datetime

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import json
import pathlib
import re
import shutil
from typing import Pattern, Union
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from core_utils.article.article import Article
from core_utils.article.io import to_meta, to_raw
from core_utils.config_dto import ConfigDTO
from core_utils.constants import ASSETS_PATH, CRAWLER_CONFIG_PATH


class IncorrectSeedURLError(Exception):
    """
    Raised when seed URL does not match standard pattern "https?://(www.)?".
    """


class NumberOfArticlesOutOfRangeError(Exception):
    """
    Raised when number of articles is out of range from 1 to 150.
    """


class IncorrectNumberOfArticlesError(Exception):
    """
    Raised when total number of articles to parse is not integer or less than 0.
    """


class IncorrectHeadersError(Exception):
    """
    Raised when headers are not in a form of dictionary.
    """


class IncorrectEncodingError(Exception):
    """
    Raised when encoding is not in a form of string.
    """


class IncorrectTimeoutError(Exception):
    """
    Raised when timeout value is not a positive integer or more than 60.
    """


class IncorrectVerifyError(Exception):
    """
    Raised when verify certificate value is neither true nor false.
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
        config = self._extract_config_content()
        self._seed_urls = config.seed_urls
        self._num_articles = config.total_articles
        self._headers = config.headers
        self._encoding = config.encoding
        self._timeout = config.timeout
        self._should_verify_certificate = config.should_verify_certificate
        self._headless_mode = config.headless_mode
        self._validate_config_content()

    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        with open(self.path_to_config, encoding='utf-8') as file:
            config_data = json.load(file)
        if not isinstance(config_data, dict):
            raise TypeError('Config_data must be a dictionary')
        config = ConfigDTO(**config_data)
        return config

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        if (not isinstance(self._seed_urls, list)
                or not all(isinstance(url, str) for url in self._seed_urls)
                or not all(url.startswith('https://kgd.ru/news/biz') for url in self._seed_urls)):
            raise (IncorrectSeedURLError
                   ('Seed URL does not match standard pattern "https?://(www.)?"'))
        if (not isinstance(self._num_articles, int) or isinstance(self._num_articles, bool)
                or self._num_articles < 0):
            raise (IncorrectNumberOfArticlesError
                   ('Total number of articles to parse is not integer or less than 0'))
        if not 1 <= self._num_articles <= 150:
            raise (NumberOfArticlesOutOfRangeError
                   ('Total number of articles is out of range from 1 to 150'))
        if not isinstance(self._headers, dict):
            raise IncorrectHeadersError('Headers are not in a form of dictionary')
        if not isinstance(self._encoding, str):
            raise IncorrectEncodingError('Encoding is not in a form of string')
        if (not isinstance(self._timeout, int)
                or not 0 < self._timeout <= 60):
            raise IncorrectTimeoutError('Timeout value must be a positive integer less than 60')
        if (not isinstance(self._should_verify_certificate, bool)
                or not isinstance(self._headless_mode, bool)):
            raise IncorrectVerifyError('Verify certificate value must True or False')

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
    headers = config.get_headers()
    timeout = config.get_timeout()
    verify = config.get_verify_certificate()
    return requests.get(url, headers=headers, timeout=timeout, verify=verify)


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
        base_url = 'https://kgd.ru'
        url_pattern = re.compile(r'(?:https?://kgd\.ru)?/news/biz/(?:$|[^\s]+)')

        extracted_urls = []
        for link in article_bs.find_all('a', href=True):
            href = link['href'].strip()
            if not href:
                continue
            extracted_urls.append(href)

        for url in extracted_urls:
            if isinstance(url, str):
                if not url.startswith('http'):
                    url = urljoin(base_url, url)
                if url_pattern.fullmatch(url) and url not in self.urls:
                    return url
        return ''

    def find_articles(self) -> None:
        """
        Find articles.
        """
        seed_urls = self.config.get_seed_urls()
        max_num_articles = self.config.get_num_articles()

        for url in seed_urls:

            try:
                response = make_request(url, self.config)
                if not response.ok:
                    continue

                article_bs = BeautifulSoup(response.text, 'lxml')
                if len(self.urls) < max_num_articles:
                    while True:
                        article_url = self._extract_url(article_bs)
                        if article_url == '':
                            break
                        if article_url not in self.urls:
                            self.urls.append(article_url)
                else:
                    break

            except requests.exceptions.RequestException:
                continue

    def get_search_urls(self) -> list:
        """
        Get seed_urls param.

        Returns:
            list: seed_urls param
        """
        seed_urls = self.config.get_seed_urls()
        return seed_urls


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
        self.article = Article(full_url, article_id)

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        block = article_soup.find('div', {'class': 'itemFullText'})
        article_texts = block.find_all('p')
        article_texts = [el.text for el in article_texts]
        self.article.text = '\n'.join(article_texts)

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        title = article_soup.find('h1', {'class': 'itemTitle'})
        self.article.title = title.text if title else 'NOT FOUND'

        authors = article_soup.find_all('div', {'class': 'itemAuthorName'})
        self.article.author = [a.get_text(strip=True) for author in authors
                               for a in author.find_all('a')] if authors else ['NOT FOUND']

        date = article_soup.find('span', {'class': 'itemDateCreated'})
        self.article.date = self.unify_date_format(date.text.strip())

        topics = article_soup.find_all('span', {'class': 'itemCategory'})
        self.article.topics = [a.get_text(strip=True) for topic in topics
                               for a in topic.find_all('a')] if topics else []

    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """
        return datetime.datetime.strptime(date_str, "%d.%m.%Y %H:%M")

    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """
        if not self.article.url:
            return False
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
    path = pathlib.Path(base_path)
    if path.exists() and path.is_dir():
        shutil.rmtree(base_path)
    path.mkdir(parents=True, exist_ok=True)


def main() -> None:
    """
    Entrypoint for scrapper module.
    """
    configuration = Config(CRAWLER_CONFIG_PATH)
    crawler = Crawler(config=configuration)
    prepare_environment(ASSETS_PATH)
    crawler.find_articles()

    article_id = 1
    for url in crawler.urls:
        parser = HTMLParser(url, article_id, configuration)
        article = parser.parse()
        if len(article.text) <= 100 or not article.text:
            continue
        article_id += 1
        if isinstance(article, Article):
            to_raw(article)
            to_meta(article)


if __name__ == "__main__":
    main()
