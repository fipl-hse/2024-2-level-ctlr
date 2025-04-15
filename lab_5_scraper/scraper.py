"""
Crawler implementation.
"""

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import pathlib
from typing import Pattern, Union
from bs4 import BeautifulSoup
import datetime
from core_utils.article.article import Article
from core_utils.constants import ASSETS_PATH, CRAWLER_CONFIG_PATH, PROJECT_ROOT

import config.static_checks.requirements_check
from core_utils.config_dto import ConfigDTO
import json
import requests
import re
import shutil

class IncorrectSeedURLError(Exception):
    """
    Check seed URL does match or not standard pattern https?://(www.)?.
    """
    pass


class NumberOfArticlesOutOfRangeError(Exception):
    """
    Check total number of articles is out of range from 1 to 150 or not.
    """
    pass


class IncorrectNumberOfArticlesError(Exception):
    """
    Check total number of articles to parse is not integer or less than 0.
    """
    pass


class IncorrectHeadersError(Exception):
    """
    Check headers are in a form of dictionary or not.
    """
    pass


class IncorrectEncodingError(Exception):
    """
    Check that encoding must be specified as a string.
    """
    pass


class IncorrectTimeoutError(Exception):
    """
    Check that  timeout value must be a positive integer less than 60.
    """
    pass


class IncorrectVerifyError(Exception):
    """
    Check verify certificate value must either be ``True`` or ``False``.
    """
    pass


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
        prepare_environment(ASSETS_PATH)

    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        with open(self.path_to_config, 'r', encoding='utf-8') as file:
            config_data = json.load(file)
        config_dto = ConfigDTO(**config_data)

        return config_dto

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        if not isinstance(self._seed_urls, list) or not all(isinstance(url, str) for url in self._seed_urls):
            raise IncorrectSeedURLError("Seed URL must be a string and be in the list")
        url_pattern = r'^(https?://(www\.)?[^/]+.*)$'
        if not all(re.match(url_pattern, url) for url in self._seed_urls):
            raise IncorrectSeedURLError("Seed URL must be a valid URL format")
        if self._num_articles not in range(151):
            raise NumberOfArticlesOutOfRangeError("Number of articles is out of range")
        if (not (isinstance(self._num_articles, int) and self._num_articles >= 0) or
                isinstance(self._num_articles, bool)):
            raise IncorrectNumberOfArticlesError("Number of articles is not a positive integer")
        if not isinstance(self._headers, dict) or not all(isinstance(key, str) for key in self._headers.keys())\
                or not all(isinstance(value, str) for value in self._headers.values()):
            raise IncorrectHeadersError("Headers must be presented as a dictionary with strings")
        if not isinstance(self._encoding, str):
            raise IncorrectEncodingError("Encoding must be a string")
        if not isinstance(self._timeout, int) or self._timeout not in range(61):
            raise IncorrectTimeoutError("Timeout is out of range - 60")
        if not isinstance(self._headless_mode, bool) or not isinstance(self._should_verify_certificate, bool):
            raise IncorrectVerifyError("Headless mode and should_verify_certificate must be a bool")

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
    timeout = config.get_timeout()
    headers = config.get_headers()
    verify = config.get_verify_certificate()
    response = requests.get(url, headers=headers, timeout=timeout, verify=verify)
    response.raise_for_status()
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
        urls = article_bs.find_all('a', href=lambda href: href and href.startswith('https://zvezdaaltaya.ru/'))
        unique_hrefs = list(set(url['href'] for url in urls))

        for url_href in unique_hrefs:
            if isinstance(url_href, str):
                if url_href not in self.urls and url_href not in self.get_search_urls():
                    return url_href

        return ''
    def find_articles(self) -> None:
        """
        Find articles.
        """
        prepare_environment(ASSETS_PATH)
        seed_urls = self.config.get_seed_urls()
        for url in seed_urls:
            try:
                fix = make_request(url, self.config)
                if fix.status_code > 400:
                    continue
                for i in range(10):
                    article_bs = BeautifulSoup(fix.text, 'lxml')
                    while len(self.urls) <= self.config.get_num_articles():
                        article_url = self._extract_url(article_bs)
                        if article_url is "":
                            break
                        self.urls.append(article_url)

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
        article = Article(full_url, article_id)
        self.article = article

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        text = article_soup.find_all('div', {'class': 'news-detail__detail-text'})[0].text
        self.article.text = text

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        response = make_request(self.article.url, self.config)
        if response.status_code < 400:
            soup = BeautifulSoup(response.text, 'lxml')
            self._fill_article_with_text(soup)
        return None

    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """

    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """


def prepare_environment(base_path: Union[pathlib.Path, str]) -> None:
    """
    Create ASSETS_PATH folder if no created and remove existing folder.

    Args:
        base_path (Union[pathlib.Path, str]): Path where articles stores
    """
    try:
        shutil.rmtree(base_path)
    except FileNotFoundError:
        pass
    pathlib.Path(base_path).mkdir(parents=True)
#"https://zvezdaaltaya.ru/", "https://zvezdaaltaya.ru/category/novosti/",
#        "https://zvezdaaltaya.ru/category/novosti/na-dosuge/", "https://zvezdaaltaya.ru/category/novosti/yurist/",
#    "https://zvezdaaltaya.ru/category/novosti/zdravoohranenie/", "https://zvezdaaltaya.ru/category/novosti/politika/"

def main() -> None:
    """
    Entrypoint for scrapper module.
    """


if __name__ == "__main__":
    main()
