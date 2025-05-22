"""
Crawler implementation.
"""
import datetime

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import json
import time
import pathlib
from random import randint
from typing import Pattern, Union

import requests
from bs4 import BeautifulSoup

import core_utils
from core_utils.article.article import Article
from core_utils.config_dto import ConfigDTO


class IncorrectEncodingError(Exception):
    """Incorrect Encoding Error"""


class IncorrectHeadersError(Exception):
    """Incorrect Headers Error"""


class IncorrectNumberOfArticlesError(Exception):
    """Incorrect Number Of Articles Error"""


class IncorrectSeedURLError(Exception):
    """Incorrect Seed URL Error"""


class IncorrectTimeoutError(Exception):
    """Incorrect Timeout Error"""


class IncorrectVerifyError(Exception):
    """Incorrect Verify Error"""


class NumberOfArticlesOutOfRangeError(Exception):
    """Number Of Articles Out Of Range Error"""


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
        with open(self.path_to_config, "r", encoding="utf-8") as file:
            data = json.load(file)
        return ConfigDTO(
            seed_urls=data['seed_urls'],
            total_articles_to_find_and_parse=data['total_articles_to_find_and_parse'],
            headers=data['headers'],
            encoding=data['encoding'],
            timeout=data['timeout'],
            should_verify_certificate=data['should_verify_certificate'],
            headless_mode=data['headless_mode']
        )

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        if not isinstance(self._seed_urls, list):
            raise IncorrectSeedURLError('incorrect url')
        for url in self._seed_urls:
            if not isinstance(url, str) or not 'http://vzm-vesti.ru/' in url:
                raise IncorrectSeedURLError('incorrect url')
        if not isinstance(self._num_articles, int) or self._num_articles <= 0:
            raise IncorrectNumberOfArticlesError('number is not int or less that 0')
        if self._num_articles < 0 or self._num_articles > 150:
            raise NumberOfArticlesOutOfRangeError('wrong number of articles')
        if not isinstance(self._headers, dict):
            raise IncorrectHeadersError('incorrect type of headers')
        if not isinstance(self._encoding, str):
            raise IncorrectEncodingError('incorrect type of encoding')
        if not isinstance(self._timeout, int) or self._timeout <= 0 or self._timeout > 60:
            raise IncorrectTimeoutError('incorrect timeouts')
        if not isinstance(self._should_verify_certificate, bool) or not \
                isinstance(self._headless_mode, bool):
            raise IncorrectVerifyError('type is not bool')

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
    time.sleep(randint(1, 5))
    site_request = requests.get(url=url,
                                headers=config.get_headers(),
                                timeout=config.get_timeout(),
                                verify=config.get_verify_certificate())
    return site_request


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
        config: Config

    def _extract_url(self, article_bs: BeautifulSoup) -> str:
        """
        Find and retrieve url from HTML.

        Args:
            article_bs (bs4.BeautifulSoup): BeautifulSoup instance

        Returns:
            str: Url from HTML
        """
        url = 'http://vzm-vesti.ru/'
        reqs = requests.get(url)
        soup = BeautifulSoup(reqs.text, 'html.parser')
        urls = ''

        for link in soup.find_all('a'):
            urls+link.get('href')
        return urls

    def find_articles(self) -> None:
        """
        Find articles.
        """

    def get_search_urls(self) -> list:
        """
        Get seed_urls param.

        Returns:
            list: seed_urls param
        """


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

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """

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


def main() -> None:
    """
    Entrypoint for scrapper module.
    """


if __name__ == "__main__":  # second change
    main()
