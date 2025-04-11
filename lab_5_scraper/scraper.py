"""
Crawler implementation.
"""

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import pathlib
from typing import Pattern, Union
import json
import requests
from core_utils.config_dto import ConfigDTO
import datetime
import re
import os
from bs4 import BeautifulSoup
from core_utils.article.article import Article
from core_utils.constants import ASSETS_PATH, CRAWLER_CONFIG_PATH

class IncorrectSeedURLError(Exception):
    pass

class NumberOfArticlesOutOfRangeError(Exception):
    pass

class IncorrectNumberOfArticlesError(Exception):
    pass

class IncorrectHeadersError(Exception):
    pass

class IncorrectEncodingError(Exception):
    pass

class IncorrectTimeoutError(Exception):
    pass

class IncorrectVerifyError(Exception):
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
        self.config_content = self._extract_config_content()
        self._validate_config_content()

    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        with open(self.path_to_config, "r") as config_file:
            data = json.load(config_file)
        return ConfigDTO(**data)

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        if not isinstance(self.config_content.seed_urls, list) or not all(isinstance(url, str) for url in self.config_content.seed_urls) or not all(re.compile("https?://(www.)?").match(url) for url in self.config_content.seed_urls):
            raise IncorrectSeedURLError("Seed URLs have wrong format")

        if self.config_content.total_articles not in range(0, 151):
            raise NumberOfArticlesOutOfRangeError("Number of articles is out of range")

        if not isinstance(self.config_content.total_articles, int) or self.config_content.total_articles < 0:
            raise IncorrectNumberOfArticlesError("№ of articles has wrong format or № of articles < 0")

        if not isinstance(self.config_content.headers, dict):
            raise IncorrectHeadersError("Headers must be a dict")

        if not isinstance(self.config_content.encoding, str):
            raise IncorrectEncodingError("Encoding must be a string")

        if not isinstance(self.config_content.timeout, int) or not self.config_content.timeout in range(0, 61):
            raise IncorrectTimeoutError("Timeout has wrong format ot timeout is out of range")

        if not isinstance(self.config_content.headless_mode, bool):
            raise IncorrectVerifyError("Headless mode has wrong format")

        # if not isinstance(self.config_content.should_verify_certificate, bool):
        #     raise ValueError("Certificate verifying must be of a bool type")


    def get_seed_urls(self) -> list[str]:
        """
        Retrieve seed urls.

        Returns:
            list[str]: Seed urls
        """
        return self._extract_config_content().seed_urls

    def get_num_articles(self) -> int:
        """
        Retrieve total number of articles to scrape.

        Returns:
            int: Total number of articles to scrape
        """
        return self._extract_config_content().total_articles

    def get_headers(self) -> dict[str, str]:
        """
        Retrieve headers to use during requesting.

        Returns:
            dict[str, str]: Headers
        """
        return self._extract_config_content().headers

    def get_encoding(self) -> str:
        """
        Retrieve encoding to use during parsing.

        Returns:
            str: Encoding
        """
        return self._extract_config_content().encoding

    def get_timeout(self) -> int:
        """
        Retrieve number of seconds to wait for response.

        Returns:
            int: Number of seconds to wait for response
        """
        return self._extract_config_content().timeout

    def get_verify_certificate(self) -> bool:
        """
        Retrieve whether to verify certificate.

        Returns:
            bool: Whether to verify certificate or not
        """
        return self._extract_config_content().should_verify_certificate

    def get_headless_mode(self) -> bool:
        """
        Retrieve whether to use headless mode.

        Returns:
            bool: Whether to use headless mode or not
        """
        return self._extract_config_content().headless_mode


def make_request(url: str, config: Config) -> requests.models.Response:
    """
    Deliver a response from a request with given configuration.

    Args:
        url (str): Site url
        config (Config): Configuration

    Returns:
        requests.models.Response: A response from a request
    """
    if not isinstance(url, str) or not isinstance(config, Config):
        raise TypeError

    request = requests.get(url, headers=Config.get_headers(), timeout=Config.get_timeout(), verify=Config.get_verify_certificate())

    return request



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


    def _extract_url(self, article_bs: BeautifulSoup) -> str:
        """
        Find and retrieve url from HTML.

        Args:
            article_bs (bs4.BeautifulSoup): BeautifulSoup instance

        Returns:
            str: Url from HTML
        """

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


if __name__ == "__main__":
    "first change"
    main()
