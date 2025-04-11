"""
Crawler implementation.
"""

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import pathlib
from typing import Pattern, Union
from bs4 import BeautifulSoup
import re

import config.static_checks.requirements_check
from core_utils.config_dto import ConfigDTO
import json
import requests

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
        config = self._extract_config_content()
        for url in config.seed_urls:
            if "https://zvezdaaltaya.ru" not in url:
                raise IncorrectSeedURLError
        if config.total_articles < 1 or config.total_articles > 150:
            raise NumberOfArticlesOutOfRangeError
        if not isinstance(config.total_articles, int) or config.total_articles < 0:
            raise IncorrectNumberOfArticlesError
        if not isinstance(config.headers, dict) and not all(isinstance(key, str) for key in config.headers.keys())\
                and not all(isinstance(value, str) for value in config.headers.values()):
            raise IncorrectHeadersError
        if not isinstance(config.encoding, str):
            raise IncorrectEncodingError
        if config.timeout < 0 or config.timeout > 60:
            raise IncorrectTimeoutError
        if not isinstance(config.headless_mode, bool) or not isinstance(config.should_verify_certificate, bool):
            raise IncorrectVerifyError


    def get_seed_urls(self) -> list[str]:
        """
        Retrieve seed urls.

        Returns:
            list[str]: Seed urls
        """
        config = self._extract_config_content()
        return config.seed_urls

    def get_num_articles(self) -> int:
        """
        Retrieve total number of articles to scrape.

        Returns:
            int: Total number of articles to scrape
        """
        config = self._extract_config_content()
        return config.total_articles

    def get_headers(self) -> dict[str, str]:
        """
        Retrieve headers to use during requesting.

        Returns:
            dict[str, str]: Headers
        """
        config = self._extract_config_content()
        return config.headers

    def get_encoding(self) -> str:
        """
        Retrieve encoding to use during parsing.

        Returns:
            str: Encoding
        """
        config = self._extract_config_content()
        return config.encoding

    def get_timeout(self) -> int:
        """
        Retrieve number of seconds to wait for response.

        Returns:
            int: Number of seconds to wait for response
        """
        config = self._extract_config_content()
        return config.timeout

    def get_verify_certificate(self) -> bool:
        """
        Retrieve whether to verify certificate.

        Returns:
            bool: Whether to verify certificate or not
        """
        config = self._extract_config_content()
        return config.should_verify_certificate

    def get_headless_mode(self) -> bool:
        """
        Retrieve whether to use headless mode.

        Returns:
            bool: Whether to use headless mode or not
        """
        config = self._extract_config_content()
        return config.headless_mode


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
        link = article_bs.find('a', class_='tdb-drop-down-search-inner')
        if link:
            return link.get('href')

        return ""

    def find_articles(self) -> None:
        """
        Find articles.
        """
        seed_urls = self.get_search_urls()
        for url in seed_urls:
            try:
                fix = make_request(url, self.config)
                article_bs = BeautifulSoup(fix.text, 'html.parser')
                article_url = self._extract_url(article_bs)
                if article_url and article_url not in self.urls:
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
    if base_path.exists():
        if any(base_path.iterdir()):
            for item in base_path.iterdir():
                if item.is_dir():
                    for sub_item in item.iterdir():
                        sub_item.unlink()
                    item.rmdir()
                else:
                    item.unlink()
    base_path.mkdir(parents=True, exist_ok=True)


def main() -> None:
    """
    Entrypoint for scrapper module.
    """


if __name__ == "__main__":
    # first change
    main()
