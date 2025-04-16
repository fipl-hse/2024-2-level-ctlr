"""
Crawler implementation.
"""

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import json
import pathlib
import shutil
import requests
from bs4 import BeautifulSoup
from typing import Pattern, Union
from core_utils.config_dto import ConfigDTO
from core_utils.constants import ASSETS_PATH, CRAWLER_CONFIG_PATH


class IncorrectSeedURLError(Exception):
    """
    Raised when seed URL is not a valid URL
    """


class NumberOfArticlesOutOfRangeError(Exception):
    """
    Raised when total number of articles is out of range from 1 to 150
    """


class IncorrectNumberOfArticlesError(Exception):
    """
    Raised when total number of articles to parse is not integer or less than 0
    """


class IncorrectHeadersError(Exception):
    """
    Raised when headers are not a dictionary
    """


class IncorrectEncodingError(Exception):
    """
    Raised when encoding is not a string
    """


class IncorrectTimeoutError(Exception):
    """
    Raised when timeout value is not a positive integer less than 60
    """


class IncorrectVerifyError(Exception):
    """
    Raised when verify certificate value is not True or False
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
        extractions = self._extract_config_content()
        self._seed_urls = extractions.seed_urls
        self._headers = extractions.headers
        self._total_articles_to_find_and_parse = extractions.total_articles_to_find_and_parse
        self._encoding = extractions.encoding
        self._timeout = extractions.timeout
        self._should_verify_certificate = extractions.should_verify_certificate
        self._headless_mode = extractions.headless_mode
        self._validate_config_content()

    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        with open(self.path_to_config, "r", encoding="utf-8") as file:
            config_values_file = json.load(file)
            config_values = ConfigDTO(config_values_file["seed_urls"],
                                      config_values_file["total_articles_to_find_and_parse"],
                                      config_values_file["headers"],
                                      config_values_file["encoding"], config_values_file["timeout"],
                                      config_values_file["should_verify_certificate"],
                                      config_values_file["headless_mode"])
            return config_values

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        if not isinstance(self._seed_urls, list) or \
                not all(isinstance(url, str) for url in self._seed_urls) or \
                not all(isinstance(url.startswith("https://polkrug.ru")) for url in self._seed_urls):
            raise IncorrectSeedURLError("Seed URL is not a valid URL")
        if not isinstance(self._total_articles_to_find_and_parse, int) or \
                isinstance(self._total_articles_to_find_and_parse, bool) or \
                self._total_articles_to_find_and_parse < 0:
            raise IncorrectNumberOfArticlesError("Number of articles is not integer or less than 0")
        if self._total_articles_to_find_and_parse > 150:
            raise NumberOfArticlesOutOfRangeError("Total number of articles is out of range")
        if not isinstance(self._headers, dict):
            raise IncorrectHeadersError("Headers are not in a form of dictionary")
        if not isinstance(self._encoding, str):
            raise IncorrectEncodingError("Encoding is not a string")
        if self._timeout < 0 or self._timeout > 60:
            raise IncorrectTimeoutError("Timeout is out of range")
        if not isinstance(self._should_verify_certificate, bool):
            raise IncorrectVerifyError("Verify certificate value is not an instance of bool")
        if not isinstance(self._headless_mode, bool):
            raise IncorrectVerifyError('Headless mode value is not an instance of bool')

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
        return self._total_articles_to_find_and_parse

    def get_headers(self) -> dict[str, str]:
        """
        Retrieve headers to use during requesting.

        Returns:
            dict[str, str]: Headers
        """
        self._headers

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
        self._should_verify_certificate

    def get_headless_mode(self) -> bool:
        """
        Retrieve whether to use headless mode.

        Returns:
            bool: Whether to use headless mode or not
        """
        self._headless_mode


def make_request(url: str, config: Config) -> requests.models.Response:
    """
    Deliver a response from a request with given configuration.

    Args:
        url (str): Site url
        config (Config): Configuration

    Returns:
        requests.models.Response: A response from a request
    """
    request = requests.get(url, headers=config.get_headers(), timeout=config.get_timeout(),
                           verify=config.get_verify_certificate())
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
    try:
        shutil.rmtree(base_path)
    except FileNotFoundError:
        pass
    pathlib.Path(base_path).mkdir(parents=True)




def main() -> None:
    """
    Entrypoint for scrapper module.
    """


if __name__ == "__main__":
    main()
    #Hello)))