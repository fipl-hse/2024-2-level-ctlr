"""
Crawler implementation.
"""
import json

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import pathlib
import shutil
from typing import Pattern, Union

import requests
from bs4 import BeautifulSoup

from core_utils.config_dto import ConfigDTO
from core_utils.constants import CRAWLER_CONFIG_PATH


class IncorrectSeedURLError(Exception):
    """
    Raises when seed URL does not match standard pattern
    """
    pass


class NumberOfArticlesOutOfRangeError(Exception):
    """
    Raises when total number of articles is out of range from 1 to 150
    """
    pass


class IncorrectNumberOfArticlesError(Exception):
    """
    Raises when total number of articles to parse is not integer or less than 0
    """
    pass


class IncorrectHeadersError(Exception):
    """
    Raises when headers are not in a form of dictionary
    """
    pass


class IncorrectEncodingError(Exception):
    """
    Raises when encoding is not a string
    """
    pass


class IncorrectTimeoutError(Exception):
    """
    Raises when timeout value is not a positive integer less than 60
    """
    pass


class IncorrectVerifyError(Exception):
    """
    Raises when verify certificate value is neither ``True`` nor ``False``
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
        self._validate_config_content()
        config = self._extract_config_content()
        self.seed_urls = config.seed_urls
        self.total_articles = config.total_articles
        self.headers = config.headers
        self.encoding = config.encoding
        self.timeout = config.timeout
        self.should_verify_certificate = config.should_verify_certificate
        self.headless_mode = config.headless_mode

    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        with open("lab_5_scraper/scraper_config.json", 'r', encoding='UTF-8') as file:
            config_dto = json.load(file)
        return ConfigDTO(**config_dto)

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        if not all(url.startswith('https://literaturno.com') for url in self.seed_urls):
            raise IncorrectSeedURLError('Seed URL does not match standard pattern')
        if self.total_articles > 150:
            raise NumberOfArticlesOutOfRangeError('Total number of articles is out of range')
        if not isinstance(self.total_articles, int) or self.total_articles < 0:
            raise IncorrectNumberOfArticlesError('Total number of articles is not integer or less than 0')
        if not isinstance(self.headers, dict):
            raise IncorrectHeadersError('Headers are not in a form of dictionary')
        if not isinstance(self.encoding, str):
            raise IncorrectEncodingError('Encoding is not in a form of string')
        if not isinstance(self.timeout, int) and not 0 <= self.timeout <= 60:
            raise IncorrectTimeoutError('Wrong timeout value')
        if not isinstance(self.should_verify_certificate, bool):
            raise IncorrectVerifyError('should_verify_certificate must be bool')

    def get_seed_urls(self) -> list[str]:
        """
        Retrieve seed urls.

        Returns:
            list[str]: Seed urls
        """
        return self.seed_urls

    def get_num_articles(self) -> int:
        """
        Retrieve total number of articles to scrape.

        Returns:
            int: Total number of articles to scrape
        """
        return self.total_articles

    def get_headers(self) -> dict[str, str]:
        """
        Retrieve headers to use during requesting.

        Returns:
            dict[str, str]: Headers
        """
        return self.headers

    def get_encoding(self) -> str:
        """
        Retrieve encoding to use during parsing.

        Returns:
            str: Encoding
        """
        return self.encoding

    def get_timeout(self) -> int:
        """
        Retrieve number of seconds to wait for response.

        Returns:
            int: Number of seconds to wait for response
        """
        return self.timeout

    def get_verify_certificate(self) -> bool:
        """
        Retrieve whether to verify certificate.

        Returns:
            bool: Whether to verify certificate or not
        """
        return self.should_verify_certificate

    def get_headless_mode(self) -> bool:
        """
        Retrieve whether to use headless mode.

        Returns:
            bool: Whether to use headless mode or not
        """
        return self.headless_mode


def make_request(url: str, config: Config) -> requests.models.Response:
    """
    Deliver a response from a request with given configuration.

    Args:
        url (str): Site url
        config (Config): Configuration

    Returns:
        requests.models.Response: A response from a request
    """
    if not isinstance(url, str):
        raise ValueError('URL is not a string')
    response = requests.get(url, headers=config.get_headers(), timeout=config.get_timeout(),
                            verify=config.get_verify_certificate())
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
        all_links = article_bs.find_all(class_='description')
        if all_links:
            for link in all_links:
                href = link['href']
                if href not in self.urls:
                    return href
        return "stop"

    def find_articles(self) -> None:
        """
        Find articles.
        """
        for seed_url in self.get_search_urls():
            response = requests.get(seed_url, headers=self.config.get_headers())
            if not response.ok:
                continue
            url = self._extract_url(BeautifulSoup(response.text, 'lxml'))
            for _ in range(13):
                if url == 'stop':
                    continue
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
    if pathlib.Path(base_path).is_dir():
        shutil.rmtree(base_path)
    pathlib.Path(base_path).mkdir(parents=True)


def main() -> None:
    """
    Entrypoint for scrapper module.
    """
    configuration = Config(path_to_config=CRAWLER_CONFIG_PATH)


if __name__ == "__main__":
    main()
