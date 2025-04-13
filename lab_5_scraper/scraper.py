"""
Crawler implementation.
"""

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import pathlib
from typing import Pattern, Union
from core_utils.constants import CRAWLER_CONFIG_PATH
from core_utils.article.article import Article
from core_utils.config_dto import ConfigDTO
import json
import os
import shutil
import requests
from bs4 import BeautifulSoup
from core_utils.article.io import to_raw
import datetime


class IncorrectSeedURLError(Exception):
    pass


class IncorrectNumberOfArticlesError(Exception):
    pass


class NumberOfArticlesOutOfRangeError(Exception):
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
        with open(self.path_to_config, 'r') as file:
            data = json.load(file)
            return ConfigDTO(
                data['seed_urls'],
                data['headers'],
                data['total_articles_to_find_and_parse'],
                data['encoding'],
                data['timeout'],
                data['should_verify_certificate'],
                data['headless_mode']
            )

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        with open(self.path_to_config, 'r') as file:
            data = json.load(file)
        if (not isinstance(data['seed_urls'], list)
                or not all('https://sovsakh.ru/' in url for url in data['seed_urls'])):
            raise IncorrectSeedURLError
        if (not isinstance(data['total_articles_to_find_and_parse'], int)
                or isinstance(data['total_articles_to_find_and_parse'], bool)
                or data['total_articles_to_find_and_parse'] < 0):
            raise IncorrectNumberOfArticlesError
        if data['total_articles_to_find_and_parse'] > 150:
            raise NumberOfArticlesOutOfRangeError
        if not isinstance(data['headers'], dict):
            raise IncorrectHeadersError
        if not isinstance(data['encoding'], str):
            raise IncorrectEncodingError
        if not isinstance(data['timeout'], int) or not 0 <= data['timeout'] < 60:
            raise IncorrectTimeoutError
        if not isinstance(data['should_verify_certificate'], bool):
            raise IncorrectVerifyError
        if not isinstance(data['headless_mode'], bool):
            raise IncorrectVerifyError

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
    return requests.get(
        url,
        headers=config.get_headers(),
        timeout=config.get_timeout()
    )


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
        urls = article_bs.find_all('a', href=True)
        for url in urls:
            if url:
                return url['href']

    def find_articles(self) -> None:
        """
        Find articles.
        """
        for url in self.get_search_urls():
            if len(self.urls) != self.config.get_num_articles():
                response = make_request(url, self.config)
                if not response.ok:
                    continue
                soup = BeautifulSoup(response.text, 'html.parser')
                got_url = self._extract_url(soup)
                if not got_url:
                    continue
                self.urls.append(got_url)

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
        self.article.text = '\n'.join(texts)

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        self.article.title = article_soup.find('h1', {'class': 'entry-title'}).text
        self.article.author = ['NOT FOUND']

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
        soup = BeautifulSoup(make_request(self.article.url, self.config).text, 'html.parser')
        self._fill_article_with_text(soup)
        return self.article


def prepare_environment(base_path: Union[pathlib.Path, str]) -> None:
    """
    Create ASSETS_PATH folder if no created and remove existing folder.

    Args:
        base_path (Union[pathlib.Path, str]): Path where articles stores
    """
    if os.makedirs(base_path, exist_ok=True):
        return None
    if len(os.listdir(base_path)) > 0:
        shutil.rmtree(base_path)
        os.mkdir(base_path)


def main() -> None:
    """
    Entrypoint for scrapper module.
    """
    config = Config(CRAWLER_CONFIG_PATH)
    crawler = Crawler(config)
    for ind, url in enumerate(crawler.urls):
        parser = HTMLParser(url, ind, config)
        to_raw(parser.parse())

if __name__ == "__main__":
    main()
