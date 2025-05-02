"""
Crawler implementation.
"""

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument

import json
import pathlib
import shutil
from typing import Pattern, Union
from core_utils.config_dto import ConfigDTO
from core_utils.article.io import to_raw
import re
import datetime
from core_utils.article.article import Article
import requests
from bs4 import BeautifulSoup
from core_utils.constants import (
    ASSETS_PATH,
    CRAWLER_CONFIG_PATH,
    NUM_ARTICLES_UPPER_LIMIT,
    TIMEOUT_LOWER_LIMIT,
    TIMEOUT_UPPER_LIMIT
)


class IncorrectSeedURLError(Exception):
    """URL list cannot be empty"""


class NumberOfArticlesOutOfRangeError(Exception):
    """Number of articles to find and parse must be between 1 and 150"""


class NumberOfArticlesError(Exception):
    """Total articles to find and parse must be an integer"""


class IncorrectHeadersError(Exception):
    """Headers must be a valid dictionary"""


class IncorrectEncodingError(Exception):
    """Encoding must be a string"""


class IncorrectTimeoutError(Exception):
    """"Timeout must be an integer between 1 and 60"""


class IncorrectVerifyError(Exception):
    """Verify certificate must be a boolean"""


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
        cnf = self._extract_config_content()
        self._seed_urls = cnf.seed_urls
        self._num_articles = cnf.total_articles
        self._headers = cnf.headers
        self._encoding = cnf.encoding
        self._timeout = cnf.timeout
        self._should_verify_certificate = cnf.should_verify_certificate
        self._headless_mode = cnf.headless_mode

    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        with self.path_to_config.open('r', encoding='utf-8') as f:
            data_from_json = json.load(f)
            return ConfigDTO(
                seed_urls=data_from_json.get('seed_urls', []),
                total_articles_to_find_and_parse=data_from_json.get(
                    'total_articles_to_find_and_parse',
                    0
                ),
                headers=data_from_json.get('headers', {}),
                encoding=data_from_json.get('encoding', 'utf-8'),
                timeout=data_from_json.get('timeout', 15),
                should_verify_certificate=data_from_json.get('should_verify_certificate', True),
                headless_mode=data_from_json.get('headless_mode', True)
            )

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        config_dto = self._extract_config_content()
        if not isinstance(config_dto.seed_urls, list):
            raise IncorrectSeedURLError
        for url in config_dto.seed_urls:
            if not re.match(r"https?://(www\.)?", url):
                raise IncorrectSeedURLError
        if (
                not isinstance(config_dto.total_articles, int)
                or config_dto.total_articles < 1
                or isinstance(config_dto.total_articles, bool)
        ):
            raise NumberOfArticlesError
        if config_dto.total_articles > NUM_ARTICLES_UPPER_LIMIT:
            raise NumberOfArticlesOutOfRangeError
        if not isinstance(config_dto.headers, dict):
            raise IncorrectHeadersError
        if not isinstance(config_dto.encoding, str):
            raise IncorrectEncodingError
        if (
                not isinstance(config_dto.timeout, int)
                or not TIMEOUT_LOWER_LIMIT < config_dto.timeout < TIMEOUT_UPPER_LIMIT
        ):
            raise IncorrectTimeoutError
        if not isinstance(config_dto.should_verify_certificate, bool):
            raise IncorrectVerifyError
        if not isinstance(config_dto.headless_mode, bool):
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

    headers = config.get_headers()
    timeout = config.get_timeout()
    verify = config.get_verify_certificate()

    response = requests.get(url, headers=headers, timeout=timeout, verify=verify)
    response.raise_for_status()
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
        self._config = config
        self._seed_urls = self._config.get_seed_urls()
        self.urls = []

    def _extract_url(self, article_bs: BeautifulSoup) -> str:
        """
        Find and retrieve url from HTML.

        Args:
            article_bs (bs4.BeautifulSoup): BeautifulSoup instance

        Returns:
            str: Url from HTML
        """
        href = article_bs.find("a").get('href')
        if href and href.startswith("http://express-kamchatka1.ru/"):
            return href
        return ""

    def find_articles(self) -> None:
        """
        Find articles.
        """
        for seed_url in self._seed_urls:
            res = make_request(seed_url, self._config)
            bs = BeautifulSoup(res.content, "lxml")
            for header in bs.find_all('h1', class_='entry-title'):
                if self._config.get_num_articles() <= len(self.urls):
                    return None
                url = self._extract_url(header)
                if url and url not in self.urls:
                    self.urls.append(url)

    def get_search_urls(self) -> list:
        """
        Get seed_urls param.

        Returns:
            list: seed_urls param
        """
        return self._seed_urls


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
        self._config = config
        self._article_id = article_id
        self._full_url = full_url
        self.article = Article(self._full_url, self._article_id)

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        div_finder = article_soup.find('div', class_='entry-content')
        paragraphs = div_finder.find_all("p")

        get_text = [text.get_text(strip=True) for text in paragraphs]

        self.article.text = "\n".join(get_text)

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
        response = make_request(self._full_url, self._config)
        bs = BeautifulSoup(response.text, "lxml")
        self._fill_article_with_text(bs)
        return self.article


def prepare_environment(base_path: Union[pathlib.Path, str]) -> None:
    """
    Create ASSETS_PATH folder if no created and remove existing folder.

    Args:
        base_path (Union[pathlib.Path, str]): Path where articles stores
    """
    if base_path.exists():
        shutil.rmtree(base_path)
    base_path.mkdir(parents=True)


def main() -> None:
    """
    Entrypoint for scrapper module.
    """
    prepare_environment(ASSETS_PATH)
    configuration = Config(CRAWLER_CONFIG_PATH)
    crawler = Crawler(configuration)
    crawler.find_articles()

    for article_id, url in enumerate(crawler.urls, start=1):
        parser = HTMLParser(full_url=url, article_id=article_id,
                            config=configuration)
        article = parser.parse()
        if isinstance(article, Article):
            to_raw(article)


if __name__ == "__main__":
    main()