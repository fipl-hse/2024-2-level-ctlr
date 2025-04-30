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
from random import randint
from time import sleep
from core_utils.constants import (
    ASSETS_PATH,
    CRAWLER_CONFIG_PATH,
    NUM_ARTICLES_UPPER_LIMIT,
    TIMEOUT_LOWER_LIMIT,
    TIMEOUT_UPPER_LIMIT
)


class IncorrectSeedURLError(Exception):
    """Seed URLs list cannot be empty"""


class NumberOfArticlesOutOfRangeError(Exception):
    """Total articles to find and parse must be between 1 and 150"""


class IncorrectNumberOfArticlesError(Exception):
    """Total articles to find and parse must be an integer"""


class IncorrectHeadersError(Exception):
    """Headers must be a dictionary"""


class IncorrectEncodingError(Exception):
    """Encoding must be a string"""


class IncorrectTimeoutError(Exception):
    """"Timeout must be a positive integer less than 60"""


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
        config_data = self._extract_config_content()
        self._validate_config_content()
        self._seed_urls = config_data.seed_urls
        self._num_articles = config_data.total_articles
        self._headers = config_data.headers
        self._encoding = config_data.encoding
        self._timeout = config_data.timeout
        self._should_verify_certificate = config_data.should_verify_certificate
        self._headless_mode = config_data.headless_mode


    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """

        with self.path_to_config.open('r', encoding='utf-8') as file:
            data = json.load(file)
            return ConfigDTO(
                seed_urls=data.get('seed_urls', []),
                total_articles_to_find_and_parse=data.get('total_articles_to_find_and_parse', 0),
                headers=data.get('headers', {}),
                encoding=data.get('encoding', 'utf-8'),
                timeout=data.get('timeout', 30),
                should_verify_certificate=data.get('should_verify_certificate', True),
                headless_mode=data.get('headless_mode', True)
            )

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        config_dto = self._extract_config_content()

        if not isinstance(config_dto.seed_urls, list):
            raise IncorrectSeedURLError

        url_pattern = r"https?://.*/"
        for url in config_dto.seed_urls:
            if not isinstance(url, str) or not re.match(url_pattern, url):
                raise IncorrectSeedURLError

        if (
                not isinstance(config_dto.total_articles, int)
                or isinstance(config_dto.total_articles, bool)
                or config_dto.total_articles < 1
        ):
            raise IncorrectNumberOfArticlesError

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
    sleep_time = randint(1, 3)
    sleep(sleep_time)

    request = requests.get(
        url,
        headers=config.get_headers(),
        timeout=config.get_timeout(),
        verify=config.get_verify_certificate()
    )

    request.encoding = config.get_encoding()
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

        href = article_bs.find("a").get('href')

        if href and href.startswith("https://livennov.ru/"):
            return href

        return ""

    def find_articles(self) -> None:
        """
        Find articles.
        """
        for seed_url in self._seed_urls:
            res = make_request(seed_url, self._config)

            soup = BeautifulSoup(res.content, "lxml")

            for paragraph in soup.find_all('h1', class_='entry-title'):
                if len(self.urls) >= self._config.get_num_articles():
                    return None

                url = self._extract_url(paragraph)

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
        self._full_url = full_url
        self._article_id = article_id
        self._config = config
        self.article = Article(self._full_url, self._article_id)

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """

        "change"
        main_bs = article_soup.find(
            'div',
            class_='entry-content',
            )
        text_tag = main_bs.find_all("p")

        find_text = [text.get_text(strip=True) for text in text_tag]

        self.article.text = "\n".join(find_text)

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        response = make_request(self._full_url, self._config)
        main_bs = BeautifulSoup(response.text, "lxml")
        self._fill_article_with_text(main_bs)
        return self.article
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
        main_bs = BeautifulSoup(response.text, "lxml")
        self._fill_article_with_text(main_bs)
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
    configuration = Config(CRAWLER_CONFIG_PATH)
    prepare_environment(ASSETS_PATH)
    crawler = Crawler(configuration)
    crawler.find_articles()

    for article_id, url in enumerate(crawler.urls, start=1):
        parser = HTMLParser(
            full_url=url,
            article_id=article_id,
            config=configuration
        )
        article = parser.parse()

        if isinstance(article, Article):
            to_raw(article)

if __name__ == "__main__":
    main()
