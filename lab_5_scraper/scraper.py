"""
Crawler implementation.
"""

import datetime

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import json
import re
import time
from pathlib import Path
from random import randint
from typing import Pattern, Union
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from core_utils.article import io
from core_utils.article.article import Article
from core_utils.config_dto import ConfigDTO
from core_utils.constants import (
    ASSETS_PATH,
    CRAWLER_CONFIG_PATH,
    NUM_ARTICLES_UPPER_LIMIT,
    TIMEOUT_LOWER_LIMIT,
    TIMEOUT_UPPER_LIMIT,
)


class IncorrectSeedURLError(Exception):
    """
    Raised when the seed URL is not written correctly in the configuration file.
    """
    def __init__(self, msg):
        super().__init__(msg)


class NumberOfArticlesOutOfRangeError(Exception):
    """
    Raised when the number of articles is too large in the configuration file.
    """
    def __init__(self, msg):
        super().__init__(msg)


class IncorrectNumberOfArticlesError(Exception):
    """
    Raised when the number of articles is too small or not an integer in the configuration file.
    """
    def __init__(self, msg):
        super().__init__(msg)


class IncorrectHeadersError(Exception):
    """
    Raised when the headers are not in a form of dictionary in the configuration file.
    """
    def __init__(self, msg):
        super().__init__(msg)


class IncorrectEncodingError(Exception):
    """
    Raised when the encoding is not specified as a string in the configuration file.
    """
    def __init__(self, msg):
        super().__init__(msg)


class IncorrectTimeoutError(Exception):
    """
    Raised when the timeout is too large or not a positive integer in the configuration file.
    """
    def __init__(self, msg):
        super().__init__(msg)


class IncorrectVerifyError(Exception):
    """
    Raised when the verify certificate value is neither True nor False in the configuration file.
    """
    def __init__(self, msg):
        super().__init__(msg)


class Config:
    """
    Class for unpacking and validating configurations.
    """
    path_to_config: Path

    def __init__(self, path_to_config: Path) -> None:
        """
        Initialize an instance of the Config class.

        Args:
            path_to_config (pathlib.Path): Path to configuration.
        """
        self.path_to_config = path_to_config
        with open(self.path_to_config, "r", encoding="utf-8") as file:
            self.config_dict = json.load(file)
        self._validate_config_content()
        dto = self._extract_config_content()
        self._seed_urls = dto.seed_urls
        self._num_articles = dto.total_articles
        self._headers = dto.headers
        self._encoding = dto.encoding
        self._timeout = dto.timeout
        self._should_verify_certificate = dto.should_verify_certificate
        self._headless_mode = dto.headless_mode

    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        return ConfigDTO(**self.config_dict)

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        if not (isinstance(self.config_dict["seed_urls"], list)):
            raise ValueError("Seed URLs should be a list of strings")
        for url in self.config_dict["seed_urls"]:
            if not re.match("https?://(www.)?", url):
                raise IncorrectSeedURLError("Incorrect seed URL format in the config")
        if not (isinstance(self.config_dict["total_articles_to_find_and_parse"], int) and
                self.config_dict["total_articles_to_find_and_parse"] > 0):
            raise IncorrectNumberOfArticlesError("Number of articles in the config is out of range")
        if self.config_dict["total_articles_to_find_and_parse"] > NUM_ARTICLES_UPPER_LIMIT:
            raise NumberOfArticlesOutOfRangeError("Number of articles in the config is not an int or less than 0")
        if not isinstance(self.config_dict["headers"], dict):
            raise IncorrectHeadersError("Headers need to be specified as a dictionary in the config")
        if not isinstance(self.config_dict["encoding"], str):
            raise IncorrectEncodingError("Encoding should be a string in the config")
        if not (isinstance(self.config_dict["timeout"], int) and
                TIMEOUT_LOWER_LIMIT < self.config_dict["timeout"] < TIMEOUT_UPPER_LIMIT):
            raise IncorrectTimeoutError("Timeout is incorrect or out of range in the config")
        if not isinstance(self.config_dict["should_verify_certificate"], bool):
            raise IncorrectVerifyError("Verify Certificate value should be True of False in the config")

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
    time.sleep(randint(1, 3))
    response = requests.get(
        url,
        headers=config.get_headers(),
        timeout=config.get_timeout())
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
        return article_bs.a["href"]

    def find_articles(self) -> None:
        """
        Find articles.
        """
        urls_to_iter = self.config.get_seed_urls()
        for seed_url in urls_to_iter:
            loaded_html = make_request(seed_url, self.config)
            if loaded_html.status_code >= 400:
                continue
            soup = BeautifulSoup(loaded_html.text, "html.parser")
            links = soup.find_all("a", href=True)
            for link in links:
                if not any(art_prefix in link["href"] for art_prefix in ("news/", "article/")):
                    continue
                link_soup = BeautifulSoup(str(link), "html.parser")
                link_str = (str(urlparse(seed_url).scheme) + "://" +
                            str(urlparse(seed_url).netloc) +
                            self._extract_url(link_soup))
                if not link_str in self.urls:
                    self.urls.append(link_str)
                    if len(self.urls) == self.config.get_num_articles():
                        return

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
        self.config = config
        self.article = Article(full_url, article_id)

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        text_blocks = article_soup.find_all("p", style=re.compile("text-align: ?justify"))
        self.article.text = " ".join([block.text for block in text_blocks])

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        self.article.title = article_soup.find("h1").text
        author = article_soup.find("a", class_="italic")
        if author:
            self.article.author.append(author.text)
        lower_author = article_soup.find("p", style=re.compile("text-align: ?right"))
        if lower_author:
            self.article.author.append(lower_author.text)

    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """
        return datetime.datetime.strptime(date_str, "%d/%m/%y %H:%M")

    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """
        loaded_html = make_request(self.article.url, self.config)
        article_bs = BeautifulSoup(loaded_html.text, "html.parser")
        self._fill_article_with_text(article_bs)
        self._fill_article_with_meta_information(article_bs)
        return self.article


def prepare_environment(base_path: Union[Path, str]) -> None:
    """
    Create ASSETS_PATH folder if no created and remove existing folder.

    Args:
        base_path (Union[pathlib.Path, str]): Path where articles stores
    """
    if base_path.exists():
        for f in base_path.iterdir():
            f.unlink()
        base_path.rmdir()
    base_path.mkdir()


def main() -> None:
    """
    Entrypoint for scrapper module.
    """
    config = Config(CRAWLER_CONFIG_PATH)
    prepare_environment(ASSETS_PATH)
    crawler = Crawler(config=config)
    crawler.find_articles()
    for art_id, art_url in enumerate(crawler.urls):
        parser = HTMLParser(full_url=art_url, article_id=art_id, config=config)
        parsed_article = parser.parse()
        io.to_raw(parsed_article)
        io.to_meta(parsed_article)


if __name__ == "__main__":
    main()
