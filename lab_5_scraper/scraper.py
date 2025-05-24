"""
Crawler implementation.
"""

import datetime
import json
import logging

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import pathlib
import shutil
from typing import Pattern, Union

import requests
from bs4 import BeautifulSoup

from core_utils.article.article import Article
from core_utils.article.io import to_meta, to_raw
from core_utils.config_dto import ConfigDTO
from core_utils.constants import ASSETS_PATH, CRAWLER_CONFIG_PATH


class IncorrectSeedURLError(Exception):
    """Raised when seed URL is not valid."""
    pass


class NumberOfArticlesOutOfRangeError(Exception):
    """Raised when the number of articles is not in the allowed range (1 to 150)."""
    pass


class IncorrectNumberOfArticlesError(Exception):
    """Raised when the number of articles is not a positive integer."""
    pass


class IncorrectHeadersError(Exception):
    """Raised when headers are not provided as a dictionary."""
    pass


class IncorrectEncodingError(Exception):
    """Raised when encoding is not provided as a string."""
    pass


class IncorrectTimeoutError(Exception):
    """Raised when timeout is not a positive integer less than 60."""
    pass


class IncorrectVerifyError(Exception):
    """Raised when certificate verification is not a boolean."""
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
        with open(self.path_to_config, 'r', encoding='utf-8') as config_file:
            self.raw_config_data = json.load(config_file)
        self._validate_config_content()
        self.config_data = self._extract_config_content()

        self._seed_urls = self.config_data.seed_urls
        self._num_articles = self.config_data.total_articles
        self._headers = self.config_data.headers
        self._encoding = self.config_data.encoding
        self._timeout = self.config_data.timeout
        self._should_verify_certificate = self.config_data.should_verify_certificate
        self._headless_mode = self.config_data.headless_mode
        self._total_articles_to_find_and_parse = self.config_data.total_articles


    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        config_data = self.raw_config_data

        return ConfigDTO(
            seed_urls=config_data['seed_urls'],
            total_articles_to_find_and_parse=config_data['total_articles_to_find_and_parse'],
            headers=config_data['headers'],
            encoding=config_data['encoding'],
            timeout=config_data['timeout'],
            should_verify_certificate=config_data['should_verify_certificate'],
            headless_mode=config_data['headless_mode']
        )

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        config_data = self.raw_config_data
        if not isinstance(config_data.get('seed_urls'), list) or not config_data['seed_urls']:
            raise IncorrectSeedURLError("Seed URLs must be a non-empty list.")
        for url in config_data['seed_urls']:
            if not isinstance(url, str) or not url.startswith(('http://', 'https://')):
                raise IncorrectSeedURLError(f"Invalid URL format: {url}")

        total_articles = config_data.get('total_articles_to_find_and_parse')
        if not isinstance(total_articles, int) or total_articles < 1:
            raise IncorrectNumberOfArticlesError("Number of articles must be a positive integer.")
        if total_articles > 150:
            raise NumberOfArticlesOutOfRangeError("Number of articles must not exceed 150.")

        if not isinstance(config_data.get('headers'), dict):
            raise IncorrectHeadersError("Headers must be provided as a dictionary.")

        if not isinstance(config_data.get('encoding'), str):
            raise IncorrectEncodingError("Encoding must be a string.")

        timeout = config_data.get('timeout')
        if not isinstance(timeout, int) or not (0 < timeout < 60):
            raise IncorrectTimeoutError("Timeout must be a positive integer less than 60.")

        if not isinstance(config_data.get('should_verify_certificate'), bool):
            raise IncorrectVerifyError("Verify certificate flag must be a boolean.")

        if not isinstance(config_data.get('headless_mode'), bool):
            raise IncorrectVerifyError("Headless mode must be either True or False")

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
    response = requests.get(
        url,
        headers=config.get_headers(),
        timeout=config.get_timeout(),
        verify=config.get_verify_certificate()
    )
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
        if article_bs and article_bs.has_attr("href"):
            href = article_bs["href"]
            if href.startswith("http"):
                return href
            return "https://www.flypobeda.ru" + href
        return ""

    def find_articles(self) -> None:
        """
        Find articles.
        """
        for url in self.get_search_urls():
            response = make_request(url, self.config)

            if response is None or response.status_code != 200:
                logging.warning(f"Failed to access {url}")
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            article_blocks = soup.find_all("a", class_="dp-nlixs6-root")

            for block in article_blocks:
                article_url = self._extract_url(block)
                if article_url and article_url not in self.urls:
                    self.urls.append(article_url)

                if len(self.urls) >= self.config.get_num_articles():
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
        self.full_url = full_url
        self.article_id = article_id
        self.config = config
        self.article = Article(url=full_url, article_id=article_id)

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        content_blocks = article_soup.find_all(
            "div",
            class_=["dp-q37iie-root-root", "dp-qa3f2j-root"]
        )

        text_parts = []

        for block in content_blocks:
            if block.find_parent("footer"):
                continue

            text = block.get_text(separator="\n", strip=True)
            if text:
                text_parts.append(text)

        full_text = "\n".join(text_parts)
        self.article.text = full_text

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        title_tag = article_soup.find("h1")
        title = title_tag.get_text(strip=True)

        author = ["NOT FOUND"]

        self.article.title = title
        self.article.author = author

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
        response = make_request(self.full_url, self.config)

        if response is None or response.status_code != 200:
            return False

        article_bs = BeautifulSoup(response.text, "html.parser")

        self._fill_article_with_text(article_bs)
        self._fill_article_with_meta_information(article_bs)

        return self.article


def prepare_environment(base_path: Union[pathlib.Path, str]) -> None:
    """
    Create ASSETS_PATH folder if no created and remove existing folder.

    Args:
        base_path (Union[pathlib.Path, str]): Path where articles stores
    """
    base_path = pathlib.Path(base_path)

    if base_path.exists():
        shutil.rmtree(base_path)

    base_path.mkdir(parents=True, exist_ok=True)


def main() -> None:
    """
    Entrypoint for scrapper module.
    """
    configuration = Config(path_to_config=CRAWLER_CONFIG_PATH)
    prepare_environment(ASSETS_PATH)
    crawler = Crawler(configuration)
    crawler.find_articles()
    for article_id, url in enumerate(crawler.urls, start=1):
        parser = HTMLParser(full_url=url, article_id=article_id, config=configuration)
        article = parser.parse()
        if isinstance(article, Article):
            to_raw(article)
            to_meta(article)


if __name__ == "__main__":
    main()
