"""
Crawler implementation.
"""

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import os
import shutil
import pathlib
import re
import json
import requests
from core_utils.article.article import Article
from core_utils.article.io import to_raw
from core_utils.constants import CRAWLER_CONFIG_PATH, ASSETS_PATH
from core_utils.config_dto import ConfigDTO
from typing import Pattern, Union
from bs4 import BeautifulSoup
import datetime


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
        self.config_dto = self._extract_config_content()
        self._validate_config_content()


    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        with open(self.path_to_config) as f:
            config_data = json.load(f)

        return ConfigDTO(
            seed_urls=config_data["seed_urls"],
            total_articles_to_find_and_parse=config_data["total_articles_to_find_and_parse"],
            headers=config_data["headers"],
            encoding=config_data["encoding"],
            timeout=config_data["timeout"],
            should_verify_certificate=config_data["should_verify_certificate"],
            headless_mode=config_data["headless_mode"]
        )


    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        for url in self.config_dto.seed_urls:
            if not re.match(r"https?://(www\.)?", url):
                raise IncorrectSeedURLError

        if not isinstance(self.config_dto.total_articles,
                          int) or self.config_dto.total_articles < 1 or self.config_dto.total_articles > 150:
            raise NumberOfArticlesOutOfRangeError

        if not isinstance(self.config_dto.headers, dict):
            raise IncorrectHeadersError

        if not isinstance(self.config_dto.encoding, str) or not self.config_dto.encoding:
            raise IncorrectEncodingError

        if not isinstance(self.config_dto.timeout, int) or self.config_dto.timeout < 1 or self.config_dto.timeout >= 60:
            raise IncorrectTimeoutError

        if not isinstance(self.config_dto.should_verify_certificate, bool):
            raise IncorrectVerifyError

    def get_seed_urls(self) -> list[str]:
        """
        Retrieve seed urls.

        Returns:
            list[str]: Seed urls
        """

    def get_num_articles(self) -> int:
        """
        Retrieve total number of articles to scrape.

        Returns:
            int: Total number of articles to scrape
        """

    def get_headers(self) -> dict[str, str]:
        """
        Retrieve headers to use during requesting.

        Returns:
            dict[str, str]: Headers
        """

    def get_encoding(self) -> str:
        """
        Retrieve encoding to use during parsing.

        Returns:
            str: Encoding
        """

    def get_timeout(self) -> int:
        """
        Retrieve number of seconds to wait for response.

        Returns:
            int: Number of seconds to wait for response
        """

    def get_verify_certificate(self) -> bool:
        """
        Retrieve whether to verify certificate.

        Returns:
            bool: Whether to verify certificate or not
        """

    def get_headless_mode(self) -> bool:
        """
        Retrieve whether to use headless mode.

        Returns:
            bool: Whether to use headless mode or not
        """


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

    try:
        response = requests.get(url, headers=headers, timeout=timeout, verify=verify)
        response.raise_for_status()
        return response
    except:
        raise requests.RequestException


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
        links = article_bs.find_all('a', href=True)
        for link in links:
            full_url = link['href']
            if full_url.startswith('/'):
                full_url = f"{link['base']}{full_url}"
            if re.match(r'https?://', full_url):
                return full_url

        return ""

    def find_articles(self) -> None:
        """
        Find articles.
        """
        seed_urls = self.config.get_seed_urls()
        for seed in seed_urls:
            response = make_request(seed, self.config)
            if response is None or response.status_code != 200:
                continue
            soup = BeautifulSoup(response.content.decode(self.config.get_encoding()), 'html.parser')
            article_url = self._extract_url(soup)
            if article_url and article_url not in self.urls:
                self.urls.append(article_url)

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
        content_div = article_soup.find('div', class_='content')
        if content_div:
            paragraphs = content_div.find_all('p')
            self.article.text = ' '.join([para.get_text(strip=True) for para in paragraphs])

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
        try:
            response = requests.get(self.full_url)
            response.raise_for_status()

            article_soup = BeautifulSoup(response.content, 'html.parser')

            self._fill_article_with_text(article_soup)

            return self.article

        except requests.RequestException as e:
            print(f"Error fetching the URL {self.full_url}: {e}")
            return False


def prepare_environment(base_path: Union[pathlib.Path, str]) -> None:
    """
    Create ASSETS_PATH folder if no created and remove existing folder.

    Args:
        base_path (Union[pathlib.Path, str]): Path where articles stores
    """
    assets_path = ASSETS_PATH

    if assets_path.exists():
        if assets_path.is_dir() and any(assets_path.iterdir()):
            shutil.rmtree(assets_path)

    assets_path.mkdir(parents=True, exist_ok=True)


def main() -> None:
    """
    Entrypoint for scrapper module.
    """
    configuration = Config(path_to_config=CRAWLER_CONFIG_PATH)
    full_url = "https://example.com/article/123"
    article_id = 123

    parser = HTMLParser(full_url, article_id, configuration)

    article = parser.parse()

    if isinstance(article, Article):
        try:
            to_raw(article)
            print(f"Article saved successfully: {article.title}")
        except Exception as e:
            print(f"Error saving article: {e}")
    else:
        print("Failed to parse article.")


if __name__ == "__main__":
    main()
