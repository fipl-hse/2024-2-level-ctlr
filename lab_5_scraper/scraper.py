"""
Crawler implementation.
"""

import datetime
import json

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import pathlib
import re
import shutil
from typing import Pattern, Union

import requests
from bs4 import BeautifulSoup

from core_utils.article.article import Article
from core_utils.article.io import to_meta, to_raw
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
     Raising an error when a seed URL does not match standard pattern "https?://(www.)?"
    """


class NumberOfArticlesOutOfRangeError(Exception):
    """
     Raising an error when total number of articles is out of range from 1 to 150
    """


class IncorrectNumberOfArticlesError(Exception):
    """
     Raising an error when total number of articles to parse is not integer or less than 0
    """


class IncorrectHeadersError(Exception):
    """
     Raising an error when headers are not in a form of dictionary
    """


class IncorrectEncodingError(Exception):
    """
     Raising an error when encoding is not specified as a string
    """


class IncorrectTimeoutError(Exception):
    """
     Raising an error when timeout value is not a positive integer less than 60
    """


class IncorrectVerifyError(Exception):
    """
     Raising an error when verify certificate value is not True or False
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
        self._validate_config_content()
        self.config = self._extract_config_content()

        self._seed_urls = self.config.seed_urls
        self._num_articles = self.config.total_articles
        self._headers = self.config.headers
        self._encoding = self.config.encoding
        self._timeout = self.config.timeout
        self._should_verify_certificate = self.config.should_verify_certificate
        self._headless_mode = self.config.headless_mode

    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        with self.path_to_config.open("r", encoding="UTF-8") as config_file:
            config = json.load(config_file)
            return ConfigDTO(**config)

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        config = self._extract_config_content()

        # Seed URL validation
        correct_seed_url_regex = re.compile("https?://(www.)?")
        if not (isinstance(config.seed_urls, list) and all(
                correct_seed_url_regex.match(url) for url in config.seed_urls)):
            raise IncorrectSeedURLError

        # Number of articles validation
        if not (isinstance(config.total_articles, int) and config.total_articles > 0):
            raise IncorrectNumberOfArticlesError
        if config.total_articles < 1 or config.total_articles > NUM_ARTICLES_UPPER_LIMIT:
            raise NumberOfArticlesOutOfRangeError

        # Headers validation
        if not isinstance(config.headers, dict):
            raise IncorrectHeadersError

        # Encoding validation
        if not isinstance(config.encoding, str):
            raise IncorrectEncodingError

        # Timeout validation
        if not (isinstance(config.timeout, int)
                and TIMEOUT_LOWER_LIMIT < config.timeout < TIMEOUT_UPPER_LIMIT):
            raise IncorrectTimeoutError

        # Verify certificate and headless mode validation
        if not (isinstance(config.should_verify_certificate, bool) and isinstance(
                config.headless_mode, bool)):
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
    response = requests.get(url=url,
                            headers=config.get_headers(),
                            timeout=config.get_timeout(),
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
        for title_tag in article_bs.find_all("h3", class_="news-title"):
            link_tag = title_tag.find("a", href=True)
            if link_tag:
                href = link_tag["href"]
                if not href.startswith("/news"):
                    continue
                full_url = "https://www.baikal-daily.ru" + str(href)
                if full_url and full_url not in self.urls:
                    return full_url
        return ""

    def find_articles(self) -> None:
        """
        Find articles.
        """
        for seed_url in self.config.get_seed_urls():
            if len(self.urls) == self.config.get_num_articles():
                break

            try:
                response = make_request(url=seed_url, config=self.config)
                response.raise_for_status()
            except requests.HTTPError:
                continue

            page_bs = BeautifulSoup(response.text, 'html.parser')

            extracted_url = self._extract_url(page_bs)
            while extracted_url:
                self.urls.append(extracted_url)
                if len(self.urls) == self.config.get_num_articles():
                    break
                extracted_url = self._extract_url(page_bs)

    def get_search_urls(self) -> list:
        """
        Get seed_urls param.

        Returns:
            list: seed_urls param
        """
        return self.config.get_seed_urls()


# # 10
# # 4, 6, 8, 10


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
        article_div = article_soup.find("div", class_="news-text")
        text = []
        for element in article_div:
            if element.get_text().strip():
                text.append(element.get_text(strip=True, separator="\n"))
        self.article.text = "\n".join(text)

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        self.article.article_id = self.article_id
        self.article.url = self.full_url
        self.article.author = ["NOT FOUND"]
        self.article.title = article_soup.find("h1", attrs={"itemprop": "headline"}).text

        time_tag = article_soup.find("span", class_="news-date-time").find("time")
        self.article.date = self.unify_date_format(time_tag.text.strip())

        topic_tag = article_soup.find("div", class_="news-tags").find_all("a")
        self.article.topics = [topic.text.strip() for topic in topic_tag]

    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """
        return datetime.datetime.strptime(date_str, "%d.%m.%Y %H:%M")

    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """
        response = make_request(self.full_url, self.config)
        if response.ok:
            article_bs = BeautifulSoup(response.text, 'html.parser')
            self._fill_article_with_text(article_bs)
            self._fill_article_with_meta_information(article_bs)
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
    configuration = Config(path_to_config=CRAWLER_CONFIG_PATH)
    prepare_environment(ASSETS_PATH)
    crawler = Crawler(config=configuration)
    crawler.find_articles()
    print(f"It is needed to find {configuration.get_num_articles()} articles")
    print(f"Crawler has found {len(crawler.urls)} urls")
    parser = HTMLParser("https://www.baikal-daily.ru/news/20/497902/", 1, configuration)
    parsed_article = parser.parse()
    if isinstance(parsed_article, Article):
        to_raw(parsed_article)
        to_meta(parsed_article)


if __name__ == "__main__":
    main()
