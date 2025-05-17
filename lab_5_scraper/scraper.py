"""
Crawler implementation.
"""

import datetime

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import json
import re
import shutil
import time
from pathlib import Path
from random import uniform
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


class NumberOfArticlesOutOfRangeError(Exception):
    """
    Raised when the number of articles is too large in the configuration file.
    """


class IncorrectNumberOfArticlesError(Exception):
    """
    Raised when the number of articles is too small or not an integer in the configuration file.
    """


class IncorrectHeadersError(Exception):
    """
    Raised when the headers are not in a form of dictionary in the configuration file.
    """


class IncorrectEncodingError(Exception):
    """
    Raised when the encoding is not specified as a string in the configuration file.
    """


class IncorrectTimeoutError(Exception):
    """
    Raised when the timeout is too large or not a positive integer in the configuration file.
    """


class IncorrectVerifyError(Exception):
    """
    Raised when the verify certificate value is neither True nor False in the configuration file.
    """


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
        self.dto = self._extract_config_content()
        self._validate_config_content()
        self._seed_urls = self.dto.seed_urls
        self._num_articles = self.dto.total_articles
        self._headers = self.dto.headers
        self._encoding = self.dto.encoding
        self._timeout = self.dto.timeout
        self._should_verify_certificate = self.dto.should_verify_certificate
        self._headless_mode = self.dto.headless_mode

    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        with open(self.path_to_config, "r", encoding="utf-8") as file:
            config_dict = json.load(file)
        return ConfigDTO(**config_dict)

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        if not isinstance(self.dto.seed_urls, list):
            raise IncorrectSeedURLError(
                "Seed URLs should be a list of strings"
            )
        for url in self.dto.seed_urls:
            if not re.match("https?://(www.)?", url):
                raise IncorrectSeedURLError(
                    "Incorrect seed URL format in the config"
                )
        if not (isinstance(self.dto.total_articles, int) and self.dto.total_articles > 0):
            raise IncorrectNumberOfArticlesError(
                "Number of articles in the config is not an int or less than 1"
            )
        if self.dto.total_articles > NUM_ARTICLES_UPPER_LIMIT:
            raise NumberOfArticlesOutOfRangeError(
                "Number of articles in the config is out of range"
            )
        if not isinstance(self.dto.headers, dict):
            raise IncorrectHeadersError(
                "Headers need to be specified as a dictionary in the config"
            )
        if not isinstance(self.dto.encoding, str):
            raise IncorrectEncodingError(
                "Encoding should be a string in the config"
            )
        if not (isinstance(self.dto.timeout, int) and
                TIMEOUT_LOWER_LIMIT < self.dto.timeout < TIMEOUT_UPPER_LIMIT):
            raise IncorrectTimeoutError(
                "Timeout is incorrect or out of range in the config"
            )
        if not isinstance(self.dto.should_verify_certificate, bool):
            raise IncorrectVerifyError(
                "Verify Certificate value should be True of False in the config"
            )
        if not isinstance(self.dto.headless_mode, bool):
            raise IncorrectVerifyError(
                "Headless mode should be True of False in the config"
            )

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
    time.sleep(uniform(5, 8))
    response = requests.get(
        url,
        headers=config.get_headers(),
        timeout=config.get_timeout()
    )
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
        if article_bs.a is None:
            raise ValueError("Failed to reach the tag containing a link")
        link_text = article_bs.a["href"]
        if not isinstance(link_text, str):
            raise ValueError("The link is not a string")
        return link_text

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


class CrawlerRecursive(Crawler):
    """
    Recursive crawler implementation.
    """

    def __init__(self, config: Config) -> None:
        """
        Initialize an instance of the CrawlerRecursive class.

        Args:
            config (Config): Configuration
        """
        super().__init__(config)
        self.start_url = self.get_search_urls()[0]

    def find_articles(self) -> None:
        """
        Find articles.
        """
        loaded_html = make_request(self.start_url, self.config)
        if loaded_html.status_code >= 400:
            return
        soup = BeautifulSoup(loaded_html.text, "html.parser")
        links = soup.find_all("a", href=True)
        for link in links:
            if any(not_seed in link["href"] for not_seed in (
                "#", ".png", "javascript:", "mailto:", "tel:", "http"
            )):
                continue
            link_soup = BeautifulSoup(str(link), "html.parser")
            link_str = (str(urlparse(self.start_url).scheme) + "://" +
                        str(urlparse(self.start_url).netloc) +
                        self._extract_url(link_soup))
            if any(art_prefix in link["href"] for art_prefix in (
                    "news/", "article/", "blog/", "history", "contest/"
            )):
                with open(ASSETS_PATH / "article_urls.txt", "r", encoding="utf-8") as art_file:
                    url_list = art_file.readlines()
                if len(url_list) >= self.config.get_num_articles():
                    return
                if not link_str + "\n" in url_list:
                    with open(ASSETS_PATH / "article_urls.txt", "a", encoding="utf-8") as art_file:
                        art_file.write(link_str + "\n")
            else:
                with open(ASSETS_PATH / "seed_urls.txt", "r", encoding="utf-8") as seed_file:
                    seed_url_list = seed_file.readlines()
                if not link_str + "\n" in seed_url_list:
                    with open(ASSETS_PATH / "seed_urls.txt", "a", encoding="utf-8") as seed_file:
                        seed_file.write(link_str + "\n")
        with open(ASSETS_PATH / "seed_urls_crawled.txt", "a", encoding="utf-8") as c_seed_file:
            c_seed_file.write(self.start_url + "\n")
        with open(ASSETS_PATH / "seed_urls.txt", "r", encoding="utf-8") as seed_file:
            seed_url_list = seed_file.readlines()
        with open(ASSETS_PATH / "seed_urls_crawled.txt", "r", encoding="utf-8") as c_seed_file:
            crawled_url_list = c_seed_file.readlines()
        for seed in seed_url_list:
            if not seed + "\n" in crawled_url_list:
                self.start_url = seed
                self.find_articles()
                return


# 10
# 4, 6, 8, 10


class HTMLParser:
    """
    HTMLParser implementation.
    """
    article: Article

    def __init__(self, full_url: str, article_id: int, config: Config) -> None:
        """
        Initialize an instance of the HTMLParser class.

        Args:
            full_url (str): Site url
            article_id (int): Article id
            config (Config): Configuration
        """
        self.config = config
        self.full_url = full_url
        self.article_id = article_id
        self.article = Article(self.full_url, self.article_id)

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        text_blocks = article_soup.find_all("p")
        self.article.text = " ".join([
            block.text for block in text_blocks
            if not block.has_attr("style") or not "right" in block["style"]
        ])

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        title_exists = article_soup.find("h1")
        if title_exists:
            self.article.title = title_exists.text.strip()
        else:
            self.article.title = "NO TITLE"
        self.article.date = self.unify_date_format("01.01.1000 00:00")
        author = article_soup.find("a", class_="italic")
        if author:
            self.article.author.append(author.text.strip())
        else:
            lower_author = article_soup.find("p", style=re.compile("text-align: ?right"))
            if lower_author:
                self.article.author.append(lower_author.text.strip())
            else:
                self.article.author.append("NOT FOUND")

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
        if not isinstance(self.article.url, str):
            raise ValueError("The article URL is not a string")
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
        shutil.rmtree(base_path)
    base_path.mkdir(parents=True)


def main() -> None:
    """
    Entrypoint for scrapper module.
    """
    config = Config(CRAWLER_CONFIG_PATH)
    if (ASSETS_PATH / "session_ended.txt").exists():
        prepare_environment(ASSETS_PATH)
        with open(ASSETS_PATH / "article_urls.txt", "w", encoding="utf-8"):
            pass
        with open(ASSETS_PATH / "seed_urls.txt", "w", encoding="utf-8"):
            pass
        with open(ASSETS_PATH / "seed_urls_crawled.txt", "w", encoding="utf-8"):
            pass
    crawler = CrawlerRecursive(config=config)
    crawler.find_articles()
    with open(ASSETS_PATH / "session_ended.txt", "w", encoding="utf-8") as lock_file:
        lock_file.write("If this file exists, all articles have been collected.")
    with open(ASSETS_PATH / "article_urls.txt", "r", encoding="utf-8") as art_file:
        crawler.urls = art_file.readlines()
    for art_id, art_url in enumerate(crawler.urls):
        parser = HTMLParser(full_url=art_url, article_id=art_id+1, config=config)
        parsed_article = parser.parse()
        if not isinstance(parsed_article, Article):
            raise ValueError("Parsing failed")
        io.to_raw(parsed_article)
        io.to_meta(parsed_article)


if __name__ == "__main__":
    main()
