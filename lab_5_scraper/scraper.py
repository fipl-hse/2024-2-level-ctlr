"""
Crawler implementation.
"""

import datetime
import json

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import pathlib
import shutil
from random import randint
from time import sleep
from typing import Pattern, Union

import requests
from bs4 import BeautifulSoup, Tag

from core_utils.article.article import Article
from core_utils.article.io import to_meta, to_raw
from core_utils.config_dto import ConfigDTO
from core_utils.constants import ASSETS_PATH, CRAWLER_CONFIG_PATH


class IncorrectSeedURLError(Exception):
    """
        Raises when seed URL does not match standard pattern
        """


class NumberOfArticlesOutOfRangeError(Exception):
    """
        Raises when total number of articles is out of range from 1 to 150
        """


class IncorrectNumberOfArticlesError(Exception):
    """
        Raises when total number of articles to parse is not integer or less than 0
        """


class IncorrectHeadersError(Exception):
    """
        Raises when headers are not in a form of dictionary
        """


class IncorrectEncodingError(Exception):
    """
        Raises when encoding is not specified as a string
        """


class IncorrectTimeoutError(Exception):
    """
        Raises when timeout value is not a positive integer less than 60
        """


class IncorrectVerifyError(Exception):
    """
        Raises when verify certificate value is not either True or False
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
        config = self._extract_config_content()
        self._seed_urls = config.seed_urls
        self._num_articles = config.total_articles
        self._headers = config.headers
        self._encoding = config.encoding
        self._timeout = config.timeout
        self._should_verify_certificate = config.should_verify_certificate
        self._headless_mode = config.headless_mode
        self._validate_config_content()

    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        with open(self.path_to_config, encoding='utf-8') as file:
            config = json.load(file)
        return ConfigDTO(
            seed_urls=config["seed_urls"],
            total_articles_to_find_and_parse=config["total_articles_to_find_and_parse"],
            headers=config["headers"],
            encoding=config["encoding"],
            timeout=config["timeout"],
            should_verify_certificate=config["should_verify_certificate"],
            headless_mode=config["headless_mode"]
        )

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        if not (isinstance(self._seed_urls, list)
                and all(isinstance(url, str) for url in self._seed_urls)
                and all(url.startswith("https://govoritnn.ru") for url in self._seed_urls)):
            raise IncorrectSeedURLError("Seed URL is not a valid URL")
        if (not isinstance(self._num_articles, int)
                or self._num_articles < 0
                or isinstance(self._num_articles, bool)):
            raise (IncorrectNumberOfArticlesError
                   ('Total number of articles to parse is not integer or less than 0'))
        if self._num_articles > 150:
            raise (NumberOfArticlesOutOfRangeError
                   ('Total number of articles is out of range from 1 to 150'))
        if not isinstance(self._headers, dict):
            raise IncorrectHeadersError('Headers are not in a form of dictionary;')
        if not isinstance(self._encoding, str):
            raise IncorrectEncodingError('Encoding must be specified as a string')
        if not isinstance(self._timeout, int) or self._timeout > 60 or self._timeout < 0:
            raise IncorrectTimeoutError('Timeout value must be a positive integer less than 60')
        if not isinstance(self._should_verify_certificate, bool):
            raise IncorrectVerifyError('Verify certificate value must either be True or False')
        if not isinstance(self._headless_mode, bool):
            raise IncorrectVerifyError('Headless mode value must either be True or False')

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
    response = requests.get(url,
                            headers=config.get_headers(),
                            timeout=config.get_timeout(),
                            verify=config.get_verify_certificate(),
                            )
    response.encoding = config.get_encoding()
    #sleep(randint(1, 10))
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
        link = article_bs.find('a', {'href': True, 'class': 'lsd-arch-link'})
        if link:
            href = link.get("href")
            if isinstance(href, str):
                return href
        return ""

    def find_articles(self) -> None:
        """
        Find articles.
        """
        for seed_url in self.get_search_urls():
            if len(self.urls) >= self.config.get_num_articles():
                break
            response = make_request(seed_url, self.config)
            if response.ok:
                soup = BeautifulSoup(response.text, 'lxml')
                articles = soup.find_all('article', class_='blog-post-wrap')
                for article in articles:
                    if len(self.urls) >= self.config.get_num_articles():
                        break
                    url = self._extract_url(article)
                    if url and url not in self.urls:
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
        body_content = article_soup.find('div', class_='body-content post-content-wrap')
        body_paragraphs = []
        for p in body_content.find_all('p'):
            text = p.get_text(strip=True)
            if not text.startswith('Фото:') and text:
                body_paragraphs.append(text)
        article_text = "\n\n".join(body_paragraphs)

        self.article.text = article_text

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        div = article_soup.find('div', class_='col-md-6 col-md-push-3')
        if div and div.find('h1', class_='entry-title'):
            entry_title = div.find('h1', class_='entry-title')
            if entry_title:
                self.article.title = entry_title.get_text(strip=True)

            article_author = div.find('h3', class_='user-name')
            if article_author:
                self.article.author = ([article_author.get_text(strip=True)]
                                       if article_author else ["NOT FOUND"])

            date_meta = article_soup.find('meta', {'property': 'article:published_time'})
            if date_meta:
                date_str = date_meta.get('datetime') or date_meta.get('content')
                if isinstance(date_str, str):
                    self.article.date = self.unify_date_format(date_str)
                else:
                    self.article.date = datetime.datetime.now().replace(microsecond=0)
            else:
                self.article.date = datetime.datetime.now().replace(microsecond=0)

            self.article.topics = []

    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """
        date_str = date_str[:-6]
        for el in date_str:
            if el.isalpha():
                date_str = date_str.replace(el, ' ')
        return datetime.datetime(int(date_str[:4]), int(date_str[5:7]),
                                 int(date_str[8:10]), int(date_str[11:13]), int(date_str[14:16]))

    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """
        response = make_request(self.full_url, self.config)
        if not response.ok:
            return self.article
        article_bs = BeautifulSoup(response.text, 'lxml')
        self._fill_article_with_text(article_bs)
        self._fill_article_with_meta_information(article_bs)
        return self.article


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
    prepare_environment(ASSETS_PATH)
    crawler = Crawler(config=configuration)
    crawler.find_articles()
    for i, full_url in enumerate(crawler.urls):
        parser = HTMLParser(full_url=full_url, article_id=i+1, config=configuration)
        article = parser.parse()
        if isinstance(article, Article):
            to_raw(article)
            to_meta(article)


if __name__ == "__main__":
    main()
