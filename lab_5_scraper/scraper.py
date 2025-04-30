"""
Crawler implementation for Mordovia News website.
"""

import datetime
import json
import pathlib
import re
import shutil
from random import randint
from time import sleep
from typing import Pattern, Union

import requests
from bs4 import BeautifulSoup

from core_utils.article.article import Article
from core_utils.article.io import to_meta, to_raw
from core_utils.constants import ASSETS_PATH, CRAWLER_CONFIG_PATH


class IncorrectSeedURLError(Exception):
    """
    Raised when seed URL does not match the standard pattern for Mordovia News.
    """


class NumberOfArticlesOutOfRangeError(Exception):
    """
    Raised when number of articles is out of range from 1 to 150.
    """


class IncorrectNumberOfArticlesError(Exception):
    """
    Raised when total number of articles to parse is not integer or is less than 0.
    """


class IncorrectHeadersError(Exception):
    """
    Raised when headers are not a dictionary.
    """


class IncorrectEncodingError(Exception):
    """
    Raised when encoding is not a string.
    """


class IncorrectTimeoutError(Exception):
    """
    Raised when timeout value is not a positive integer less than 60.
    """


class IncorrectVerifyError(Exception):
    """
    Raised when verify certificate value is not True or False.
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
        self._seed_urls = []
        self._num_articles = 0
        self._headers = {}
        self._encoding = ""
        self._timeout = 0
        self._should_verify_certificate = True
        self._headless_mode = False
        self._load_config()
        self._validate_config_content()

    def _load_config(self) -> None:
        """
        Load configuration from JSON file.
        """
        with open(self.path_to_config, 'r', encoding='utf-8') as f:
            config = json.load(f)

        self._seed_urls = config['seed_urls']
        self._num_articles = config['total_articles_to_find_and_parse']
        self._headers = config['headers']
        self._encoding = config['encoding']
        self._timeout = config['timeout']
        self._should_verify_certificate = config['should_verify_certificate']
        self._headless_mode = config['headless_mode']

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        if not isinstance(self._seed_urls, list) or not all(isinstance(url, str) for url in self._seed_urls):
            raise IncorrectSeedURLError("Seed URLs must be a list of strings")

        if not all(
                url.startswith(('http://mordovia-news.ru', 'https://mordovia-news.ru'))
                for url in self._seed_urls
        ):
            raise IncorrectSeedURLError("Seed URLs must belong to Mordovia News domain (http or https)")

        if not isinstance(self._num_articles, int) or self._num_articles <= 0:
            raise IncorrectNumberOfArticlesError("Number of articles must be a positive integer")

        if self._num_articles > 150:
            raise NumberOfArticlesOutOfRangeError("Number of articles must be between 1 and 150")

        if not isinstance(self._headers, dict):
            raise IncorrectHeadersError("Headers must be a dictionary")

        if not isinstance(self._encoding, str):
            raise IncorrectEncodingError("Encoding must be a string")

        if not isinstance(self._timeout, int) or not 0 < self._timeout <= 60:
            raise IncorrectTimeoutError("Timeout must be an integer between 1 and 60")

        if not isinstance(self._should_verify_certificate, bool):
            raise IncorrectVerifyError("Certificate verification must be a boolean")

        if not isinstance(self._headless_mode, bool):
            raise IncorrectVerifyError("Headless mode must be a boolean")

    def get_seed_urls(self) -> list[str]:
        return self._seed_urls

    def get_num_articles(self) -> int:
        return self._num_articles

    def get_headers(self) -> dict[str, str]:
        return self._headers

    def get_encoding(self) -> str:
        return self._encoding

    def get_timeout(self) -> int:
        return self._timeout

    def get_verify_certificate(self) -> bool:
        return self._should_verify_certificate

    def get_headless_mode(self) -> bool:
        return self._headless_mode


def make_request(url: str, config: Config) -> requests.models.Response:
    """
    Make HTTP request to specified URL with given configuration.
    """
    try:
        response = requests.get(
            url,
            headers=config.get_headers(),
            timeout=config.get_timeout(),
            verify=config.get_verify_certificate()
        )
        response.encoding = config.get_encoding()
        return response
    except requests.exceptions.RequestException as e:
        print(f"Error making request to {url}: {e}")
        raise


class Crawler:
    """
    Crawler implementation for Mordovia News website.
    """

    url_pattern = re.compile(r'news-\d-\d{5}.htm')

    def __init__(self, config: Config) -> None:
        self.config = config
        self.urls = []
        prepare_environment(ASSETS_PATH)

    def _extract_url(self, article_bs: BeautifulSoup) -> str:
        """
        Extract article URL from BeautifulSoup object.
        """
        article_link = article_bs.find_all('dd', {'class': 'title'})
        for url in article_link:
            url = url.find('a')
            if 'https://mordovia-news.ru/' + url['href'] not in self.urls:
                return 'https://mordovia-news.ru/' + url['href'] if article_link else ''
        return ''

    def find_articles(self) -> None:
        """
        Find and collect article URLs from seed URLs.
        """
        for url in self.get_search_urls():
            response = make_request(url, self.config)
            if len(self.urls) >= self.config.get_num_articles():
                break
            if response.ok:
                for _ in range(10):
                    article_url = self._extract_url(BeautifulSoup(response.text, 'lxml'))
                    if article_url == '':
                        break
                    self.urls.append(article_url)
            continue

    def get_search_urls(self) -> list:
        return self.config.get_seed_urls()


class HTMLParser:
    """
    HTML parser implementation for Mordovia News articles.
    """

    def __init__(self, full_url: str, article_id: int, config: Config) -> None:
        self.full_url = full_url
        self.article_id = article_id
        self.config = config
        self.article = Article(full_url, article_id)

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Extract and set article text.
        """
        text_blocks = article_soup.find_all('p')
        self.article.text = '\n'.join(block.get_text().strip() for block in text_blocks)

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Extract and set article meta information.
        """
        title = article_soup.find('h1')
        self.article.title = title.get_text().strip() if title else 'No title'

        author = article_soup.find('span', class_='author')
        self.article.author = author.get_text().strip() if author else 'NOT FOUND'

        date_element = article_soup.find('time')
        if date_element:
            date_str = date_element.get('datetime') or date_element.get_text()
            self.article.date = self.unify_date_format(date_str)


    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format from various string representations.
        """
        try:
            return datetime.datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            pass

        try:
            return datetime.datetime.strptime(date_str, '%d.%m.%Y')
        except ValueError:
            pass

        return datetime.datetime.now()

    def parse(self) -> Union[Article, bool, list]:
        """
        Parse article content and metadata.
        """
        response = make_request(self.full_url, self.config)
        if response.status_code != 200:
            return False

        soup = BeautifulSoup(response.text, 'html.parser')
        self._fill_article_with_text(soup)
        self._fill_article_with_meta_information(soup)
        return self.article


def prepare_environment(base_path: Union[pathlib.Path, str]) -> None:
    """
    Prepare environment for scraping by cleaning and creating directories.
    """
    path = pathlib.Path(base_path)
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)


def main() -> None:
    """
    Main entry point for the scraper.
    """
    config = Config(CRAWLER_CONFIG_PATH)
    crawler = Crawler(config)
    crawler.find_articles()

    for idx, url in enumerate(crawler.urls[:config.get_num_articles()], 1):
        sleep(randint(1, 3))  # Be polite with delays between requests
        parser = HTMLParser(url, idx, config)
        article = parser.parse()

        if isinstance(article, Article):
            to_raw(article)
            to_meta(article)


if __name__ == "__main__":

    main()