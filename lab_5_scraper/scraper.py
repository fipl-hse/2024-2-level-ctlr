"""
Crawler implementation.
"""

import datetime
import json

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import pathlib
import shutil
from typing import Pattern, Union

import requests
import requests.compat
from bs4 import BeautifulSoup

from core_utils.article.article import Article
from core_utils.article.io import to_meta, to_raw
from core_utils.config_dto import ConfigDTO
from core_utils.constants import ASSETS_PATH, CRAWLER_CONFIG_PATH


class IncorrectSeedURLError(Exception):
    """Raised when seed URLs don't match the expected pattern."""


class NumberOfArticlesOutOfRangeError(Exception):
    """Raised when total articles are not between 1 and 150."""


class IncorrectNumberOfArticlesError(Exception):
    """Raised if article count is not a positive integer."""


class IncorrectHeadersError(Exception):
    """Raised if headers are not a dictionary."""


class IncorrectEncodingError(Exception):
    """Raised if encoding is not a string."""


class IncorrectTimeoutError(Exception):
    """Raised if timeout is not a positive integer < 60."""


class IncorrectVerifyError(Exception):
    """Raised if SSL verify is not True/False."""


my_website = 'https://nta-pfo.ru/'


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
        self.config = self._extract_config_content()
        self._seed_urls = self.config.seed_urls
        self._num_articles = self.config.total_articles
        self._headers = self.config.headers
        self._encoding = self.config.encoding
        self._timeout = self.config.timeout
        self._should_verify_certificate = self.config.should_verify_certificate
        self._headless_mode = self.config.headless_mode
        self._validate_config_content()

    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        with open(self.path_to_config, 'r', encoding='utf-8') as file:
            config_data = json.load(file)
        return ConfigDTO(**config_data)

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        if not isinstance(self._seed_urls, list):
            raise IncorrectSeedURLError("Seed urls must be a list")
        for url in self._seed_urls:
            if not url.startswith(my_website):
                raise IncorrectSeedURLError(f"Invalid seed URL. Must start with {my_website}")

        if (not isinstance(self._num_articles, int) or isinstance(self._num_articles, bool)
                or self._num_articles < 0):
            raise IncorrectNumberOfArticlesError("Article count must be an integer.")

        if self._num_articles > 150:
            raise NumberOfArticlesOutOfRangeError("Article count must be between 1 and 150.")

        if not isinstance(self._headers, dict):
            raise IncorrectHeadersError("Headers must be a dictionary.")

        if not isinstance(self._encoding, str):
            raise IncorrectEncodingError("Encoding must be a string.")

        if not isinstance(self._timeout, int) or self._timeout <= 0 or self._timeout >= 60:
            raise IncorrectTimeoutError("Timeout must be a positive integer < 60.")

        if not isinstance(self._should_verify_certificate, bool):
            raise IncorrectVerifyError("SSL verify must be True or False.")

        if not isinstance(self._headless_mode, bool):
            raise IncorrectVerifyError('headless_mode value should be an instance of bool')

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
    response = requests.get(url, headers=config.get_headers(),
                            timeout=config.get_timeout(), verify=config.get_verify_certificate())
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
        article_link = article_bs.find('a', href=lambda x: x and '/news/' in x)
        if article_link and article_link.get('href'):
            absolute_url = requests.compat.urljoin(my_website, article_link['href'])
            if absolute_url.startswith(my_website):
                return absolute_url
        return ''

    def find_articles(self) -> None:
        """
        Find articles.
        """
        for seed_url in self.get_search_urls():
            response = make_request(seed_url, self.config)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                while True:
                    url = self._extract_url(soup)
                    if not url:
                        break
                    if url not in self.urls:
                        self.urls.append(url)
                    soup.find('a', href=lambda x: x and url.endswith(x)).decompose()

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
        self.article = Article(full_url, article_id)

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        text_blocks = article_soup.find_all(['p', 'div'])
        cleaned_text = []

        for block in text_blocks:
            text = block.get_text(' ', strip=True)
            if text and len(text.split()) > 5:
                cleaned_text.append(text)

        self.article.text = '\n'.join(cleaned_text) if cleaned_text else 'No text found'

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        title = article_soup.find('h1') or article_soup.find('title')
        self.article.title = title.get_text(strip=True) if title else self.full_url

        author = article_soup.find(class_=lambda x: x and 'author' in x.lower())
        self.article.author = author.get_text(strip=True) if author else 'Unknown author'

        date_element = (article_soup.find('time') or
                        article_soup.find(class_=lambda x: x and 'date' in x.lower()))
        if date_element:
            date_str = date_element.get('datetime') or date_element.get_text(strip=True)
            self.article.date = self.unify_date_format(date_str)
        else:
            self.article.date = datetime.datetime.now()

    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """
        for fmt in ('%Y-%m-%d', '%d.%m.%Y', '%B %d, %Y', '%d %B %Y', '%Y-%m-%dT%H:%M:%S'):
            try:
                return datetime.datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return datetime.datetime.fromisoformat(date_str.split('T')[0])

    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """
        response = make_request(self.article.url, self.config)
        if response.ok:
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
    path = pathlib.Path(base_path)

    if path.exists():
        shutil.rmtree(path)

    path.mkdir(parents=True, exist_ok=True)


def main() -> None:
    """
    Entrypoint for scrapper module.
    """
    prepare_environment(ASSETS_PATH)
    configuration = Config(path_to_config=CRAWLER_CONFIG_PATH)
    crawler = Crawler(config=configuration)
    crawler.find_articles()
    for i, full_url in enumerate(crawler.urls, 1):
        parser = HTMLParser(full_url=full_url, article_id=i, config=configuration)
        article = parser.parse()
        if isinstance(article, Article):
            to_raw(article)
            to_meta(article)


if __name__ == "__main__":
    # first change
    main()
