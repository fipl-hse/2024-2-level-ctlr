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
from bs4 import BeautifulSoup

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
    Raises when encoding is not a string
    """


class IncorrectTimeoutError(Exception):
    """
    Raises when timeout value is not a positive integer less than 60
    """


class IncorrectVerifyError(Exception):
    """
    Raises when verify certificate value is neither ``True`` nor ``False``
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
        with open(self.path_to_config, 'r', encoding='UTF-8') as file:
            config_dto = json.load(file)
        return ConfigDTO(**config_dto)

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        if (not isinstance(self._seed_urls, list) or
                not all(isinstance(url, str) for url in self._seed_urls)):
            raise IncorrectSeedURLError('Wrong parameters of _seed_urls in Config')
        if not all(url.startswith('https://literaturno.com') for url in self._seed_urls):
            raise IncorrectSeedURLError('Seed URL does not match standard pattern')
        if not isinstance(self._num_articles, int) or self._num_articles <= 0:
            raise IncorrectNumberOfArticlesError('Number of articles is not int or less than 0')
        if self._num_articles > 150:
            raise NumberOfArticlesOutOfRangeError('Total number of articles is out of range')
        if not isinstance(self._headers, dict):
            raise IncorrectHeadersError('Headers are not in a form of dictionary')
        if not isinstance(self._encoding, str):
            raise IncorrectEncodingError('Encoding is not in a form of string')
        if not isinstance(self._timeout, int) or not 0 <= self._timeout <= 60:
            raise IncorrectTimeoutError('Wrong timeout value')
        if (not isinstance(self._should_verify_certificate, bool) or
                not isinstance(self._headless_mode, bool)):
            raise IncorrectVerifyError('should_verify_certificate must be bool')

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
    if not isinstance(url, str):
        raise ValueError('URL is not a string')
    response = requests.get(url, headers=config.get_headers(), timeout=config.get_timeout(),
                            verify=config.get_verify_certificate())
    response.encoding = config.get_encoding()
    sleep(randint(3, 5))
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
        all_links = article_bs.find_all(class_='description')
        if all_links:
            for link in all_links:
                href = link['href']
                if href not in self.urls and isinstance(href, str):
                    return href
        return ''

    def find_articles(self) -> None:
        """
        Find articles.
        """
        for seed_url in self.get_search_urls():
            if len(self.urls) == self.config.get_num_articles():
                break
            response = make_request(seed_url, self.config)
            if not response or not response.ok:
                continue
            soup = BeautifulSoup(response.text, 'lxml')
            divs = soup.find_all('div')
            for div in divs:
                url = self._extract_url(div)
                if not url:
                    continue
                self.urls.append(url)
                if len(self.urls) == self.config.get_num_articles():
                    break

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
        self.article = Article(full_url, article_id)
        self.config = config

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        full_text = []
        extract = article_soup.find('p', class_='description').text
        full_text.append(extract)
        article_body = article_soup.find('div', class_='entry-content')
        for part in article_body.find_all(['p', 'ol', 'ul']):
            link = part.find('a')
            if link and 'Читайте «Литературно»' in part.text:
                break
            text = part.text
            if text:
                full_text.append(text)
        self.article.text = '\n'.join(full_text)

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        title = article_soup.find('p', class_='single-news-title').text
        self.article.title = title
        self.article.author = ['NOT FOUND']
        date_json = article_soup.find(
            'script', {'type': 'application/ld+json', 'class': 'yoast-schema-graph'}).text
        date = json.loads(date_json)
        for elem in date.get('@graph'):
            if elem.get('@type') == 'WebPage':
                date_published = elem.get('datePublished')
                self.article.date = self.unify_date_format(date_published)

    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """
        return datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S%z")

    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """
        if self.article.url is None:
            return False
        article_bs = BeautifulSoup(make_request(self.article.url, self.config).text, 'lxml')
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
    for index, url in enumerate(crawler.urls, start=1):
        parser = HTMLParser(url, index, configuration)
        article = parser.parse()
        if isinstance(article, Article):
            to_raw(article)
            to_meta(article)


if __name__ == "__main__":
    main()
