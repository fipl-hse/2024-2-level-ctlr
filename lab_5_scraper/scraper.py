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
from core_utils.constants import ASSETS_PATH, CRAWLER_CONFIG_PATH


class IncorrectSeedURLError(Exception):
    """
    Raising an error when a seed URL does not match standard pattern https?://(www.)?
    """


class NumberOfArticlesOutOfRangeError(Exception):
    """
    Raising an error when the total number of articles is out of range from 1 to 150;
    """


class IncorrectNumberOfArticlesError(Exception):
    """
    Raising an error when the total number of articles to parse is not integer
    """


class IncorrectHeadersError(Exception):
    """
    Raising an error when headers are not in a form of dictionary;
    """


class IncorrectEncodingError(Exception):
    """
    Raising an error when encoding is not a string;
    """


class IncorrectTimeoutError(Exception):
    """
    Raising an error when the timeout value is not a positive integer less than 60;
    """


class IncorrectVerifyError(Exception):
    """
    Raising an error when verify certificate is neither True nor False
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
        self._config = self._extract_config_content()
        self._validate_config_content()
        self._seed_urls = self._config.seed_urls
        self._num_articles = self._config.total_articles
        self._headers = self._config.headers
        self._encoding = self._config.encoding
        self._timeout = self._config.timeout
        self._should_verify_certificate = self._config.should_verify_certificate
        self._headless_mode = self._config.headless_mode



    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        with open(self.path_to_config, encoding='utf-8') as file:
            data = json.load(file)
        return ConfigDTO(**data)

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        config = self._config
        url_pattern = re.compile(r'https?://(?:www\.)?ria\.ru(?:/.*)?')

        if not isinstance(config.seed_urls, list) or \
                not all(re.match(url_pattern, x) for x in config.seed_urls):
            raise IncorrectSeedURLError('Oops, seed urls have to be a list '
                                        'of correct urls ~_~')

        if not isinstance(config.total_articles, int):
            raise IncorrectNumberOfArticlesError('Oops, number of articles has '
                                                 'to be an integer ~_~')
        if config.total_articles <= 0:
            raise IncorrectNumberOfArticlesError('Oops, there has to be at '
                                                 'least one article ~_~')
        if config.total_articles > 150:
            raise NumberOfArticlesOutOfRangeError('Oops, too many articles ~_~')

        if not isinstance(config.headers, dict):
            raise IncorrectHeadersError('Oops, headers have to be dictionary ~_~')

        if not isinstance(config.encoding, str):
            raise IncorrectEncodingError('Oops, encoding has to be a string ~_~')

        if not isinstance(config.timeout, int) or config.timeout <= 0:
            raise IncorrectTimeoutError('Oops, timeout has to be a positive integer ~_~')

        if config.timeout > 60:
            raise IncorrectTimeoutError('Oops, timeout has to be 60 seconds or less ~_~')

        if not isinstance(config.should_verify_certificate, bool):
            raise IncorrectVerifyError('Oops, certificate verification has to be a boolean value ~_~')

        if not isinstance(config.headless_mode, bool):
            raise IncorrectVerifyError('Oops, headless mode has to be a boolean value ~_~')

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

    return requests.get(
        url=url,
        timeout=config.get_timeout(),
        headers=config.get_headers(),
        verify=config.get_verify_certificate()
    )


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
        self.urls = []


    def _extract_url(self, article_bs: BeautifulSoup) -> str:
        """
        Find and retrieve url from HTML.

        Args:
            article_bs (bs4.BeautifulSoup): BeautifulSoup instance

        Returns:
            str: Url from HTML
        """
        link = article_bs.find('a', class_='list-item__title')
        href = link.get('href') if link else None

        if isinstance(href, str):
            if href.startswith('/'):
                return f"https://ria.ru{href}"
            if href.startswith('http'):
                return href
        return ''

    def find_articles(self) -> None:
        """
        Find articles.
        """
        seed_urls = self.get_search_urls()
        targets_needed = self._config.get_num_articles()

        for url in seed_urls:
            if len(self.urls) != targets_needed:
                response = make_request(url, self._config)
                if not response.ok:
                    continue
                bs = BeautifulSoup(response.text, 'lxml')
                extracted_url = self._extract_url(bs)
                while extracted_url:
                    self.urls.append(extracted_url)
                    if len(self.urls) == targets_needed:
                        break
                    extracted_url = self._extract_url(bs)
            if len(self.urls) == targets_needed:
                break


    def get_search_urls(self) -> list:
        """
        Get seed_urls param.

        Returns:
            list: seed_urls param
        """
        return self._config.get_seed_urls()


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
        article_blocks = article_soup.find_all('div', class_='article__text')
        article_text = []
        for block in article_blocks:
            text = block.get_text(separator=' ', strip=True)
            if text:
                article_text.append(text)
        self.article.text = '\n'.join(article_text)

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        title = article_soup.find('div', class_='article__title')

        self.article.title = title.text if title else 'NOT FOUND'

        self.article.author = ['NOT FOUND']
        date_block = article_soup.find('div', class_='article__info-date')
        if date_block and date_block.a:
            raw_date = date_block.a.text.strip()
            self.article.date = self.unify_date_format(raw_date)

        topic_tags = article_soup.find_all('a', rel='tag')
        self.article.topics = [tag.text.strip() for tag in topic_tags] if topic_tags else []

    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """
        return datetime.datetime.strptime(date_str, "%H:%M %d.%m.%Y")


    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """
        response = make_request(self.full_url, self.config)
        if not response.ok:
            return False
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
    if base_path.exists():
        shutil.rmtree(base_path)
    base_path.mkdir(parents=True)


def main() -> None:
    """
    Entrypoint for scrapper module.
    """
    config = Config(path_to_config=CRAWLER_CONFIG_PATH)
    prepare_environment(base_path=ASSETS_PATH)
    crawler = Crawler(config=config)
    crawler.find_articles()
    for index, url in enumerate(crawler.urls):
        parser = HTMLParser(full_url=url, article_id=index + 1, config=config)
        article = parser.parse()
        if isinstance(article, Article):
            to_raw(article)
            to_meta(article)


if __name__ == "__main__":
    main()
