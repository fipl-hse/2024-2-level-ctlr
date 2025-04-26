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
    """Seed urls are not presented as a list or are not strings"""


class IncorrectNumberOfArticlesError(Exception):
    """Number of articles is not an integer or less than zero"""


class NumberOfArticlesOutOfRangeError(Exception):
    """Number of articles is bigger than 150"""


class IncorrectHeadersError(Exception):
    """Headers are not presented as a dictionary"""


class IncorrectEncodingError(Exception):
    """Encoding value is not a string"""


class IncorrectTimeoutError(Exception):
    """Timeout value is not an integer or less than 1 or bigger than 60"""


class IncorrectVerifyError(Exception):
    """Verify values are not a boolean"""


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
        self.config_dto = self._extract_config_content()
        self._seed_urls = self.config_dto.seed_urls
        self._num_articles = self.config_dto.total_articles
        self._headers = self.config_dto.headers
        self._encoding = self.config_dto.encoding
        self._timeout = self.config_dto.timeout
        self._should_verify_certificate = self.config_dto.should_verify_certificate
        self._headless_mode = self.config_dto.headless_mode

    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        with open(self.path_to_config, 'r', encoding='UTF-8') as file:
            data = json.load(file)
            return ConfigDTO(**data)

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        data = self._extract_config_content()
        if (not isinstance(data.seed_urls, list)
                or not all('https://sovsakh.ru/' in url for url in data.seed_urls)):
            raise IncorrectSeedURLError
        if (not isinstance(data.total_articles, int)
                or isinstance(data.total_articles, bool)
                or data.total_articles < 0):
            raise IncorrectNumberOfArticlesError
        if data.total_articles > 150:
            raise NumberOfArticlesOutOfRangeError
        if not isinstance(data.headers, dict):
            raise IncorrectHeadersError
        if not isinstance(data.encoding, str):
            raise IncorrectEncodingError
        if not isinstance(data.timeout, int) or not 0 < data.timeout <= 60:
            raise IncorrectTimeoutError
        if not isinstance(data.should_verify_certificate, bool):
            raise IncorrectVerifyError
        if not isinstance(data.headless_mode, bool):
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
    # sleep(randint(1, 10))
    response = requests.get(
        url,
        headers=config.get_headers(),
        timeout=config.get_timeout(),
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
        # block = article_bs.find('div', {'class': 'td_block_inner tdb-block-inner td-fix-index'})
        # if not block:
        #     article_bs.find('div', {'class': 'td-block-span6 td-post-prev-post'})
        urls = article_bs.find_all('a', href=True)
        for url in urls:
            if not url:
                continue
            href = url['href']
            if (href not in self.urls and href.startswith('https://sovsakh.ru/')
                    and 'category' not in href and 'reklama' not in href
                    and href.count('/') == 4):
                return str(href)
        return ''

    def find_articles(self) -> None:
        """
        Find articles.
        """
        urls = self.get_search_urls()
        for url in urls:
            if len(self.urls) > self.config.get_num_articles():
                return None
            response = make_request(url, self.config)
            if not response.ok:
                continue
            soup = BeautifulSoup(response.text, 'lxml')
            got_url = self._extract_url(soup)
            while got_url:
                if not got_url or got_url in self.urls:
                    break
                self.urls.append(got_url)
                got_url = self._extract_url(soup)
        return None

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
        self.article = Article(url=full_url, article_id=article_id)
        self.config = config

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        block = article_soup.find('div', {'class': 'td-ss-main-content'})
        texts = block.find_all('p')
        texts = [el.text for el in texts]
        self.article.text = '\n'.join(texts)

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        self.article.title = article_soup.find('h1', {'class': 'entry-title'}).text
        block = article_soup.find('div', {'class': 'td-ss-main-content'})
        texts = block.find_all('p')
        self.article.author = ['NOT FOUND']
        if len(texts) > 2:
            author = [el.text for el in texts][-2]
            if author and len(author) < 20:
                self.article.author = [author]
        self.article.date = self.unify_date_format(article_soup.find(
            'time',
            {'class': 'entry-date updated td-module-date'}).text
            )
        topics = article_soup.find_all('li', {'class': 'entry-category'})
        self.article.topics = [topic.text for topic in topics]

    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """
        ru_to_eng_months = {
            'января': 'Jan',
            'февраля': 'Feb',
            'марта': 'Mar',
            'апреля': 'Apr',
            'мая': 'May',
            'июня': 'Jun',
            'июля': 'Jul',
            'августа': 'Aug',
            'сентября': 'Sep',
            'октября': 'Oct',
            'ноября': 'Nov',
            'декабря': 'Dec'
        }
        date = date_str.split(' ')
        date[1] = ru_to_eng_months[date[1]]
        return datetime.datetime.strptime(' '.join(date), '%d %b %Y %H:%M')

    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """
        if not self.article.url:
            return False
        soup = BeautifulSoup(make_request(self.article.url, self.config).text, 'lxml')
        self._fill_article_with_text(soup)
        self._fill_article_with_meta_information(soup)
        return self.article


def prepare_environment(base_path: Union[pathlib.Path, str]) -> None:
    """
    Create ASSETS_PATH folder if no created and remove existing folder.

    Args:
        base_path (Union[pathlib.Path, str]): Path where articles stores
    """
    base_path = pathlib.Path(base_path)
    base_path.mkdir(exist_ok=True, parents=True)
    if any(base_path.iterdir()):
        shutil.rmtree(base_path)
        base_path.mkdir(parents=True)


class CrawlerRecursive(Crawler):
    """
    Recursive Crawler implementation.
    """
    def __init__(self, config: Config):
        """
        Initialize an instance of the CrawlerRecursive class.
        """
        super().__init__(config)
        self.start_url = "https://sovsakh.ru/category/obshhestvo/"
        self.recursive_path = ASSETS_PATH.parent / "recursive_articles.json"
        self.urls = []

    def find_articles(self) -> None:
        """
        Find articles.
        """
        if self.urls:
            with open(self.recursive_path, 'r', encoding=self.config.get_encoding()) as file:
                self.urls = json.load(file)
        if len(self.urls) < self.config.get_num_articles():
            response = make_request(self.start_url, self.config)
            if not response.ok:
                return
            soup = BeautifulSoup(response.text, 'lxml')
            got_url = self._extract_url(soup)
            if not got_url:
                if self.start_url == "https://sovsakh.ru/category/obshhestvo/":
                    pass
                else:
                    self.start_url = self.urls[self.urls.index(self.start_url) - 1]
            while got_url:
                if not got_url or got_url in self.urls:
                    break
                self.urls.append(got_url)
                with open(self.recursive_path, 'w', encoding=self.config.get_encoding()) as file:
                    json.dump(self.urls, file, indent=4)
                got_url = self._extract_url(soup)
            self.find_articles()


def main() -> None:
    """
    Entrypoint for scrapper module.
    """
    config = Config(CRAWLER_CONFIG_PATH)
    crawler = Crawler(config)
    prepare_environment(ASSETS_PATH)
    crawler.find_articles()
    article_id = 1
    for url in crawler.urls:
        parser = HTMLParser(url, article_id, config)
        article = parser.parse()
        if not article.text or len(article.text) <= 50:
            continue
        article_id += 1
        if isinstance(article, Article):
            to_raw(article)
            to_meta(article)


if __name__ == "__main__":
    main()
