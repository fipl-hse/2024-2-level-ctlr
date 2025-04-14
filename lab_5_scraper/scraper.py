"""
Crawler implementation.
"""

import datetime
import json

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import pathlib
import shutil
from typing import Pattern, Union

from bs4 import BeautifulSoup
import requests

from core_utils.article.article import Article
from core_utils.article.io import to_raw
from core_utils.config_dto import ConfigDTO
from core_utils.constants import CRAWLER_CONFIG_PATH, ASSETS_PATH


class IncorrectSeedURLError(Exception):
    """
    Raised when seed URL is not a valid URL
    """
    pass


class NumberOfArticlesOutOfRangeError(Exception):
    """
    Raised when number of articles is out of range from 1 to 150
    """
    pass


class IncorrectNumberOfArticlesError(Exception):
    """
    Raised when total number of articles to parse is not integer or is less than 0
    """
    pass


class IncorrectHeadersError(Exception):
    """
    Raised when headers are not a dictionary
    """
    pass


class IncorrectEncodingError(Exception):
    """
    Raised when encoding is not a string
    """
    pass


class IncorrectTimeoutError(Exception):
    """
    Raised when timeout value is not a positive integer less than 60
    """
    pass


class IncorrectVerifyError(Exception):
    """
    Raised when verify certificate value is not True or False
    """
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
        return ConfigDTO(**config)

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        if not isinstance(self._seed_urls, list) or \
                not all(isinstance(url, str) for url in self._seed_urls):
            raise IncorrectSeedURLError('Parameter _seed_urls of Config is malformed')
        if not all(url.startswith('https://ugra-news.ru') for url in self._seed_urls):
            raise IncorrectSeedURLError('Not all URLs belong to the original website')
        if not isinstance(self._num_articles, int) or \
                isinstance(self._num_articles, bool) or self._num_articles < 0:
            raise IncorrectNumberOfArticlesError('Invalid number of articles to pass')
        if self._num_articles > 150:
            raise NumberOfArticlesOutOfRangeError(
                'Number of articles out of range: should be between 1 and 150')
        if not isinstance(self._headers, dict):
            raise IncorrectHeadersError('Headers is not an instance of dict')
        if not isinstance(self._encoding, str):
            raise IncorrectEncodingError('Encoding is not an instance of str')
        if self._timeout not in range(61):
            raise IncorrectTimeoutError('Timeout out of range')
        if not isinstance(self._should_verify_certificate, bool):
            raise IncorrectVerifyError('should_verify_certificate is not an instance of bool')
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
    if not isinstance(url, str):
        raise ValueError('url is not str')
    request = requests.get(url, headers=config.get_headers(), timeout=config.get_timeout(),
                           verify=config.get_verify_certificate())
    return request


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
        prepare_environment(ASSETS_PATH)

    def _extract_url(self, article_bs: BeautifulSoup) -> str:
        """
        Find and retrieve url from HTML.

        Args:
            article_bs (bs4.BeautifulSoup): BeautifulSoup instance

        Returns:
            str: Url from HTML
        """
        all_a_links = article_bs.find_all('a', {'class': 'news-card photo'})
        for a_elem in all_a_links:
            href = a_elem['href']
            full_link = 'https://ugra-news.ru' + href
            if full_link not in self.urls:
                return full_link
        return 'STOP_SEED_URL_ITERATION'

    def find_articles(self) -> None:
        """
        Find articles.
        """
        for seed_url in self.get_search_urls():
            try:
                response = make_request(seed_url, self.config)
                if response.status_code != 200:
                    continue
                for i in range(10):
                    url = self._extract_url(BeautifulSoup(response.text, 'lxml'))
                    if url == 'STOP_SEED_URL_ITERATION':
                        break
                    if url not in self.urls:
                        self.urls.append(url)
                    if len(self.urls) >= self.config.get_num_articles():
                        break
            except ValueError:
                continue

    def get_search_urls(self) -> list:
        """
        Get seed_urls param.

        Returns:
            list: seed_urls param
        """
        return self.config.get_seed_urls()


class CrawlerRecursive(Crawler):
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
        super().__init__(config)
        self.urls = []
        self.start_url = 'https://ugra-news.ru/'

    def find_articles(self) -> None:
        """
        Finds articles doing recursive crawling.
        """
        pass


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
        self.config = config
        self.article = Article(full_url, article_id)

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        text = article_soup.find_all('div', {'class': 'news-detail__detail-text'})[0].text
        self.article.text = text

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        news_details = article_soup.find('div', {'class': 'news-detail'})
        title = news_details.find('h1', {'class': 'title'}).text
        self.article.title = title
        author = article_soup.find('div', {'class': 'author-news__info-authors'}).text
        self.article.author = [author.replace('\n', '').strip()]
        date = article_soup.find('span', {'class': 'author-news__info-text'}).text
        self.article.date = self.unify_date_format(date)
        topic = article_soup.find('a', {'class': 'tags min yellow'}).text.strip()
        self.article.topics = [topic]

    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """
        day, month, time = date_str.split()
        hour, minute = time.split(':')
        months_list = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
                       'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
        month_index = months_list.index(month) + 1
        return datetime.datetime(2025, month_index, int(day), int(hour), int(minute))

    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """
        response = make_request(self.article.url, self.config)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'lxml')
            self._fill_article_with_text(soup)
            self._fill_article_with_meta_information(soup)
        return self.article


def prepare_environment(base_path: Union[pathlib.Path, str]) -> None:
    """
    Create ASSETS_PATH folder if no created and remove existing folder.

    Args:
        base_path (Union[pathlib.Path, str]): Path where articles stores
    """
    try:
        shutil.rmtree(base_path)
    except FileNotFoundError:
        pass
    pathlib.Path(base_path).mkdir(parents=True)


def main() -> None:
    """
    Entrypoint for scrapper module.
    """
    config = Config(CRAWLER_CONFIG_PATH)
    crawler = Crawler(config)
    crawler.find_articles()
    for idx, url in enumerate(crawler.urls[:1]):
        parser = HTMLParser(url, idx + 1, config)
        article = parser.parse()
        to_raw(article)


if __name__ == "__main__":
    main()
