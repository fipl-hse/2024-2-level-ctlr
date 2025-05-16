"""
Crawler implementation.
"""
# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import datetime
import json
import pathlib
import shutil
from random import randint, uniform
from time import sleep
from typing import Pattern, Union

import requests
from bs4 import BeautifulSoup

from core_utils.article.article import Article
from core_utils.article.io import to_meta, to_raw
from core_utils.config_dto import ConfigDTO
from core_utils.constants import ASSETS_PATH, CRAWLER_CONFIG_PATH, PROJECT_ROOT


class IncorrectSeedURLError(Exception):
    """
    Seed urls are not presented as a list or are not strings
    """


class IncorrectNumberOfArticlesError(Exception):
    """
    Number of articles is not an integer or less than zero
    """


class NumberOfArticlesOutOfRangeError(Exception):
    """
    Number of articles is bigger than 150
    """


class IncorrectHeadersError(Exception):
    """
    Headers are not presented as a dictionary
    """


class IncorrectEncodingError(Exception):
    """
    Encoding value is not a string
    """


class IncorrectTimeoutError(Exception):
    """
    Timeout value is not an integer or less than 1 or bigger than 60
    """


class IncorrectVerifyError(Exception):
    """
    Verify values are not a boolean
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
        self.config_dto = self._extract_config_content()
        self._validate_config_content()
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
        if (not isinstance(self.config_dto.seed_urls, list)
                or not all('https://sovsakh.ru/' in url for url in self.config_dto.seed_urls)):
            raise IncorrectSeedURLError
        if (not isinstance(self.config_dto.total_articles, int)
                or isinstance(self.config_dto.total_articles, bool)
                or self.config_dto.total_articles < 0):
            raise IncorrectNumberOfArticlesError
        if self.config_dto.total_articles > 150:
            raise NumberOfArticlesOutOfRangeError
        if not isinstance(self.config_dto.headers, dict):
            raise IncorrectHeadersError
        if not isinstance(self.config_dto.encoding, str):
            raise IncorrectEncodingError
        if (not isinstance(self.config_dto.timeout, int) or self.config_dto.timeout <= 0
                or self.config_dto.timeout > 60):
            raise IncorrectTimeoutError
        if not isinstance(self.config_dto.should_verify_certificate, bool):
            raise IncorrectVerifyError
        if not isinstance(self.config_dto.headless_mode, bool):
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
    sleep(uniform(0.1, 0.5))
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
        url = article_bs.find('a', href=True).get('href')
        return str(url) if url else ''

    def find_articles(self) -> None:
        """
        Find articles.
        """
        for url in self.get_search_urls():
            response = make_request(url, self.config)
            if not response.ok:
                continue
            soup = BeautifulSoup(response.text, 'lxml')
            blocks = soup.find_all('h3', {'class': 'entry-title td-module-title'})
            for block in blocks:
                got_url = self._extract_url(block)
                if got_url and got_url not in self.urls and got_url.count('/') == 4:
                    self.urls.append(got_url)
                if len(self.urls) == self.config.get_num_articles():
                    return None
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
        if not block:
            self.article.text = ''
            return None
        texts = block.find_all('p')
        texts = [el.text for el in texts]
        self.article.text = '\n'.join(texts)
        return None

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        title = article_soup.find('h1', {'class': 'entry-title'})
        if title:
            self.article.title = title.text
        block = article_soup.find('div', {'class': 'td-ss-main-content'})
        if not block:
            block = article_soup.find('div', {'class': 'tdb-block-inner td-fix-index'})
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
        if not self.article.text:
            self.article.text = 'NOT FOUND'
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
    if base_path.exists():
        shutil.rmtree(base_path)
    base_path.mkdir(parents=True)


def prepare_recursive_environment(base_path: Union[pathlib.Path, str]) -> None:
    """
    Create ASSETS_PATH folder if no created and remove existing folder.

    Args:
        base_path (Union[pathlib.Path, str]): Path where articles stores
    """
    base_path = pathlib.Path(base_path)
    if not base_path.exists():
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
        self.start_url = 'https://sovsakh.ru/'
        self.folder_path = PROJECT_ROOT / "recursive_articles.json"
        self.urls = []
        self.loaded_urls = []
        self.data = {
            'all_urls': [],
            'looked_urls': []
        }

    def find_articles(self) -> None:
        """
        Find articles.
        """
        path = pathlib.Path(self.folder_path)
        if path.exists() and path.stat().st_size > 0:
            with open(self.folder_path, 'r', encoding=self.config.get_encoding()) as file:
                self.data = json.load(file)
        self.loaded_urls = self.data['all_urls']
        if len(self.urls) == self.config.get_num_articles():
            return None
        if not self.urls and not self.loaded_urls:
            url = self.start_url
        elif not self.urls and self.loaded_urls:
            url = self.loaded_urls[-1]
        else:
            n = randint(0, len(self.urls) - 1)
            url = self.urls[n]
        self.data['looked_urls'].append(url)
        response = make_request(url, self.config)
        if not response.ok:
            return None
        soup = BeautifulSoup(response.text, 'lxml')
        blocks1 = soup.find_all('h3', {'class': 'entry-title td-module-title'})
        blocks2 = soup.find_all('div', {'class': 'td-post-next-prev-content'})
        blocks3 = soup.find_all('div', {'class': 'td-related-span4'})
        blocks = blocks1 + blocks2 + blocks3
        for block in blocks:
            got_url = self._extract_url(block)
            if (got_url and got_url not in self.urls and got_url not in self.loaded_urls
                    and got_url.count('/') == 4):
                self.urls.append(got_url)
                self.data['all_urls'].append(got_url)
                with open(self.folder_path, 'w', encoding=self.config.get_encoding()) as file:
                    json.dump(self.data, file, indent=4)
            if len(self.urls) == self.config.get_num_articles():
                return None
        self.find_articles()
        return None


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
        if not article or not article.text or len(article.text) < 50:
            continue
        if isinstance(article, Article):
            to_raw(article)
            to_meta(article)
            article_id += 1


def recursive_main() -> None:
    """
    Entrypoint for scrapper module.
    """
    config = Config(CRAWLER_CONFIG_PATH)
    crawler = CrawlerRecursive(config)
    prepare_environment(ASSETS_PATH)
    crawler.find_articles()
    article_id = 1
    for url in crawler.urls:
        parser = HTMLParser(url, article_id, config)
        article = parser.parse()
        if not article or not article.text or len(article.text) < 50:
            continue
        if isinstance(article, Article):
            to_raw(article)
            to_meta(article)
            article_id += 1


if __name__ == "__main__":
    recursive_main()
