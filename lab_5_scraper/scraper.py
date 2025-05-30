"""
Crawler implementation.
"""

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import pathlib
import shutil
from typing import Pattern, Union
from core_utils.config_dto import ConfigDTO
from core_utils.constants import ASSETS_PATH, CRAWLER_CONFIG_PATH
from core_utils.article.article import Article
import json
import requests
from bs4 import BeautifulSoup
from time import sleep
from random import randint
from core_utils.article.io import to_raw
import datetime

class IncorrectSeedURLError(Exception):
    """
    Raises error when seed URL does not match standard pattern "https?://(www.)?"
    """
class NumberOfArticlesOutOfRangeError(Exception):
    """
    Raises error when total number of articles is out of range from 1 to 150
    """
class IncorrectNumberOfArticlesError(Exception):
    """
    Raises error when total number of articles to parse is not integer or less than 0
    """
class IncorrectHeadersError(Exception):
    """
    Raises error when headers are not in a form of dictionary
    """
class IncorrectEncodingError(Exception):
    """
    Raises error when encoding is not specified as a string
    """
class IncorrectTimeoutError(Exception):
    """
    Raises error when timeout value is not a positive integer less than 60
    """
class IncorrectVerifyError(Exception):
    """
    Raises error when verify certificate value is not either True or False
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
        config = self._extract_config_content()
        self._encoding = config.encoding
        self._headers = config.headers
        self._headless_mode = config.headless_mode
        self._seed_urls = config.seed_urls
        self._timeout = config.timeout
        self._num_articles = config.total_articles
        self._should_varify_certificate = config.should_verify_certificate



    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        with open(self.path_to_config, encoding ='UTF-8') as file:
            config_data = json.load(file)
        return ConfigDTO(**config_data)

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        with open(self.path_to_config, encoding ='UTF-8') as file:
            config_data = json.load(file)
        for url in config_data['seed_urls']:
            if "https://krai-dorogobuzhskii.ru" not in url or not isinstance(config_data['seed_urls'],list)\
                    or not config_data['seed_urls']:
                raise IncorrectSeedURLError('Seed URL does not match standard pattern.')

        if config_data['total_articles_to_find_and_parse'] < 1 or config_data['total_articles_to_find_and_parse'] > 150 or not isinstance(config_data['total_articles_to_find_and_parse'], int):
            raise NumberOfArticlesOutOfRangeError('Number of articles must be '
                                                  'in range from 1 to 150')

        if (not isinstance(config_data['total_articles_to_find_and_parse'], int) or
                config_data['total_articles_to_find_and_parse'] <= 0):
            raise IncorrectNumberOfArticlesError('Number of articles must be an integer'
                                                 'and not less than 0')

        if not isinstance(config_data['headers'], dict):
            raise IncorrectHeadersError('Headers must be in a form of dictionary')

        if not isinstance(config_data['encoding'], str):
            raise IncorrectEncodingError('Encoding must be specified as a string')

        if (not isinstance(config_data['timeout'], int)
                or not (config_data['timeout'] < 60 and config_data['timeout'] > 0)):
            raise IncorrectTimeoutError('Timeout must be a positive integer and less than 60')

        if not isinstance(config_data['should_verify_certificate'], bool) or not \
                isinstance(config_data['headless_mode'], bool):
            raise IncorrectVerifyError('Verify certificate value must be either True or False')






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
        return self._should_varify_certificate

    def get_headless_mode(self) -> bool:
        """
        Retrieve whether to use headless mode.

        Returns:
            bool: Whether to use headless mode or not
        """


def make_request(url: str, config: Config) -> requests.models.Response:
    """
    Deliver a response from a request with given configuration.

    Args:
        url (str): Site url
        config (Config): Configuration

    Returns:
        requests.models.Response: A response from a request
    """
    return requests.get(url = url,
                        headers = config.get_headers(),
                        timeout = config.get_timeout(),
                        verify = config.get_verify_certificate())

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
        self.urls = []
        self.config = config

    def _extract_url(self, article_bs: BeautifulSoup) -> str:
        """
        Find and retrieve url from HTML.

        Args:
            article_bs (bs4.BeautifulSoup): BeautifulSoup instance

        Returns:
            str: Url from HTML
        """
        article = article_bs.find('a', class_ = '')
        href = article.get('href') if article else None
        if isinstance(href, str):
            return href
        return ''

    def find_articles(self) -> None:
        """
        Find articles.
        """
        for seed_url in self.get_search_urls():
            response = make_request(seed_url, self.config)
            if len(self.urls) < self.config.get_num_articles():
                while True:
                    extracted_url = self._extract_url(BeautifulSoup(response.text, 'lxml'))
                    if extracted_url not in self.urls:
                        self.urls.append(extracted_url)

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
        content = article_soup.find('div', class_ = "entry-content clear" )
        text_marker = content.find_all('p')
        gettext = [text.get_text(strip=True) for text in text_marker]
        self.article.text = '\n'.join(gettext)

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        response = make_request(self.full_url, self.config)
        if response.ok:
            article_bs = BeautifulSoup(response.text, 'lxml')
            self._fill_article_with_text(article_bs)
        return self.article

    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """
        return datetime.datetime.strptime(date_str,'%d.%m.%Y')

    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """


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
    for i, url in enumerate(crawler.urls):
        sleep(randint(5,15))
        parser = HTMLParser(url, i+1, configuration)
        article = parser.parse()
        if isinstance(article, Article):
            to_raw(article)


if __name__ == "__main__":
    main()
