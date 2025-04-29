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
from bs4 import BeautifulSoup

from core_utils.article.article import Article
from core_utils.article.io import to_meta, to_raw
from core_utils.config_dto import ConfigDTO
from core_utils.constants import ASSETS_PATH, CRAWLER_CONFIG_PATH, NUM_ARTICLES_UPPER_LIMIT


class IncorrectSeedURLError (Exception):
    '''Incorrect seed Url Error'''

class NumberOfArticlesOutOfRangeError(Exception):
    '''Total number of articles to parse is out of range'''

class IncorrectNumberOfArticlesError(Exception):
    '''Total number of articles is incorrect'''

class IncorrectHeadersError(Exception):
    '''Headers are saved incorrectly'''

class IncorrectEncodingError(Exception):
    '''Incorrect encoding Error'''

class IncorrectTimeoutError(Exception):
    '''Incorrect Timeout Error'''

class IncorrectVerifyError(Exception):
    '''Verify is not bool Error'''

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
        with open(self.path_to_config, 'r', encoding='UTF-8') as config_data:
            config_data_to_return = json.load(config_data)
            dto = ConfigDTO(**config_data_to_return)
        return dto


    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        config_data = self._extract_config_content()

        if not isinstance(config_data.seed_urls, list):
            raise IncorrectSeedURLError(
                'Seed URLs must be a list of strings, not a single string')
        for url in config_data.seed_urls:
            if not url.startswith('https://'):
                raise IncorrectSeedURLError(
                    'Seed URL does not match standard pattern "https?://(www.)?"')

        if  not (isinstance(config_data.total_articles, int) and config_data.total_articles > 0):
            raise IncorrectNumberOfArticlesError(
                'Total number of articles to parse is not integer or less than 0')

        if (config_data.total_articles > NUM_ARTICLES_UPPER_LIMIT or
            config_data.total_articles < 1):
            raise NumberOfArticlesOutOfRangeError(
                'Total number of articles is out of range from 1 to 150')

        if not isinstance(config_data.headers, dict):
            raise IncorrectHeadersError('Headers are not in a form of dictionary')


        if not isinstance(config_data.encoding, str):
            raise IncorrectEncodingError('Encoding must be specified as a string')


        if (not isinstance(config_data.timeout, int) or
            config_data.timeout < 0 or
            config_data.timeout > 60):
            raise IncorrectTimeoutError('Timeout value must be a positive integer less than 60')

        if not isinstance(config_data.should_verify_certificate, bool):
            raise IncorrectVerifyError('Verify certificate value must either be "True" or "False"')

        if not isinstance(config_data.headless_mode, bool):
            raise IncorrectVerifyError('Headless mode must either be true or false')


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
        raise ValueError('URL must be a string!')
    site_request = requests.get(url=url,
                                    headers=config.get_headers(),
                                    timeout=config.get_timeout(),
                                    verify=config.get_verify_certificate())
    return site_request

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
        cards = article_bs.find_all(class_='card-title ms5')
        for card in cards:
            url = card.find("a", href=True)["href"]
            link = f'https://www.riakchr.ru{str(url)}'
            if link not in self.urls:
                return link
        return ''

    def find_articles(self) -> None:
        """
        Find articles.
        """
        seed_urls = self.config.get_seed_urls()
        for seed_url in seed_urls:
            if len(self.urls) >= self.config.get_num_articles():
                break

            seed_url_request = make_request(seed_url, self.config)
            if not seed_url_request.ok:
                continue

            seed_url_bs = BeautifulSoup(seed_url_request.text, 'html.parser')
            num_new_urls = len(set(seed_url_bs.find_all(class_='card-title ms5')))

            for num_of_urls in range(num_new_urls):
                found_article_url = self._extract_url(seed_url_bs)
                if found_article_url:
                    self.urls.append(found_article_url)
                else:
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
        self.full_url = full_url
        self.article_id = article_id
        self.config = config
        self.article = Article(url=self.full_url, article_id=self.article_id)

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        text = article_soup.find(class_="article-content").text
        self.article.text = text

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        self.article.author = ["NOT FOUND"]
        self.article.article_id = self.article_id
        self.article.title = article_soup.find(class_="ms7 titlenews").find("span").text
        date_to_unify = article_soup.find(class_="date").text.strip()
        print(date_to_unify)
        self.article.date = self.unify_date_format(date_to_unify)

    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """
        day, month, year = date_str.split('.')
        date = datetime.datetime(year=int(year), month=int(month), day=int(day))
        return date

    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """
        web_page = make_request(self.full_url, config=self.config)
        article_bs = BeautifulSoup(web_page.text, 'html.parser')
        self._fill_article_with_text(article_bs)
        self._fill_article_with_meta_information(article_bs)
        return self.article


def prepare_environment(base_path: Union[pathlib.Path, str]) -> None:
    """
    Create ASSETS_PATH folder if no created and remove existing folder.

    Args:
        base_path (Union[pathlib.Path, str]): Path where articles stores
    """
    if base_path.is_dir():
        shutil.rmtree(base_path)
    base_path.mkdir(parents=True)


def main() -> None:
    """
    Entrypoint for scrapper module.
    """

    configuration = Config(path_to_config=CRAWLER_CONFIG_PATH)

    crawler = Crawler(config=configuration)
    crawler.find_articles()
    urls = crawler.urls
    for i, full_url in enumerate(urls):
        parser = HTMLParser(full_url=full_url, article_id=i + 1, config=configuration)
        article_parsed = parser.parse()
        if isinstance(article_parsed, Article):
            to_raw(article_parsed)
            to_meta(article_parsed)




if __name__ == "__main__":
    main()
