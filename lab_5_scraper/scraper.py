"""
Crawler implementation.
"""

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import datetime
import json
import shutil

import pathlib
from typing import Pattern, Union

import requests
from bs4 import BeautifulSoup

from core_utils.article.article import Article
from core_utils.config_dto import ConfigDTO
from core_utils.constants import ASSETS_PATH, CRAWLER_CONFIG_PATH


class IncorrectSeedURLError(Exception):
    """
    Raised when seed URL does not match standard pattern "https?://(www.)?".
    """


class NumberOfArticlesOutOfRangeError(Exception):
    """
    Raised when total number of articles is out of range from 1 to 150.
    """


class IncorrectNumberOfArticlesError(Exception):
    """
    Raised when total number of articles to parse is not integer or less than 0.
    """


class IncorrectHeadersError(Exception):
    """
    Raised when headers are not in a form of dictionary.
    """


class IncorrectEncodingError(Exception):
    """
    Raised when encoding must be specified as a string.
    """


class IncorrectTimeoutError(Exception):
    """
    Raised when timeout value must be a positive integer less than 60.
    """


class IncorrectVerifyError(Exception):
    """
    Raised when verify certificate value must either be True or False.
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
        if not isinstance(path_to_config, pathlib.Path):
            raise TypeError('Inappropriate type of path_to_config')
        self.path_to_config = path_to_config
        self.config = self._extract_config_content()
        self._seed_urls = self.get_seed_urls()
        self._num_articles = self.get_num_articles()
        self._headers = self.get_headers()
        self._encoding = self.get_encoding()
        self._timeout = self.get_timeout()
        self._should_verify_certificate = self.get_verify_certificate()
        self._validate_config_content()

    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        with open(self.path_to_config, encoding='utf-8') as config_file:
            config_file = json.load(config_file)
        if not isinstance(config_file, dict):
            raise TypeError('Inappropriate type of config_file')
        config = ConfigDTO(**config_file)
        return config

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        if (not isinstance(self._seed_urls, list)
                or not all(isinstance(url, str) for url in self._seed_urls)
                or not all(url.startswith("https://mel.fm") for url in self.config.seed_urls)):
            raise IncorrectSeedURLError('Seed URL does not match standard pattern "https?://(www.)?"')
        if (not isinstance(self._num_articles, int) or isinstance(self._num_articles, bool)
                or self._num_articles < 0):
            raise IncorrectNumberOfArticlesError('Total number of articles to parse is not integer or less than 0')
        if self._num_articles > 150:
            raise NumberOfArticlesOutOfRangeError('Total number of articles is out of range from 1 to 150')
        if not isinstance(self.config.headers, dict):
            raise IncorrectHeadersError('Headers are not in a form of dictionary')
        if not isinstance(self.config.encoding, str):
            raise IncorrectEncodingError('Encoding must be specified as a string')
        if (not isinstance(self.config.timeout, int)
                or self.config.timeout < 0
                or self.config.timeout > 60):
            raise IncorrectTimeoutError('Timeout value must be a positive integer less than 60')
        if (not isinstance(self.config.should_verify_certificate, bool)
                or not isinstance(self.config.headless_mode, bool)):
            raise IncorrectVerifyError('Verify certificate value must either be True or False')

    def get_seed_urls(self) -> list[str]:
        """
        Retrieve seed urls.

        Returns:
            list[str]: Seed urls
        """
        seed_urls = self.config.seed_urls
        return seed_urls

    def get_num_articles(self) -> int:
        """
        Retrieve total number of articles to scrape.

        Returns:
            int: Total number of articles to scrape
        """
        num_articles = self.config.total_articles
        return num_articles

    def get_headers(self) -> dict[str, str]:
        """
        Retrieve headers to use during requesting.

        Returns:
            dict[str, str]: Headers
        """
        headers = self.config.headers
        return headers

    def get_encoding(self) -> str:
        """
        Retrieve encoding to use during parsing.

        Returns:
            str: Encoding
        """
        encoding = self.config.encoding
        return encoding

    def get_timeout(self) -> int:
        """
        Retrieve number of seconds to wait for response.

        Returns:
            int: Number of seconds to wait for response
        """
        timeout = self.config.timeout
        return timeout

    def get_verify_certificate(self) -> bool:
        """
        Retrieve whether to verify certificate.

        Returns:
            bool: Whether to verify certificate or not
        """
        verify_certificate = self.config.should_verify_certificate
        return verify_certificate

    def get_headless_mode(self) -> bool:
        """
        Retrieve whether to use headless mode.

        Returns:
            bool: Whether to use headless mode or not
        """
        headless_mode = self.config.headless_mode
        return headless_mode


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
        raise TypeError('Inappropriate type of url')
    response = requests.get(url,
                            headers=config.get_headers(),
                            timeout=config.get_timeout(),
                            verify=config.get_verify_certificate())
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
        articles = article_bs.find_all('a', {'class': 'card__url card-half__url'})
        articles.extend(article_bs.find_all('a', {'class': 'card__url card-double__url'}))
        articles.extend(article_bs.find_all('a', {'class': 'card__url card-single'}))
        urls = ['https://mel.fm' + article['href'] for article in articles]
        for url in urls:
            if not url or not isinstance(url, str):
                return 'error'
            if url not in self.urls:
                if isinstance(url, str):
                    return url
            if url in self.urls:
                continue
        return ""

    def find_articles(self) -> None:
        """
        Find articles.
        """
        for url in self.get_search_urls():
            try:
                response = make_request(url, self.config)
                if response.ok:
                    while True:
                        url = self._extract_url(BeautifulSoup(response.text, 'lxml'))
                        if url in ("", "error"):
                            break
                        self.urls.append(url)
                if response.status_code != 200:
                    continue
            except ValueError:
                break

    def get_search_urls(self) -> list:
        """
        Get seed_urls param.

        Returns:
            list: seed_urls param
        """
        seed_urls = self.config.get_seed_urls()
        return seed_urls


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
        # try:
        article_text = ''
        raw_text = article_soup.find('div',
                                     {'class': 'b-pb-publication-body '
                                                     'b-pb-publication-body_pablo'}).find_all('p')
        raw_text.extend(article_soup.find('div',
                                     {'class': 'b-pb-publication-body '
                                                     'b-pb-publication-body_pablo'}).find_all('ol'))
        raw_text.extend(article_soup.find('div',
                                     {'class': 'b-pb-publication-body '
                                                    'b-pb-publication-body_pablo'}).find_all('ul'))
        for part in raw_text:
            article_text += " ".join(part.find_all(string=True)) + '\n'
        self.article.text = article_text

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """

    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """

    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """
        response = make_request(url=self.full_url, config=self.config)
        if response.ok:
            article_bs = BeautifulSoup(response.text, 'lxml')
            self._fill_article_with_text(article_bs)
        return self.article


def prepare_environment(base_path: Union[pathlib.Path, str]) -> None:
    """
    Create ASSETS_PATH folder if no created and remove existing folder.

    Args:
        base_path (Union[pathlib.Path, str]): Path where articles stores
    """
    try:
        base_path.mkdir(exist_ok=False)
    except FileExistsError:
        if base_path.stat().st_size != 0:
            shutil.rmtree(base_path)
            base_path.mkdir()
        else:
            pass
    except AttributeError:
        if not isinstance(base_path, str):
            raise TypeError from AttributeError('Inappropriate type of base_path')
        new_path = pathlib.Path(base_path)
        new_path.mkdir()


def main() -> None:
    """
    Entrypoint for scrapper module.
    """
    configuration = Config(path_to_config=CRAWLER_CONFIG_PATH)
    prepare_environment(ASSETS_PATH)
    crawler = Crawler(config=configuration)
    crawler.find_articles()
    # parser = HTMLParser(full_url=full_url, article_id=i, config=configuration)


if __name__ == "__main__":
    main()
