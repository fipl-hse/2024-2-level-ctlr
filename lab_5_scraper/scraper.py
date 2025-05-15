"""
Crawler implementation.
"""

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument

import datetime
import json
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
    Seed URL does not match standard pattern "https?://(www.)?"
    """


class IncorrectNumberOfArticlesError(Exception):
    """
    Total number of articles is out of range from 1 to 150
    """


class NumberOfArticlesOutOfRangeError(Exception):
    """
    Total number of articles to parse is not integer or less than 0
    """


class IncorrectHeadersError(Exception):
    """
    Headers are not in a form of dictionary
    """


class IncorrectEncodingError(Exception):
    """
    Encoding must be specified as a string
    """


class IncorrectTimeoutError(Exception):
    """
    Timeout value must be a positive integer less than 60
    """


class IncorrectVerifyError(Exception):
    """
    Verify certificate value must either be True or False
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

        self._seed_urls = config.seed_urls
        self._num_articles = config.total_articles
        self._headers = config.headers
        self._encoding = config.encoding
        self._timeout = config.timeout
        self._should_verify_certificate = config.should_verify_certificate
        self._headless_mode = config.headless_mode

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
        with open(self.path_to_config, 'r', encoding='utf-8') as file:
            config_data = json.load(file)

        if not isinstance(config_data['seed_urls'], list):
            raise IncorrectSeedURLError('Seed URL must be a list :(')

        for seed_url in config_data['seed_urls']:
            if not re.match(r'https?://(www\.)?', seed_url):
                raise IncorrectSeedURLError('Seed URL does not match standard pattern :(')

        if (not isinstance(config_data['total_articles_to_find_and_parse'], int) or
                config_data['total_articles_to_find_and_parse'] <= 0):
            raise IncorrectNumberOfArticlesError('Number of articles must be '
                                                 'an integer and be positive :(')

        if config_data['total_articles_to_find_and_parse'] > 150:
            raise NumberOfArticlesOutOfRangeError('Number of articles cannot be more than 150 :(')

        if not isinstance(config_data['headers'], dict):
            raise IncorrectHeadersError('Headers must be a dictionary :(')

        if not isinstance(config_data['encoding'], str):
            raise IncorrectEncodingError('Encoding must be a string :(')

        if (not isinstance(config_data['timeout'], int) or config_data['timeout'] <= 0 or
                config_data['timeout'] > 60):
            raise IncorrectTimeoutError('Timeout must be an integer, more than 0 '
                                        'and less than 60 :(')

        if not isinstance(config_data['should_verify_certificate'], bool):
            raise IncorrectVerifyError('Certificate verification must be boolean :(')

        if not isinstance(config_data['headless_mode'], bool):
            raise IncorrectVerifyError('Headless mode must be boolean :(')

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
    response = requests.get(url=url,
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
        self.url_pattern = re.compile(r'https://moyaokruga\.ru'
                                      r'/mayakdelty/Articles\.aspx\?articleId=\d+')

    def _extract_url(self, article_bs: BeautifulSoup) -> str:
        """
        Find and retrieve url from HTML.

        Args:
            article_bs (bs4.BeautifulSoup): BeautifulSoup instance

        Returns:
            str: Url from HTML
        """
        article_links = article_bs.find_all('a', href=True)

        for link in article_links:
            href = str(link['href'])
            if re.match(self.url_pattern, href):
                return href
        return ''

    def find_articles(self) -> None:
        """
        Find articles.
        """
        required_articles = self.config.get_num_articles()

        for seed_url in self.get_search_urls():
            response = make_request(seed_url, self.config)
            if not response.ok:
                continue

            soup = BeautifulSoup(response.text, 'lxml')
            article_divs = soup.find_all('div')

            for div in article_divs:
                div_soup = BeautifulSoup(str(div), 'lxml')
                href = self._extract_url(div_soup)
                if re.match(self.url_pattern, href) and href not in self.urls:
                    self.urls.append(href)
                    if len(self.urls) >= required_articles:
                        print(f'Articles number achieved: {len(self.urls)}')
                        return

        print(f'Total: {len(self.urls)} articles (required: {required_articles})')

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
        self._full_url = full_url
        self._article_id = article_id
        self._config = config
        self.article = Article(self._full_url, self._article_id)

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        paragraphs = article_soup.find_all('p')
        self.article.text = '\n'.join(p.get_text(strip=True) for p in paragraphs)

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        title_tag = article_soup.find('a', id=
                                      'MainMasterContentPlaceHolder_InsidePlaceHolder_'
                                      'articleHeader')
        self.article.title = title_tag.get_text().strip() if title_tag else "NO TITLE"

        author = article_soup.find('a', class_='red', id=re.compile(r'.*_authorName$'))
        self.article.author = [author.get_text().strip()] if author else ["NOT FOUND"]

        time_tag = article_soup.find('time', id=re.compile
                                     (r'^MainMasterContentPlaceHolder_.*_articleTime$'))
        if time_tag and time_tag.text:
            self.article.date = self.unify_date_format(time_tag.text.strip())

        self.article.topics = []

    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """
        return datetime.datetime.strptime(date_str, '%d.%m.%Y')

    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """
        response = make_request(self._full_url, self._config)

        if not response or not response.ok:
            return False

        soup = BeautifulSoup(response.text, "lxml")
        self._fill_article_with_text(soup)
        self._fill_article_with_meta_information(soup)

        return self.article


def prepare_environment(base_path: Union[pathlib.Path, str]) -> None:
    """
    Create ASSETS_PATH folder if no created and remove existing folder.

    Args:
        base_path (Union[pathlib.Path, str]): Path where articles stores
    """
    path = pathlib.Path(base_path)
    if path.exists() and path.is_dir():
        shutil.rmtree(base_path)
    path.mkdir(parents=True, exist_ok=True)


def main() -> None:
    """
    Entrypoint for scrapper module.
    """
    config = Config(CRAWLER_CONFIG_PATH)
    prepare_environment(ASSETS_PATH)

    crawler = Crawler(config)
    crawler.find_articles()

    for article_id, url in enumerate(crawler.urls, start=1):
        parser = HTMLParser(url, article_id, config)
        article = parser.parse()
        if not article:
            continue
        if isinstance(article, Article):
            to_raw(article)
            to_meta(article)


if __name__ == "__main__":
    main()
