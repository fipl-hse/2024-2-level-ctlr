"""
Crawler implementation.
"""
import datetime
import json

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import pathlib
import shutil
from typing import Pattern, Union
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from core_utils.article.article import Article
from core_utils.article.io import to_meta, to_raw
from core_utils.config_dto import ConfigDTO
from core_utils.constants import ASSETS_PATH, CRAWLER_CONFIG_PATH


class IncorrectSeedURLError(Exception):
    """
    Seed URL does not match standard pattern "https?://(www.)?".
    """


class NumberOfArticlesOutOfRangeError(Exception):
    """
    Number of articles is out of range from 1 to 150.
    """


class IncorrectNumberOfArticlesError(Exception):
    """
    Number of articles to parse is not integer.
    """


class IncorrectHeadersError(Exception):
    """
    Headers are not in a form of dictionary.
    """


class IncorrectEncodingError(Exception):
    """
    Encoding must be specified as a string.
    """


class IncorrectTimeoutError(Exception):
    """
    Timeout value must be a positive integer less than 60.
    """


class IncorrectVerifyError(Exception):
    """
    Verify certificate value must either be True or False.
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
        prepare_environment(ASSETS_PATH)
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
        with open(self.path_to_config, 'r', encoding='utf-8') as f:
            config_dict = json.load(f)
        return ConfigDTO(**config_dict)

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        if not (isinstance(self._seed_urls, list) and all(isinstance(url, str) for url in self._seed_urls)):
            raise IncorrectSeedURLError('Seed URLs must be a list of strings')
        if not all(url.startswith('https://www.universalinternetlibrary.ru/') for url in
                   self._seed_urls):
            raise IncorrectSeedURLError('Invalid seed URL pattern')

        if not isinstance(self._num_articles, int) or self._num_articles <= 0:
            raise IncorrectNumberOfArticlesError("Number of articles must be integer")

        if not (1 <= self._num_articles <= 150):
            raise NumberOfArticlesOutOfRangeError("Number of articles must be 1-150")

        if not isinstance(self._headers, dict):
            raise IncorrectHeadersError("Headers must be a dictionary")

        if not isinstance(self._encoding, str):
            raise IncorrectEncodingError("Encoding must be a string")

        if (not isinstance(self._timeout, int) or
                not (1 <= self._timeout <= 60)):
            raise IncorrectTimeoutError("Timeout must be integer 1-60")

        if not isinstance(self._should_verify_certificate, bool):
            raise IncorrectVerifyError("Certificate verification must be boolean")

        if not isinstance(self._headless_mode, bool):
            raise IncorrectVerifyError("Headless mode must be boolean")

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
        block = article_bs.find('div', {'class': 'post-card__thumbnail'})
        if not block:
            return ''

        urls = block.find_all('a', href=True)
        for url in urls:
            href = url.get('href', '')
            if (href.startswith('https://www.universalinternetlibrary.ru')
                    and href not in self.urls):
                return href
        return ''

    def find_articles(self) -> None:
        """
        Find articles.
        """
        for seed_url in self.get_search_urls():
            if len(self.urls) >= self._config.get_num_articles():
                break

            try:
                response = make_request(seed_url, self._config)
                if not response.ok:
                    print(f"Request failed for URL: {seed_url} - Status code: {response.status_code}")
                    continue
            except (requests.RequestException, requests.Timeout):
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            articles_blocks = soup.find_all('div', class_='post-card__thumbnail')

            for block in articles_blocks:
                if len(self.urls) >= self._config.get_num_articles():
                    return

                link = block.find('a', href=True)
                if not link:
                    continue

                absolute_url = urljoin(seed_url, link['href'])
                if absolute_url not in self.urls:
                    self.urls.append(absolute_url)

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
        self.article = Article(url=full_url, article_id=article_id)

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        article_content = article_soup.find("div", class_="site-content-inner")
        if not article_content:
            self.article.text = "Article content not found"
            return

        text_blocks = [
            element.get_text(strip=True, separator="\n")
            for element in article_content.find_all(recursive=True)
            if element.get_text().strip()
        ]

        self.article.text = "\n\n".join(text_blocks)

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        title = article_soup.find('h1', {'class': 'entry-title'})
        self.article.title = title.text.strip() if title else "NOT FOUND"
        self.article.author = ['NOT FOUND']
        date = article_soup.find('time', {'itemprop': 'dateModified'}).text
        self.article.date = self.unify_date_format(date)
        self.article.topics = []

    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """
        eng = {
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

        date_parts = date_str.split(' ')
        month = date_parts[1].rstrip(',')
        if month in eng:
            date_parts[1] = eng[month]
        else:
            raise ValueError(f"Month '{month}' not recognized.")
        new_date_str = ' '.join(date_parts)

        return datetime.datetime.strptime(new_date_str, '%d %b %Y')

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
    configuration = Config(path_to_config=CRAWLER_CONFIG_PATH)
    crawler = Crawler(config=configuration)
    crawler.find_articles()
    for i, full_url in enumerate(crawler.urls, start=1):
        parser = HTMLParser(full_url=full_url, article_id=i, config=configuration)
        article = parser.parse()
        if isinstance(article, Article):
            to_raw(article)
            to_meta(article)


if __name__ == "__main__":
    main()
