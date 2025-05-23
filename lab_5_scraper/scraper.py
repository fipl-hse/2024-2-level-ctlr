"""
Crawler implementation.
"""

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import datetime
import json
import pathlib
import re
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
        self.config = self._extract_config_content()

        self._seed_urls = self.config.seed_urls
        self._num_articles = self.config.total_articles
        self._headers = self.config.headers
        self._encoding = self.config.encoding
        self._timeout = self.config.timeout
        self._should_verify_certificate = self.config.should_verify_certificate
        self._headless_mode = self.config.headless_mode

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
            raise IncorrectSeedURLError('Seed URLs must be specified as a a list')

        for seed_url in config_data['seed_urls']:
            if not re.match(r'https?://(www\.)?', seed_url):
                raise IncorrectSeedURLError('Seed URL does not match standard pattern')

        if (not isinstance(config_data['total_articles_to_find_and_parse'], int) or
                config_data['total_articles_to_find_and_parse'] <= 0):
            raise IncorrectNumberOfArticlesError('Total number of articles '
                                                 'to parse is not integer or less than 0')

        if config_data['total_articles_to_find_and_parse'] > 150:
            raise NumberOfArticlesOutOfRangeError('Total number of articles '
                                                  'is out of range from 1 to 150')

        if not isinstance(config_data['headers'], dict):
            raise IncorrectHeadersError('Headers are not in a form of dictionary')

        if not isinstance(config_data['encoding'], str):
            raise IncorrectEncodingError('Encoding must be specified as a string')

        if (not isinstance(config_data['timeout'], int) or config_data['timeout'] <= 0 or
                config_data['timeout'] > 60):
            raise IncorrectTimeoutError('Timeout value must be a positive integer less than 60')

        if not isinstance(config_data['should_verify_certificate'], bool):
            raise IncorrectVerifyError('Verify certificate value must either be True or False')

        if not isinstance(config_data['headless_mode'], bool):
            raise IncorrectVerifyError('Headless mode value must either be True or False')

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
        self.url_pattern = re.compile(r'index\.php\?area=news&id=\d+')

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
            if re.match(self.url_pattern, href) and href not in self.urls:
                return href
        return ''

    def find_articles(self) -> None:
        """
        Find articles.
        """
        required_articles = self.config.get_num_articles()
        articles_collected = 1

        for seed_url in self.get_search_urls():
            response = make_request(seed_url, self.config)
            if not response.ok:
                continue

            soup = BeautifulSoup(response.text, 'lxml')
            article_links = soup.find_all('a', href=True)

            for link in article_links:
                href = link['href']
                if re.match(self.url_pattern, href) and href not in self.urls:
                    url = "https://v-life.ru/" + href
                    article = HTMLParser(url, articles_collected, self.config).parse()
                    if isinstance(article, Article) and len(article.text) > 50:
                        self.urls.append(url)
                        articles_collected += 1

                if articles_collected >= required_articles + 1:
                    print(f'Articles number achieved: {articles_collected - 1}')
                    return

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
        self.article = Article(self.full_url, self.article_id)

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        articles = article_soup.find_all("div", {"id": "content_main"})
        self.article.text = articles[1].text

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        news_post = article_soup.find_all('table', {"width": "100%",
                                                    "border": "0",
                                                    "cellspacing": "1",
                                                    "cellpadding": "3"})[1]
        title = news_post.find('h1')
        self.article.title = title.get_text().strip() if title else "NO TITLE"
        authors = news_post.find("td", {"align": "right"})
        self.article.author = authors.get_text().split(",") if authors else ["NOT FOUND"]

        publish_date = news_post.find("td", {"class": "small"})
        if publish_date and publish_date.get_text():
            self.article.date = self.unify_date_format(
                publish_date.get_text().split(":")[1].strip())

        topics = news_post.find("td", {"style": "FONT-SIZE: 11px;"})
        self.article.topics = topics.get_text().split(":")[1].split(",") if topics else None

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
        response = make_request(self.full_url, self.config)
        if response.ok:
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
    base_path.mkdir(parents=True, exist_ok=True)
    for file in base_path.iterdir():
        file.unlink(missing_ok=True)


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
