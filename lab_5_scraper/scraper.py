"""
Crawler implementation.
"""

# pylint: disable=too-many-arguments, too-many-instance-attributes,
# unused-import, undefined-variable, unused-argument

import datetime
import json
import pathlib
import re
from re import Pattern
from typing import Union

import requests
from bs4 import BeautifulSoup

from core_utils.article.article import Article
from core_utils.article.io import prepare_environment, to_meta, to_raw
from core_utils.config_dto import ConfigDTO
from core_utils.constants import (
    ASSETS_PATH,
    CRAWLER_CONFIG_PATH,
    NUM_ARTICLES_UPPER_LIMIT,
    TIMEOUT_LOWER_LIMIT,
    TIMEOUT_UPPER_LIMIT,
)


class IncorrectSeedURLError(Exception):
    """Raised when one or more seed URLs in the configuration are invalid."""


class NumberOfArticlesOutOfRangeError(Exception):
    """Raised when the requested number of articles exceeds the allowed upper limit."""


class IncorrectNumberOfArticlesError(Exception):
    """Raised when the number of articles to scrape is not a positive integer."""


class IncorrectHeadersError(Exception):
    """Raised when headers provided in the configuration are not in a valid dictionary format."""


class IncorrectEncodingError(Exception):
    """Raised when the specified encoding is not a valid non-empty string."""


class IncorrectTimeoutError(Exception):
    """Raised when the timeout value is not an integer or falls outside the acceptable range."""


class IncorrectVerifyError(Exception):
    """Raised when the certificate verification flag is not a boolean."""


class Config:
    """
    Class for unpacking and validating configurations.
    """

    def __init__(self, path_to_config: pathlib.Path) -> None:
        self.path_to_config = path_to_config
        self._seed_urls: list[str]
        self._num_articles: int
        self._headers: dict[str, str]
        self._encoding: str
        self._timeout: int
        self._should_verify_certificate: bool
        self._headless_mode: bool
        self._config_data = self._extract_config_content()
        self._validate_config_content()
        self._seed_urls = self._config_data.seed_urls
        self._num_articles = self._config_data.total_articles_to_find_and_parse
        self._headers = self._config_data.headers
        self._encoding = self._config_data.encoding
        self._timeout = self._config_data.timeout
        self._should_verify_certificate = self._config_data.should_verify_certificate
        self._headless_mode = self._config_data.headless_mode
        self.url_pattern = re.compile(
            r'^https://www\.kchetverg\.ru/\d{4}/\d{2}/\d{2}/[\w\-]+/?$'
        )

    def _extract_config_content(self) -> ConfigDTO:
        with open(self.path_to_config, 'r', encoding='utf-8') as file:
            config_data = json.load(file)

        return ConfigDTO(
            seed_urls=config_data["seed_urls"],
            total_articles_to_find_and_parse=config_data["total_articles_to_find_and_parse"],
            headers=config_data["headers"],
            encoding=config_data["encoding"],
            timeout=config_data["timeout"],
            should_verify_certificate=config_data["should_verify_certificate"],
            headless_mode=config_data["headless_mode"]
        )

    def _validate_config_content(self) -> None:
        url_pattern = re.compile(r'https?://(www\.)?.+')
        if(
    not isinstance(self._config_data.seed_urls, list)
    or not self._config_data.seed_urls
    ):
        raise IncorrectSeedURLError("seed_urls must be a non-empty list")
            for url in self._config_data.seed_urls:
                if not re.match(url_pattern, url):
                    raise IncorrectSeedURLError(f"Invalid seed URL: {url}")

        if(
    not isinstance(self._config_data.total_articles_to_find_and_parse, int)
    or self._config_data.total_articles_to_find_and_parse < 1:
    ):
          raise IncorrectNumberOfArticlesError("total_articles_to_find_and_parse must be an integer >= 1")
        if self._config_data.total_articles_to_find_and_parse > NUM_ARTICLES_UPPER_LIMIT:
            raise NumberOfArticlesOutOfRangeError(
                f"total_articles_to_find_and_parse must be <= {NUM_ARTICLES_UPPER_LIMIT}"
            )

        if not isinstance(self._config_data.headers, dict):
            raise IncorrectHeadersError("headers must be a dictionary")

        if not isinstance(self._config_data.encoding, str) or not self._config_data.encoding:
            raise IncorrectEncodingError("encoding must be a non-empty string")

        if not isinstance(self._config_data.timeout, int) or not (
            TIMEOUT_LOWER_LIMIT <= 
            self._config_data.timeout < TIMEOUT_UPPER_LIMIT
        ):
            raise IncorrectTimeoutError(
                f"timeout must be an integer in range [{TIMEOUT_LOWER_LIMIT}, {TIMEOUT_UPPER_LIMIT})"
            )

        if not isinstance(self._config_data.should_verify_certificate, bool):
            raise IncorrectVerifyError("should_verify_certificate must be a boolean")

        if not isinstance(self._config_data.headless_mode, bool):
            raise IncorrectVerifyError("headless_mode must be a boolean")

    def get_seed_urls(self) -> list[str]:
        """
        Retrieve seed urls.
        Returns:
            list[str]: Seed urls
        """
        return self._config_data.seed_urls

    def get_num_articles(self) -> int:
        """
        Retrieve total number of articles to scrape.
        Returns:
            int: Total number of articles to scrape
        """
        assert isinstance(self._config_data.total_articles_to_find_and_parse, int)
        return self._config_data.total_articles_to_find_and_parse

    def get_headers(self) -> dict[str, str]:
        """
        Retrieve headers to use during requesting.
        Returns:
            dict[str, str]: Headers
        """
        return self._config_data.headers

    def get_encoding(self) -> str:
        """
        Retrieve encoding to use during parsing.
        Returns:
            str: Encoding
        """
        return self._config_data.encoding

    def get_timeout(self) -> int:
        """
        Retrieve number of seconds to wait for response.
        Returns:
            int: Number of seconds to wait for response
        """
        return self._config_data.timeout

    def get_verify_certificate(self) -> bool:
        """
        Retrieve whether to verify certificate.
        Returns:
            bool: Whether to verify certificate or not
        """
        return self._config_data.should_verify_certificate

    def get_headless_mode(self) -> bool:
        """
        Retrieve whether to use headless mode.
        Returns:
            bool: Whether to use headless mode or not
        """
        return self._config_data.headless_mode


def make_request(url: str, config: Config) -> requests.models.Response:
    """
    Deliver a response from a request with given configuration.
    Args:
        url (str): Site url
        config (Config): Configuration
    Returns:
        requests.models.Response: A response from a request
    """
    response = requests.get(
        url,
        headers=config.get_headers(),
        timeout=config.get_timeout(),
        verify=config.get_verify_certificate()
    )
    response.raise_for_status()
    return response


class Crawler:
    """
    Crawler implementation.
    """

    url_pattern: Union[Pattern, str]

    def __init__(self, config: Config) -> None:
        """
        Initialize an instance of the Crawler class.
        Args:
            config (Config): Configuration
        """
        self._config = config
        self.urls = []

    def _extract_url(self, article_bs: BeautifulSoup) -> list[str]:
        """
        Find and retrieve url from HTML.

        Args:
            article_bs (bs4.BeautifulSoup): BeautifulSoup instance

        Returns:
            str: Url from HTML
        """
        article_urls = []
        pattern = re.compile(
            r'^https://www\.kchetverg\.ru/\d{4}/\d{2}/\d{2}/[\w\-]+/?$'
        )

        for a_tag in article_bs.find_all('a', href=True):
            href = a_tag['href']
            if pattern.match(href):
                article_urls.append(href)

        return article_urls

    def find_articles(self) -> None:
        """
        Find articles.
        """
        max_articles = self._config.get_num_articles()
        seed_urls = self._config.get_seed_urls()
        for url in seed_urls:
            if len(self.urls) >= max_articles:
                break
            try:
                response = make_request(url, self._config)
                soup = BeautifulSoup(response.text, 'html.parser')

                article_urls = self._extract_url(soup)

                for article_url in article_urls:
                    if article_url not in self.urls:
                        self.urls.append(article_url)
                    if len(self.urls) >= max_articles:
                        break

            except requests.RequestException as e:
                print(f"Error fetching {url}: {e}")

    def get_search_urls(self) -> list:
        """
        Get seed_urls param.
        Returns:
            list: seed_urls param
        """
        return self._config.get_seed_urls()


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
        article = article_soup.find("div", class_="entry-inner")
        full_text = ""
        paragraphs = article.find_all("p") if article else []

        if not paragraphs:
            full_text += article.get_text(strip=True, separator=" ") + "\n" if article else ""
        else:
            for p in paragraphs:
                full_text += p.get_text(strip=True) + "\n"

        self.article.text = full_text.strip()

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        title_tag = article_soup.find('h1')
        self.article.title = title_tag.get_text(strip=True) if title_tag else 'NOT FOUND'

        author_tags = article_soup.find_all(class_=re.compile(r'post_author', re.I))
        authors = [tag.get_text(strip=True) for tag in author_tags if tag.get_text(strip=True)]
        self.article.author = authors if authors else ["NOT FOUND"]

        byline = article_soup.find('p', class_='post-byline')
        if byline and 'Опубликовано:' in byline.text:
            date_raw = byline.text.strip().split('Опубликовано:')[1].split('|')[0].strip()
            self.article.date = self.unify_date_format(date_raw)

        topics = []
        category_li = article_soup.find('ul', class_='meta-single')
        if category_li:
            category_links = category_li.find_all('a', rel='category tag')
            topics = [a.text.strip() for a in category_links]

        self.article.topics = topics

    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format
            Returns:
            datetime.datetime: Datetime object
        """
        months_ru = {
            'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
            'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
            'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
        }
        parts = date_str.strip().split()
        day = parts[0]
        month = months_ru[parts[1].lower()]
        year = parts[2]
        time = parts[4]
        return datetime.datetime.strptime(f'{year}-{month}-{day} {time}', '%Y-%m-%d %H:%M')

    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """
        try:
            response = make_request(self.full_url, self.config)
            if not response or response.status_code != 200:
                return False
            article_soup = BeautifulSoup(response.text, 'html.parser')
            self._fill_article_with_meta_information(article_soup)
            self._fill_article_with_text(article_soup)
            return self.article
        except requests.exceptions.RequestException as e:
            print(f"Request error when fetching article {self.full_url}: {e}")
            return False
        except AttributeError as e:
            print(f"Attribute error when parsing article {self.full_url}: {e}")
            return False


def main() -> None:
    """
    Entrypoint for scrapper module.
    """
    configuration = Config(path_to_config=CRAWLER_CONFIG_PATH)
    prepare_environment(ASSETS_PATH)
    crawler = Crawler(config=configuration)
    crawler.find_articles()
    for i, url in enumerate(crawler.urls, start=1):
        parser = HTMLParser(full_url=url, article_id=i, config=configuration)
        article = parser.parse()
        if isinstance(article, Article):
            to_raw(article)
            to_meta(article)
        else:
            print("Parsing failed or returned unexpected result.")


if __name__ == "__main__":
    main()

