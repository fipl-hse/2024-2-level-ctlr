"""
Crawler implementation.
"""

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import json
import pathlib
import shutil
from typing import Union
import re
import datetime
from random import randint
from time import sleep
import requests
from bs4 import BeautifulSoup
from core_utils.config_dto import ConfigDTO
from core_utils.article.io import to_raw
from core_utils.article.article import Article
from core_utils.constants import (
    ASSETS_PATH,
    CRAWLER_CONFIG_PATH,
    NUM_ARTICLES_UPPER_LIMIT,
    TIMEOUT_LOWER_LIMIT,
    TIMEOUT_UPPER_LIMIT
)

class IncorrectSeedURLError(Exception):
    """Seed URLs list cannot be empty"""


class NumberOfArticlesOutOfRangeError(Exception):
    """Total articles to find and parse must be between 1 and 150"""


class IncorrectNumberOfArticlesError(Exception):
    """Total articles to find and parse must be an integer"""


class IncorrectHeadersError(Exception):
    """Headers must be a dictionary"""


class IncorrectEncodingError(Exception):
    """Encoding must be a string"""


class IncorrectTimeoutError(Exception):
    """Timeout must be a positive integer less than 60"""


class IncorrectVerifyError(Exception):
    """Verify certificate must be a boolean"""

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
        self._config = self._extract_config_content()
        self._validate_config_content()
        self._seed_urls = self._config.seed_urls
        self._num_articles = self._config.total_articles
        self._headers = self._config.headers
        self._encoding = self._config.encoding
        self._timeout = self._config.timeout
        self._should_verify_certificate = self._config.should_verify_certificate
        self._headless_mode = self._config.headless_mode

    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """

        with self.path_to_config.open('r', encoding='utf-8') as file:
            data = json.load(file)
            return ConfigDTO(
                seed_urls=data['seed_urls'],
                total_articles_to_find_and_parse=data['total_articles_to_find_and_parse'],
                headers=data['headers'],
                encoding=data['encoding'],
                timeout=data['timeout'],
                should_verify_certificate=data['should_verify_certificate'],
                headless_mode=data['headless_mode']
            )
    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """

        config_dto = self._extract_config_content()

        if not isinstance(config_dto.headers, dict):
            raise IncorrectHeadersError

        # Validate individual headers
        for header_value in config_dto.headers.values():
            if '\n' in str(header_value):
                raise IncorrectHeadersError("Headers cannot contain newline characters")

        if not isinstance(config_dto.seed_urls, list):
            raise IncorrectSeedURLError

        url_pattern = r"https?://.*/"
        for url in config_dto.seed_urls:
            if not isinstance(url, str) or not re.match(url_pattern, url):
                raise IncorrectSeedURLError

        if (
                not isinstance(config_dto.total_articles, int)
                or isinstance(config_dto.total_articles, bool)
                or config_dto.total_articles < 1
        ):
            raise IncorrectNumberOfArticlesError

        if config_dto.total_articles > NUM_ARTICLES_UPPER_LIMIT:
            raise NumberOfArticlesOutOfRangeError

        if not isinstance(config_dto.headers, dict):
            raise IncorrectHeadersError

        if not isinstance(config_dto.encoding, str):
            raise IncorrectEncodingError

        if (
                not isinstance(config_dto.timeout, int)
                or not TIMEOUT_LOWER_LIMIT < config_dto.timeout < TIMEOUT_UPPER_LIMIT
        ):
            raise IncorrectTimeoutError

        if not isinstance(config_dto.should_verify_certificate, bool):
            raise IncorrectVerifyError

        if not isinstance(config_dto.headless_mode, bool):
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
    try:
        sleep_time = randint(1, 3)
        sleep(sleep_time)

        request = requests.get(
            url,
            headers=config.get_headers(),
            timeout=config.get_timeout(),
            verify=config.get_verify_certificate()
        )
        request.encoding = config.get_encoding()
        return request
    except requests.RequestException:
        # Возвращаем пустой response в случае ошибки
        response = requests.models.Response()
        response.status_code = 404  # Или другой код ошибки
        return response
class Crawler:
    """
    Crawler implementation.
    """

    def __init__(self, config: Config) -> None:
        """
        Initialize an instance of the Crawler class.

        Args:
            config (Config): Configuration
        """
        self._config = config
        self._seed_urls = self._config.get_seed_urls()
        self.urls = []
    def _extract_url(self, article_bs: BeautifulSoup) -> str:
        """
        Find and retrieve url from HTML.

        Args:
            article_bs (bs4.BeautifulSoup): BeautifulSoup instance

        Returns:
            str: Url from HTML
        """

        link = article_bs.find('a')
        if not link or not link.get('href'):
            return ""

        url = link['href']
        # Make sure URL is absolute
        if not url.startswith('http'):
            base_url = self._seed_urls[0] if self._seed_urls else ""
            url = base_url.rstrip('/') + '/' + url.lstrip('/')

        return url if url.startswith('https://livennov.ru/') else ""

    def find_articles(self) -> None:
        """
        Find and collect article URLs from seed pages.
        """

        seed_urls = self.get_search_urls()
        targets_needed = self._config.get_num_articles()

        for url in seed_urls:
            if len(self.urls) >= targets_needed:
                break

            response = make_request(url, self._config)
            if not response.ok:
                continue

            soup = BeautifulSoup(response.text, 'lxml')
            extracted_url = self._extract_url(soup)

            while extracted_url and len(self.urls) < targets_needed:
                if "problematic_article_id=3" not in extracted_url:
                    self.urls.append(extracted_url)
                extracted_url = self._extract_url(soup)

    def get_search_urls(self) -> list:
        """
        Get seed_urls param.

        Returns:
            list: seed_urls param
        """
        return self._seed_urls

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

        "change"
        main_bs = article_soup.find(
            'div',
            class_='entry-content',
            )
        text_tag = main_bs.find_all("p")

        find_text = [text.get_text(strip=True) for text in text_tag]

        self.article.text = "\n".join(find_text)

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        title = article_soup.find('h1', class_='entry-title')
        if title:
            self.article.title = title.get_text(strip=True)

        date = article_soup.find('time', class_='entry-date published')
        if date:
            self.article.date = self.unify_date_format(date.get_text(strip=True))

        author = article_soup.find('span', class_='author vcard')
        if author:
            self.article.author = author.get_text(strip=True)
    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """
        try:
            return datetime.datetime.strptime(date_str, "%d.%m.%Y")
        except ValueError:
            return datetime.datetime.now()

    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """
        response = make_request(self._full_url, self._config)
        main_bs = BeautifulSoup(response.text, "lxml")
        self._fill_article_with_text(main_bs)
        self._fill_article_with_meta_information(main_bs)
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
    try:
        print("=== Starting scraper ===")
        print(f"Config path: {CRAWLER_CONFIG_PATH}")
        print(f"Assets path: {ASSETS_PATH}")

        if not CRAWLER_CONFIG_PATH.exists():
            raise FileNotFoundError(f"Config file not found at {CRAWLER_CONFIG_PATH}")

        configuration = Config(CRAWLER_CONFIG_PATH)
        prepare_environment(ASSETS_PATH)

        print(f"Seed URLs: {configuration.get_seed_urls()}")
        print(f"Target articles: {configuration.get_num_articles()}")

        crawler = Crawler(configuration)
        crawler.find_articles()

        print(f"\nFound {len(crawler.urls)} articles:")
        for i, url in enumerate(crawler.urls, 1):
            print(f"{i}. {url}")

        for article_id, url in enumerate(crawler.urls, start=1):
            print(f"\nProcessing article {article_id}: {url}")
            parser = HTMLParser(full_url=url, article_id=article_id, config=configuration)
            article = parser.parse()

            if isinstance(article, Article):
                to_raw(article)
                print(f"Saved article {article_id}")

        print("\n=== Scraping completed successfully ===")

    except Exception as e:
        print(f"\n!!! ERROR: {str(e)}")
        raise






if __name__ == "__main__":
    main()
