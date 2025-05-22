"""
Crawler implementation.
"""

import datetime

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import json
import pathlib
import re
import shutil
from random import randint
from time import sleep
from typing import Union

import requests
from bs4 import BeautifulSoup

from core_utils.article.article import Article
from core_utils.article.io import to_raw
from core_utils.config_dto import ConfigDTO
from core_utils.constants import (
    ASSETS_PATH,
    CRAWLER_CONFIG_PATH,
    NUM_ARTICLES_UPPER_LIMIT,
    TIMEOUT_LOWER_LIMIT,
    TIMEOUT_UPPER_LIMIT,
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

        if not isinstance(self._config.headers, dict):
            raise IncorrectHeadersError("Headers must be a dictionary")

        if not self._config.seed_urls:
            raise IncorrectSeedURLError("Seed URLs cannot be empty")

        if not all(isinstance(url, str) and url.startswith('http') for url in self._config.seed_urls):
            raise IncorrectSeedURLError("All seed URLs must be valid HTTP/HTTPS URLs")

        if not isinstance(self._config.total_articles, int) or self._config.total_articles < 1:
            raise IncorrectNumberOfArticlesError(
                f"Number of articles must be positive integer, got {self._config.total_articles}"
            )

        if self._config.total_articles > NUM_ARTICLES_UPPER_LIMIT:
            raise NumberOfArticlesOutOfRangeError(
                f"Number of articles exceeds limit of {NUM_ARTICLES_UPPER_LIMIT}"
            )

        if not isinstance(self._config.encoding, str):
            raise IncorrectEncodingError("Encoding must be a string")

        if not (TIMEOUT_LOWER_LIMIT < self._config.timeout < TIMEOUT_UPPER_LIMIT):
            raise IncorrectTimeoutError(
                f"Timeout must be between {TIMEOUT_LOWER_LIMIT} and {TIMEOUT_UPPER_LIMIT} seconds"
            )

        if not isinstance(self._config.should_verify_certificate, bool):
            raise IncorrectVerifyError("Certificate verification flag must be boolean")

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
        sleep(randint(1, 3))  # Random delay between requests

        response = requests.get(
            url,
            headers=config.get_headers(),
            timeout=config.get_timeout(),
            verify=config.get_verify_certificate()
        )
        response.raise_for_status()  # Raises exception for 4XX/5XX responses
        response.encoding = config.get_encoding()
        return response

    except requests.RequestException as e:
        print(f"Request failed for {url}: {str(e)}")
        response = requests.models.Response()
        response.status_code = 404
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
    Find articles from seed URLs.
        """

        for seed_url in self._seed_urls:
            page = 1
            while len(self.urls) < self._config.get_num_articles():
                try:
                    # Handle pagination for seed URLs
                    current_url = f"{seed_url}page/{page}/" if page > 1 else seed_url
                    response = make_request(current_url, self._config)

                    if response.status_code != 200:
                        break  # Stop if page not found

                    soup = BeautifulSoup(response.text, "lxml")

                    # More flexible article detection
                    articles = soup.find_all("article") or \
                               soup.find_all("div", class_=re.compile(r'post|article|item|entry')) or \
                               soup.find_all(class_=re.compile(r'post|article|item|entry'))

                    for article in articles:
                        if len(self.urls) >= self._config.get_num_articles():
                            return

                        # More flexible link extraction
                        link = article.find("a", href=True)
                        if not link:
                            continue

                        url = link["href"]

                        # Normalize URL
                        if not url.startswith(("http", "www")):
                            if url.startswith("/"):
                                url = f"{seed_url.rstrip('/')}{url}"
                            else:
                                url = f"{seed_url.rstrip('/')}/{url.lstrip('/')}"

                        # More flexible domain check
                        if "livennov.ru" in url and url not in self.urls:
                            self.urls.append(url)

                    # Stop if no new articles found on this page
                    if not articles:
                        break

                    page += 1

                except Exception as e:
                    print(f"Error processing {seed_url} page {page}: {str(e)}")
                    break
    def get_search_urls(self) -> list:
        """
        Get seed_urls param.

        Returns:
            list: seed_urls param
        """
        return self._seed_urls


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

    def _save_meta(self) -> None:
        """
        Save article metadata to _meta.json file.
        """
        meta_data = {
            "id": self._article_id,
            "title": self.article.title,
            "author": self.article.author,
            "url": self._full_url
        }

        meta_path = ASSETS_PATH / f"{self._article_id}_meta.json"
        with open(meta_path, 'w', encoding='utf-8') as meta_file:
            json.dump(meta_data, meta_file, ensure_ascii=False, indent=4)

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """

        "change"
        main_bs = article_soup.find('div', class_='entry-content')
        text_tag = main_bs.find_all("p") if main_bs else []
        find_text = [text.get_text(strip=True) for text in text_tag]
        self.article.text = "\n".join(find_text)

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        title = article_soup.find('h1', class_='entry-title') or \
                article_soup.find('h1') or \
                article_soup.find('title')
        self.article.title = title.get_text(strip=True) if title else "Без названия"

        date = article_soup.find('time', class_='entry-date') or \
               article_soup.find('time') or \
               article_soup.find('span', class_='date')
        self.article.date = self.unify_date_format(date.get_text(strip=True)) if date else datetime.datetime.now()

        author = article_soup.find('span', class_='author') or \
                 article_soup.find('a', class_='author') or \
                 article_soup.find('span', class_='byline')
        self.article.author = author.get_text(strip=True) if author else "Неизвестный автор"

    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """
        try:
            # Пробуем разные форматы даты
            for fmt in ('%d.%m.%Y', '%Y-%m-%d', '%d %B %Y', '%B %d, %Y'):
                try:
                    return datetime.datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            return datetime.datetime.now()
        except:
            return datetime.datetime.now()


    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """
        try:
            response = make_request(self._full_url, self._config)
            if response.status_code != 200:
                return False

            soup = BeautifulSoup(response.text, 'lxml')
            self._fill_article_with_text(soup)
            self._fill_article_with_meta_information(soup)
            self._save_meta()
            return self.article
        except Exception as e:
            print(f"Error parsing article: {str(e)}")
            return False
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
