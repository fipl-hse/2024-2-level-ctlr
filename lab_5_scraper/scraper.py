"""
Crawler implementation.
"""

# pylint: disable=too-many-arguments, too-many-instance-attributes,
# unused-import, undefined-variable, unused-argument

import datetime
import json
import pathlib
import re
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
        self._config_data = self._extract_config_content()
        self._validate_config_content()
        self._seed_urls: list[str] = self._config_data.seed_urls
        self._num_articles: int = self._config_data.total_articles_to_find_and_parse
        self._headers: dict[str, str] = self._config_data.headers
        self._encoding: str = self._config_data.encoding
        self._timeout: int = self._config_data.timeout
        self._should_verify_certificate: bool = self._config_data.should_verify_certificate
        self._headless_mode: bool = self._config_data.headless_mode
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
        """
        Ensure configuration parameters are not corrupt.
        """
        url_pattern = re.compile(r'https?://(www\.)?.+')

        if not isinstance(self._config_data.seed_urls, list) or not self._config_data.seed_urls:
            raise IncorrectSeedURLError("seed_urls must be a non-empty list")

        for url in self._config_data.seed_urls:
            if not re.match(url_pattern, url):
                raise IncorrectSeedURLError(f"Invalid seed URL: {url}")

        if not isinstance(self._config_data.total_articles_to_find_and_parse, int) or \
                self._config_data.total_articles_to_find_and_parse < 1:
            raise IncorrectNumberOfArticlesError(
                "total_articles_to_find_and_parse must be an integer >= 1"
            )

        if (self._config_data.total_articles_to_find_and_parse
            > NUM_ARTICLES_UPPER_LIMIT
           ):
            raise NumberOfArticlesOutOfRangeError(
                f"total_articles_to_find_and_parse must be <= {NUM_ARTICLES_UPPER_LIMIT}"
            )

        if not isinstance(self._config_data.headers, dict):
            raise IncorrectHeadersError("headers must be a dictionary")

        if not isinstance(self._config_data.encoding, str) or not self._config_data.encoding:
            raise IncorrectEncodingError("encoding must be a non-empty string")

        if not isinstance(self._config_data.timeout, int) or not (
            TIMEOUT_LOWER_LIMIT <= self._config_data.timeout < TIMEOUT_UPPER_LIMIT
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
        return self._seed_urls

    def get_total_articles(self) -> int:
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

    def get_verify_cert(self) -> bool:
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
    Make an HTTP request with custom headers and timeout.

    Args:
        url (str): URL to make request to.
        config (Config): Configuration object.

    Returns:
        requests.models.Response: A response from a request
    """
    response = requests.get(
        url,
        headers=config.get_headers(),
        timeout=config.get_timeout(),
        verify=config.get_verify_cert()
    )
    response.raise_for_status()
    return response


class Crawler:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.urls: list[str] = []

    def find_articles(self) -> list[str]:
        """
        Find article URLs from seed URLs.

        Returns:
            list[str]: List of article URLs
        """
        seed_urls = self.config.get_seed_urls()
        max_articles = self.config.get_total_articles()

        for url in seed_urls:
            if len(self.urls) >= max_articles:
                break

            try:
                response = make_request(url, self.config)
            except requests.RequestException:
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            article_tags = soup.find_all("article")
            for tag in article_tags:
                if len(self.urls) >= max_articles:
                    break
                a_tag = tag.find("a", href=True)
                if a_tag:
                    href = a_tag["href"]
                    if href not in self.urls:
                        self.urls.append(href)

        return self.urls


class HTMLParser:
    def __init__(self, full_url: str, article_id: int, config: Config) -> None:
        self.full_url = full_url
        self.article_id = article_id
        self.config = config
        self.article: Article | None = None

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

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        title_tag = article_soup.find("h1")
        text_tag = article_soup.find("div", class_="entry-content")

        title = title_tag.get_text(strip=True) if title_tag else ""
        text = text_tag.get_text(strip=True) if text_tag else ""

        if not text:
            self.article = None
            return

        self.article = Article(url=self.full_url, article_id=self.article_id)
        self.article.title = title
        self.article.text = text

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
            return self.article if self.article is not None else False
        except requests.exceptions.RequestException as e:
            print(f"Request error when fetching article {self.full_url}: {e}")
            return False
        except AttributeError as e:
            print(f"Attribute error when parsing article {self.full_url}: {e}")
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
