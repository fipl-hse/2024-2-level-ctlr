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
from bs4.element import Tag

from core_utils.article.article import Article
from core_utils.article.io import to_meta, to_raw
from core_utils.config_dto import ConfigDTO
from core_utils.constants import ASSETS_PATH, CRAWLER_CONFIG_PATH


class IncorrectSeedURLError(Exception):
    """
    Seed URL does not match standard pattern 'https?://(www.)?'
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
        config_data = self._load_config_from_file()

        self._seed_urls = config_data["seed_urls"]
        self._num_articles = config_data["total_articles_to_find_and_parse"]
        self._headers = config_data["headers"]
        self._encoding = config_data["encoding"]
        self._timeout = config_data["timeout"]
        self._should_verify_certificate = config_data["verify_certificate"]
        self._headless_mode = config_data["headless_mode"]

        self._validate_config_content()

    def _load_config_from_file(self) -> dict:
        """
        Load config from json file.
        """
        with open(self.path_to_config, 'r', encoding='utf-8') as file:
            config_dict = json.load(file)
        return config_dict

    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        with open(self.path_to_config, 'r', encoding='utf-8') as file:
            config_dict = json.load(file)
        return ConfigDTO(**config_dict)

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        if not isinstance(self._seed_urls, list) or not all(isinstance(url, str) for url in self._seed_urls):
            raise IncorrectSeedURLError("Seed URLs must be a list of strings.")

        if not all(url.startswith('https://pravdasevera.ru/') for url in self._seed_urls):
            raise IncorrectSeedURLError("Each seed URL must start with 'https://pravdasevera.ru/'")

        if not isinstance(self._encoding, str):
            raise IncorrectEncodingError("Encoding must be a string.")

        if not isinstance(self._headers, dict) or not all(
                isinstance(k, str) and isinstance(v, str) for k, v in self._headers.items()
        ):
            raise IncorrectHeadersError("Headers must be a dictionary with string keys and values.")

        if not isinstance(self._should_verify_certificate, bool):
            raise IncorrectVerifyError("Should verify certificate must be either True or False.")

        if not isinstance(self._headless_mode, bool):
            raise IncorrectVerifyError("Headless mode must be either True or False.")

        if not isinstance(self._timeout, int) or self._timeout < 0 or self._timeout > 60:
            raise IncorrectTimeoutError("Timeout must be an integer between 0 and 60.")

        if not isinstance(self._num_articles, int) or self._num_articles <= 0:
            raise IncorrectNumberOfArticlesError("num_articles must be a positive integer")

        if self._num_articles > 150:
            raise NumberOfArticlesOutOfRangeError("num_articles must not be too large")

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
    return requests.get(
        url,
        headers=config.get_headers(),
        timeout=config.get_timeout(),
        verify=config.get_verify_certificate()
    )


class Crawler:
    """
    Crawler implementation.
    """
    url_pattern: Union[Pattern, str]

    def __init__(self, config: Config) -> None:
        """
        Initialize an instance of the Crawler class.
        """
        self.config = config
        self.urls = []

    from urllib.parse import urljoin

    def _extract_url(self, article_bs: Tag) -> str:
        """
        Find and retrieve url from HTML.
        """
        preview = article_bs.find('div', class_='post-card__thumbnail')
        if preview:
            link_tag = preview.find('a', href=True)
            if link_tag:
                href = link_tag['href']
                full_url = urljoin('https://pravdasevera.ru', href)
                if full_url.startswith('https://pravdasevera.ru') and full_url not in self.urls:
                    return full_url
        return ''

    def find_articles(self) -> None:
        """
        Find articles.
        """
        for seed_url in self.get_search_urls():
            if len(self.urls) >= self.config.get_num_articles():
                break
            try:
                response = make_request(seed_url, self.config)
            except Exception as e:
                print(f"Ошибка при запросе к {seed_url}: {e}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            blocks = soup.find_all('div', class_='post-card__thumbnail')
            for block in blocks:
                if len(self.urls) >= self.config.get_num_articles():
                    break
                href = self._extract_url(block)
                if href:
                    self.urls.append(href)


    def get_search_urls(self) -> list:
        """
        Get seed_urls param.
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
        self.article = Article(url=full_url, article_id=article_id)

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        content = article_soup.find('div', class_='site-content-inner')
        if content:
            paragraphs = [
                tag.get_text(strip=True)
                for tag in content.find_all(recursive=True)
                if isinstance(tag, Tag) and tag.get_text(strip=True)
            ]
            self.article.text = "\n\n".join(paragraphs)
        else:
            self.article.text = "No content found."

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        title_tag = article_soup.find('h1', class_='entry-title')
        self.article.title = title_tag.text.strip() if title_tag else "NOT FOUND"

        date_tag = article_soup.find('time', itemprop='dateModified')
        if date_tag:
            self.article.date = self.unify_date_format(date_tag.text)
        else:
            self.article.date = datetime.datetime.now()

        self.article.author = ['NOT FOUND']
        self.article.topics = []

    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """
        months = {
            'января': 'Jan', 'февраля': 'Feb', 'марта': 'Mar',
            'апреля': 'Apr', 'мая': 'May', 'июня': 'Jun',
            'июля': 'Jul', 'августа': 'Aug', 'сентября': 'Sep',
            'октября': 'Oct', 'ноября': 'Nov', 'декабря': 'Dec'
        }

        parts = date_str.strip().split()
        parts[1] = months.get(parts[1].rstrip(','), 'Jan')
        new_date = ' '.join(parts)
        return datetime.datetime.strptime(new_date, '%d %b %Y')

    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """
        try:
            response = make_request(self.full_url, self.config)
        except requests.RequestException as e:
            print(f"Request failed for {self.full_url}: {e}")
            return False

        if not response.ok:
            print(f"Bad response ({response.status_code}) from {self.full_url}")
            return False

        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            self._fill_article_with_text(soup)
            self._fill_article_with_meta_information(soup)
            return self.article
        except Exception as e:
            print(f"Error parsing article from {self.full_url}: {e}")
            return False


def prepare_environment(base_path: Union[pathlib.Path, str]) -> None:
    """
    Create ASSETS_PATH folder if no created and remove existing folder.

    Args:
        base_path (Union[pathlib.Path, str]): Path where articles stores
    """
    path = pathlib.Path(base_path)
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)


def main() -> None:
    """
    Entrypoint for scrapper module.
    """
    config = Config(path_to_config=CRAWLER_CONFIG_PATH)

    prepare_environment(ASSETS_PATH)

    crawler = Crawler(config)
    crawler.find_articles()

    for i, url in enumerate(crawler.urls, start=1):
        parser = HTMLParser(url, i, config)
        article = parser.parse()
        if isinstance(article, Article):
            to_raw(article)
            to_meta(article)


if __name__ == "__main__":
    main()
