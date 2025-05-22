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

class IncorrectVerifyCertificateError(Exception):
    pass


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
        self.urls = []
        self.path_to_config = path_to_config
        config_data = self._load_config_from_file()
        self._seed_urls = config_data["seed_urls"]
        self._num_articles = config_data["total_articles_to_find_and_parse"]
        self._headers = config_data["headers"]
        self._encoding = config_data["encoding"]
        self._timeout = config_data["timeout"]
        self._should_verify_certificate = config_data.get("should_verify_certificate", True)
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
        if not isinstance(self._should_verify_certificate, bool):
            raise IncorrectVerifyError(
                "Checking that scraper can handle incorrect verify certificate argument.\n"
                "Verify certificate must be either True or False"
            )

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
            raise IncorrectVerifyCertificateError("Verify certificate must be either True or False.")

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

    def _extract_url(self, article_bs: BeautifulSoup, base_url: str) -> str:
        """
        Find and retrieve url from HTML.
        """
        link_tag = article_bs.find('a', href=True)
        if link_tag:
            href = link_tag['href']
            full_url = urljoin(base_url, href)
            if full_url not in self.urls:
                return full_url
        return ''

    def is_valid_article_url(self, url: str) -> bool:
        return 'pravdasevera.ru' in url and '/202' in url

    def find_articles(self) -> None:
        """
        Find articles.
        """
        self.urls = []
        headers = self.config._headers
        timeout = self.config._timeout
        total_needed = self.config._num_articles

        for seed_url in self.config._seed_urls:
            try:
                response = requests.get(seed_url, headers=headers, timeout=timeout,
                                        verify=self.config._should_verify_certificate)
                response.raise_for_status()
                response.encoding = self.config._encoding

                soup = BeautifulSoup(response.content, 'html.parser', from_encoding='utf-8')
                links = soup.find_all('a', href=True)

                for link in links:
                    href = link['href']
                    full_url = urljoin(seed_url, href)
                    if self.is_valid_article_url(full_url):
                        if full_url not in self.urls:
                            self.urls.append(full_url)
                        if len(self.urls) >= total_needed:
                            return

            except requests.RequestException:
                continue


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
        text_div = article_soup.find('div', class_='block--article__text')
        if text_div:
            paragraphs = [p.get_text(strip=True) for p in text_div.find_all('p') if p.get_text(strip=True)]
            self.article.text = '\n\n'.join(paragraphs) if paragraphs else 'NOT FOUND'
        else:
            self.article.text = 'NOT FOUND'

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """

        title_selectors = [
            {'type': 'css', 'selector': 'div.block--article_top h1'},
            {'type': 'css', 'selector': 'h1.article-title'},
            {'type': 'meta', 'property': 'og:title'},
            {'type': 'tag', 'name': 'h1'}
        ]

        for selector in title_selectors:
            try:
                if selector['type'] == 'css':
                    title_tag = article_soup.select_one(selector['selector'])
                elif selector['type'] == 'meta':
                    title_tag = article_soup.find('meta', property=selector['property'])
                else:
                    title_tag = article_soup.find(selector['name'])

                if title_tag:
                    title_text = title_tag.get('content', '') if selector['type'] == 'meta' else title_tag.get_text()
                    clean_title = title_text.strip()
                    if clean_title:
                        self.article.title = clean_title
                        break
            except Exception as e:
                print(f"[DEBUG] Ошибка при поиске заголовка: {str(e)[:50]}")
        else:
            self.article.title = "NOT FOUND"

        self.article.author = ["NOT FOUND"]

        try:
            parts = self.full_url.split('/')
            year, month, day = parts[4], parts[5], parts[6]
            self.article.date = datetime.datetime(int(year), int(month), int(day))
        except:
            self.article.date = datetime.datetime.now()

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

        try:
            parts = date_str.strip().split()
            if len(parts) >= 3:
                day = parts[0]
                month = months.get(parts[1], 'Jan')
                year = parts[2]
                date_str_en = f"{day} {month} {year}"
                return datetime.datetime.strptime(date_str_en, '%d %b %Y')
            else:
                return datetime.datetime.now()
        except Exception:
            return datetime.datetime.now()

    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """
        try:
            response = requests.get(
                self.full_url,
                headers=self.config.get_headers(),
                timeout=self.config.get_timeout(),
                verify=self.config.get_verify_certificate()
            )
            if response.encoding.lower() not in ('utf-8', 'utf8'):
                response.encoding = 'windows-1251'
            response.raise_for_status()
            response.encoding = self.config.get_encoding()

            soup = BeautifulSoup(response.text, 'html.parser')

            self._fill_article_with_meta_information(soup)
            self._fill_article_with_text(soup)

        except Exception as e:
            print(f'[ERROR] Failed to parse {self.full_url}: {e}')
            self.article.title = 'NOT FOUND'
            self.article.author = ['NOT FOUND']
            self.article.date = datetime.datetime.now()
            self.article.text = 'NOT FOUND'

        return self.article


def prepare_environment(base_path: Union[pathlib.Path, str]) -> None:
    """
    Create ASSETS_PATH folder if no created and remove existing folder.

    Args:
        base_path (Union[pathlib.Path, str]): Path where articles stores
    """
    base_path = pathlib.Path(base_path)
    if base_path.exists():
        shutil.rmtree(base_path)
    base_path.mkdir(parents=True)



def main() -> None:
    """
    Entrypoint for scrapper module.
    """
    prepare_environment(ASSETS_PATH)

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
