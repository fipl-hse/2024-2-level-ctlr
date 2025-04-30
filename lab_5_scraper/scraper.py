"""
Crawler implementation.
"""

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import pathlib
from typing import Pattern, Union
import json
from core_utils.config_dto import ConfigDTO
from core_utils.article.article import Article
from core_utils.article.io import to_raw
from core_utils.constants import CRAWLER_CONFIG_PATH, ASSETS_PATH
import shutil
import requests
from bs4 import BeautifulSoup

class IncorrectSeedURLError(Exception):
    """
    Seed URL does not match standard pattern 'https?://(www.)?'
    """
class NumberOfArticlesOutOfRangeError(Exception):
    """
    Total number of articles is out of range from 1 to 150
    """


class IncorrectNumberOfArticlesError(Exception):
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
        extr_config = self._extract_config_content()
        self.seed_urls = extr_config.seed_urls
        self.total_articles = extr_config.total_articles
        self.headers = extr_config.headers
        self.encoding = extr_config.encoding
        self.timeout = extr_config.timeout
        self.should_verify_certificate = extr_config.should_verify_certificate
        self.headless_mode = extr_config.headless_mode

    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        with self.path_to_config.open("r", encoding="utf-8") as config_file:
            config = json.load(config_file)
            return ConfigDTO(**config)

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        if not ('https://vestiprim.ru/' in url for url in self.seed_urls):
            raise IncorrectSeedURLError
        if self.total_articles < 1 or self.total_articles > 150:
            raise NumberOfArticlesOutOfRangeError
        if not isinstance(self.total_articles, int) or \
                self.total_articles < 0:
            raise IncorrectNumberOfArticlesError
        if not isinstance(self.headers, dict):
            raise IncorrectHeadersError
        if not isinstance(self.encoding, str):
            raise IncorrectEncodingError
        if self.timeout < 0 or self.timeout > 60:
            raise IncorrectTimeoutError
        if not isinstance(self.should_verify_certificate, bool) or \
                not isinstance(self.headless_mode, bool):
            raise IncorrectVerifyError

    def get_seed_urls(self) -> list[str]:
        """
        Retrieve seed urls.

        Returns:
            list[str]: Seed urls
        """
        return self.seed_urls

    def get_num_articles(self) -> int:
        """
        Retrieve total number of articles to scrape.

        Returns:
            int: Total number of articles to scrape
        """
        return self.total_articles

    def get_headers(self) -> dict[str, str]:
        """
        Retrieve headers to use during requesting.

        Returns:
            dict[str, str]: Headers
        """
        return self.headers

    def get_encoding(self) -> str:
        """
        Retrieve encoding to use during parsing.

        Returns:
            str: Encoding
        """
        return self.encoding

    def get_timeout(self) -> int:
        """
        Retrieve number of seconds to wait for response.

        Returns:
            int: Number of seconds to wait for response
        """
        return self.timeout

    def get_verify_certificate(self) -> bool:
        """
        Retrieve whether to verify certificate.

        Returns:
            bool: Whether to verify certificate or not
        """
        return self.should_verify_certificate

    def get_headless_mode(self) -> bool:
        """
        Retrieve whether to use headless mode.

        Returns:
            bool: Whether to use headless mode or not
        """
        return self.headless_mode


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
    response.encoding=config.get_encoding()
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
        all_links = article_bs.find_all('a', class_='img_box')
        for link in all_links:
            href_link = link.get('href')
            if isinstance(href_link, str) and href_link not in self.urls:
                return href_link
            return ''

    def find_articles(self) -> None:
        """
        Find articles.
        """
        for seed in self.get_search_urls():
            response = make_request(seed, self.config)
            if not response.ok:
                continue
            soup = BeautifulSoup(response.text, 'lxml')
            url = self._extract_url(soup)
            while url and len(self.urls) != self.config.get_num_articles():
                self.urls.append(url)
                url = self._extract_url(soup)
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
        self.article = Article(url=full_url, article_id=article_id)
    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        div = article_soup.find('div', class_='article_text')
        text = []
        if div is not None:
            for block in div:
                if block.get_text():
                    text.append(block.get_text(separator='\n', strip=True))
                self.article.text = '\n'.join(text)

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
        response = make_request(self.full_url, self.config)
        if response.ok:
            soup = BeautifulSoup(response.text, 'lxml')
            self._fill_article_with_text(soup)
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
    configuration = Config(CRAWLER_CONFIG_PATH)
    prepare_environment(ASSETS_PATH)
    crawler = Crawler(config=configuration)
    crawler.find_articles()
    parser = HTMLParser(full_url="https://vestiprim.ru/news/ptrnews/163045-generalnoe-konsulstvo-respubliki-belarus-otkrylos-v-primore.html", article_id=1, config=configuration)
    parsed_article = parser.parse()
    if isinstance(parsed_article, Article):
        to_raw(parsed_article)
    print(to_raw(parsed_article))

if __name__ == "__main__":
    main()
