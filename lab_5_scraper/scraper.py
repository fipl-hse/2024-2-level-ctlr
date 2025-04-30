"""
Crawler implementation.
"""

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import pathlib
from typing import Pattern, Union
from core_utils.config_dto import ConfigDTO
import json
import shutil
import requests
from bs4 import BeautifulSoup
from core_utils.constants import CRAWLER_CONFIG_PATH

class IncorrectSeedURLError(Exception):
    pass

class NumberOfArticlesOutOfRangeError(Exception):
    pass

class IncorrectNumberOfArticlesError(Exception):
    pass

class IncorrectHeadersError(Exception):
    pass

class IncorrectEncodingError(Exception):
    pass

class IncorrectTimeoutError(Exception):
    pass

class IncorrectVerifyError(Exception):
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
        self.path_to_config = path_to_config
        self._validate_config_content()
        extraction = self._extract_config_content()
        self._seed_urls = extraction.seed_urls
        self._num_articles = extraction.total_articles
        self._headers = extraction.headers
        self._encoding = extraction.encoding
        self._timeout = extraction.timeout
        self._should_verify_certificate = extraction.should_verify_certificate
        self._headless_mode = extraction.headless_mode


    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        with open('scraper_config.json', 'r') as config_file:
            config_data = json.load(config_file)
        return ConfigDTO(**config_data)
    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        if not isinstance(self._seed_urls, list) or \
                not all(isinstance(url, str) for url in self._seed_urls) or \
                not all(url.startswith("https://polkrug.ru") for url in self._seed_urls):
            raise IncorrectSeedURLError("Seed URL is not a valid URL")
        if not isinstance(self._num_articles, int) or \
                isinstance(self._num_articles, bool) or \
                self._num_articles < 0:
            raise IncorrectNumberOfArticlesError("Number of articles is not integer or less than 0")
        if self._num_articles > 150:
            raise NumberOfArticlesOutOfRangeError("Total number of articles is out of range")
        if not isinstance(self._headers, dict):
            raise IncorrectHeadersError("Headers are not in a form of dictionary")
        if not isinstance(self._encoding, str):
            raise IncorrectEncodingError("Encoding is not a string")
        if not isinstance(self._timeout, int):
            raise IncorrectTimeoutError('Timeout is not int')
        if self._timeout < 0 or self._timeout > 60:
            raise IncorrectTimeoutError("Timeout is out of range")
        if not isinstance(self._should_verify_certificate, bool):
            raise IncorrectVerifyError("Verify certificate value is not an instance of bool")
        if not isinstance(self._headless_mode, bool):
            raise IncorrectVerifyError('Headless mode value is not an instance of bool')


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
        self._seed_urls = self._config.get_seed_urls()


    def _extract_url(self, article_bs: BeautifulSoup) -> str:
        """
        Find and retrieve url from HTML.

        Args:
            article_bs (bs4.BeautifulSoup): BeautifulSoup instance

        Returns:
            str: Url from HTML
        """
        href = article_bs.find("a").get('href')

        if href and href.startswith("https://kamvesti.com/"):
            return href

        return ""

    def find_articles(self) -> None:
        """
        Find articles.
        """
        for seed_url in self._seed_urls:
            res = make_request(seed_url, self._config)

            soup = BeautifulSoup(res.content, "lxml")

            for paragraph in soup.find_all('h1', class_='entry-title'):
                if len(self.urls) >= self._config.get_num_articles():
                    return None

                url = self._extract_url(paragraph)

                if url and url not in self.urls:
                    self.urls.append(url)

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
        self.full_url = full_url
        self.article_id = article_id
        self.config = config


    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
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
        response = make_request(self.full_url, self.config)
        if response.ok:
            article_bs = BeautifulSoup(response.text, "lxml")
            self._fill_article_with_text(article_bs)
        return self.article

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


def prepare_environment(base_path: Union[pathlib.Path, str]) -> None:
    """
    Create ASSETS_PATH folder if no created and remove existing folder.

    Args:
        base_path (Union[pathlib.Path, str]): Path where articles stores
    """
    base_path = pathlib.Path(base_path)
    assets_path = base_path / 'ASSETS_PATH'
    if assets_path.exists() and assets_path.is_dir():
        shutil.rmtree(assets_path)
    assets_path.mkdir(parents=True, exist_ok=True)




def main() -> None:
    """
    Entrypoint for scrapper module.
    """
    configuration = Config(path_to_config=CRAWLER_CONFIG_PATH)
    crawler = Crawler(config=configuration)
    crawler.find_articles()
    for i, url in enumerate(crawler.urls):
        parser = HTMLParser(url, i + 1, configuration)
        article = parser.parse()
        if isinstance(article, Article):
            to_raw(article)



if __name__ == "__main__":
    main()
