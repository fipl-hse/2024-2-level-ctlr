"""
Crawler implementation.
"""

import datetime
import json

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import pathlib
import re
import shutil
from typing import Pattern, Union

import requests
from bs4 import BeautifulSoup

from core_utils.article.article import Article
from core_utils.article.io import to_meta, to_raw
from core_utils.config_dto import ConfigDTO
from core_utils.constants import ASSETS_PATH, CRAWLER_CONFIG_PATH, PROJECT_ROOT


def is_valid_url(url: str) -> bool:
    """
    Checks if url is valid.
    Args:
        url: url string to check
    """
    standard_pattern = r'https?://\S+|www\.\S+'
    if re.match(standard_pattern, url) is None:
        return False
    return True

class IncorrectSeedURLError(TypeError):
    """
    ...
    """
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)
class NumberOfArticlesOutOfRangeError(ValueError):
    """
    ...
    """
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)

class IncorrectNumberOfArticlesError(TypeError):
    """
    ...
    """
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)

class IncorrectHeadersError(TypeError):
    """
    ...
    """
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)

class IncorrectEncodingError(TypeError):
    """
    ...
    """
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)

class IncorrectTimeoutError(ValueError):
    """
    ...
    """
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class IncorrectVerifyError(TypeError):
    """
    ...
    """

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)

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
        prepare_environment(ASSETS_PATH)
        values = self._extract_config_content()
        self._seed_urls = values.seed_urls
        self._num_articles = values.total_articles
        self._timeout = values.timeout
        self._headers = values.headers
        self._encoding = values.encoding
        self._should_verify_certificate = values.should_verify_certificate
        self._headless_mode = values.headless_mode

    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        with open(self.path_to_config, 'r', encoding='utf-8') as file:
            config_values_dict = json.load(file)
            config_values = ConfigDTO(config_values_dict['seed_urls'],
                                      config_values_dict['total_articles_to_find_and_parse'],
                                      config_values_dict['headers'],
                                      config_values_dict['encoding'],
                                      config_values_dict['timeout'],
                                      config_values_dict['should_verify_certificate'],
                                      config_values_dict['headless_mode'])
        return config_values

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        with open(self.path_to_config, 'r', encoding='utf-8') as file:
            config_values_dict = json.load(file)

            if (not (isinstance(config_values_dict['seed_urls'], list)
                     and all(isinstance(seed_url, str)
                             for seed_url in config_values_dict['seed_urls']))):
                raise IncorrectSeedURLError('Seed URLs must be a list of strings')
            for seed_url in config_values_dict['seed_urls']:
                if not is_valid_url(seed_url):
                    raise IncorrectSeedURLError(
                        'seed URL does not match standard pattern "https?://(www.)?"')

            if (not (isinstance(config_values_dict['total_articles_to_find_and_parse'], int) and
                     config_values_dict['total_articles_to_find_and_parse'] >= 0) or
                    isinstance(config_values_dict['total_articles_to_find_and_parse'], bool)):
                raise IncorrectNumberOfArticlesError(
                    'total number of articles to parse is not integer or less than 0')

            if config_values_dict['total_articles_to_find_and_parse'] > 150:
                raise NumberOfArticlesOutOfRangeError(
                    'total number of articles is out of range from 1 to 150')

            if not isinstance(config_values_dict['headers'], dict):
                raise IncorrectHeadersError('headers are not in a form of dictionary')

            if not isinstance(config_values_dict['encoding'], str):
                raise IncorrectEncodingError('encoding must be specified as a string')

            if (not isinstance(config_values_dict['timeout'], int) or
                    config_values_dict['timeout'] not in range(61)):
                raise IncorrectTimeoutError('timeout value must be a positive integer less than 60')

            if not isinstance(config_values_dict['should_verify_certificate'], bool):
                raise IncorrectVerifyError('verify certificate value must either be True or False')

            if not isinstance(config_values_dict['headless_mode'], bool):
                raise IncorrectVerifyError('headless mode value must either be True or False')

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
    response = requests.get(url,headers=config.get_headers(),
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

    def _extract_url(self, article_bs: BeautifulSoup) -> str:
        """
        Find and retrieve url from HTML.

        Args:
            article_bs (bs4.BeautifulSoup): BeautifulSoup instance

        Returns:
            str: Url from HTML
        """
        urls = article_bs.find_all('a',
                                   href=lambda href:
                                   href and href.startswith('https://gtrksakha.ru/news/20'))
        unique_hrefs = list(set(url['href'] for url in urls))
        for url_href in unique_hrefs:
            if not isinstance(url_href, str):
                return ''
            if url_href not in self.urls and url_href not in self.get_search_urls():
                return url_href
        return ''

    def find_articles(self) -> None:
        """
        Find articles.
        """
        for seed_url in self.config.get_seed_urls():
            response = make_request(seed_url, self.config)
            if response.status_code == 200:
                article_bs = BeautifulSoup(response.text, 'lxml')
                while len(self.urls) < self.config.get_num_articles():
                    extracted_url = self._extract_url(article_bs)
                    if extracted_url=='':
                        break
                    self.urls.append(extracted_url)

    def get_search_urls(self) -> list:
        """
        Get seed_urls param.

        Returns:
            list: seed_urls param
        """
        return self.config.get_seed_urls()

class CrawlerRecursive(Crawler):
    """
    Recursive crawler implementation.
    """

    #: Url pattern
    url_pattern: Union[Pattern, str]

    def __init__(self, config: Config) -> None:
        """
        Initialize an instance of the CrawlerRecursive class.

        Args:
            config (Config): Configuration
        """
        super().__init__(config)
        self.start_url = self.config.get_seed_urls()[0]
        self.urls_file_path = PROJECT_ROOT / "lab_5_scraper" / "recursive_crawler_urls.json"
        if not self.urls_file_path.is_file():
            with self.urls_file_path.open('w') as urls_file:
                json.dump([], urls_file)


    def find_articles(self) -> None:
        """
        Find articles.
        """
        with self.urls_file_path.open('r') as urls_file:
            self.urls = json.load(urls_file)
            print('URLs already collected by recursive crawler:', len(self.urls))
        if len(self.urls) >= self.config.get_num_articles():
            return
        for seed_url in self.config.get_seed_urls():
            response = make_request(seed_url, self.config)
            if response.status_code == 200:
                article_bs = BeautifulSoup(response.text, 'lxml')
                while len(self.urls) < self.config.get_num_articles():
                    extracted_url = self._extract_url(article_bs)
                    if extracted_url=='':
                        break
                    self.urls.append(extracted_url)
                    with self.urls_file_path.open('w') as urls_file:
                        json.dump(self.urls, urls_file, indent=4)
                    self.find_articles()

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
        article = Article(full_url, article_id)
        self.article = article

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        try:
            article_text = ''
            article = article_soup.find('div', {"class":"news-fulltext"}).find_all('p')
            for paragraph in article:
                article_text += ''.join(paragraph.find_all(string=True)) + '\n'
            self.article.text = article_text
        except AttributeError:
            article_text = ''
            article = article_soup.find('div', {"class": "news-fulltext"}).find_all('p')
            for paragraph in article:
                article_text += ''.join(paragraph.find_all(string=True)) + '\n'
            self.article.text = article_text

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        self.article.title = article_soup.find("h1", class_="news-title").get_text()
        script_tag = article_soup.find('script', attrs={'type':"application/ld+json"})
        json_data = json.loads(script_tag.text)
        self.article.author = [json_data['author']['name']]
        raw_date = str(json_data['datePublished'])
        self.article.date = self.unify_date_format(raw_date)
        self.article.topics = [rubric.get_text()
                               for rubric in
                               article_soup.find_all('a',
                                                     {'class': "badge badge-rubric me-2"})]

    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """
        return datetime.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S+09:00')

    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """
        response = make_request(self.full_url, self.config)
        if response.status_code == 200:
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
    try:
        pathlib.Path(base_path).mkdir(parents=True)
    except FileExistsError:
        shutil.rmtree(base_path)
        pathlib.Path(base_path).mkdir(parents=True)


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
    print("Crawler finished working.")


    recursive_crawler = CrawlerRecursive(config=configuration)
    recursive_crawler.find_articles()
    print("Recursive crawler finished working.")


if __name__ == "__main__":
    main()
