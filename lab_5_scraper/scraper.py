"""
Crawler implementation.
"""

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument
import datetime
import json
import pathlib
import re
import shutil
from typing import Pattern, Union

import requests
from bs4 import BeautifulSoup

from core_utils.article.article import Article
from core_utils.article.io import to_meta, to_raw
from core_utils.config_dto import ConfigDTO
from core_utils.constants import (
    ASSETS_PATH,
    CRAWLER_CONFIG_PATH,
    NUM_ARTICLES_UPPER_LIMIT,
    TIMEOUT_LOWER_LIMIT,
    TIMEOUT_UPPER_LIMIT,
)


class IncorrectSeedURLError(Exception):
    """
     Exception on URL pattern different from "https?://(www.)?"
    """


class NumberOfArticlesOutOfRangeError(Exception):
    """
     Exception on number of articles being out of range from 1 to 150
    """


class IncorrectNumberOfArticlesError(Exception):
    """
     Exception on non-integer number of articles / number of articles being < 0
    """


class IncorrectHeadersError(Exception):
    """
     Exception on headers being in non-dictionary form
    """


class IncorrectEncodingError(Exception):
    """
     Exception on non-string encoding values
    """


class IncorrectTimeoutError(Exception):
    """
     Exception on timeout values being other than a positive integer < 60
    """


class IncorrectVerifyError(Exception):
    """
     Exception on verification certificate values other than True or False
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
        with self.path_to_config.open("r", encoding="utf-8") as config_file:
            cnfg = json.load(config_file)
            return ConfigDTO(**cnfg)

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """
        cnfg = self._extract_config_content()
        uptn = re.compile("https?://(www.)?")
        if not (isinstance(cnfg.seed_urls, list) and all(
                uptn.match(url) for url in cnfg.seed_urls)):
            raise IncorrectSeedURLError('Incorrect seed URL value')
        if not (isinstance(cnfg.total_articles, int) and cnfg.total_articles > 0):
            raise IncorrectNumberOfArticlesError('ERROR: Number of articles is not an integer')
        if cnfg.total_articles < 1 or cnfg.total_articles > NUM_ARTICLES_UPPER_LIMIT:
            raise NumberOfArticlesOutOfRangeError('ERROR: Number of articles out of 1-150 range')
        if not isinstance(cnfg.headers, dict):
            raise IncorrectHeadersError('Incorrect headers value type')
        if not isinstance(cnfg.encoding, str):
            raise IncorrectEncodingError('Incorrect encoding value')
        if not (isinstance(cnfg.timeout, int)
                and TIMEOUT_LOWER_LIMIT < cnfg.timeout < TIMEOUT_UPPER_LIMIT):
            raise IncorrectTimeoutError('ERROR: Timeout is not an integer < 61')
        if not (isinstance(cnfg.should_verify_certificate, bool) and isinstance(
                cnfg.headless_mode, bool)):
            raise IncorrectVerifyError('ERROR: Values are not boolean')

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
    out = requests.get(url=url, headers=config.get_headers(), timeout=config.get_timeout(),
                       verify=config.get_verify_certificate())
    return out


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
        self.burl = "https://daily-nn.ru/"

    def _extract_url(self, article_bs: BeautifulSoup) -> str:
        """
        Find and retrieve url from HTML.

        Args:
            article_bs (bs4.BeautifulSoup): BeautifulSoup instance

        Returns:
            str: Url from HTML
        """
        for ttag in article_bs.find_all("p", class_="text_new_block"):
            ltag = ttag.find("a", href=True)
            if ltag:
                href = ltag["href"]
                url = self.burl + str(href)
                if url and url not in self.urls:
                    return url
        return ""

    def find_articles(self) -> None:
        """
        Find articles.
        """
        for surl in self.config.get_seed_urls():
            if len(self.urls) == self.config.get_num_articles():
                break

            try:
                response = make_request(url=surl, config=self.config)
                response.raise_for_status()
            except requests.HTTPError:
                continue
            pbs = BeautifulSoup(response.text, 'html.parser')
            url = self._extract_url(pbs)
            while url:
                self.urls.append(url)
                if len(self.urls) == self.config.get_num_articles():
                    break
                url = self._extract_url(pbs)

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
        adiv = article_soup.find("div", class_="content_cn")
        text = []
        for a in adiv:
            if a.get_text().strip():
                text.append(a.get_text(strip=True, separator="\n"))
        self.article.text = "\n".join(text)

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        self.article.article_id = self.article_id
        self.article.url = self.full_url
        self.article.author = ["No information"]
        self.article.title = article_soup.find("h1").text
        tmt = article_soup.find("div", class_="claer public_data").find("span")
        self.article.date = self.unify_date_format(tmt.text.strip())
        tpt = article_soup.find("div", class_="claer public_data").find_all("a")
        self.article.topics = [t.text.strip() for t in tpt]

    def unify_date_format(self, date_str: str) -> datetime.datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """
        print(date_str)
        tm, dt = date_str.split(", ")
        di, mi, yi = map(str, dt.split(" "))
        d = int(di)
        y = int(yi)
        m = 4
        if mi == 'января':
            m = 1
        if mi == 'февраля':
            m = 2
        if mi == 'марта':
            m = 3
        if mi == 'апреля':
            m = 4
        if mi == 'мая':
            m = 5
        if mi == 'июня':
            m = 6
        if mi == 'июля':
            m = 7
        if mi == 'августа':
            m = 8
        if mi == 'сентября':
            m = 9
        if mi == 'октября':
            m = 10
        if mi == 'ноября':
            m = 11
        if mi == 'декабря':
            m = 12
        hr, mnt = map(int, tm.split(":"))
        return datetime.datetime(y, m, d, hr, mnt)

    def parse(self) -> Union[Article, bool, list]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """
        rsp = make_request(self.full_url, self.config)
        if rsp.ok:
            pbs = BeautifulSoup(rsp.text, 'html.parser')
            self._fill_article_with_text(pbs)
            self._fill_article_with_meta_information(pbs)
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
    ini = Config(path_to_config=CRAWLER_CONFIG_PATH)
    prepare_environment(ASSETS_PATH)
    crl = Crawler(config=ini)
    crl.find_articles()
    print(f"Articles required: {ini.get_num_articles()}")
    print(f"Found: {len(crl.urls)} urls")
    for furl in crl.urls:
        prs = HTMLParser(furl, 1, ini)
        art_p = prs.parse()
        if isinstance(art_p, Article):
            to_raw(art_p)
            #to_meta(art_p)


if __name__ == "__main__":
    main()
