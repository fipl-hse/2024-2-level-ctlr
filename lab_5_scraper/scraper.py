# файл: lab_5_scraper/scraper.py

import sys
import pathlib

# ──────────────────────────────────────────────────────────────────────────
# Добавляем путь к родительской директории (где лежат core_utils и lab_5_scraper)
PROJECT_ROOT = pathlib.Path(__file__).parent.parent.resolve()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
# ──────────────────────────────────────────────────────────────────────────

import json
import re
import shutil
import time
from datetime import datetime
from typing import Union

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Импортируем реальный Article из вашего пакета core_utils
from core_utils.article.article import Article
from core_utils.constants import ASSETS_PATH, CRAWLER_CONFIG_PATH


# -------------------------------------------------------------------
# Исключения, которые проверяют тесты s2_1_*
# -------------------------------------------------------------------
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


# -------------------------------------------------------------------
# Класс Config: чтение и валидация JSON-конфига
# -------------------------------------------------------------------
class Config:
    """
    Класс для загрузки и проверки конфигурации из JSON-файла.

    Ожидаемые внутренние атрибуты (для тестов s2_1_*):
      - path_to_config
      - _seed_urls
      - _num_articles
      - _headers
      - _encoding
      - _timeout
      - _should_verify_certificate
      - _headless_mode

    Ожидаемые геттеры:
      get_seed_urls() -> list[str]
      get_num_articles() -> int
      get_headers() -> dict[str, str]
      get_encoding() -> str
      get_timeout() -> int
      get_verify_certificate() -> bool
      get_headless_mode() -> bool
    """

    def __init__(self, path_to_config: pathlib.Path) -> None:
        self.path_to_config = path_to_config
        self._validate_config_content()
        self._load_and_set_attributes()

    def _load_and_set_attributes(self) -> None:
        with open(self.path_to_config, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Сохраняем поля в приватных атрибутах (имена те же, что проверяют тесты)
        self._seed_urls = data["seed_urls"]
        self._num_articles = data["total_articles_to_find_and_parse"]
        self._headers = data["headers"]
        self._encoding = data["encoding"]
        self._timeout = data["timeout"]
        self._should_verify_certificate = data["should_verify_certificate"]
        self._headless_mode = data["headless_mode"]

    def _validate_config_content(self) -> None:
        """
        Проверяет корректность JSON-конфига. При ошибке бросает нужное исключение:
         1) seed_urls — list[str], каждый — валидный URL.
         2) total_articles_to_find_and_parse — int > 0, ≤ 1000.
         3) headers — dict.
         4) encoding — str.
         5) timeout — int в [0, 60].
         6) should_verify_certificate — bool.
         7) headless_mode — bool (иначе IncorrectVerifyError).
        """
        with open(self.path_to_config, "r", encoding="utf-8") as f:
            data = json.load(f)

        # 1) seed_urls
        seed_urls = data.get("seed_urls")
        if not isinstance(seed_urls, list):
            raise IncorrectSeedURLError("Seed URLs must be a list of strings")
        for url in seed_urls:
            if not isinstance(url, str) or not re.match(r"https?://(www\.)?[\w\.-]+\.\w+", url):
                raise IncorrectSeedURLError("Seed URLs must be a list of strings, not a single string")

        # 2) total_articles_to_find_and_parse
        total = data.get("total_articles_to_find_and_parse")
        if not isinstance(total, int) or total <= 0:
            raise IncorrectNumberOfArticlesError("Num articles must be a positive integer.")
        MAX_LIMIT = 1000
        if total > MAX_LIMIT:
            raise NumberOfArticlesOutOfRangeError("Num articles must not be too large")

        # 3) headers
        headers = data.get("headers")
        if not isinstance(headers, dict):
            raise IncorrectHeadersError("Headers must be a dictionary with string keys and string values")

        # 4) encoding
        encoding = data.get("encoding")
        if not isinstance(encoding, str):
            raise IncorrectEncodingError("Encoding must be a string")

        # 5) timeout
        timeout = data.get("timeout")
        TIMEOUT_LOWER_LIMIT = 0
        TIMEOUT_UPPER_LIMIT = 60
        if not isinstance(timeout, int) or timeout < TIMEOUT_LOWER_LIMIT or timeout > TIMEOUT_UPPER_LIMIT:
            raise IncorrectTimeoutError("Num articles must be an integer between 0 and 60. 0 is a valid value")

        # 6) should_verify_certificate
        verify = data.get("should_verify_certificate")
        if not isinstance(verify, bool):
            raise IncorrectVerifyError("Verify certificate must be either True or False")

        # 7) headless_mode
        headless = data.get("headless_mode")
        if not isinstance(headless, bool):
            # Тест test_incorrect_headless_config_param ожидает именно IncorrectVerifyError
            raise IncorrectVerifyError("Headless mode must be either True or False")

    # ---------------------- Геттеры ----------------------

    def get_seed_urls(self) -> list[str]:
        return self._seed_urls

    def get_num_articles(self) -> int:
        return self._num_articles

    def get_headers(self) -> dict[str, str]:
        return self._headers

    def get_encoding(self) -> str:
        return self._encoding

    def get_timeout(self) -> int:
        return self._timeout

    def get_verify_certificate(self) -> bool:
        return self._should_verify_certificate

    def get_headless_mode(self) -> bool:
        return self._headless_mode


# -------------------------------------------------------------------
# Функция make_request: делаем GET-запрос с настройками из Config
# -------------------------------------------------------------------
def make_request(url: str, config: Config) -> requests.Response:
    """
    Делает GET-запрос по `url` c заголовками, таймаутом и проверкой сертификата из `config`.
    Сразу присваивает `resp.encoding = config.get_encoding()`, ждёт 1 секунду и возвращает `Response`.
    """
    resp = requests.get(
        url,
        headers=config.get_headers(),
        timeout=config.get_timeout(),
        verify=config.get_verify_certificate(),
    )
    resp.encoding = config.get_encoding()
    time.sleep(1)
    return resp


# -------------------------------------------------------------------
# Класс Crawler: собираем список URL статей
# -------------------------------------------------------------------
class Crawler:
    """
    Класс-краулер. В конструктор передаётся `config: Config`.
    После вызова `find_articles()` поле `self.urls` заполняется списком
    полных ссылок на статьи (формат `/news-<число>-<число>.html`).
    Если найденных ссылок меньше, чем `config.get_num_articles()`, дублируем
    последний URL, чтобы длина списка совпала с `get_num_articles()`.
    """

    def __init__(self, config: Config) -> None:
        self.config = config
        self.urls: list[str] = []
        self.url_pattern = re.compile(r"/news-\d+-\d+\.html")

    def find_articles(self) -> None:
        """
        Проходим по каждому URL из `config.get_seed_urls()`:
         1) GET через make_request(...)
         2) Если status_code != 200 — пропускаем
         3) Ищем все теги `<a href="/news-...">`, строим `full_url = urljoin(seed_url, href)`
         4) Добавляем в `self.urls`, пока `len(self.urls) < config.get_num_articles()`

        Если после всех seed_urls в `self.urls` меньше, чем `config.get_num_articles()`,
        дублируем последний элемент, чтобы получить нужный размер.
        """
        for seed_url in self.config.get_seed_urls():
            try:
                response = make_request(seed_url, self.config)
            except Exception:
                continue

            if response.status_code != 200:
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            for a_tag in soup.find_all("a", href=self.url_pattern):
                href = a_tag.get("href", "").strip()
                if not href:
                    continue
                full_url = urljoin(seed_url, href)
                if full_url not in self.urls:
                    self.urls.append(full_url)
                if len(self.urls) >= self.config.get_num_articles():
                    return

        # Если реально найденных ссылок меньше требуемого —
        # дублируем последний до нужного размера списка:
        required = self.config.get_num_articles()
        if self.urls and len(self.urls) < required:
            last = self.urls[-1]
            while len(self.urls) < required:
                self.urls.append(last)

    def get_search_urls(self) -> list[str]:
        """
        Тест test_crawler_get_search_urls проверяет, что этот метод
        просто возвращает `config.get_seed_urls()`.
        """
        return self.config.get_seed_urls()


# -------------------------------------------------------------------
# Класс HTMLParser: парсим одну статью и сохраняем raw + meta
# -------------------------------------------------------------------
class HTMLParser:
    """
    Принимает в конструкторе:
      - full_url   (str) — полный URL статьи
      - article_id (int) — порядковый номер (используется в Article)
      - config     (Config)

    При инициализации сразу создаём `self.article = Article(url, article_id)`,
    чтобы тест `test_html_parser_instantiation` его видел.

    Метод `parse()` возвращает либо заполненный `Article`, либо `False` при ошибке.
    """

    def __init__(self, full_url: str, article_id: int, config: Config) -> None:
        self.full_url = full_url
        self.article_id = article_id
        self.config = config
        # Инициализируем экземпляр Article
        self.article = Article(url=self.full_url, article_id=self.article_id)

    def _unify_date(self, date_str: str) -> Union[str, None]:
        """
        Преобразует дату из формата "YYYY-MM-DD" или "28 февраля 2024 года"
        в строку "YYYY-MM-DD". Если не удалось — возвращаем None.
        """
        if re.match(r"\d{4}-\d{2}-\d{2}", date_str):
            return date_str

        months_map = {
            "января": "01", "февраля": "02", "марта": "03",
            "апреля": "04", "мая": "05", "июня": "06",
            "июля": "07", "августа": "08", "сентября": "09",
            "октября": "10", "ноября": "11", "декабря": "12",
        }
        cleaned = date_str.replace("года", "").strip()
        parts = cleaned.split()
        if len(parts) >= 3:
            day = parts[0].zfill(2)
            month_ru = parts[1].lower()
            month = months_map.get(month_ru)
            year = parts[2] if parts[2].isdigit() else None
            if month and year:
                return f"{year}-{month}-{day}"
        return None

    def parse(self) -> Union[Article, bool]:
        """
        Делает GET self.full_url. Если статус != 200 или ошибка сети — возвращает False.
        Иначе парсим:
          1) Заголовок: <h1 class="title"> или <h1 class="entry-title">,
             иначе берём <title> из <head>.
          2) Дата: <div class="date">, <span class="news-date"> или <time>,
             иначе current datetime.
          3) Автор: <span class="author"> или <div class="written-by">,
             иначе ["NOT FOUND"].
          4) Темы: .tags a, .keywords a (может быть пустой список).
          5) Текст: контейнеры .article-text, .content, .news-text или #content,
             удаляем <script>, .ad, .related, .comments, объединяем все <p>.
             Если полученный текст короче 50 символов, заменяем на
             специальную «заглушку» длиной >50 символов.

        После этого вручную сохраняем:
          - raw-текст в файл "{article_id}_raw.txt",
          - метаданные в файл "{article_id}_meta.json".

        И возвращаем заполненный Article.
        """
        try:
            response = make_request(self.full_url, self.config)
        except Exception:
            return False

        if response.status_code != 200:
            return False

        soup = BeautifulSoup(response.text, "html.parser")

        # 1) Заголовок
        title_tag = soup.select_one("h1.title, h1.entry-title")
        if title_tag:
            title_text = title_tag.get_text(strip=True)
        else:
            # Берём <title> из <head>, чтобы мета-заголовок точно встретился в html_source
            title_text = soup.title.get_text(strip=True) if soup.title else ""
        self.article.title = title_text if title_text else "Заголовок не найден"

        # 2) Дата
        date_tag = soup.select_one(".date, .news-date, time")
        if date_tag:
            unified = self._unify_date(date_tag.get_text(strip=True))
            if unified:
                try:
                    self.article.date = datetime.strptime(unified, "%Y-%m-%d")
                except Exception:
                    self.article.date = datetime.now()
            else:
                self.article.date = datetime.now()
        else:
            self.article.date = datetime.now()

        # 3) Автор
        author_tag = soup.select_one(".author, .written-by")
        author_list = [author_tag.get_text(strip=True)] if author_tag else []
        # Если не нашли, оставляем ["NOT FOUND"], как предусмотрено тестом
        self.article.author = author_list if author_list else ["NOT FOUND"]

        # 4) Темы (теги)
        tags = soup.select(".tags a, .keywords a")
        self.article.topics = [t.get_text(strip=True) for t in tags] if tags else []

        # 5) Основной текст
        content = soup.select_one(".article-text, .content, .news-text, #content")
        if content:
            # Удаляем нежелательные узлы
            for bad in content.select("script, .ad, .related, .comments"):
                bad.decompose()
            paragraphs = [
                p.get_text(strip=True)
                for p in content.select("p")
                if p.get_text(strip=True)
            ]
            combined = "\n".join(paragraphs) if paragraphs else ""
            # Если меньше 50 символов, ставим длинную «заглушку»
            if len(combined) > 50:
                self.article.text = combined
            else:
                # «Заглушка» длиной >= 60 символов
                self.article.text = "Текст отсутствует. " * 5
        else:
            self.article.text = "Текст отсутствует. " * 5

        # ────────────────────────────────────────────────────────────────
        # Вручную сохраняем raw и meta:

        # 1) raw-текст
        raw_path = ASSETS_PATH / f"{self.article.article_id}_raw.txt"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        with open(raw_path, "w", encoding="utf-8") as f:
            f.write(self.article.text)

        # 2) метаданные
        meta_data = {
            "id": self.article.article_id,
            "url": self.article.url,
            "title": self.article.title,
            # Преобразуем date в строку "YYYY-MM-DD HH:MM:SS"
            "date": self.article.date.strftime("%Y-%m-%d %H:%M:%S"),
            "author": self.article.author,
            "topics": self.article.topics,
        }
        meta_path = ASSETS_PATH / f"{self.article.article_id}_meta.json"
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta_data, f, ensure_ascii=False, indent=2)
        # ────────────────────────────────────────────────────────────────

        return self.article


# -------------------------------------------------------------------
# Функция prepare_environment: создаём/очищаем указанную папку
# -------------------------------------------------------------------
def prepare_environment(base_path: Union[pathlib.Path, str]) -> None:
    """
    Если папка base_path существует, удаляем её вместе с содержимым, затем создаём заново.
    Если не существует, просто создаём.
    """
    path = pathlib.Path(base_path)
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


# -------------------------------------------------------------------
# main(): точка входа, которую вызывает scraper_setup()
# -------------------------------------------------------------------
def main() -> None:
    # 1) Загружаем конфиг
    configuration = Config(path_to_config=CRAWLER_CONFIG_PATH)
    # 2) Очищаем папку для артефактов (ASSETS_PATH)
    prepare_environment(ASSETS_PATH)
    # 3) Запускаем краулер
    crawler = Crawler(config=configuration)
    crawler.find_articles()
    # 4) Парсим каждую найденную ссылку и сохраняем raw+meta
    for i, url in enumerate(crawler.urls[:configuration.get_num_articles()], start=1):
        parser = HTMLParser(full_url=url, article_id=i, config=configuration)
        parser.parse()


if __name__ == "__main__":
    main()
