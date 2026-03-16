from urllib.parse import urlparse
from bs4 import BeautifulSoup


BAD_DOMAINS = [
    "yandex.ru",
    "dzen.ru"
]


def clean_html(text: str) -> str:
    """
    Удаляет HTML из RSS summary
    """

    if not text:
        return ""

    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text(" ", strip=True)


def extract_real_google_url(entry):
    """
    Google RSS содержит redirect ссылки.
    Пытаемся извлечь настоящую ссылку.
    """

    link = entry.get("link")

    if not link:
        return None

    if "news.google.com" not in link:
        return link

    summary = entry.get("summary", "")

    if not summary:
        return link

    soup = BeautifulSoup(summary, "html.parser")
    a = soup.find("a")

    if a and a.get("href"):
        return a["href"]

    return link


def is_probably_article(url: str) -> bool:
    """
    Отсекает главные страницы сайтов
    """

    try:

        parsed = urlparse(url)

        path = parsed.path.strip("/")

        if not path:
            return False

        depth = path.count("/")

        if depth < 1:
            return False

        if len(path) < 10:
            return False

        return True

    except Exception:
        return False


def is_bad_domain(url: str) -> bool:

    for domain in BAD_DOMAINS:

        if domain in url:
            return True

    return False
