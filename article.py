import requests
import trafilatura

from readability import Document
from newspaper import Article
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


def fetch_html(url: str):

    try:

        r = requests.get(url, headers=HEADERS, timeout=20)

        if r.status_code != 200:
            return None

        return r.text

    except Exception:
        return None


def extract_with_trafilatura(html):

    try:

        text = trafilatura.extract(
            html,
            include_comments=False,
            include_tables=False
        )

        if text and len(text) > 300:
            return text

    except Exception:
        pass

    return None


def extract_with_readability(html):

    try:

        doc = Document(html)

        content = doc.summary()

        soup = BeautifulSoup(content, "html.parser")

        text = soup.get_text(" ", strip=True)

        if text and len(text) > 300:
            return text

    except Exception:
        pass

    return None


def extract_with_newspaper(url):

    try:

        article = Article(url)

        article.download()

        article.parse()

        text = article.text

        if text and len(text) > 300:
            return text

    except Exception:
        pass

    return None


def extract_article_text(url, rss_summary=None):
    """
    Универсальный extractor
    """

    html = fetch_html(url)

    if not html:
        return rss_summary

    text = extract_with_trafilatura(html)

    if text:
        return text

    text = extract_with_readability(html)

    if text:
        return text

    text = extract_with_newspaper(url)

    if text:
        return text

    return rss_summary
