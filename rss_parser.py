import feedparser
import hashlib
from urllib.parse import urlparse
import concurrent.futures
import threading
from datetime import datetime, timedelta, timezone
import requests
from bs4 import BeautifulSoup
import re
import time
from difflib import SequenceMatcher
import pickle
import numpy as np
import pymorphy3

from natasha import (
    Segmenter,
    NewsEmbedding,
    NewsNERTagger,
    Doc
)
from database import get_connection, update_newsletter_stats, create_newsletter
from config import *

from news_processing import (
    clean_html,
    extract_real_google_url,
    is_probably_article,
    is_bad_domain
)

from article_extractor import extract_article_text

from llm import (
    is_about_belarus,
    classify_topic_llm
)

db_lock = threading.Lock()
google_news_requests_counter = 0

WORD_RE = re.compile(r'\b\w+\b', re.UNICODE)
segmenter = Segmenter()
TOPIC_EMBEDDINGS = {}
emb = NewsEmbedding()

ner_tagger = NewsNERTagger(emb)

morph = pymorphy3.MorphAnalyzer()
def tokenize(text: str) -> set[str]:
    return set(WORD_RE.findall(text.lower()))

def normalize_title(title: str) -> str:
    if not title:
        return ""
    title = title.lower()
    title = re.sub(r'\d+', '', title)
    title = re.sub(r'[^\w\s]', '', title)
    title = re.sub(r'\s+', ' ', title).strip()
    return title

def is_blacklisted(url: str, title: str = "") -> bool:
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    if domain.startswith('www.'):
        domain = domain[4:]
    path = parsed.path.lower()
    title_lower = title.lower() if title else ""

    for b in BLACKLIST_DOMAINS:
        if b in domain:
            return True
    if any(k in path for k in BLACKLIST_KEYWORDS):
        return True
    if title and any(k in title_lower for k in BLACKLIST_KEYWORDS):
        return True
    return False

def init_topic_embeddings():

    for topic_id, topic_data in TOPICS.items():

        text = " ".join(topic_data.get("keywords", []))

        emb = compute_embedding_sync(text)

        if emb:
            TOPIC_EMBEDDINGS[topic_id] = emb

def classify_topic_embedding(text):

    emb = compute_embedding_sync(text)

    if not emb:
        return None

    best_topic = None
    best_score = 0

    for topic, topic_emb in TOPIC_EMBEDDINGS.items():

        sim = cosine_similarity(emb, topic_emb)

        if sim > best_score:

            best_score = sim
            best_topic = topic

    if best_score > 0.55:
        return best_topic

    return None

def build_google_rss(query: str) -> str:
    q = query.replace(" ", "+")
    return f"https://news.google.com/rss/search?q={q}&hl=ru&gl=BY&ceid=BY:ru"

def fingerprint(title, link):
    parsed = urlparse(link)
    domain = parsed.netloc.lower()
    norm_title = normalize_title(title)
    base = f"{domain}|{norm_title[:100]}"
    return hashlib.sha1(base.encode()).hexdigest()

def calculate_similarity(t1, t2):
    return SequenceMatcher(None, t1.lower(), t2.lower()).ratio()

def cosine_similarity(a, b):
    a = np.array(a)
    b = np.array(b)
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-8)

def is_similar_news(t1, t2, threshold=SIMILARITY_THRESHOLD):

    t1 = normalize_title(t1)
    t2 = normalize_title(t2)

    return calculate_similarity(t1, t2) > threshold

def keyword_has_context(keyword: str, text: str, context_list: list) -> bool:
    if not context_list:
        return True
    for ctx in context_list:
        if ctx in text:
            return True
    return False

def classify_topic_keywords(title: str, text: str):
    # Разделяем заголовок и текст для учёта важности
    title_lower = title.lower()
    text_lower = text.lower()
    combined = title_lower + " " + text_lower
    lemmas = lemmatize_text(combined)  # леммы всего текста (можно оставить)

    topic_scores = {}

    for topic_id, topic_data in TOPICS.items():
        if topic_id == "other":
            continue

        score = 0
        keywords = topic_data.get("keywords", [])
        negatives = topic_data.get("exclude", [])

        # --- Положительные баллы ---
        # Сущности (сильный сигнал) – добавляем сразу +6, если сущность встречается где угодно
        for ent, ent_topic in ENTITY_TOPIC.items():
            if ent_topic == topic_id and ent in combined:
                score += 6

        # Ключевые слова: считаем вхождения и учитываем заголовок
        for kw in keywords:
            # Проверяем вхождение в заголовок (важнее)
            if kw in title_lower:
                # За первое вхождение +4, за каждое следующее +1 (до +7)
                cnt_title = title_lower.count(kw)
                score += 4 + min(cnt_title - 1, 3)  # максимум +7 за заголовок
            # Вхождение в текст
            if kw in text_lower:
                cnt_text = text_lower.count(kw)
                score += 2 + min(cnt_text - 1, 3)   # максимум +5 за текст
            # Леммы (как дополнительный признак, если слово в другой форме)
            if kw in lemmas:
                score += 1

        # --- Негативные баллы (усиливаем) ---
        neg_penalty = 0
        for neg in negatives:
            if neg in combined:
                # Штраф -8 за каждое негативное слово (можно сделать прогрессивным)
                neg_penalty += 8
        # Если негативных много, они могут полностью обнулить тему
        score -= neg_penalty

        # Если после всего score > 0, добавляем в кандидаты
        if score > 0:
            topic_scores[topic_id] = score

    if not topic_scores:
        return None, 0

    best_topic = max(topic_scores, key=topic_scores.get)
    return best_topic, topic_scores[best_topic]

def compute_embedding_sync(text: str) -> list | None:
    try:
        resp = requests.post(
            OLLAMA_EMBED_URL,
            json={"model": "nomic-embed-text", "prompt": text[:1000]},
            timeout=60
        )
        resp.raise_for_status()
        return resp.json().get("embedding")
    except Exception as e:
        print(f"[EMBEDDING ERROR] {e}")
        return None

def update_news_embedding(news_id: int, text: str):

    emb = compute_embedding_sync(text)

    if emb:

        with db_lock:

            conn = get_connection()

            conn.execute(
                "UPDATE news SET embedding = ? WHERE id = ?",
                (pickle.dumps(emb), news_id)
            )

            conn.commit()

            conn.close()

        print(f"[EMBEDDING] Saved for news {news_id}")

def lemmatize_text(text: str):

    words = WORD_RE.findall(text.lower())

    lemmas = set()

    for w in words:

        try:
            lemma = morph.parse(w)[0].normal_form
            lemmas.add(lemma)
        except:
            continue

    return lemmas

def ner_detect_belarus(text: str) -> bool:

    try:

        doc = Doc(text)

        doc.segment(segmenter)

        doc.tag_ner(ner_tagger)

        for span in doc.spans:

            if span.type == "LOC":

                loc = span.text.lower()

                if loc in BELARUS_CITIES:
                    return True

                if loc in BELARUS_REGIONS:
                    return True

                if "беларус" in loc:
                    return True

        return False

    except Exception as e:

        print("[NER error]", e)

        return False

def fast_belarus_filter(title: str, text: str, url: str) -> bool:

    parsed = urlparse(url)

    domain = parsed.netloc.lower()

    if domain.startswith("www."):
        domain = domain[4:]

    # Stage 1 — .by домен
    if domain.endswith(BELARUS_DOMAIN):
        return True

    combined = f"{title} {text}".lower()

    # Stage 2 — ключевые слова
    for w in BELARUS_WORDS:

        if w in combined:
            return True

    # Stage 3 — города
    for city in BELARUS_CITIES:

        if city in combined:
            return True

    # Stage 4 — регионы
    for region in BELARUS_REGIONS:

        if region in combined:
            return True

    # Stage 5 — NER
    if ner_detect_belarus(combined):

        return True

    return False

def check_for_duplicates_enhanced(title: str, topic: str | None, link: str, c, text_preview: str = "") -> bool:
    clean_link = link.split('?')[0].split('#')[0]

    # Точное совпадение ссылки
    exact = c.execute("""
        SELECT id FROM news 
        WHERE link LIKE ? AND published >= datetime('now', '-24 hours')
    """, (clean_link + '%',)).fetchone()
    if exact:
        return True

    norm_title = normalize_title(title)
    if text_preview:
        norm_preview = normalize_title(text_preview[:300])  # очищенный сниппет

    # Поиск за последние 24 часа (можно увеличить до 48, если нужно)
    if topic:
        rows = c.execute("""
            SELECT id, title, normalized_title, full_text
            FROM news 
            WHERE topic = ? AND published >= datetime('now', '-24 hours')
        """, (topic,)).fetchall()
    else:
        rows = c.execute("""
            SELECT id, title, normalized_title, full_text
            FROM news 
            WHERE published >= datetime('now', '-24 hours')
        """).fetchall()

    for row in rows:
        # Сравнение заголовков
        existing_norm = row['normalized_title'] or normalize_title(row['title'])
        if is_similar_news(norm_title, existing_norm):
            print(f"[DUPLICATE] similar title: {title[:60]}")
            return True

        # Сравнение текстового сниппета (если доступен)
        if text_preview and row['full_text']:
            old_preview = normalize_title(row['full_text'][:300])
            if calculate_similarity(norm_preview, old_preview) > 0.8:
                print(f"[DUPLICATE] similar text content: {title[:60]}")
                return True

    return False

def process_news_entry(entry, source="rss"):
    try:
        title = entry.get('title', '').strip()
        url = extract_real_google_url(entry)

        if not title or not url:
            return None

        url = url.split('?')[0]

        if is_bad_domain(url):
            print(f"[SKIP] bad domain: {title[:60]}")
            return None

        if is_blacklisted(url, title):
            print(f"[SKIP] blacklist: {title[:60]}")
            return None

        if not is_probably_article(url):
            print(f"[SKIP] not article: {title[:60]}")
            return None

        published = None
        if hasattr(entry, 'published_parsed') and entry.published_parsed:
            published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
            published = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
        else:
            print(f"[SKIP] no date: {title[:60]}")
            return None

        if published < datetime.now(timezone.utc) - timedelta(hours=MAX_NEWS_AGE_HOURS):
            print(f"[SKIP OLD] {title[:60]}")
            return None

        fp = fingerprint(title, url)

        with db_lock:
            conn = get_connection()
            c = conn.cursor()
            exists = c.execute(
                "SELECT id FROM news WHERE fingerprint=?",
                (fp,)
            ).fetchone()
            conn.close()

        if exists:
            print("[SKIP] duplicate fingerprint")
            return None

        # --- Извлечение summary из RSS (теперь выполняется всегда) ---
        raw_summary = entry.get('summary', '') or entry.get('description', '')
        summary = clean_html(raw_summary)                     # очищенный текст для анализа
        rss_summary_raw = raw_summary[:1000] if raw_summary else None   # оригинальный (обрезанный) для БД

        # Быстрая фильтрация по Беларуси (на основе заголовка и RSS summary)
        if not fast_belarus_filter(title, summary, url):
            if not is_about_belarus(title, summary):
                print(f"[SKIP] Belarus title filter: {title[:60]}")
                return None

        # Извлечение полного текста статьи
        text = extract_article_text(url, summary)

        if not text or len(text) < 150:
            print(f"[SKIP] no text: {title[:60]}")
            return None

        # Повторная фильтрация по Беларуси (уже с полным текстом)
        fast_ok = fast_belarus_filter(title, text, url)
        if not fast_ok:
            if not is_about_belarus(title, text):
                print(f"[SKIP] not Belarus related: {title[:60]}")
                return None
            else:
                print(f"[LLM Belarus OK] {title[:60]}")

        # Классификация темы
        topic, score = classify_topic_keywords(title, text)
        if score >= 4:
            pass
        elif score >= 2:
            topic = classify_topic_llm(title, text, TOPICS)
        else:
            topic = "other"

        # Финальная проверка на дубликаты (усиленная)
        with db_lock:
            conn = get_connection()
            c = conn.cursor()
            dup = check_for_duplicates_enhanced(title, topic, url, c, text_preview=text[:300])
            if dup:
                conn.close()
                print("[SKIP] duplicate (enhanced)")
                return None

            parsed = urlparse(url)
            real_source = parsed.netloc.lower()
            norm_title = normalize_title(title)
            important = 0

            # Вставка записи в БД
            c.execute("""
                INSERT INTO news
                (title, summary, full_text, link,
                 topic, published, fingerprint,
                 important, source, fetched_at,
                 real_source, normalized_title, rss_summary)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, ?, ?)
            """, (
                title[:500],
                None,
                text[:4000],
                url,
                topic,
                published,
                fp,
                important,
                source,
                real_source,
                norm_title[:200],
                rss_summary_raw          # ← используем сохранённый оригинальный summary
            ))

            news_id = c.lastrowid
            conn.commit()
            conn.close()

        print(f"[OK] {topic}: {title[:70]} (ID {news_id})")

        # Запуск фонового обновления эмбеддинга
        threading.Thread(
            target=update_news_embedding,
            args=(news_id, title + " " + text[:200]),
            daemon=True
        ).start()

        return {"id": news_id, "topic": topic, "important": important}

    except Exception as e:
        print(f"[ERROR] process_news_entry: {e}")
        return None

def parse_feed(url, source="rss"):
    try:
        print(f"[RSS] Загрузка: {url}")
        if "rsshub.app/telegram" in url:
            print("[RSS] Telegram cooldown...")
            time.sleep(3)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8"
        }
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        results = []
        for entry in feed.entries[:20]:
            res = process_news_entry(entry, source)
            if res:
                results.append(res)
        return results
    except Exception as e:
        print(f"[ERROR] parse_feed {url}: {e}")
        return []

_init_done = False
def ensure_topic_embeddings():
    global _init_done
    if not _init_done and TOPIC_EMBEDDINGS:
        # уже инициализировано?
        return
    init_topic_embeddings()
    _init_done = True

def collect_and_save_news():
    ensure_topic_embeddings()
    print(f"[RSS] Начало сбора новостей...")
    all_results = []
    newsletter_id = create_newsletter()

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_PARALLEL_REQUESTS) as ex:
        futs = {ex.submit(parse_feed, url, "rss"): url for url in BASE_RSS}
        for fut in concurrent.futures.as_completed(futs):
            url = futs[fut]
            try:
                res = fut.result()
                all_results.extend(res)
                print(f"[RSS] {url} -> {len(res)} нов.")
            except Exception as e:
                print(f"[RSS] Ошибка {url}: {e}")

    google_queries = []
    for topic_data in TOPICS.values():
        google_queries.extend(topic_data.get("queries", []))
    google_queries = list(set(google_queries))[:MAX_GOOGLE_NEWS_REQUESTS]

    if google_queries:
        print(f"[RSS] Google News: {len(google_queries)} запросов")
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            futs = {ex.submit(parse_feed, build_google_rss(q), "google"): q for q in google_queries}
            for fut in concurrent.futures.as_completed(futs):
                q = futs[fut]
                try:
                    res = fut.result()
                    all_results.extend(res)
                    print(f"[RSS] Google '{q}' -> {len(res)} нов.")
                except Exception as e:
                    print(f"[RSS] Google '{q}' ошибка: {e}")

    stats = {}
    for r in all_results:
        t = r["topic"]
        if t not in stats:
            stats[t] = {"total": 0, "important": 0}
        stats[t]["total"] += 1
        if r["important"]:
            stats[t]["important"] += 1

    with db_lock:
        conn = get_connection()
        c = conn.cursor()
        for topic in stats:
            c.execute("""
                DELETE FROM news 
                WHERE topic = ? AND id NOT IN (
                    SELECT id FROM news WHERE topic = ? ORDER BY published DESC LIMIT ?
                )
            """, (topic, topic, MAX_NEWS_PER_TOPIC))
        conn.commit()
        conn.close()

    update_newsletter_stats(newsletter_id, stats)
    print(f"[RSS] Сбор завершён. Всего новостей: {len(all_results)}")
    return stats
