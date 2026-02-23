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

from database import get_connection, update_newsletter_stats, create_newsletter
from config import *
from llm import analyze_news, validate_topic

db_lock = threading.Lock()
google_news_requests_counter = 0

WORD_RE = re.compile(r'\b\w+\b', re.UNICODE)

def tokenize(text: str) -> set[str]:
    return set(WORD_RE.findall(text.lower()))

# =========================
# НОВАЯ ФУНКЦИЯ: нормализация заголовка
# =========================
def normalize_title(title: str) -> str:
    """Приводит заголовок к каноническому виду: lower case, удаление цифр и пунктуации, лишних пробелов"""
    if not title:
        return ""
    title = title.lower()
    title = re.sub(r'\d+', '', title)                # удаляем цифры
    title = re.sub(r'[^\w\s]', '', title)            # удаляем пунктуацию
    title = re.sub(r'\s+', ' ', title).strip()       # нормализуем пробелы
    return title

def is_blacklisted(url: str, title: str = "") -> bool:
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    # Убираем www.
    if domain.startswith('www.'):
        domain = domain[4:]
    path = parsed.path.lower()
    title_lower = title.lower() if title else ""

    # Логируем проверяемый домен
    print(f"[BLACKLIST CHECK] domain={domain}, path={path}")

    # Проверка по домену (частичное совпадение — если домен содержит любой из запрещённых)
    for b in BLACKLIST_DOMAINS:
        if b in domain:
            print(f"[BLACKLIST] domain '{domain}' contains '{b}' -> blocked")
            return True

    if any(k in path for k in BLACKLIST_KEYWORDS):
        print(f"[BLACKLIST] path contains keyword -> blocked")
        return True

    if title and any(k in title_lower for k in BLACKLIST_KEYWORDS):
        print(f"[BLACKLIST] title contains keyword -> blocked")
        return True

    return False

def is_wrong_region(text: str) -> bool:
    """Возвращает True, если новость явно не про Беларусь, а про другие регионы."""
    text = text.lower()
    if any(b in text for b in BELARUS_KEYWORDS):
        return False
    if not any(r in text for r in RUSSIA_REGIONS):
        return False
    return True

def build_google_rss(query: str) -> str:
    q = query.replace(" ", "+")
    return f"https://news.google.com/rss/search?q={q}&hl=ru&gl=BY&ceid=BY:ru"

def fingerprint(title, link):
    parsed = urlparse(link)
    domain = parsed.netloc.lower()
    norm_title = normalize_title(title)   # используем улучшенную нормализацию
    base = f"{domain}|{norm_title[:100]}"
    return hashlib.sha1(base.encode()).hexdigest()

def calculate_similarity(t1, t2):
    """Используем difflib для более точного сравнения строк"""
    return SequenceMatcher(None, t1.lower(), t2.lower()).ratio()

def is_similar_news(t1, t2, threshold=SIMILARITY_THRESHOLD):
    return calculate_similarity(t1, t2) > threshold

# =========================
# ИЗМЕНЁННАЯ функция detect_topic с весовыми коэффициентами и сущностями
# =========================
def detect_topic(title: str, content: str = "") -> str | None:
    """
    2-stage topic detection:
    1) scoring with weights (entities + keywords + penalties)
    2) LLM validation for borderline cases
    """
    text = f"{title} {content}".lower()
    title_lower = title.lower()

    # Региональный фильтр (пока оставляем строгий, позже заменим на штрафы)
    if STRICT_BELARUS_ONLY and is_wrong_region(text):
        print(f"[REGION SKIP] {title[:60]}")
        return None

    # Инициализируем счётчики для каждой темы
    scores = {tid: 0 for tid in TOPICS}

    # 1. Entity matching (вес +3)
    for entity, topic in ENTITY_TOPIC.items():
        if entity in text:
            scores[topic] += 3
            print(f"[ENTITY] {entity} -> {topic} +3")

    # 2. Keyword matching в заголовке (вес +2) и в теле (вес +1)
    # Используем границы слов для точного совпадения
    def word_in_text(word, target_text):
        return re.search(rf'\b{re.escape(word)}\b', target_text) is not None

    for tid, topic_data in TOPICS.items():
        # Пропускаем, если есть exclude слова
        if any(ex in text for ex in topic_data.get("exclude", [])):
            continue

        keywords = topic_data["keywords"]
        for kw in keywords:
            kw_lower = kw.lower()
            # Сначала проверяем в заголовке (целое слово)
            if word_in_text(kw_lower, title_lower):
                scores[tid] += 2
                print(f"[KEYWORD] '{kw}' in title of {tid} +2")
            # Затем в теле
            elif word_in_text(kw_lower, text):
                scores[tid] += 1
                print(f"[KEYWORD] '{kw}' in body of {tid} +1")

    # 3. Применяем негативные штрафы (например, для политики)
    for tid, score in scores.items():
        if tid == "politics":
            for neg in POLITICS_NEGATIVE:
                if neg in text:
                    scores[tid] -= 2
                    print(f"[NEGATIVE] '{neg}' in politics -2")

    # 4. Отбираем кандидатов с минимальным порогом (например, 2)
    candidates = [(tid, score) for tid, score in scores.items() if score >= 2]
    if not candidates:
        print(f"[NO CANDIDATE] {title[:60]}")
        return None

    # Сортируем по убыванию score
    candidates.sort(key=lambda x: x[1], reverse=True)
    top_score = candidates[0][1]

    # 5. Если уверенность высокая (>= HIGH_CONFIDENCE_THRESHOLD), возвращаем топ-тему без LLM
    if top_score >= HIGH_CONFIDENCE_THRESHOLD:
        best_topic = candidates[0][0]
        print(f"[HIGH CONF] {best_topic} with score {top_score}: {title[:60]}")
        return best_topic

    # 6. Иначе используем LLM для уточнения среди топ-2 кандидатов
    for tid, _ in candidates[:2]:
        topic_name = TOPICS[tid].get("name") or TOPICS[tid]["title"]
        try:
            if validate_topic(title, content[:1200], topic_name):
                print(f"[LLM OK] {topic_name}: {title[:60]}")
                return tid
        except Exception as e:
            print("[LLM validate error]", e)
            continue

    # 7. Если LLM не подтвердил, возвращаем кандидата с максимальным score
    fallback = candidates[0][0]
    print(f"[FALLBACK] {TOPICS[fallback]['title']}: {title[:60]}")
    return fallback

def extract_news_content(url: str) -> str:
    """Извлекает полный текст новости (до 4000 символов)"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        }
        resp = requests.get(url, timeout=REQUEST_TIMEOUT, headers=headers)
        resp.raise_for_status()
        if resp.encoding is None:
            resp.encoding = 'utf-8'
        soup = BeautifulSoup(resp.content, 'html.parser', from_encoding=resp.encoding)

        # Удаляем ненужные элементы
        for tag in soup(['script', 'style', 'nav', 'footer', 'aside', 'header', 'iframe', 'form', 'noscript']):
            tag.decompose()

        # Поиск основного контента
        article = (
            soup.find('article') or
            soup.find('div', class_=re.compile(r'article|content|post|news_text|text|main', re.I)) or
            soup.find('div', {'itemprop': 'articleBody'}) or
            soup.find('div', id=re.compile(r'content|article|text|main', re.I)) or
            soup.find('main')
        )

        if article:
            text = article.get_text(separator=' ', strip=True)
        else:
            body = soup.find('body')
            text = body.get_text(separator=' ', strip=True) if body else soup.get_text(separator=' ', strip=True)

        # Очистка от мусорных блоков
        text = re.sub(r'\s+', ' ', text)
        patterns = [
            r'Читайте также.*',
            r'Подписывайтесь.*',
            r'Источник.*',
            r'Если вы заметили ошибку.*',
            r'Поделиться.*',
            r'Комментарии.*',
            r'Другие новости.*',
            r'Реклама.*',
        ]
        for pat in patterns:
            text = re.sub(pat, '', text, flags=re.IGNORECASE)

        return text[:4000].strip()
    except Exception as e:
        print(f"[ERROR] extract_news_content {url}: {e}")
        return ""

# =========================
# ИЗМЕНЁННАЯ функция check_for_duplicates: теперь использует нормализованные заголовки и difflib, topic может быть None
# =========================
def check_for_duplicates(title: str, topic: str | None, link: str, c) -> bool:
    """
    Проверка на дубликаты:
    1) Похожие заголовки за последние 24 часа (с порогом SIMILARITY_THRESHOLD)
    2) Точное совпадение ссылки (после очистки от параметров)
    Если topic == None, ищем по всем темам.
    """
    # Очищаем ссылку от UTM-меток и якорей
    clean_link = link.split('?')[0].split('#')[0]

    # Проверка на точное совпадение ссылки
    exact = c.execute("""
        SELECT id FROM news 
        WHERE link LIKE ? AND published >= datetime('now', '-24 hours')
    """, (clean_link + '%',)).fetchone()
    if exact:
        return True

    # Получаем все заголовки за последние 24 часа
    if topic:
        rows = c.execute("""
            SELECT title, normalized_title FROM news 
            WHERE topic = ? AND published >= datetime('now', '-24 hours')
        """, (topic,)).fetchall()
    else:
        rows = c.execute("""
            SELECT title, normalized_title FROM news 
            WHERE published >= datetime('now', '-24 hours')
        """).fetchall()

    norm_title = normalize_title(title)
    for row in rows:
        existing_norm = row['normalized_title'] or normalize_title(row['title'])
        if is_similar_news(norm_title, existing_norm):
            return True
    return False

def parse_feed(url, source="rss"):
    try:
        print(f"[RSS] Загрузка: {url}")
        if "rsshub.app/telegram" in url:
            print("[RSS] Telegram cooldown...")
            time.sleep(3)
        headers = {
            "User-Agent":
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8"
        }
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        results = []
        for entry in feed.entries[:15]:
            res = process_news_entry(entry, source)
            if res:
                results.append(res)
        return results
    except Exception as e:
        print(f"[ERROR] parse_feed {url}: {e}")
        return []

def process_news_entry(entry, source="rss"):
    try:
        title = entry.get('title', '').strip()
        link = entry.get('link', '').split('?')[0]

        if not title or not link:
            return None

        if is_blacklisted(link, title):
            print(f"[SKIP] blacklist: {title[:60]}")
            return None

        # -----------------------------
        # ДАТА ПУБЛИКАЦИИ (UTC)
        # -----------------------------
        published = None
        if hasattr(entry, 'published_parsed') and entry.published_parsed:
            published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
            published = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
        else:
            print(f"[SKIP] no date: {title[:60]}")
            return None

        # Фильтр по возрасту
        if published < datetime.now(timezone.utc) - timedelta(hours=MAX_NEWS_AGE_HOURS):
            print(f"[SKIP OLD] {title[:60]}")
            return None

        fp = fingerprint(title, link)

        # =====================================================
        # БЫСТРАЯ ПРОВЕРКА ПО FINGERPRINT
        # =====================================================
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

        # =====================================================
        # ПРОВЕРКА НА ПОХОЖИЕ ЗАГОЛОВКИ (ПЕРЕНЕСЕНА ДО ИЗВЛЕЧЕНИЯ КОНТЕНТА)
        # =====================================================
        with db_lock:
            conn = get_connection()
            c = conn.cursor()
            dup = check_for_duplicates(title, None, link, c)  # пока тема неизвестна, ищем по всем
            conn.close()

        if dup:
            print("[SKIP] similar title found")
            return None

        # =====================================================
        # ТЯЖЁЛАЯ РАБОТА БЕЗ БЛОКИРОВКИ
        # =====================================================
        content = extract_news_content(link)

        topic = detect_topic(title, content[:500] if content else title)

        if not topic:
            print("[SKIP] topic none")
            return None

        # LLM (summary & importance)
        important = 0
        summary = title

        if source != "google":  # для Google News не тратим ресурсы на анализ
            try:
                analysis = analyze_news(title, content)
                summary = analysis.get("summary", "") or title
                important = analysis.get("important", 0)
            except Exception as e:
                print("[LLM FAIL]", e)

        parsed = urlparse(link)
        real_source = parsed.netloc.lower()
        norm_title = normalize_title(title)  # для сохранения в БД

        # =====================================================
        # ВСТАВКА (LOCK)
        # =====================================================
        with db_lock:
            conn = get_connection()
            c = conn.cursor()

            # Повторная проверка на дубликаты на случай гонки (двойная проверка)
            dup2 = check_for_duplicates(title, topic, link, c)
            if dup2:
                conn.close()
                return None

            c.execute("""
                INSERT INTO news
                (title, summary, full_text, link,
                 topic, published, fingerprint,
                 important, source, fetched_at, real_source, normalized_title)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, ?)
            """, (
                title[:500],
                summary[:1000],
                content[:4000],
                link,
                topic,
                published,
                fp,
                important,
                source,
                real_source,
                norm_title[:200]
            ))

            news_id = c.lastrowid
            conn.commit()
            conn.close()

        print(f"[OK] {topic}: {title[:70]}")

        return {
            "id": news_id,
            "topic": topic,
            "important": important
        }

    except Exception as e:
        print(f"[ERROR] process_news_entry: {e}")
        return None

def collect_and_save_news():
    print(f"[RSS] Начало сбора новостей...")
    all_results = []
    newsletter_id = create_newsletter()

    # 1. Базовые RSS
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

    # 2. Google News
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

    # Статистика для newsletter_stats
    stats = {}
    for r in all_results:
        t = r["topic"]
        if t not in stats:
            stats[t] = {"total": 0, "important": 0}
        stats[t]["total"] += 1
        if r["important"]:
            stats[t]["important"] += 1

    # Ограничение кол-ва новостей в теме
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