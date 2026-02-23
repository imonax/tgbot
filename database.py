import sqlite3
from config import TOPICS
from datetime import datetime, timedelta

DB_FILE = "botdb_new.db"

def get_connection():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_db():
    conn = get_connection()
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        last_message_id INTEGER,
        registered_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Добавлено поле normalized_title
    c.execute("""
    CREATE TABLE IF NOT EXISTS news (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        summary TEXT,
        full_text TEXT,
        link TEXT,
        topic TEXT,
        published DATETIME,
        fingerprint TEXT UNIQUE,
        important INTEGER DEFAULT 0,
        source TEXT,
        real_source TEXT,
        fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        normalized_title TEXT
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS questions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        news_id INTEGER,
        question TEXT,
        answer TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(user_id),
        FOREIGN KEY(news_id) REFERENCES news(id)
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS newsletters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        newsletter_date DATE UNIQUE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS newsletter_stats (
        newsletter_id INTEGER,
        topic TEXT,
        total_news INTEGER,
        important_news INTEGER,
        FOREIGN KEY(newsletter_id) REFERENCES newsletters(id)
    )
    """)

    c.execute("CREATE INDEX IF NOT EXISTS idx_news_topic_published ON news(topic, published)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_news_fetched ON news(fetched_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_news_normalized_title ON news(normalized_title)")

    # Миграция для добавления normalized_title, если её нет
    try:
        c.execute("ALTER TABLE news ADD COLUMN normalized_title TEXT")
    except sqlite3.OperationalError:
        pass

    conn.commit()
    conn.close()

# Остальные функции без изменений
def get_current_newsletter():
    conn = get_connection()
    c = conn.cursor()
    today = datetime.now().date()
    result = c.execute("""
        SELECT id FROM newsletters 
        WHERE newsletter_date = date(?)
    """, (today,)).fetchone()
    conn.close()
    return result["id"] if result else None

def create_newsletter():
    conn = get_connection()
    c = conn.cursor()
    today = datetime.now().date()
    c.execute("""
        INSERT OR IGNORE INTO newsletters (newsletter_date)
        VALUES (date(?))
    """, (today,))
    row = c.execute("""
        SELECT id FROM newsletters
        WHERE newsletter_date = date(?)
    """, (today,)).fetchone()
    conn.commit()
    conn.close()
    return row["id"]

def update_newsletter_stats(newsletter_id, stats):
    conn = get_connection()
    c = conn.cursor()
    for topic, counts in stats.items():
        c.execute("""
            INSERT OR REPLACE INTO newsletter_stats 
            (newsletter_id, topic, total_news, important_news)
            VALUES (?, ?, ?, ?)
        """, (newsletter_id, topic, counts['total'], counts['important']))
    conn.commit()
    conn.close()

def get_newsletter_stats(newsletter_id):
    conn = get_connection()
    c = conn.cursor()
    stats = {}
    rows = c.execute("""
        SELECT topic, total_news, important_news 
        FROM newsletter_stats 
        WHERE newsletter_id = ?
    """, (newsletter_id,)).fetchall()
    for row in rows:
        stats[row['topic']] = {
            'total': row['total_news'],
            'important': row['important_news']
        }
    conn.close()
    return stats

def get_topic_stats_last_hours(hours=12):
    conn = get_connection()
    c = conn.cursor()
    stats = {}
    for topic in TOPICS.keys():
        total = c.execute("""
            SELECT COUNT(*) FROM news 
            WHERE topic = ? AND published >= datetime('now', ? || ' hours')
        """, (topic, f'-{hours}')).fetchone()[0]
        important = c.execute("""
            SELECT COUNT(*) FROM news 
            WHERE topic = ? AND important = 1 AND published >= datetime('now', ? || ' hours')
        """, (topic, f'-{hours}')).fetchone()[0]
        stats[topic] = {'total': total, 'important': important}
    conn.close()
    return stats