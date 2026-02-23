import requests
import json
import re
from config import OLLAMA_MODEL, OLLAMA_URL

MAX_CONTENT_LENGTH = 5000

def _extract_json(text: str) -> dict | None:
    match = re.search(r"\{.*\}", text, re.S)
    return json.loads(match.group()) if match else None

def validate_topic(title, content, topic_name):
    prompt = f"""
Определи, относится ли новость к теме "{topic_name}".
Ответь только одним словом: YES или NO.

Заголовок: {title}
Текст: {content[:1500]}
"""
    try:
        r = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0,
                    "num_predict": 5
                }
            },
            timeout=40
        )
        ans = r.json()["response"].lower().strip()
        print(f"[LLM validate] {topic_name} -> {ans}")
        return "yes" in ans
    except Exception as e:
        print(f"[LLM validate error] {e}")
        return True

def analyze_news(title: str, content: str = "") -> dict:
    content_preview = content[:MAX_CONTENT_LENGTH] if content else ""

    prompt = f"""
Ты — опытный новостной аналитик. Твоя задача — сделать краткий, но ИНФОРМАТИВНЫЙ пересказ новости и оценить её важность.

ЗАГОЛОВОК:
{title}

СОДЕРЖАНИЕ:
{content_preview}

ТРЕБОВАНИЯ К ПЕРЕСКАЗУ:
- Перескажи новость на русском языке, **минимум 3 предложения**.
- ОБЯЗАТЕЛЬНО укажи все ключевые детали: цифры, суммы, даты, имена, названия организаций, если они есть в тексте.
- НЕ используй фразы «конкретные цифры не указаны», «по информации источника» и т.п. — если цифры есть, их нужно назвать.
- Если в тексте нет цифр, просто опиши суть.
- **Пересказ не должен быть простым повторением заголовка** — это должно быть новое, развёрнутое изложение.

КРИТЕРИИ ВАЖНОСТИ (important = 1):
- Указы президента, решения правительства, новые законы.
- Крупные экономические/политические кризисы, катастрофы, теракты.
- Значимые международные соглашения, санкции.
В остальных случаях important = 0.

ОТВЕТ ДОЛЖЕН БЫТЬ СТРОГО В JSON:
{{
  "summary": "пересказ с фактами (минимум 3 предложения)",
  "important": 0 или 1
}}
"""
    try:
        r = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.15,
                    "top_p": 0.9,
                    "top_k": 20,
                    "repeat_penalty": 1.15,
                    "num_predict": 1500
                }
            },
            timeout=120
        )
        r.raise_for_status()
        raw = r.json().get("response", "")
        print(f"[LLM analyze raw] {raw[:200]}...")  # лог первых 200 символов
        raw = raw.replace("```json", "").replace("```", "").strip()
        data = _extract_json(raw)

        if not data:
            print("[LLM analyze] no JSON, forcing facts")
            return _force_facts(title, content_preview)

        summary = str(data.get("summary", "")).strip()
        important = 1 if str(data.get("important", 0)) in ("1", "true", "True") else 0

        if len(summary) < 100 or summary.lower() == title.lower()[:100]:
            print("[LLM analyze] summary too short or same as title, forcing facts")
            return _force_facts(title, content_preview)

        return {"summary": summary, "important": important}
    except Exception as e:
        print(f"[LLM] Ошибка: {e}")
        return {"summary": title[:300], "important": 0}

def _force_facts(title, content):
    prompt = f"""
Новость:
{title}
{content[:2000]}

Напиши подробный пересказ (минимум 3-4 предложения) и ОБЯЗАТЕЛЬНО укажи все ЧИСЛА, ДАТЫ, СУММЫ, которые есть в тексте.
Если цифр нет — просто опиши суть.
Ответ должен быть ТОЛЬКО JSON: {{"summary": "пересказ", "important": 0/1}}
"""
    try:
        r = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.1, "num_predict": 1200}
            },
            timeout=60
        )
        raw = r.json().get("response", "")
        print(f"[LLM force raw] {raw[:200]}...")
        data = _extract_json(raw)
        if data:
            summary = str(data.get("summary", ""))[:1000]
            if len(summary) < 50:
                summary = title[:300]
            important = 1 if str(data.get("important", 0)) in ("1", "true") else 0
            return {"summary": summary, "important": important}
    except Exception as e:
        print(f"[LLM force error] {e}")
    return {"summary": title[:300], "important": 0}

def answer_question(context: str, question: str) -> str:
    if not context or len(context.strip()) < 50:
        return "Недостаточно контекста для ответа. Попробуйте задать вопрос по другой новости."

    prompt = f"""
Ты — аналитик, отвечающий на вопросы по тексту новости.
Контекст:
{context[:4000]}

Вопрос: {question}

Требования:
- Отвечай ТОЛЬКО на русском.
- Если в контексте есть цифры/даты/имена — обязательно используй их.
- Ответ должен быть точным, по существу, 3-5 предложений.
- Если ответа нет в контексте, так и скажи.
- Не придумывай факты, которых нет в контексте.

Ответ:
"""
    try:
        r = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.15,
                    "top_p": 0.9,
                    "top_k": 20,
                    "repeat_penalty": 1.15,
                    "num_predict": 900
                }
            },
            timeout=120
        )
        answer = r.json().get("response", "").strip()
        print(f"[LLM answer] {answer[:200]}...")
        return answer if answer else "Не удалось получить ответ."
    except Exception as e:
        print(f"[LLM] Ошибка ответа на вопрос: {e}")
        return "Ошибка при обработке вопроса."