import requests
import json
import re
from config import OLLAMA_MODEL, OLLAMA_URL

MAX_CONTENT_LENGTH = 4000

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
            timeout=300
        )
        ans = r.json()["response"].lower().strip()
        print(f"[LLM validate] {topic_name} -> {ans}")
        return "yes" in ans
    except Exception as e:
        print(f"[LLM validate error] {e}")
        return True

def is_about_belarus(title: str, text: str) -> bool:

    prompt = f"""
Определи, связана ли новость с Беларусью.

YES если:
- событие происходит в Беларуси
- Беларусь является участником события
- новость касается Беларуси

NO если:
- новость полностью про другую страну
- Беларусь упоминается случайно
- это глобальная новость без связи с Беларусью

Ответь строго одним словом:
YES или NO.

Заголовок:
{title}

Текст:
{text[:1200]}
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
            timeout=300
        )

        if r.status_code != 200:
            return False

        answer = r.json().get("response", "").strip().lower()

        if answer.startswith("yes") or answer.startswith("да"):
            return True

        if answer.startswith("no") or answer.startswith("нет"):
            return False

        if "yes" in answer or "да" in answer:
            return True

        return False

    except Exception as e:

        print("[LLM Belarus filter error]", e)

        return False

def classify_topic_llm(title: str, text: str, topics: dict):

    topic_list = "\n".join(
        [f"{k} — {v['title']}" for k, v in topics.items() if k != "other"]
    )

    prompt = f"""
    Определи тему новости.

    Если новость не относится ни к одной теме — ответь other.

    Никогда не угадывай тему.

    Темы:

    {topic_list}

    Новость:
    {title}

    {text[:1200]}

    Ответь только ID темы или other.
    """

    try:

        r = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0
                }
            },
            timeout=300
        )

        if r.status_code != 200:
            return "other"

        answer = r.json().get("response", "").strip().lower()

        for topic in topics.keys():

            if topic in answer:
                return topic

        return "other"

    except Exception as e:

        print("[LLM topic error]", e)

        return "other"

def analyze_news(title: str, content: str = "") -> dict:
    context = content[:MAX_CONTENT_LENGTH] if content else title

    prompt = f"""
Ты — профессиональный новостной аналитик. Твоя задача — сделать подробный, информативный пересказ новости и оценить её важность.

Заголовок: {title}
Контекст: {context}

Требования к пересказу:
- Перескажи новость на русском языке, минимум 3-4 предложения.
- ОБЯЗАТЕЛЬНО укажи все ключевые детали: цифры, суммы, даты, имена, названия организаций, если они есть в контексте.
- Если в контексте нет конкретных цифр или имён, просто опиши суть события максимально подробно.
- НЕ используй фразы "конкретные цифры не указаны", "по информации источника" — если информация есть, её нужно привести.
- Пересказ должен быть новым текстом, а не повторением заголовка.

Критерии важности (important = 1):
- Указы президента, решения правительства, новые законы.
- Крупные экономические/политические события, кризисы, катастрофы.
- Значимые международные соглашения, санкции, изменения в законодательстве.
- События, которые могут повлиять на жизнь многих людей.
В остальных случаях important = 0.

Ответ должен быть строго в формате JSON:
{{
  "summary": "подробный пересказ с фактами (минимум 3 предложения)",
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
                    "temperature": 0.2,
                    "top_p": 0.9,
                    "top_k": 20,
                    "repeat_penalty": 1.1,
                    "num_predict": 2000
                }
            },
            timeout=300
        )
        r.raise_for_status()
        raw = r.json().get("response", "")
        print(f"[LLM analyze raw] {raw[:200]}...")
        raw = raw.replace("```json", "").replace("```", "").strip()
        data = _extract_json(raw)

        if not data:
            print("[LLM analyze] no JSON, forcing facts")
            return _force_facts(title, context)

        summary = str(data.get("summary", "")).strip()
        important = 1 if str(data.get("important", 0)) in ("1", "true", "True") else 0

        if len(summary) < 100 or summary.lower() == title.lower()[:100]:
            print("[LLM analyze] summary too short or same as title, forcing facts")
            return _force_facts(title, context)

        return {"summary": summary, "important": important}
    except Exception as e:
        print(f"[LLM] Ошибка: {e}")
        return {"summary": title[:300], "important": 0}

def _force_facts(title, context):
    prompt = f"""
Новость:
Заголовок: {title}
Контекст: {context[:2000]}

Напиши подробный пересказ (минимум 4-5 предложений) и ОБЯЗАТЕЛЬНО укажи все ЧИСЛА, ДАТЫ, СУММЫ, ИМЕНА, которые есть в тексте.
Если цифр нет — просто опиши суть как можно подробнее.
Ответ должен быть ТОЛЬКО JSON: {{"summary": "пересказ", "important": 0/1}}
"""
    try:
        r = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.1, "num_predict": 2000}
            },
            timeout=300
        )
        raw = r.json().get("response", "")
        print(f"[LLM force raw] {raw[:200]}...")
        data = _extract_json(raw)
        if data:
            summary = str(data.get("summary", ""))[:1500]
            if len(summary) < 50:
                summary = title[:300]
            important = 1 if str(data.get("important", 0)) in ("1", "true") else 0
            return {"summary": summary, "important": important}
    except Exception as e:
        print(f"[LLM force error] {e}")
    return {"summary": title[:300], "important": 0}

def answer_question(context: str, question: str) -> str:
    if not context or len(context.strip()) < 50:
        return "Извините, у меня недостаточно информации по этой новости, чтобы ответить на вопрос."

    prompt = f"""
Ты — аналитический помощник. Ответь на вопрос, используя информацию из новости (контекст), а также свои общие знания, если в контексте нет прямого ответа.

Контекст (текст новости):
{context[:4000]}

Вопрос: {question}

Требования:
- Если в контексте есть факты, обязательно опирайся на них.
- Если прямого ответа нет, ты можешь сделать логические выводы на основе контекста и своих знаний, но обязательно укажи, что это предположение или анализ.
- Ответ должен быть на русском языке, чётким и по существу.
- Если ответ невозможен даже с использованием знаний, скажи об этом.

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
                    "temperature": 0.3,
                    "top_p": 0.9,
                    "top_k": 20,
                    "repeat_penalty": 1.1,
                    "num_predict": 1000
                }
            },
            timeout=300
        )
        answer = r.json().get("response", "").strip()
        print(f"[LLM answer] {answer[:200]}...")
        return answer if answer else "Не удалось получить ответ."
    except Exception as e:
        print(f"[LLM] Ошибка ответа на вопрос: {e}")
        return "Ошибка при обработке вопроса. Попробуйте позже."
