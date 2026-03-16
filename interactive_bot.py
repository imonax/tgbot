import asyncio
from datetime import datetime, timedelta
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    ContextTypes, MessageHandler, filters
)
from database import (
    init_db, get_connection, get_current_newsletter,
    get_newsletter_stats, create_newsletter
)
from rss_parser import collect_and_save_news
from config import *
from llm import answer_question, analyze_news
from html import escape

init_db()

user_states = {}

# <-- NEW: множество для отслеживания новостей, для которых уже запущена генерация пересказа
generating_summaries = set()

def get_actual_stats():
    conn = get_connection()
    c = conn.cursor()
    rows = c.execute("""
        SELECT topic, COUNT(*) as total, SUM(important) as important
        FROM news
        WHERE published >= datetime('now', '-12 hours')
        GROUP BY topic
    """).fetchall()
    conn.close()
    stats = {}
    for row in rows:
        stats[row['topic']] = {
            'total': row['total'],
            'important': row['important'] if row['important'] else 0
        }
    return stats

def topic_menu():
    kb = []
    stats = get_actual_stats()
    for topic_id, topic_data in TOPICS.items():
        if topic_id == "other":    # <-- пропускаем тему "Другое"
            continue
        label = topic_data["title"]
        if topic_id in stats:
            total = stats[topic_id]['total']
            important = stats[topic_id]['important']
            if total:
                label += f" (+{total})"
            if important:
                label += f" ({important}‼️)"
        kb.append([InlineKeyboardButton(label, callback_data=f"topic:{topic_id}")])
    kb.append([InlineKeyboardButton("🔄 Обновить новости", callback_data="refresh_news")])
    return InlineKeyboardMarkup(kb)

# <-- MODIFIED: добавлен параметр show_generate для отображения кнопки генерации
def news_menu(topic, news_id=None, show_generate=False):
    buttons = []
    if show_generate and news_id:
        buttons.append([InlineKeyboardButton("✨ Сгенерировать пересказ", callback_data=f"gen_summary:{topic}:{news_id}")])
    buttons.extend([
        [InlineKeyboardButton("🆕 Новые", callback_data=f"new:{topic}"),
         InlineKeyboardButton("📚 Архив", callback_data=f"arch:{topic}")],
    ])
    if news_id:
        buttons.append([
            InlineKeyboardButton("❓ Задать вопрос", callback_data=f"ask_specific:{topic}:{news_id}"),
            InlineKeyboardButton("⬅ Назад к списку", callback_data=f"list:{topic}:0")
        ])
    else:
        buttons.append([InlineKeyboardButton("⬅ Назад", callback_data="back")])
    return InlineKeyboardMarkup(buttons)

def news_list_keyboard(news_list, topic, start_idx=0, page_size=5, show_back=True):
    kb = []
    for i, news in enumerate(news_list[start_idx:start_idx + page_size], start=start_idx + 1):
        btn_text = f"{i}. {news['title'][:30]}..."
        if news.get('important'):
            btn_text = "❗" + btn_text
        kb.append([InlineKeyboardButton(btn_text, callback_data=f"news:{topic}:{news['id']}")])
    nav_buttons = []
    if start_idx > 0:
        nav_buttons.append(
            InlineKeyboardButton("⬅ Назад", callback_data=f"page:{topic}:{max(0, start_idx - page_size)}"))
    if start_idx + page_size < len(news_list):
        nav_buttons.append(InlineKeyboardButton("Вперёд ➡", callback_data=f"page:{topic}:{start_idx + page_size}"))
    if nav_buttons:
        kb.append(nav_buttons)
    if show_back:
        kb.append([InlineKeyboardButton("⬅ Назад к теме", callback_data=f"topic:{topic}")])
    return InlineKeyboardMarkup(kb)

def question_menu(topic, news_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❓ Ещё вопрос", callback_data=f"ask_specific:{topic}:{news_id}")],
        [InlineKeyboardButton("📖 К новости", callback_data=f"news:{topic}:{news_id}")],
        [InlineKeyboardButton("📋 К списку новостей", callback_data=f"list:{topic}:0")]
    ])

async def render(app, user_id, text, kb, mode=None, edit_message=True, message_id=None):
    conn = get_connection()
    c = conn.cursor()
    target_id = None
    if edit_message:
        if message_id is not None:
            target_id = message_id
        else:
            if user_id in user_states and 'last_message_id' in user_states[user_id]:
                target_id = user_states[user_id]['last_message_id']
            else:
                row = c.execute("SELECT last_message_id FROM users WHERE user_id = ?", (user_id,)).fetchone()
                if row:
                    target_id = row["last_message_id"]
        if target_id:
            try:
                await app.bot.edit_message_text(
                    chat_id=user_id,
                    message_id=target_id,
                    text=text,
                    reply_markup=kb,
                    parse_mode=mode,
                    disable_web_page_preview=False
                )
                if user_id not in user_states:
                    user_states[user_id] = {}
                user_states[user_id]['last_message_id'] = target_id
                conn.close()
                return
            except Exception as e:
                print(f"[RENDER EDIT ERROR] {e} (user={user_id}, msg={target_id})")
    msg = await app.bot.send_message(
        user_id,
        text,
        reply_markup=kb,
        parse_mode=mode,
        disable_web_page_preview=False
    )
    c.execute("UPDATE users SET last_message_id = ? WHERE user_id = ?", (msg.message_id, user_id))
    conn.commit()
    if user_id not in user_states:
        user_states[user_id] = {}
    user_states[user_id]['last_message_id'] = msg.message_id
    conn.close()

async def display_news(app, user_id, topic, news_id, message_id):
    conn = get_connection()
    c = conn.cursor()
    row = c.execute("""
        SELECT title, summary, link, important, full_text, rss_summary
        FROM news 
        WHERE id = ?
    """, (news_id,)).fetchone()
    conn.close()
    if not row:
        await app.bot.send_message(user_id, "Новость не найдена")
        return

    important_mark = "❗ " if row['important'] else ""
    safe_title = escape(row['title'])

    # Определяем, есть ли уже сгенерированный пересказ
    if row['summary']:
        summary_text = escape(row['summary'])
        show_generate = False
    else:
        # Проверяем, не генерируется ли эта новость прямо сейчас для этого пользователя
        if user_id in user_states and user_states[user_id].get('generating_news_id') == news_id:
            summary_text = "⏳ <i>Генерирую краткое содержание... (задача выполняется)</i>"
            show_generate = False
        else:
            # Если есть RSS-саммари – показываем его как запасной вариант
            if row['rss_summary']:
                summary_text = escape(row['rss_summary']) + "\n\n<i>(предварительный пересказ из RSS)</i>"
            else:
                summary_text = "<i>Пересказ не сгенерирован. Нажмите кнопку ниже, чтобы сгенерировать.</i>"
            show_generate = True

    text = f"""{important_mark}<b>{safe_title}</b>

{summary_text}

<a href="{row['link']}">📖 Читать полностью →</a>"""

    await render(
        app,
        user_id,
        text,
        news_menu(topic, news_id, show_generate=show_generate),
        "HTML",
        edit_message=True,
        message_id=message_id
    )

# <-- NEW: фоновая задача для генерации пересказа
async def generate_summary_and_update(app, user_id, topic, news_id, message_id):
    try:
        # Получаем данные новости
        conn = get_connection()
        c = conn.cursor()
        row = c.execute("SELECT title, full_text, link FROM news WHERE id = ?", (news_id,)).fetchone()
        conn.close()
        if not row:
            return

        full_text = row['full_text']
        if not full_text or len(full_text) < 200:
            from rss_parser import extract_news_content
            print(f"[RETRY] Повторное извлечение текста для генерации пересказа (news_id={news_id})")
            new_text = extract_news_content(row['link'])
            if new_text and len(new_text) > 200:
                full_text = new_text
                conn = get_connection()
                c = conn.cursor()
                c.execute("UPDATE news SET full_text=? WHERE id=?", (full_text[:4000], news_id))
                conn.commit()
                conn.close()

        loop = asyncio.get_running_loop()
        analysis = await loop.run_in_executor(None, analyze_news, row['title'], full_text)

        conn = get_connection()
        c = conn.cursor()
        c.execute("UPDATE news SET summary=?, important=? WHERE id=?",
                  (analysis['summary'][:1000], analysis['important'], news_id))
        conn.commit()
        conn.close()

        # Обновляем сообщение с новостью
        await display_news(app, user_id, topic, news_id, message_id)

    except Exception as e:
        print(f"[ERROR] generate_summary_and_update: {e}")
        # В случае ошибки можно показать сообщение об ошибке, но пока просто очищаем состояние
    finally:
        generating_summaries.discard(news_id)
        if user_id in user_states:
            if user_states[user_id].get('generating_news_id') == news_id:
                user_states[user_id]['generating_news_id'] = None

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    conn = get_connection()
    conn.execute("""
        INSERT OR REPLACE INTO users (user_id, username, first_name, last_name) 
        VALUES (?, ?, ?, ?)
    """, (u.id, u.username, u.first_name, u.last_name))
    conn.commit()
    conn.close()
    await render(
        ctx.application,
        u.id,
        "📰 Последние новости. Выберите тему:",
        topic_menu(),
        edit_message=False
    )

# <-- MODIFIED: используем display_news вместо старого show_news
async def buttons(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id

    # Определяем текущий message_id
    if user_id in user_states and 'last_message_id' in user_states[user_id]:
        current_message_id = user_states[user_id]['last_message_id']
    else:
        current_message_id = query.message.message_id

    conn = get_connection()
    c = conn.cursor()

    if data == "refresh_news":
        await query.edit_message_text("⏳ Обновляю новости...")
        loop = asyncio.get_running_loop()
        stats = await loop.run_in_executor(None, collect_and_save_news)
        if stats:
            await render(
                ctx.application,
                user_id,
                "✅ Новости обновлены. Выберите тему:",
                topic_menu(),
                edit_message=True,
                message_id=current_message_id
            )
        else:
            await render(
                ctx.application,
                user_id,
                "⚠️ Не удалось обновить новости",
                topic_menu(),
                edit_message=True,
                message_id=current_message_id
            )
        conn.close()
        return

    if user_id in user_states and user_states[user_id].get("state") != "asking_question":
        del user_states[user_id]

    if data == "back":
        await render(
            ctx.application,
            user_id,
            "📰 Выберите тему:",
            topic_menu(),
            edit_message=True,
            message_id=current_message_id
        )
        conn.close()
        return

    if data.startswith("page:"):
        _, topic, start_idx = data.split(":")
        start_idx = int(start_idx)
        rows = c.execute("""
            SELECT id, title, summary, link, important
            FROM news 
            WHERE topic = ? AND published >= datetime('now', '-12 hours')
            ORDER BY published DESC
        """, (topic,)).fetchall()
        if rows:
            news_list = [dict(row) for row in rows]
            text = f"📰 {TOPICS[topic]['title']}\n\nВыберите новость:"
            await render(
                ctx.application,
                user_id,
                text,
                news_list_keyboard(news_list, topic, start_idx),
                edit_message=True,
                message_id=current_message_id
            )
        else:
            await render(
                ctx.application,
                user_id,
                "📰 Новостей нет",
                news_menu(topic),
                edit_message=True,
                message_id=current_message_id
            )
        conn.close()
        return

    if data.startswith("list:"):
        _, topic, start_idx = data.split(":")
        start_idx = int(start_idx)
        rows = c.execute("""
            SELECT id, title, summary, link, important
            FROM news 
            WHERE topic = ? AND published >= datetime('now', '-12 hours')
            ORDER BY published DESC
        """, (topic,)).fetchall()
        if rows:
            news_list = [dict(row) for row in rows]
            text = f"📰 {TOPICS[topic]['title']}\n\nВыберите новость:"
            await render(
                ctx.application,
                user_id,
                text,
                news_list_keyboard(news_list, topic, start_idx),
                edit_message=True,
                message_id=current_message_id
            )
        else:
            await render(
                ctx.application,
                user_id,
                "📰 Новостей нет",
                news_menu(topic),
                edit_message=True,
                message_id=current_message_id
            )
        conn.close()
        return

    if data.startswith("news:"):
        _, topic, news_id = data.split(":")
        news_id = int(news_id)
        # <-- MODIFIED: вызываем display_news вместо старого show_news
        await display_news(ctx.application, user_id, topic, news_id, current_message_id)
        conn.close()
        return

    if data.startswith("gen_summary:"):
        _, topic, news_id = data.split(":")
        news_id = int(news_id)

        # Проверяем, не занят ли пользователь другой генерацией
        if user_id in user_states and user_states[user_id].get('generating_news_id') is not None:
            current_gen = user_states[user_id]['generating_news_id']
            if current_gen == news_id:
                await query.answer("Эта новость уже генерируется", show_alert=True)
            else:
                await query.answer("Сначала дождитесь завершения генерации для другой новости", show_alert=True)
            conn.close()
            return

        # Проверяем глобальное множество (на случай параллельных запросов)
        if news_id in generating_summaries:
            await query.answer("Генерация уже запущена (возможно, другим пользователем?)", show_alert=True)
            conn.close()
            return

        # Помечаем начало генерации
        generating_summaries.add(news_id)
        if user_id not in user_states:
            user_states[user_id] = {}
        user_states[user_id]['generating_news_id'] = news_id
        await query.answer("Начинаю генерацию пересказа...", show_alert=False)

        # Получаем данные для отображения прогресса
        conn2 = get_connection()
        c2 = conn2.cursor()
        row = c2.execute("SELECT title, link, important FROM news WHERE id = ?", (news_id,)).fetchone()
        conn2.close()
        if row:
            important_mark = "❗ " if row['important'] else ""
            safe_title = escape(row['title'])
            progress_text = f"""{important_mark}<b>{safe_title}</b>

    ⏳ <i>Генерирую краткое содержание...</i>

    <a href="{row['link']}">📖 Читать полностью →</a>"""
            await render(
                ctx.application,
                user_id,
                progress_text,
                news_menu(topic, news_id, show_generate=False),  # без кнопки генерации
                "HTML",
                edit_message=True,
                message_id=current_message_id
            )
        else:
            # На всякий случай, если запрос не дал результата – продолжаем без редактирования
            pass

        # Запускаем фоновую задачу
        asyncio.create_task(generate_summary_and_update(ctx.application, user_id, topic, news_id, current_message_id))
        conn.close()
        return

    if data.startswith("ask_specific:"):
        _, topic, news_id = data.split(":")
        news_id = int(news_id)
        row = c.execute("""
            SELECT title, full_text, summary FROM news WHERE id = ?
        """, (news_id,)).fetchone()
        if not row:
            await query.answer("Новость не найдена", show_alert=True)
            conn.close()
            return
        user_states[user_id] = {
            "state": "asking_question",
            "topic": topic,
            "news_id": news_id,
            "context": row['full_text'] if row['full_text'] else row['summary'],
            "last_message_id": current_message_id
        }
        await ctx.bot.send_message(
            user_id,
            f"📝 Задайте вопрос по новости:\n\n<b>{row['title'][:100]}...</b>\n\nВведите ваш вопрос:",
            parse_mode="HTML"
        )
        conn.close()
        return

    parts = data.split(":")
    kind = parts[0]
    topic = parts[1] if len(parts) > 1 else None

    if kind == "topic":
        await render(
            ctx.application,
            user_id,
            f"📰 {TOPICS[topic]['title']}\nВыберите действие:",
            news_menu(topic),
            edit_message=True,
            message_id=current_message_id
        )
    elif kind in ("new", "arch"):
        since = "now','-12 hours" if kind == "new" else f"now','-{ARCHIVE_DAYS} days"
        rows = c.execute(f"""
            SELECT id, title, summary, link, important
            FROM news 
            WHERE topic = ? AND published >= datetime('{since}')
            ORDER BY published DESC
            LIMIT ?
        """, (topic, MAX_NEWS_PER_TOPIC)).fetchall()
        if rows:
            news_list = [dict(row) for row in rows]
            text = f"📰 {TOPICS[topic]['title']}\n\nВыберите новость:"
            await render(
                ctx.application,
                user_id,
                text,
                news_list_keyboard(news_list, topic, 0, 5),
                edit_message=True,
                message_id=current_message_id
            )
        else:
            await render(
                ctx.application,
                user_id,
                f"📰 {TOPICS[topic]['title']}\n\nНовостей нет",
                news_menu(topic),
                edit_message=True,
                message_id=current_message_id
            )
    elif kind == "ask":
        row = c.execute("""
            SELECT id, title, full_text, summary
            FROM news 
            WHERE topic = ?
            ORDER BY published DESC 
            LIMIT 1
        """, (topic,)).fetchone()
        if not row:
            await query.answer("Новостей по этой теме нет", show_alert=True)
            conn.close()
            return
        user_states[user_id] = {
            "state": "asking_question",
            "topic": topic,
            "news_id": row["id"],
            "context": row['full_text'] if row['full_text'] else row['summary'],
            "last_message_id": current_message_id
        }
        await ctx.bot.send_message(
            user_id,
            f"📝 Задайте вопрос по последней новости:\n\n<b>{row['title'][:100]}...</b>\n\nВведите ваш вопрос:",
            parse_mode="HTML"
        )
    conn.close()

async def receive_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    if user_id not in user_states or user_states[user_id].get("state") != "asking_question":
        return
    state = user_states[user_id]
    if not text or len(text) < 3:
        await update.message.reply_text("❌ Вопрос слишком короткий. Попробуйте еще раз.")
        return
    await update.message.reply_text("⏳ Обрабатываю вопрос...")
    try:
        context = state.get("context", "")
        if not context or len(context) < 50:
            conn = get_connection()
            c = conn.cursor()
            row = c.execute("SELECT full_text, summary FROM news WHERE id = ?", (state["news_id"],)).fetchone()
            conn.close()
            if row and row['full_text']:
                context = row['full_text']
            elif row and row['summary']:
                context = row['summary']
            else:
                context = ""
        answer = answer_question(context, text)
        conn = get_connection()
        c = conn.cursor()
        c.execute("""
            INSERT INTO questions (user_id, news_id, question, answer)
            VALUES (?, ?, ?, ?)
        """, (user_id, state["news_id"], text, answer))
        conn.commit()
        conn.close()
        await update.message.reply_text(
            f"📝 <b>Ответ на ваш вопрос:</b>\n\n{answer}\n\n"
            f"<i>Вы можете задать ещё вопрос или вернуться к новостям.</i>",
            parse_mode="HTML",
            reply_markup=question_menu(state["topic"], state["news_id"])
        )
    except Exception as e:
        print(f"[ERROR] Ошибка обработки вопроса: {e}")
        await update.message.reply_text(
            "❌ Произошла ошибка при обработке вопроса. Попробуйте еще раз.",
            reply_markup=question_menu(state["topic"], state["news_id"])
        )

async def scheduled_newsletter(context: ContextTypes.DEFAULT_TYPE):
    print(f"[SCHEDULER] Запуск рассылки {datetime.now()}")
    loop = asyncio.get_running_loop()
    stats = await loop.run_in_executor(None, collect_and_save_news)
    if not stats:
        print("[SCHEDULER] Новостей нет")
        return
    conn = get_connection()
    users = conn.execute("SELECT user_id FROM users").fetchall()
    conn.close()
    for user in users:
        try:
            await render(
                context.application,
                user["user_id"],
                "📰 Свежие новости! Выберите тему:",
                topic_menu(),
                edit_message=False
            )
        except Exception as e:
            print(f"[SEND ERROR] {user['user_id']} -> {e}")

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(buttons))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, receive_message))
    job_queue = app.job_queue
    if job_queue:
        job_queue.run_daily(
            scheduled_newsletter,
            time=datetime.strptime("06:00", "%H:%M").time()
        )
        job_queue.run_daily(
            scheduled_newsletter,
            time=datetime.strptime("18:00", "%H:%M").time()
        )
        async def startup(context: ContextTypes.DEFAULT_TYPE):
            print("[BOT] Первый сбор новостей...")
            await scheduled_newsletter(context)
        job_queue.run_once(startup, when=5)
    print("[BOT] Запуск...")
    app.run_polling()

if __name__ == "__main__":
    main()
