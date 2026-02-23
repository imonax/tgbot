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
from llm import answer_question
from html import escape

init_db()

# Глобальное состояние
user_states = {}

def get_actual_stats():
    """Получить актуальное количество новостей за последние 12 часов по каждой теме"""
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
    """Генерация меню тем на основе актуальных данных из БД"""
    kb = []
    stats = get_actual_stats()

    for topic_id, topic_data in TOPICS.items():
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

def news_menu(topic, news_id=None):
    buttons = [
        [InlineKeyboardButton("🆕 Новые", callback_data=f"new:{topic}"),
         InlineKeyboardButton("📚 Архив", callback_data=f"arch:{topic}")],
    ]

    if news_id:
        buttons.append([
            InlineKeyboardButton("❓ Задать вопрос", callback_data=f"ask_specific:{topic}:{news_id}"),
            InlineKeyboardButton("⬅ Назад к списку", callback_data=f"list:{topic}:0")
        ])
    else:
        buttons.append([InlineKeyboardButton("❓ Задать вопрос по теме", callback_data=f"ask:{topic}")])
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
    """
    Отправить или отредактировать сообщение.
    Если edit_message=True и передан message_id, пытаемся редактировать его.
    Иначе используем last_message_id из БД.
    При неудаче отправляем новое сообщение и обновляем last_message_id.
    """
    conn = get_connection()
    c = conn.cursor()

    if edit_message:
        target_id = message_id
        if target_id is None:
            row = c.execute(
                "SELECT last_message_id FROM users WHERE user_id = ?",
                (user_id,)
            ).fetchone()
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
                conn.close()
                return
            except Exception as e:
                print(f"[RENDER EDIT ERROR] {e}")
                # не удалось отредактировать — отправим новое

    # Отправляем новое сообщение
    msg = await app.bot.send_message(
        user_id,
        text,
        reply_markup=kb,
        parse_mode=mode,
        disable_web_page_preview=False
    )

    # Обновляем last_message_id
    c.execute(
        "UPDATE users SET last_message_id = ? WHERE user_id = ?",
        (msg.message_id, user_id)
    )

    conn.commit()
    conn.close()

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    conn = get_connection()
    conn.execute("""
        INSERT OR REPLACE INTO users 
        (user_id, username, first_name, last_name) 
        VALUES (?, ?, ?, ?)
    """, (u.id, u.username, u.first_name, u.last_name))
    conn.commit()
    conn.close()

    # Отправляем новое сообщение (основное)
    await render(
        ctx.application,
        u.id,
        "📰 Последние новости. Выберите тему:",
        topic_menu(),
        edit_message=False
    )

async def buttons(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data
    user_id = query.from_user.id

    # Определяем ID сообщения, которое будем редактировать.
    # Если в состоянии сохранён main_message_id, используем его (это основное сообщение).
    # Иначе используем текущее сообщение, на котором нажата кнопка.
    if user_id in user_states and 'main_message_id' in user_states[user_id]:
        current_message_id = user_states[user_id]['main_message_id']
    else:
        current_message_id = query.message.message_id

    conn = get_connection()
    c = conn.cursor()

    if data == "refresh_news":
        # Сразу редактируем текущее сообщение, показывая ожидание
        await query.edit_message_text("⏳ Обновляю новости...")
        loop = asyncio.get_running_loop()
        stats = await loop.run_in_executor(
            None,
            collect_and_save_news
        )
        if stats:
            await render(
                ctx.application,
                user_id,
                "✅ Новости обновлены. Выберите тему:",
                topic_menu(),
                edit_message=True,
                message_id=current_message_id  # заменяем "⏳ Обновляю новости..." на результат
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

    # Сброс состояния, если не в режиме вопроса
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

    # Навигация по страницам
    if data.startswith("page:"):
        _, topic, start_idx = data.split(":")
        start_idx = int(start_idx)

        rows = c.execute("""
            SELECT id, title, summary, link, important
            FROM news 
            WHERE topic = ? 
            AND published >= datetime('now', '-12 hours')
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
            WHERE topic = ? 
            AND published >= datetime('now', '-12 hours')
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

        row = c.execute("""
            SELECT title, summary, link, important
            FROM news 
            WHERE id = ?
        """, (news_id,)).fetchone()

        if row:
            important_mark = "❗ " if row['important'] else ""
            safe_title = escape(row['title'])
            # Если summary совпадает с заголовком или пуст, показываем уведомление
            if not row['summary'] or row['summary'].strip() == row['title'].strip():
                safe_summary = "<i>Краткое содержание отсутствует</i>"
            else:
                safe_summary = escape(row['summary'])

            text = f"""{important_mark}<b>{safe_title}</b>

{safe_summary}

<a href="{row['link']}">📖 Читать полностью →</a>"""

            await render(
                ctx.application,
                user_id,
                text,
                news_menu(topic, news_id),
                "HTML",
                edit_message=True,
                message_id=current_message_id
            )
        else:
            await query.edit_message_text("Новость не найдена")
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

        # Сохраняем основное сообщение в состоянии
        user_states[user_id] = {
            "state": "asking_question",
            "topic": topic,
            "news_id": news_id,
            "context": row['full_text'] if row['full_text'] else row['summary'],
            "main_message_id": current_message_id  # запоминаем основное сообщение
        }

        # Отправляем отдельное сообщение для ввода вопроса (не редактируем основное)
        await ctx.bot.send_message(
            user_id,
            f"📝 Задайте вопрос по новости:\n\n<b>{row['title'][:100]}...</b>\n\nВведите ваш вопрос:",
            parse_mode="HTML"
        )
        conn.close()
        return

    # Разбор остальных команд (topic, new, arch, ask)
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
            WHERE topic = ? 
            AND published >= datetime('{since}')
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
            "main_message_id": current_message_id
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
        # Убедимся, что контекст не пуст
        context = state.get("context", "")
        if not context or len(context) < 50:
            # Попробуем получить полный текст из БД ещё раз
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

        # Отправляем ответ как новое сообщение (не трогаем основное)
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

    # Состояние не удаляем, чтобы можно было задать ещё вопрос

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
                "📰 Новые новости! Выберите тему:",
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