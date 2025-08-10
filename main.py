import os
import sqlite3
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes

DB_FILE = "videos.db"
BOT_TOKEN = os.getenv("BOT_TOKEN")

# ===== DB 초기화 =====
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS uploads (
            user_id INTEGER,
            username TEXT,
            file_unique_id TEXT,
            date TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()
    conn.close()

# ===== 설정 저장/불러오기 =====
def set_setting(key, value):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
    conn.commit()
    conn.close()

def get_setting(key):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT value FROM settings WHERE key=?", (key,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

# ===== 주기 계산 =====
def get_current_cycle():
    anchor_str = get_setting("anchor_date")
    cycle_len = int(get_setting("cycle_len") or 8)

    if not anchor_str:
        return None, None

    anchor = datetime.strptime(anchor_str, "%m/%d")
    now = datetime.now()

    # anchor의 연도를 현재 연도로 맞추기
    anchor = anchor.replace(year=now.year)
    if now < anchor:
        anchor = anchor.replace(year=now.year - 1)

    days_passed = (now - anchor).days
    cycles_passed = days_passed // cycle_len
    start_date = anchor + timedelta(days=cycles_passed * cycle_len)
    end_date = start_date + timedelta(days=cycle_len - 1)

    return start_date, end_date

# ===== 메시지 핸들러 =====
async def setanchor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("사용법: /setanchor MM/DD")
        return
    set_setting("anchor_date", context.args[0])
    await update.message.reply_text(f"기준일이 {context.args[0]}로 설정되었습니다.")

async def setcyclelen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("사용법: /setcyclelen 일수")
        return
    set_setting("cycle_len", context.args[0])
    await update.message.reply_text(f"주기 길이가 {context.args[0]}일로 설정되었습니다.")

async def cycle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    start, end = get_current_cycle()
    if not start:
        await update.message.reply_text("먼저 /setanchor 로 기준일을 설정하세요.")
        return
    await update.message.reply_text(f"이번 주기: {start.strftime('%m/%d')} ~ {end.strftime('%m/%d')}")

async def weekmy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    start, end = get_current_cycle()
    if not start:
        await update.message.reply_text("먼저 /setanchor 로 기준일을 설정하세요.")
        return
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        SELECT COUNT(*) FROM uploads
        WHERE user_id=? AND date BETWEEN ? AND ?
    """, (update.effective_user.id, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))
    count = c.fetchone()[0]
    conn.close()
    await update.message.reply_text(f"{update.effective_user.first_name}님의 이번 주기 업로드 수: {count}")

async def videohandler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    file = None
    if update.message.video:
        file = update.message.video
    elif update.message.document and update.message.document.file_name.lower().endswith((".mp4", ".avi")):
        file = update.message.document

    if file:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT 1 FROM uploads WHERE file_unique_id=?", (file.file_unique_id,))
        if not c.fetchone():
            c.execute("INSERT INTO uploads (user_id, username, file_unique_id, date) VALUES (?, ?, ?, ?)", (
                update.effective_user.id,
                update.effective_user.username or update.effective_user.first_name,
                file.file_unique_id,
                datetime.now().strftime("%Y-%m-%d")
            ))
            conn.commit()
        conn.close()

# ===== 봇 실행 =====
def main():
    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("setanchor", setanchor))
    app.add_handler(CommandHandler("setcyclelen", setcyclelen))
    app.add_handler(CommandHandler("cycle", cycle))
    app.add_handler(CommandHandler("weekmy", weekmy))
    app.add_handler(MessageHandler(filters.VIDEO | filters.Document.ALL, videohandler))

    app.run_polling()

if __name__ == "__main__":
    main()
