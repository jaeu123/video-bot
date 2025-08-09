import os
import sqlite3
from contextlib import closing
from typing import Optional

from telegram import Update, Message, User
from telegram.constants import ChatType
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ContextTypes, filters
)

DB_PATH = "videos.db"
BOT_TOKEN = os.getenv("BOT_TOKEN") or "8303848475:AAEBi7SS54b72hEiKrcXcQzrn-gJAru6-U8"

# =========================
# DB 초기화
# =========================
def init_db():
    with closing(sqlite3.connect(DB_PATH)) as conn, conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            chat_id                 INTEGER NOT NULL,
            file_unique_id          TEXT    NOT NULL,
            first_uploader_id       INTEGER NOT NULL,
            first_uploader_username TEXT,
            first_uploaded_at       INTEGER NOT NULL,
            PRIMARY KEY (chat_id, file_unique_id)
        )
        """)
        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_videos_chat_uploader
        ON videos (chat_id, first_uploader_id)
        """)

# =========================
# 유틸
# =========================
def get_video_unique_id(msg: Message) -> Optional[str]:
    """
    '한국 영상 집계 대상'의 file_unique_id 추출
    - 집계 포맷: mp4, avi
      * mp4: video/mp4 (일반 동영상 업로드/문서 업로드 모두)
      * avi: video/x-msvideo (대개 문서 업로드)
    - 기타 포맷(webm/mkv/mov/gif 등)은 제외
    """
    # 1) 일반 '동영상' 업로드: mp4만 허용
    if msg.video and getattr(msg.video, "mime_type", None) == "video/mp4":
        return msg.video.file_unique_id

    # 2) 파일(문서) 업로드: mp4/avi만 허용
    if msg.document:
        mt = msg.document.mime_type or ""
        if mt in ("video/mp4", "video/x-msvideo"):
            return msg.document.file_unique_id

    return None

def get_username(u: Optional[User]) -> Optional[str]:
    if not u:
        return None
    if u.username:
        return f"@{u.username}"
    full = " ".join([p for p in [u.first_name, u.last_name] if p])
    return full or None

# =========================
# 핸들러: 기본/명령어
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = (
        "안녕하세요! 한국 영상 업로드 카운터 봇입니다.\n\n"
        "규칙:\n"
        "• 우리 방은 한국 영상을 메인으로 합니다.\n"
        "• 주마다 4개씩 올려주세요.\n"
        "• 이전에 올라온 동일 영상은 카운팅하지 않습니다(중복 미포함).\n"
        "• 미성년자 관련 부적절한 콘텐츠는 금지입니다.\n"
        "• 외국 영상 업로드는 자제 부탁드립니다.\n"
        "• 집계 포맷: MP4, AVI만 인정합니다.\n\n"
        "명령어:\n"
        "/mycount — 내가 올린 한국 영상 업로드 수\n"
        "/count — 대상 메시지에 '답장'하고 실행하면 그 사람의 업로드 수\n"
        "/groupcount — 이 그룹의 한국 영상 총 개수\n"
        "/top — 한국 영상 업로드 랭킹 Top 10\n"
        "/ping — (관리자 전용) 봇 연결 점검\n"
        "새 멤버가 들어오면 자동 환영 멘트를 보냅니다 😊"
    )
    if update.message:
        await update.message.reply_text(txt)

async def mycount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await update.message.reply_text("이 기능은 그룹에서만 사용할 수 있어요.")
        return
    with closing(sqlite3.connect(DB_PATH)) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM videos WHERE chat_id=? AND first_uploader_id=?",
            (chat.id, user.id)
        ).fetchone()[0]
    await update.message.reply_text(f"{get_username(user) or user.id} 님의 한국 영상 업로드 수: {count}")

async def count_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /count — 대상자의 메시지에 '답장'해서 실행
    (username만으로는 Telegram이 user_id를 제공하지 않으므로 답장 방식 권장)
    """
    chat = update.effective_chat
    if not chat:
        return
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await update.message.reply_text("이 기능은 그룹에서만 사용할 수 있어요.")
        return

    target_user: Optional[User] = None
    if update.message and update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user

    if not target_user:
        await update.message.reply_text("대상 메시지에 '답장'한 뒤 /count 를 보내주세요.")
        return

    with closing(sqlite3.connect(DB_PATH)) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM videos WHERE chat_id=? AND first_uploader_id=?",
            (chat.id, target_user.id)
        ).fetchone()[0]

    await update.message.reply_text(f"{get_username(target_user) or target_user.id} 님의 한국 영상 업로드 수: {count}")

async def groupcount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat:
        return
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await update.message.reply_text("이 기능은 그룹에서만 사용할 수 있어요.")
        return
    with closing(sqlite3.connect(DB_PATH)) as conn:
        total = conn.execute(
            "SELECT COUNT(*) FROM videos WHERE chat_id=?",
            (chat.id,)
        ).fetchone()[0]
    await update.message.reply_text(f"이 그룹의 한국 영상 총 개수: {total}")

async def top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat:
        return
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await update.message.reply_text("이 기능은 그룹에서만 사용할 수 있어요.")
        return
    with closing(sqlite3.connect(DB_PATH)) as conn:
        rows = conn.execute(
            "SELECT first_uploader_id, COALESCE(first_uploader_username,'') AS uname, COUNT(*) AS c "
            "FROM videos WHERE chat_id=? GROUP BY first_uploader_id, uname "
            "ORDER BY c DESC LIMIT 10",
            (chat.id,)
        ).fetchall()

    if not rows:
        await update.message.reply_text("아직 집계된 한국 영상이 없어요.")
        return

    lines = ["Top 10 한국 영상 업로드 랭킹:"]
    for i, (uid, uname, c) in enumerate(rows, start=1):
        who = uname if uname else str(uid)
        lines.append(f"{i}. {who} — {c}")
    await update.message.reply_text("\n".join(lines))

# =========================
# 핸들러: 테스트용 /ping (관리자 전용)
# =========================
async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    if not (chat and user):
        return

    admins = await context.bot.get_chat_administrators(chat.id)
    admin_ids = [a.user.id for a in admins]
    if user.id not in admin_ids:
        await update.message.reply_text("⚠️ 이 명령은 관리자만 사용할 수 있습니다.")
        return

    await update.message.reply_text("pong ✅")

# =========================
# 핸들러: 영상 수집(그룹 전용)
# =========================
async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    msg = update.effective_message
    user = update.effective_user
    if not (chat and msg and user):
        return
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return  # 그룹 전용

    uniq = get_video_unique_id(msg)  # mp4/avi만 통과
    if not uniq:
        return

    uploader_id = user.id
    uploader_name = get_username(user)

    with closing(sqlite3.connect(DB_PATH)) as conn, conn:
        try:
            conn.execute(
                "INSERT INTO videos (chat_id, file_unique_id, first_uploader_id, first_uploader_username, first_uploaded_at) "
                "VALUES (?, ?, ?, ?, strftime('%s','now'))",
                (chat.id, uniq, uploader_id, uploader_name)
            )
        except sqlite3.IntegrityError:
            # 이미 이 그룹에서 등록된 동일 영상 → 재업로드는 카운팅하지 않음
            pass

# =========================
# 핸들러: 그룹 승격(migrate) 대응
# =========================
async def handle_migrate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    old_id = getattr(msg, "migrate_from_chat_id", None)
    new_id = getattr(msg, "migrate_to_chat_id", None)
    if not (old_id and new_id):
        return
    with closing(sqlite3.connect(DB_PATH)) as conn, conn:
        rows = conn.execute(
            "SELECT file_unique_id, first_uploader_id, first_uploader_username, first_uploaded_at "
            "FROM videos WHERE chat_id=?",
            (old_id,)
        ).fetchall()
        for uniq, uid, uname, ts in rows:
            try:
                conn.execute(
                    "INSERT INTO videos (chat_id, file_unique_id, first_uploader_id, first_uploader_username, first_uploaded_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (new_id, uniq, uid, uname, ts)
                )
            except sqlite3.IntegrityError:
                pass
        conn.execute("DELETE FROM videos WHERE chat_id=?", (old_id,))

# =========================
# 핸들러: 새 멤버 환영 (요청하신 문구)
# =========================
async def welcome_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    message = update.effective_message
    if not (chat and message and message.new_chat_members):
        return

    for member in message.new_chat_members:
        if member.is_bot:
            continue
        name = get_username(member) or member.first_name
        text = (
            f"🎉 {name} 님, 반갑습니다 계모임 영상방입니다.\n"
            f"우리 방은 한국 영상을 메인으로 한 주마다 4개씩 올려주시면 됩니다.\n"
            f"단, 이전에 올라온 영상은 카운팅하지 않으며, 미성년자 관련 부적절한 콘텐츠는 금지입니다.\n"
            f"외국 영상도 자제해주세요.\n"
            f"즐거운 영상방 생활 되세요!"
        )
        await message.reply_text(text)

# =========================
# main
# =========================
def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    # 명령어
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping", ping))  # 관리자 전용
    app.add_handler(CommandHandler("mycount", mycount))
    app.add_handler(CommandHandler("count", count_user))
    app.add_handler(CommandHandler("groupcount", groupcount))
    app.add_handler(CommandHandler("top", top))

    # 상태 업데이트
    app.add_handler(MessageHandler(filters.StatusUpdate.MIGRATE, handle_migrate))
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, welcome_new_member))

    # 메시지 수집: '동영상' + '문서 중 video/*' → 내부에서 mp4/avi만 선별
    video_or_doc = (filters.VIDEO | filters.Document.MimeType("video/"))
    app.add_handler(MessageHandler(video_or_doc, handle_video))

    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
