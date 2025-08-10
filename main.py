import os
import sqlite3
from contextlib import closing
from typing import Optional, Tuple
from datetime import datetime, timedelta, timezone

# KST 타임존
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None

from telegram import Update, Message, User
from telegram.constants import ChatType
from telegram.error import BadRequest
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ContextTypes, filters
)

DB_PATH = "videos.db"
BOT_TOKEN = os.getenv("BOT_TOKEN") or "PUT-YOUR-TOKEN-HERE"

# =========================
# 시간/타임존 유틸
# =========================
KST = ZoneInfo("Asia/Seoul") if ZoneInfo else timezone(timedelta(hours=9))

def now_kst() -> datetime:
    return datetime.now(tz=KST)

def to_epoch(dt: datetime) -> int:
    """TZ-aware dt -> UTC epoch seconds"""
    return int(dt.astimezone(timezone.utc).timestamp())

def from_epoch_kst(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=KST)

def fmt_ts_kst(ts: int) -> str:
    return from_epoch_kst(ts).strftime("%Y-%m-%d %H:%M")

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
            first_uploaded_at       INTEGER NOT NULL, -- UTC epoch seconds
            PRIMARY KEY (chat_id, file_unique_id)
        )
        """)
        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_videos_chat_uploader
        ON videos (chat_id, first_uploader_id)
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS chat_meta (
            chat_id         INTEGER PRIMARY KEY,
            period_start_ts INTEGER,
            period_end_ts   INTEGER,
            room_start_ts   INTEGER,
            baseline_total  INTEGER DEFAULT 0
        )
        """)
        # 자동 주기용 앵커/길이 (이미 있으면 무시)
        try:
            conn.execute("ALTER TABLE chat_meta ADD COLUMN anchor_ts INTEGER")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE chat_meta ADD COLUMN cycle_len_days INTEGER DEFAULT 8")
        except sqlite3.OperationalError:
            pass

# =========================
# 공용 유틸
# =========================
def get_video_unique_id(msg: Message) -> Optional[str]:
    """
    집계 대상: mp4/avi만
      - 일반 동영상 업로드: video/mp4
      - 문서 업로드: video/mp4 또는 video/x-msvideo(avi)
    """
    if msg.video and getattr(msg.video, "mime_type", None) == "video/mp4":
        return msg.video.file_unique_id
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

# ---- 메타 접근 유틸
def get_chat_meta(conn: sqlite3.Connection, chat_id: int):
    row = conn.execute(
        "SELECT period_start_ts, period_end_ts, room_start_ts, baseline_total, anchor_ts, COALESCE(cycle_len_days,8) "
        "FROM chat_meta WHERE chat_id=?", (chat_id,)
    ).fetchone()
    if not row:
        conn.execute("INSERT OR IGNORE INTO chat_meta(chat_id) VALUES (?)", (chat_id,))
        return (None, None, None, 0, None, 8)
    return row

def set_room_start(conn: sqlite3.Connection, chat_id: int, room_start_ts: int):
    conn.execute("""
        INSERT INTO chat_meta (chat_id, room_start_ts)
        VALUES (?, ?)
        ON CONFLICT(chat_id) DO UPDATE SET room_start_ts=excluded.room_start_ts
    """, (chat_id, room_start_ts))

def set_baseline(conn: sqlite3.Connection, chat_id: int, baseline: int):
    conn.execute("""
        INSERT INTO chat_meta (chat_id, baseline_total)
        VALUES (?, ?)
        ON CONFLICT(chat_id) DO UPDATE SET baseline_total=excluded.baseline_total
    """, (chat_id, baseline))

def set_anchor_and_len(conn: sqlite3.Connection, chat_id: int, anchor_ts: int, cycle_len_days: int):
    conn.execute("""
        INSERT INTO chat_meta (chat_id, anchor_ts, cycle_len_days)
        VALUES (?, ?, ?)
        ON CONFLICT(chat_id) DO UPDATE SET anchor_ts=excluded.anchor_ts,
                                          cycle_len_days=excluded.cycle_len_days
    """, (chat_id, anchor_ts, cycle_len_days))

def get_anchor_and_len(conn: sqlite3.Connection, chat_id: int) -> Tuple[Optional[int], int]:
    row = conn.execute(
        "SELECT anchor_ts, COALESCE(cycle_len_days,8) FROM chat_meta WHERE chat_id=?",
        (chat_id,)
    ).fetchone()
    if not row:
        conn.execute("INSERT OR IGNORE INTO chat_meta(chat_id) VALUES (?)", (chat_id,))
        return None, 8
    return row[0], row[1]

# ---- 자동 주기 계산
def parse_anchor_input(s: str) -> Optional[int]:
    """
    입력: '8/08' 또는 '2025-08-08'
    반환: 해당일 00:00:00 (KST) 기준 UTC epoch
    """
    s = s.strip()
    try:
        if "/" in s:
            M, d = map(int, s.split("/", 1))
            y = now_kst().year
            dt = datetime(y, M, d, 0, 0, 0, tzinfo=KST)
            return to_epoch(dt)
        else:
            y, M, d = map(int, s.split("-"))
            dt = datetime(y, M, d, 0, 0, 0, tzinfo=KST)
            return to_epoch(dt)
    except Exception:
        return None

def current_cycle_bounds(anchor_ts: int, cycle_len_days: int, now_dt: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    """
    앵커/주기 길이로 '현재 날짜가 속한 주기'의 시작/끝(KST)을 계산
      - 시작: anchor + n*period
      - 끝: 시작 + period - 1초
    """
    period = timedelta(days=cycle_len_days)
    anchor = from_epoch_kst(anchor_ts)
    tnow = (now_dt or now_kst()).astimezone(KST)
    if tnow < anchor:
        start = anchor
    else:
        delta = tnow - anchor
        n = int(delta.total_seconds() // period.total_seconds())
        start = anchor + n * period
    end = start + period - timedelta(seconds=1)
    return start, end

# ---- 집계 쿼리
def count_in_period(conn: sqlite3.Connection, chat_id: int, start_ts: int, end_ts: int, user_id: Optional[int] = None) -> int:
    if user_id:
        q = "SELECT COUNT(*) FROM videos WHERE chat_id=? AND first_uploader_id=? AND first_uploaded_at BETWEEN ? AND ?"
        return conn.execute(q, (chat_id, user_id, start_ts, end_ts)).fetchone()[0]
    q = "SELECT COUNT(*) FROM videos WHERE chat_id=? AND first_uploaded_at BETWEEN ? AND ?"
    return conn.execute(q, (chat_id, start_ts, end_ts)).fetchone()[0]

def count_since(conn: sqlite3.Connection, chat_id: int, start_ts: int, user_id: Optional[int] = None) -> int:
    if user_id:
        q = "SELECT COUNT(*) FROM videos WHERE chat_id=? AND first_uploader_id=? AND first_uploaded_at >= ?"
        return conn.execute(q, (chat_id, user_id, start_ts)).fetchone()[0]
    q = "SELECT COUNT(*) FROM videos WHERE chat_id=? AND first_uploaded_at >= ?"
    return conn.execute(q, (chat_id, start_ts)).fetchone()[0]

# =========================
# 기본/안내
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    txt = (
        "안녕하세요! 한국 영상 업로드 카운터 봇입니다.\n\n"
        "집계 규칙:\n"
        "• 한국 영상 기준, mp4/avi만 집계\n"
        "• 이전에 올라온 동일 영상은 중복 미포함\n\n"
        "명령어:\n"
        "/mycount — 내 전체 누적(봇 이후)\n"
        "/count — 대상 메시지에 '답장'하고 실행 → 그 사람 누적(봇 이후)\n"
        "/groupcount — 그룹 전체 누적(봇 이후)\n"
        "/top — 업로더 Top 10\n"
        "/setanchor 8/08 (관리자) — 기준 시작일 설정(한 번만)\n"
        "/setcyclelen 8 (관리자) — 주기 길이(일) 설정\n"
        "/cycle — 현재 주기(자동 계산) 표시\n"
        "/weekmy — 이번 주기 내 내 업로드 수\n"
        "/weekgroup — 이번 주기 내 그룹 업로드 총합\n"
        "/setroomstart 2024-01-01 (관리자) — 영상방 시작일 설정\n"
        "/setbaseline 350 (관리자) — 봇 이전 누적 보정치 설정\n"
        "/roomcount — 방 시작~현재 총합 = baseline + 이후분\n"
        "/latest — 마지막 업로드 시각 + 현재 누적\n"
        "/ping — (관리자) 연결 점검\n"
        "/help — 이 안내문"
    )
    if chat:
        await context.bot.send_message(chat_id=chat.id, text=txt)

# =========================
# /help
# =========================
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "📌 명령어 안내\n\n"
        "📊 집계\n"
        "/mycount — 내 전체 누적(봇 이후)\n"
        "/count — 대상 메시지에 '답장' 후 실행 → 그 사람 누적(봇 이후)\n"
        "/groupcount — 그룹 전체 누적(봇 이후)\n"
        "/top — 업로더 Top 10\n\n"
        "📅 자동 주기\n"
        "/setanchor 8/08 — 기준 시작일 설정 (관리자)\n"
        "/setcyclelen 8 — 주기 길이(일) 설정 (관리자)\n"
        "/cycle — 현재 주기(KST) 표시\n"
        "/weekmy — 이번 주기 내 내 업로드 수\n"
        "/weekgroup — 이번 주기 그룹 업로드 총합\n\n"
        "🏠 방 전체 집계\n"
        "/setroomstart YYYY-MM-DD — 영상방 시작일 설정 (관리자)\n"
        "/setbaseline 숫자 — 봇 이전 누적 보정치 (관리자)\n"
        "/roomcount — 방 시작~현재 총합 (= baseline + 이후)\n"
        "/latest — 마지막 업로드 시각 + 현재 누적\n\n"
        "🔧 기타\n"
        "/ping — 연결 점검 (관리자)\n"
        "/help — 이 안내문"
    )
    await context.bot.send_message(chat_id=update.effective_chat.id, text=help_text)

# =========================
# 기본 집계 명령
# =========================
async def mycount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat, user = update.effective_chat, update.effective_user
    if not chat or not user: return
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await context.bot.send_message(chat_id=chat.id, text="이 기능은 그룹에서만 사용할 수 있어요.")
        return
    with closing(sqlite3.connect(DB_PATH)) as conn:
        c = conn.execute(
            "SELECT COUNT(*) FROM videos WHERE chat_id=? AND first_uploader_id=?",
            (chat.id, user.id)
        ).fetchone()[0]
    await context.bot.send_message(chat_id=chat.id, text=f"{get_username(user) or user.id} 님의 누적(봇 이후): {c}")

async def count_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat: return
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await context.bot.send_message(chat_id=chat.id, text="이 기능은 그룹에서만 사용할 수 있어요.")
        return
    target: Optional[User] = update.message.reply_to_message.from_user if (update.message and update.message.reply_to_message) else None
    if not target:
        await context.bot.send_message(chat_id=chat.id, text="대상 메시지에 '답장'한 뒤 /count 를 보내주세요.")
        return
    with closing(sqlite3.connect(DB_PATH)) as conn:
        c = conn.execute(
            "SELECT COUNT(*) FROM videos WHERE chat_id=? AND first_uploader_id=?",
            (chat.id, target.id)
        ).fetchone()[0]
    await context.bot.send_message(chat_id=chat.id, text=f"{get_username(target) or target.id} 님의 누적(봇 이후): {c}")

async def groupcount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat: return
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await context.bot.send_message(chat_id=chat.id, text="이 기능은 그룹에서만 사용할 수 있어요.")
        return
    with closing(sqlite3.connect(DB_PATH)) as conn:
        total = conn.execute("SELECT COUNT(*) FROM videos WHERE chat_id=?", (chat.id,)).fetchone()[0]
    await context.bot.send_message(chat_id=chat.id, text=f"이 그룹의 한국 영상 총 개수(봇 이후): {total}")

async def top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat: return
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await context.bot.send_message(chat_id=chat.id, text="이 기능은 그룹에서만 사용할 수 있어요.")
        return
    with closing(sqlite3.connect(DB_PATH)) as conn:
        rows = conn.execute(
            "SELECT first_uploader_id, COALESCE(first_uploader_username,'') AS uname, COUNT(*) AS c "
            "FROM videos WHERE chat_id=? GROUP BY first_uploader_id, uname ORDER BY c DESC LIMIT 10",
            (chat.id,)
        ).fetchall()
    if not rows:
        await context.bot.send_message(chat_id=chat.id, text="아직 집계된 한국 영상이 없어요.")
        return
    lines = ["Top 10 한국 영상 업로드 랭킹(봇 이후):"]
    for i, (uid, uname, c) in enumerate(rows, start=1):
        who = uname if uname else str(uid)
        lines.append(f"{i}. {who} — {c}")
    await context.bot.send_message(chat_id=chat.id, text="\n".join(lines))

# =========================
# 자동 주기 관련 명령
# =========================
async def setanchor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat, user = update.effective_chat, update.effective_user
    if not (chat and user): return
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await context.bot.send_message(chat_id=chat.id, text="그룹에서만 설정할 수 있어요.")
        return
    # 관리자 체크
    try:
        admins = await context.bot.get_chat_administrators(chat.id)
        admin_ids = [a.user.id for a in admins]
    except BadRequest:
        admin_ids = []
    if user.id not in admin_ids:
        await context.bot.send_message(chat_id=chat.id, text="⚠️ 이 명령은 관리자만 사용할 수 있습니다.")
        return
    if not context.args:
        await context.bot.send_message(chat_id=chat.id, text="형식: /setanchor 8/08  또는  /setanchor 2025-08-08")
        return
    anchor_ts = parse_anchor_input(context.args[0])
    if anchor_ts is None:
        await context.bot.send_message(chat_id=chat.id, text="날짜 형식 오류입니다. 예) 8/08  또는  2025-08-08")
        return
    with closing(sqlite3.connect(DB_PATH)) as conn, conn:
        cur_anchor, cur_len = get_anchor_and_len(conn, chat.id)
        set_anchor_and_len(conn, chat.id, anchor_ts, cur_len or 8)
    await context.bot.send_message(
        chat_id=chat.id,
        text=f"앵커를 {from_epoch_kst(anchor_ts):%Y-%m-%d} (KST)로 설정했습니다.\n"
             f"이 날짜를 기준으로 {cur_len or 8}일 주기를 자동 계산합니다."
    )

async def setcyclelen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat, user = update.effective_chat, update.effective_user
    if not (chat and user): return
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await context.bot.send_message(chat_id=chat.id, text="그룹에서만 설정할 수 있어요.")
        return
    # 관리자 체크
    try:
        admins = await context.bot.get_chat_administrators(chat.id)
        admin_ids = [a.user.id for a in admins]
    except BadRequest:
        admin_ids = []
    if user.id not in admin_ids:
        await context.bot.send_message(chat_id=chat.id, text="⚠️ 이 명령은 관리자만 사용할 수 있습니다.")
        return
    if not context.args:
        await context.bot.send_message(chat_id=chat.id, text="형식: /setcyclelen 8  (일 단위)")
        return
    try:
        days = int(context.args[0])
        if days <= 0 or days > 31:
            raise ValueError
    except Exception:
        await context.bot.send_message(chat_id=chat.id, text="1~31 사이의 일수를 입력해주세요. 예) /setcyclelen 8")
        return
    with closing(sqlite3.connect(DB_PATH)) as conn, conn:
        anchor_ts, _ = get_anchor_and_len(conn, chat.id)
        if anchor_ts is None:
            await context.bot.send_message(chat_id=chat.id, text="먼저 /setanchor 로 기준 시작일을 설정해주세요.")
            return
        set_anchor_and_len(conn, chat.id, anchor_ts, days)
    await context.bot.send_message(chat_id=chat.id, text=f"주기 길이를 {days}일로 설정했습니다.")

async def cycle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat: return
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await context.bot.send_message(chat_id=chat.id, text="그룹에서만 사용할 수 있어요.")
        return
    with closing(sqlite3.connect(DB_PATH)) as conn:
        anchor_ts, days = get_anchor_and_len(conn, chat.id)
        if anchor_ts is None:
            await context.bot.send_message(chat_id=chat.id, text="아직 앵커가 없습니다. /setanchor 8/08  같은 형식으로 한 번만 설정해주세요.")
            return
        s_kst, e_kst = current_cycle_bounds(anchor_ts, days)
    await context.bot.send_message(chat_id=chat.id, text=f"현재 주기(KST): {s_kst:%Y-%m-%d} ~ {e_kst:%Y-%m-%d}")

async def weekmy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat, user = update.effective_chat, update.effective_user
    if not (chat and user): return
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await context.bot.send_message(chat_id=chat.id, text="그룹에서만 사용할 수 있어요.")
        return
    with closing(sqlite3.connect(DB_PATH)) as conn:
        anchor_ts, days = get_anchor_and_len(conn, chat.id)
        if anchor_ts is None:
            await context.bot.send_message(chat_id=chat.id, text="먼저 /setanchor 로 기준 시작일을 설정해주세요.")
            return
        s_kst, e_kst = current_cycle_bounds(anchor_ts, days)
        c = count_in_period(conn, chat.id, to_epoch(s_kst), to_epoch(e_kst), user_id=user.id)
    await context.bot.send_message(
        chat_id=chat.id,
        text=f"{get_username(user) or user.id} 님의 이번 주기 업로드 수: {c}\n"
             f"기간: {s_kst:%Y-%m-%d} ~ {e_kst:%Y-%m-%d} (KST)"
    )

async def weekgroup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat: return
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await context.bot.send_message(chat_id=chat.id, text="그룹에서만 사용할 수 있어요.")
        return
    with closing(sqlite3.connect(DB_PATH)) as conn:
        anchor_ts, days = get_anchor_and_len(conn, chat.id)
        if anchor_ts is None:
            await context.bot.send_message(chat_id=chat.id, text="먼저 /setanchor 로 기준 시작일을 설정해주세요.")
            return
        s_kst, e_kst = current_cycle_bounds(anchor_ts, days)
        c = count_in_period(conn, chat.id, to_epoch(s_kst), to_epoch(e_kst))
    await context.bot.send_message(
        chat_id=chat.id,
        text=f"이번 주기 그룹 업로드 총합: {c}\n기간: {s_kst:%Y-%m-%d} ~ {e_kst:%Y-%m-%d} (KST)"
    )

# =========================
# 방 시작/베이스라인/누적/최근
# =========================
async def setroomstart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat, user = update.effective_chat, update.effective_user
    if not (chat and user): return
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await context.bot.send_message(chat_id=chat.id, text="그룹에서만 설정할 수 있어요.")
        return
    try:
        admins = await context.bot.get_chat_administrators(chat.id)
        admin_ids = [a.user.id for a in admins]
    except BadRequest:
        admin_ids = []
    if user.id not in admin_ids:
        await context.bot.send_message(chat_id=chat.id, text="⚠️ 이 명령은 관리자만 사용할 수 있습니다.")
        return
    if not context.args:
        await context.bot.send_message(chat_id=chat.id, text="형식: /setroomstart 2024-01-01")
        return
    try:
        y, M, d = map(int, context.args[0].split("-"))
        dt = datetime(y, M, d, 0, 0, 0, tzinfo=KST)
    except Exception:
        await context.bot.send_message(chat_id=chat.id, text="형식 오류: YYYY-MM-DD 로 입력해주세요.")
        return
    with closing(sqlite3.connect(DB_PATH)) as conn, conn:
        set_room_start(conn, chat.id, to_epoch(dt))
    await context.bot.send_message(chat_id=chat.id, text=f"영상방 시작일을 {dt.date()} 로 설정했습니다.")

async def setbaseline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat, user = update.effective_chat, update.effective_user
    if not (chat and user): return
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await context.bot.send_message(chat_id=chat.id, text="그룹에서만 설정할 수 있어요.")
        return
    try:
        admins = await context.bot.get_chat_administrators(chat.id)
        admin_ids = [a.user.id for a in admins]
    except BadRequest:
        admin_ids = []
    if user.id not in admin_ids:
        await context.bot.send_message(chat_id=chat.id, text="⚠️ 이 명령은 관리자만 사용할 수 있습니다.")
        return
    if not context.args:
        await context.bot.send_message(chat_id=chat.id, text="형식: /setbaseline 123  (봇 도입 이전 누적 보정치)")
        return
    try:
        base = int(context.args[0])
    except Exception:
        await context.bot.send_message(chat_id=chat.id, text="숫자를 입력해주세요. 예) /setbaseline 120")
        return
    with closing(sqlite3.connect(DB_PATH)) as conn, conn:
        set_baseline(conn, chat.id, base)
    await context.bot.send_message(chat_id=chat.id, text=f"베이스라인을 {base} 로 설정했습니다.")

async def roomcount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat: return
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await context.bot.send_message(chat_id=chat.id, text="그룹에서만 사용할 수 있어요.")
        return
    with closing(sqlite3.connect(DB_PATH)) as conn:
        _, _, room_start_ts, baseline, _, _ = get_chat_meta(conn, chat.id)
        if not room_start_ts:
            await context.bot.send_message(chat_id=chat.id, text="먼저 /setroomstart 로 영상방 시작일을 설정해주세요.")
            return
        after = count_since(conn, chat.id, room_start_ts)
        total = baseline + after
    await context.bot.send_message(chat_id=chat.id, text=f"영상방 누적(시작~현재): {total} (= baseline {baseline} + 이후 {after})")

async def latest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat: return
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await context.bot.send_message(chat_id=chat.id, text="그룹에서만 사용할 수 있어요.")
        return
    with closing(sqlite3.connect(DB_PATH)) as conn:
        row = conn.execute("SELECT MAX(first_uploaded_at) FROM videos WHERE chat_id=?", (chat.id,)).fetchone()
        total = conn.execute("SELECT COUNT(*) FROM videos WHERE chat_id=?", (chat.id,)).fetchone()[0]
    if not row or row[0] is None:
        await context.bot.send_message(chat_id=chat.id, text="아직 업로드 기록이 없어요.")
        return
    await context.bot.send_message(chat_id=chat.id, text=f"마지막 업로드: {fmt_ts_kst(row[0])}\n현재 누적(봇 이후): {total}")

# =========================
# 관리자 전용 /ping (그룹 전용)
# =========================
async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat, user = update.effective_chat, update.effective_user
    if not (chat and user): return
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await context.bot.send_message(chat_id=chat.id, text="이 명령은 그룹에서만 사용할 수 있어요.")
        return
    try:
        admins = await context.bot.get_chat_administrators(chat.id)
        admin_ids = [a.user.id for a in admins]
    except BadRequest:
        admin_ids = []
    if user.id not in admin_ids:
        await context.bot.send_message(chat_id=chat.id, text="⚠️ 이 명령은 관리자만 사용할 수 있습니다.")
        return
    await context.bot.send_message(chat_id=chat.id, text="pong ✅")

# =========================
# 메시지 처리: 영상 저장(중복 제외)
# =========================
async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat, msg, user = update.effective_chat, update.effective_message, update.effective_user
    if not (chat and msg and user): return
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    uniq = get_video_unique_id(msg)
    if not uniq:
        return
    with closing(sqlite3.connect(DB_PATH)) as conn, conn:
        try:
            conn.execute(
                "INSERT INTO videos (chat_id, file_unique_id, first_uploader_id, first_uploader_username, first_uploaded_at) "
                "VALUES (?, ?, ?, ?, strftime('%s','now'))",
                (chat.id, uniq, user.id, get_username(user),)
            )
        except sqlite3.IntegrityError:
            # 동일 영상(그룹 내) 재업로드 → 미집계
            pass

# =========================
# 그룹 승격(migrate) 대응
# =========================
async def handle_migrate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg: return
    old_id = getattr(msg, "migrate_from_chat_id", None)
    new_id = getattr(msg, "migrate_to_chat_id", None)
    if not (old_id and new_id): return
    with closing(sqlite3.connect(DB_PATH)) as conn, conn:
        rows = conn.execute(
            "SELECT file_unique_id, first_uploader_id, first_uploader_username, first_uploaded_at "
            "FROM videos WHERE chat_id=?", (old_id,)
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
# 새 멤버 환영 멘트
# =========================
async def welcome_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat, message = update.effective_chat, update.effective_message
    if not (chat and message and message.new_chat_members): return
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
        await context.bot.send_message(chat_id=chat.id, text=text)

# =========================
# 명령어 등록 헬퍼 (대/소문자 모두 인식)
# =========================
def add_cmd(app: Application, name_or_list, func):
    names = [name_or_list] if isinstance(name_or_list, str) else list(name_or_list)
    variants = []
    for n in names:
        variants.extend({n, n.lower(), n.capitalize()})
    app.add_handler(CommandHandler(variants, func))

# =========================
# main
# =========================
def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    # 기본/도움말
    add_cmd(app, "start", start)
    add_cmd(app, "help", help_command)

    # 관리자 전용
    add_cmd(app, "ping", ping)
    add_cmd(app, "setanchor", setanchor)
    add_cmd(app, "setcyclelen", setcyclelen)
    add_cmd(app, "setroomstart", setroomstart)
    add_cmd(app, "setbaseline", setbaseline)

    # 조회/집계
    add_cmd(app, "cycle", cycle)
    add_cmd(app, "weekmy", weekmy)
    add_cmd(app, "weekgroup", weekgroup)
    add_cmd(app, "mycount", mycount)
    add_cmd(app, "count", count_user)
    add_cmd(app, "groupcount", groupcount)
    add_cmd(app, "top", top)
    add_cmd(app, "roomcount", roomcount)
    add_cmd(app, "latest", latest)

    # 상태 업데이트
    app.add_handler(MessageHandler(filters.StatusUpdate.MIGRATE, handle_migrate))
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, welcome_new_member))

    # 영상 수집 (동영상 + 문서(video/*) → 내부에서 mp4/avi만 선별)
    video_or_doc = (filters.VIDEO | filters.Document.MimeType("video/"))
    app.add_handler(MessageHandler(video_or_doc, handle_video))

    # 재시작 시 밀린 업데이트 무시 → 깔끔하게 동작
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    main()

