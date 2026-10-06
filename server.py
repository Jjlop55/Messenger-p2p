import os
import time
import uuid
import json
import sqlite3
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)
app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")

SERVER_START_TIME = time.time()

# Подключение к SQLite с защитой WAL
#
# ВАЖНО: соединение и курсор общие НЕЛЬЗЯ держать на весь процесс.
# FastAPI гоняет sync-хендлеры (def) в пуле из ~40 потоков, а async-хендлеры
# и WebSocket работают на главном потоке event-loop — и все они дергали
# один и тот же cursor. Это роняло сервер с
#   sqlite3.ProgrammingError: Recursive use of cursors not allowed
# прямо в /friends/action, после чего процесс переставал отвечать вообще,
# и помогал только перезапуск.
#
# Решение: каждый поток получает СВОЁ соединение и СВОЙ курсор.
# Имена `conn` и `cursor` сохранены — ни один из 198 вызовов не менялся.
import threading

_DB_PATH = "messenger.db"
_thread_local = threading.local()


def _new_connection():
    c = sqlite3.connect(_DB_PATH, check_same_thread=False, timeout=25.0, isolation_level=None)
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("PRAGMA busy_timeout=25000;")
    c.execute("PRAGMA synchronous=NORMAL;")
    return c


def _get_conn():
    c = getattr(_thread_local, "conn", None)
    if c is None:
        c = _new_connection()
        _thread_local.conn = c
    return c


def _get_cursor():
    cur = getattr(_thread_local, "cursor", None)
    if cur is None:
        cur = _get_conn().cursor()
        _thread_local.cursor = cur
    return cur


class _ConnProxy:
    """Подставляет соединение текущего потока вместо общего на процесс."""

    def __getattr__(self, name):
        return getattr(_get_conn(), name)

    def cursor(self):
        return _get_conn().cursor()


class _CursorProxy:
    """Подставляет курсор текущего потока вместо общего на процесс."""

    def __getattr__(self, name):
        return getattr(_get_cursor(), name)


conn = _ConnProxy()
cursor = _CursorProxy()

# ═══════════════════════════════════════════════════════════
#  ТАБЛИЦЫ
# ═══════════════════════════════════════════════════════════

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    nickname TEXT PRIMARY KEY,
    password TEXT NOT NULL,
    avatar_url TEXT DEFAULT '',
    bio TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_pwd_change TIMESTAMP,
    last_username_change TIMESTAMP,
    is_superadmin INTEGER DEFAULT 0,
    is_admin INTEGER DEFAULT 0,
    admin_perms TEXT DEFAULT '{}',
    admin_notified INTEGER DEFAULT 0,
    granted_by TEXT,
    revoked_by TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS chats (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_by TEXT NOT NULL,
    is_direct INTEGER DEFAULT 0,
    direct_user1 TEXT,
    direct_user2 TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS members (
    chat_id TEXT,
    nickname TEXT,
    UNIQUE(chat_id, nickname)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS friends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user1 TEXT NOT NULL,
    user2 TEXT NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user1, user2)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS invites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id TEXT,
    chat_name TEXT,
    from_user TEXT,
    to_user TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id TEXT,
    sender TEXT,
    text TEXT,
    file_url TEXT,
    file_type TEXT,
    reply_to_id INTEGER,
    reply_to_text TEXT,
    reply_to_sender TEXT,
    is_edited INTEGER DEFAULT 0,
    is_read INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS message_reactions (
    message_id INTEGER NOT NULL,
    nickname TEXT NOT NULL,
    emoji TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(message_id, nickname)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS delete_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_nick TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NOT NULL,
    status TEXT DEFAULT 'pending'
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    to_user TEXT NOT NULL,
    text TEXT NOT NULL,
    is_read INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS bans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_nick TEXT NOT NULL,
    banned_by TEXT NOT NULL,
    reason TEXT NOT NULL,
    ban_type TEXT DEFAULT 'permanent',
    expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS mutes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_nick TEXT NOT NULL,
    muted_by TEXT NOT NULL,
    reason TEXT,
    duration_minutes INTEGER DEFAULT 10,
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS user_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nickname TEXT NOT NULL,
    session_token TEXT UNIQUE,
    user_agent TEXT,
    ip_address TEXT,
    logged_in_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active INTEGER DEFAULT 1
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS support_tickets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_user TEXT NOT NULL,
    subject TEXT NOT NULL,
    message TEXT NOT NULL,
    status TEXT DEFAULT 'open',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    user_viewed_at TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS support_replies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_id INTEGER NOT NULL,
    from_user TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS support_perm_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    requested_by TEXT NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    decided_at TIMESTAMP,
    decided_by TEXT
)
""")

conn.commit()

cursor.execute("INSERT OR IGNORE INTO chats (id, name, created_by, is_direct) VALUES ('general', '🌐 Общий чат', 'system', 0)")
conn.commit()

# Безопасное добавление колонок
_safe_alters = [
    ("users", "avatar_url", "TEXT DEFAULT ''"),
    ("users", "bio", "TEXT DEFAULT ''"),
    ("users", "last_username_change", "TIMESTAMP"),
    ("chats", "is_direct", "INTEGER DEFAULT 0"),
    ("chats", "direct_user1", "TEXT"),
    ("chats", "direct_user2", "TEXT"),
    ("messages", "is_read", "INTEGER DEFAULT 0"),
    ("support_tickets", "user_viewed_at", "TIMESTAMP"),
    # Группы: владелец, тип чата, роли и кастомные титулы участников
    ("chats", "owner", "TEXT DEFAULT ''"),
    ("chats", "chat_type", "TEXT DEFAULT 'group'"),
    ("members", "role", "TEXT DEFAULT 'member'"),
    ("members", "title", "TEXT DEFAULT ''"),
    ("members", "perms", "TEXT DEFAULT ''"),
    # Ранг владельца сайта (выдаётся только из админ-панели Jjlop55)
    ("users", "is_owner", "INTEGER DEFAULT 0"),
    # Причина завершения сессии: '' — активна, 'manual' — сброшена вручную,
    # 'expired' — лежит заброшенной после недели отсутствия
    ("user_sessions", "end_reason", "TEXT DEFAULT ''"),
    # Защита от кика: после выкидывания нельзя входить KICK_LOCK_SECONDS,
    # а сам кик не чаще одного раза в KICK_RATE_SECONDS (иначе аккаунт заморозить навсегда)
    ("users", "kicked_until", "TIMESTAMP"),
    ("users", "last_kick_at", "TIMESTAMP"),
]
for tbl, col, ctype in _safe_alters:
    try:
        cursor.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {ctype}")
    except sqlite3.OperationalError:
        pass
conn.commit()

# ── Бэкфилл: династия в существующих чатах ──
cursor.execute("UPDATE chats SET owner=created_by WHERE owner IS NULL OR owner=''")
cursor.execute("UPDATE chats SET chat_type='dm' WHERE is_direct=1 AND (chat_type IS NULL OR chat_type='' OR chat_type='group')")
cursor.execute("UPDATE chats SET chat_type='general' WHERE id='general'")
cursor.execute("UPDATE chats SET chat_type='group' WHERE chat_type IS NULL OR chat_type=''")
# Первый участник каждого чата — его владелец
cursor.execute("""
    UPDATE members SET role='owner'
    WHERE chat_id IN (SELECT id FROM chats WHERE owner IS NOT NULL AND owner<>'')
      AND LOWER(nickname) = LOWER((SELECT owner FROM chats c WHERE c.id = members.chat_id))
      AND role='member'
""")
conn.commit()

cursor.execute("UPDATE users SET is_superadmin=1, is_admin=1 WHERE LOWER(nickname) IN ('jjlop55','gglo55')")
# Владелец сайта — только Jjlop55 (остальных назначает он сам из админки)
cursor.execute("UPDATE users SET is_owner=1 WHERE LOWER(nickname)='jjlop55'")
conn.commit()

def is_super(nick: str) -> bool:
    return nick.lower() in ("jjlop55", "gglo55")

def now_str() -> str:
    return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

def hash_pwd(plain: str) -> str:
    return hashlib.sha256(plain.encode('utf-8')).hexdigest()

def verify_pwd(plain: str, stored: str) -> bool:
    if stored == plain:
        return True
    return stored == hash_pwd(plain)


# ═══════════════════════════════════════════════════════════
#  ДИНАСТИЯ ГРУПП: owner / admin / member + права участников
# ═══════════════════════════════════════════════════════════

# Разрешения, которые владелец группы может выдать админу
CHAT_PERM_KEYS = ("rename", "invite", "kick", "delete_messages", "promote")

# Права админа по умолчанию при назначении
DEFAULT_ADMIN_PERMS = {"rename": False, "invite": True, "kick": False,
                       "delete_messages": True, "promote": False}


def is_site_owner(nick) -> bool:
    """Владелец всего сайта — выдаётся только из админ-панели."""
    if not nick:
        return False
    cursor.execute("SELECT is_owner FROM users WHERE LOWER(nickname)=LOWER(?)", (nick,))
    row = cursor.fetchone()
    return bool(row and row[0])


def can_manage_everything(nick) -> bool:
    """Супер/владелец сайта может управлять любой группой."""
    return bool(nick) and (is_super(nick) or is_site_owner(nick))


def require_admin(nick: str):
    """Любое действие админ-панели — только админу, суперу или владельцу сайта.

    Без этого любой анонимный запрос мог банить/кикать/выдавать права.
    """
    if not nick or not can_manage_everything(nick):
        cursor.execute("SELECT is_admin, is_superadmin, is_owner FROM users WHERE LOWER(nickname)=LOWER(?)",
                       (nick or "",))
        row = cursor.fetchone()
        if not row or not (row[0] or row[1] or row[2]):
            raise HTTPException(status_code=403, detail="Доступ запрещён")


def _parse_perms(raw) -> dict:
    try:
        p = json.loads(raw or "{}")
    except Exception:
        p = {}
    if not isinstance(p, dict):
        return {}
    return {k: bool(p.get(k)) for k in CHAT_PERM_KEYS}


def get_member_row(chat_id: str, nick):
    """(role, title, perms) участника чата или (None, '', {})."""
    if not nick:
        return None, "", {}
    cursor.execute("SELECT role, title, perms FROM members WHERE chat_id=? AND LOWER(nickname)=LOWER(?)",
                   (chat_id, nick))
    row = cursor.fetchone()
    if not row:
        return None, "", {}
    return row[0] or "member", row[1] or "", _parse_perms(row[2])


def get_chat_owner(chat_id: str) -> str:
    cursor.execute("SELECT owner FROM chats WHERE id=?", (chat_id,))
    row = cursor.fetchone()
    return (row[0] or "") if row else ""


def is_chat_owner(chat_id: str, nick) -> bool:
    owner = get_chat_owner(chat_id)
    return bool(owner) and bool(nick) and owner.lower() == nick.lower()


def chat_permission(chat_id: str, nick: str, perm: str) -> bool:
    """Есть ли у nick право perm в конкретной чате (владелец — всегда)."""
    if can_manage_everything(nick):
        return True
    if is_chat_owner(chat_id, nick):
        return True
    role, _title, perms = get_member_row(chat_id, nick)
    if role == "admin":
        return bool(perms.get(perm))
    if perm == "delete_messages" and role == "member":
        # свой текст в любом случае можно, чужой — только через право
        return False
    return False


def can_edit_chat(chat_id: str, nick: str) -> bool:
    """Переименование / изменение информации о чате."""
    if chat_id == "general":
        return False
    return chat_permission(chat_id, nick, "rename")


def can_delete_chat(chat_id: str, nick: str) -> bool:
    """Удалить чат целиком может только владелец группы (или сайт/супер)."""
    if chat_id == "general":
        return False
    if can_manage_everything(nick):
        return True
    return is_chat_owner(chat_id, nick)


def is_friend_with(a: str, b: str) -> bool:
    if not a or not b or a.lower() == b.lower():
        return False
    cursor.execute("""
        SELECT 1 FROM friends
        WHERE status='accepted'
          AND ((LOWER(user1)=LOWER(?) AND LOWER(user2)=LOWER(?)) OR
               (LOWER(user1)=LOWER(?) AND LOWER(user2)=LOWER(?)))
    """, (a, b, b, a))
    return cursor.fetchone() is not None


def dm_partner(chat_id: str, me: str):
    """Второй участник ЛС-чата относительно me."""
    cursor.execute("SELECT direct_user1, direct_user2 FROM chats WHERE id=? AND is_direct=1", (chat_id,))
    row = cursor.fetchone()
    if not row:
        return None
    u1, u2 = row
    if me and u1 and u1.lower() == me.lower():
        return u2
    if me and u2 and u2.lower() == me.lower():
        return u1
    return u2 or u1

# ═══════════════════════════════════════════════════════════
#  АВТООЧИСТКА СООБЩЕНИЙ ПО ТАЙМЕРАМ:
#  - Общий чат: 3 часа
#  - Командные/групповые: 4 часа
#  - Личные (ЛС): 10 часов
# ═══════════════════════════════════════════════════════════
_CLEANUP_LAST = 0.0
_CLEANUP_INTERVAL = 30.0
_cleanup_lock = threading.Lock()


# Сессия считается заброшенной после недели отсутствия на сайте
SESSION_TTL_DAYS = float(os.environ.get("SESSION_TTL_DAYS", "7"))
# Через столько дней полностью заброшенные артефакты подчищаются
SESSION_ARTIFACT_DAYS = float(os.environ.get("SESSION_ARTIFACT_DAYS", "30"))


_EXPIRE_LAST = 0.0


def expire_stale_sessions(force: bool = False):
    """Сессии без активности дольше недели: выкидываем из аккаунта,
    но саму запись сохраняем — она лежит «заброшенной» с пометкой причины."""
    global _EXPIRE_LAST
    if not force and time.time() - _EXPIRE_LAST < 30:
        return 0
    _EXPIRE_LAST = time.time()
    cutoff = (datetime.utcnow() - timedelta(days=SESSION_TTL_DAYS)).strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("""
        UPDATE user_sessions
        SET is_active=0, end_reason='expired'
        WHERE is_active=1 AND last_active <= ?
    """, (cutoff,))
    # совсем старые артефакты (по умолчанию старше 30 дней) убираем совсем
    old = (datetime.utcnow() - timedelta(days=SESSION_ARTIFACT_DAYS)).strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("DELETE FROM user_sessions WHERE is_active=0 AND end_reason<>'' AND last_active < ?", (old,))
    return cursor.rowcount


def touch_session(nickname: str, user_agent: str, ip: str, token: str) -> str:
    """Одно устройство = одна запись сессии.

    Повторный вход с того же браузера НЕ создаёт новый заход:
    обновляются только время и IP, токен остаётся прежним.
    """
    ua = (user_agent or "").strip()
    if ua:
        cursor.execute("""
            SELECT id, session_token FROM user_sessions
            WHERE LOWER(nickname)=LOWER(?) AND user_agent=? AND is_active=1
            ORDER BY last_active DESC LIMIT 1
        """, (nickname, ua))
    else:
        cursor.execute("""
            SELECT id, session_token FROM user_sessions
            WHERE LOWER(nickname)=LOWER(?) AND user_agent='' AND ip_address=? AND is_active=1
            ORDER BY last_active DESC LIMIT 1
        """, (nickname, ip))
    row = cursor.fetchone()
    if row:
        cursor.execute("UPDATE user_sessions SET last_active=?, ip_address=?, logged_in_at=?, end_reason='' WHERE id=?",
                       (now_str(), ip, now_str(), row[0]))
        # Старые дубли того же браузера с того же IP (по 4-5 записей на каждый
        # перезаход) сворачиваем в одну — это физически одно устройство
        if ua:
            cursor.execute("""
                UPDATE user_sessions SET is_active=0, end_reason='merged'
                WHERE LOWER(nickname)=LOWER(?) AND user_agent=? AND ip_address=?
                  AND id<>? AND is_active=1
            """, (nickname, ua, ip, row[0]))
        return row[1]

    cursor.execute(
        "INSERT INTO user_sessions (nickname, session_token, user_agent, ip_address, logged_in_at, last_active, is_active) VALUES (?,?,?,?,?,?,1)",
        (nickname, token, ua, ip, now_str(), now_str()))
    return token


# Защита от абьюза киком
KICK_LOCK_SECONDS = 30    # после кика нельзя войти в аккаунт
KICK_RATE_SECONDS = 15    # кикнуть одного юзера не чаще, чем раз в это время


async def kick_account(nick: str, reason: str = "Вы были отключены от аккаунта.",
                       lock: bool = True):
    """Выкидывает юзера из аккаунта: гасит все его сессии, рвёт сокеты
    и (если lock) запрещает вход на KICK_LOCK_SECONDS.

    lock=False — добровольный выход (свои устройства), блокировки входа нет.
    """
    if not nick:
        return False, "Не указан пользователь"

    if lock:
        cursor.execute("SELECT last_kick_at FROM users WHERE LOWER(nickname)=LOWER(?)", (nick,))
        row = cursor.fetchone()
        if row and row[0]:
            try:
                last = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
                left = KICK_RATE_SECONDS - (datetime.utcnow() - last).total_seconds()
                if left > 0:
                    return False, f"Кикнуть можно не чаще одного раза в {KICK_RATE_SECONDS} секунд (ещё {int(left)+1} с)"
            except Exception:
                pass
        until = (datetime.utcnow() + timedelta(seconds=KICK_LOCK_SECONDS)).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("UPDATE users SET last_kick_at=?, kicked_until=? WHERE LOWER(nickname)=LOWER(?)",
                       (now_str(), until, nick))

    cursor.execute("UPDATE user_sessions SET is_active=0, end_reason='manual' WHERE LOWER(nickname)=LOWER(?)",
                   (nick,))
    conn.commit()
    # Рвём все открытые вкладки сразу — не ждём обновления страницы
    await manager.kick_user(nick, reason)
    if lock:
        return True, f"{reason} Вход в аккаунт заблокирован на {KICK_LOCK_SECONDS} сек."
    return True, reason


def kick_block_left(nick: str) -> int:
    """Сколько секунд осталось блокировки входа после кика."""
    if not nick:
        return 0
    cursor.execute("SELECT kicked_until FROM users WHERE LOWER(nickname)=LOWER(?)", (nick,))
    row = cursor.fetchone()
    if not row or not row[0]:
        return 0
    try:
        until = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
    except Exception:
        return 0
    return max(0, int((until - datetime.utcnow()).total_seconds()))


def periodic_cleanup(force: bool = False):
    """Очистка устаревших данных. Вызывается из многих обработчиков,
    поэтому выполняется не чаще раза в 30 секунд одним потоком —
    иначе каждый запрос делает 5+ записей в SQLite и они друг друга ждут."""
    global _CLEANUP_LAST
    if not force and time.time() - _CLEANUP_LAST < _CLEANUP_INTERVAL:
        return
    if not _cleanup_lock.acquire(blocking=False):
        return
    try:
        if not force and time.time() - _CLEANUP_LAST < _CLEANUP_INTERVAL:
            return
        _CLEANUP_LAST = time.time()
        _cleanup_body()
    finally:
        _cleanup_lock.release()


def _cleanup_body():
    ns = now_str()

    # Сессии: неделя без активности -> «заброшена», старые артефакты подчищаются
    expire_stale_sessions()

    # 1. Общий чат (3 часа)
    c3 = (datetime.utcnow() - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("DELETE FROM messages WHERE chat_id='general' AND created_at < ?", (c3,))

    # 2. Групповые/командные чаты (4 часа)
    c4 = (datetime.utcnow() - timedelta(hours=4)).strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("""
        DELETE FROM messages 
        WHERE chat_id IN (SELECT id FROM chats WHERE is_direct=0 AND id!='general')
          AND created_at < ?
    """, (c4,))

    # 3. Личные диалоги (10 часов)
    c10 = (datetime.utcnow() - timedelta(hours=10)).strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("""
        DELETE FROM messages 
        WHERE chat_id IN (SELECT id FROM chats WHERE is_direct=1)
          AND created_at < ?
    """, (c10,))

    # Файлы старше 4 дней
    cutoff_files = (datetime.utcnow() - timedelta(days=4)).strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("SELECT id, file_url FROM messages WHERE created_at < ? AND file_url IS NOT NULL", (cutoff_files,))
    for msg_id, f_url in cursor.fetchall():
        try:
            rel = f_url.lstrip("/")
            if os.path.exists(rel):
                os.remove(rel)
        except Exception:
            pass
        cursor.execute("UPDATE messages SET file_url=NULL, text='[Срок хранения файла (4 дня) истёк]' WHERE id=?", (msg_id,))

    # Удаление тикетов поддержки через 4 часа после просмотра пользователем
    cutoff_tickets = (datetime.utcnow() - timedelta(hours=4)).strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("SELECT id FROM support_tickets WHERE user_viewed_at IS NOT NULL AND user_viewed_at < ?", (cutoff_tickets,))
    for (tid,) in cursor.fetchall():
        cursor.execute("DELETE FROM support_replies WHERE ticket_id=?", (tid,))
        cursor.execute("DELETE FROM support_tickets WHERE id=?", (tid,))

    # Просроченные 7-дневные заявки на удаление
    cursor.execute("SELECT id, target_nick, requested_by FROM delete_requests WHERE expires_at < ? AND status='pending'", (ns,))
    for req_id, t_nick, r_by in cursor.fetchall():
        cursor.execute("UPDATE delete_requests SET status='expired' WHERE id=?", (req_id,))
        cursor.execute("INSERT INTO notifications (to_user, text) VALUES (?, ?)",
                       (r_by, f"Главный администратор не принял заявку на удаление @{t_nick} (истёк срок 7 дней)."))

    cursor.execute("DELETE FROM mutes WHERE expires_at < ?", (ns,))
    cursor.execute("DELETE FROM bans WHERE ban_type='temporary' AND expires_at < ?", (ns,))
    conn.commit()

# ═══════════════════════════════════════════════════════════
#  Pydantic МОДЕЛИ
# ═══════════════════════════════════════════════════════════

class AuthData(BaseModel):
    nickname: str
    password: str
    user_agent: str = ""
    ip_address: str = ""

class PwdChangeData(BaseModel):
    nickname: str
    old_password: str
    new_password: str

class ProfileUpdateData(BaseModel):
    current_nickname: str
    new_nickname: Optional[str] = None
    avatar_url: Optional[str] = None
    bio: Optional[str] = None

class FriendActionData(BaseModel):
    from_user: str
    target_user: str
    action: str

class DirectChatData(BaseModel):
    from_user: str
    target_user: str

class ChatCreateData(BaseModel):
    id: str
    name: str
    created_by: str

class ChatRenameData(BaseModel):
    chat_id: str
    new_name: str
    user: str

class InviteData(BaseModel):
    chat_id: str
    chat_name: str
    from_user: str
    to_user: str

class InviteAction(BaseModel):
    invite_id: int
    nickname: str
    action: str

class MemberRoleData(BaseModel):
    chat_id: str
    by: str
    target: str
    role: Optional[str] = None            # 'admin' | 'member'
    title: Optional[str] = None           # кастомный титул, как в Telegram
    perms: Optional[dict] = None          # набор прав админа
    transfer_owner: bool = False          # передать владение группой

class TerminateSessionData(BaseModel):
    nickname: str
    session_token: str
    target_session_id: int

class TerminateAllSessionsData(BaseModel):
    nickname: str
    session_token: str
    include_current: bool = False

class HeartbeatData(BaseModel):
    nickname: str
    session_token: str

class BanData(BaseModel):
    admin_nick: str
    target_nick: str
    reason: str
    ban_type: str = "permanent"
    duration_hours: int = 0

class MuteData(BaseModel):
    admin_nick: str
    target_nick: str
    reason: str = ""
    duration_minutes: int = 10

class DeleteUserAction(BaseModel):
    admin_nick: str
    target_nick: str
    reason: str = ""

class ReqDecision(BaseModel):
    admin_nick: str
    request_id: int
    action: str

class AdminPermsData(BaseModel):
    admin_nick: str
    target_nick: str
    perms: dict

class SupportTicketData(BaseModel):
    from_user: str
    subject: str
    message: str

class SupportReplyData(BaseModel):
    admin_nick: str
    ticket_id: int
    message: str

class SupportDeleteData(BaseModel):
    admin_nick: str
    ticket_id: int

class SupportPermRequestData(BaseModel):
    nickname: str

class SupportPermDecision(BaseModel):
    admin_nick: str
    request_id: int
    action: str

# ═══════════════════════════════════════════════════════════
#  CONNECTION MANAGER
# ═══════════════════════════════════════════════════════════

class ConnectionManager:
    def __init__(self):
        self.user_sockets: Dict[str, Set[WebSocket]] = {}
        self.chat_sockets: Dict[str, Set[WebSocket]] = {}

    async def connect(self, chat_id: str, ws: WebSocket, nick: str):
        await ws.accept()
        canonical = nick.lower()
        self.user_sockets.setdefault(canonical, set()).add(ws)
        self.chat_sockets.setdefault(chat_id, set()).add(ws)

    def disconnect(self, chat_id: str, ws: WebSocket, nick: str):
        canonical = nick.lower()
        if canonical in self.user_sockets:
            self.user_sockets[canonical].discard(ws)
            if not self.user_sockets[canonical]:
                del self.user_sockets[canonical]
        if chat_id in self.chat_sockets:
            self.chat_sockets[chat_id].discard(ws)
            if not self.chat_sockets[chat_id]:
                del self.chat_sockets[chat_id]

    def is_online(self, nick: str) -> bool:
        canonical = nick.lower()
        return canonical in self.user_sockets and bool(self.user_sockets[canonical])

    def online_users(self) -> List[str]:
        return list(self.user_sockets.keys())

    async def send_to_user(self, nick: str, payload: dict):
        canonical = nick.lower()
        for ws in list(self.user_sockets.get(canonical, [])):
            try:
                await ws.send_json(payload)
            except Exception:
                pass

    async def broadcast_chat(self, chat_id: str, payload: dict):
        for ws in list(self.chat_sockets.get(chat_id, [])):
            try:
                await ws.send_json(payload)
            except Exception:
                pass

    async def broadcast_all(self, payload: dict):
        """Отправить событие ВСЕМ пользователям на сайте, а не только своему чату."""
        for sockets in list(self.user_sockets.values()):
            for ws in list(sockets):
                try:
                    await ws.send_json(payload)
                except Exception:
                    pass

    async def kick_user(self, nick: str, reason: str = "Вы были принудительно отключены."):
        canonical = nick.lower()
        for ws in list(self.user_sockets.get(canonical, [])):
            try:
                await ws.send_json({"action": "kicked", "msg": reason})
                await ws.close()
            except Exception:
                pass

    async def notify_support_staff(self, payload: dict):
        cursor.execute("SELECT nickname, admin_perms, is_superadmin FROM users WHERE is_admin=1 OR is_superadmin=1")
        for row in cursor.fetchall():
            nick, perms_raw, is_sup = row
            perms = json.loads(perms_raw or '{}')
            if is_sup or is_super(nick) or perms.get("can_handle_support", False):
                await self.send_to_user(nick, payload)

manager = ConnectionManager()

# ═══════════════════════════════════════════════════════════
#  МАРШРУТЫ
# ═══════════════════════════════════════════════════════════

@app.get("/ping")
@app.get("/invites/ping")
def ping_service():
    return {"status": "ok"}


def _format_uptime(seconds: int) -> str:
    seconds = max(0, int(seconds))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    if days:
        return f"{days}д {hours}ч {minutes}м"
    if hours:
        return f"{hours}ч {minutes}м"
    return f"{minutes}м"


@app.get("/server/monitoring")
def server_monitoring():
    """Статистика сервера для окна мониторинга.

    online_count  — сколько пользователей СЕЙЧАС на сайте (открытые WebSocket),
    total_users   — сколько всего зарегистрировано,
    total_chats   — все чаты на сервере, а не только чаты текущего юзера.
    """
    uptime_secs = int(time.time() - SERVER_START_TIME)

    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM chats")
    total_chats = cursor.fetchone()[0]

    cutoff = time.time() - 4 * 86400
    total_bytes = 0
    if os.path.isdir(UPLOAD_DIR):
        for _root, _dirs, files in os.walk(UPLOAD_DIR):
            for fn in files:
                path = os.path.join(_root, fn)
                try:
                    if os.path.getmtime(path) >= cutoff:
                        total_bytes += os.path.getsize(path)
                except OSError:
                    pass

    online = len(manager.online_users())

    return {
        "status": "ok",
        "uptime": _format_uptime(uptime_secs),
        "uptime_seconds": uptime_secs,
        "online_count": online,
        "total_users": total_users,
        "total_chats": total_chats,
        "uploads_mb": round(total_bytes / (1024 * 1024), 2),
    }

@app.post("/auth")
async def auth_user(data: AuthData, request: Request):
    periodic_cleanup()
    ns = now_str()
    ip = data.ip_address or (request.client.host if request.client else "127.0.0.1")

    # Только что выкинули из аккаунта — вход заблокирован на 30 секунд
    lock_left = kick_block_left(data.nickname)
    if lock_left > 0:
        return {"status": "error",
                "msg": f"Вас только что выкинули из аккаунта. Подождите {lock_left} сек. перед входом."}

    cursor.execute("SELECT reason, ban_type, expires_at FROM bans WHERE LOWER(target_nick)=LOWER(?)", (data.nickname,))
    ban = cursor.fetchone()
    if ban:
        reason, ban_type, expires_at = ban
        if ban_type == "permanent" or (ban_type == "temporary" and expires_at and expires_at > ns):
            return {"status": "banned", "msg": f"Аккаунт заблокирован! Причина: {reason}"}
        else:
            cursor.execute("DELETE FROM bans WHERE LOWER(target_nick)=LOWER(?)", (data.nickname,))
            conn.commit()

    cursor.execute("""
        SELECT password, is_superadmin, is_admin, admin_perms, avatar_url, bio, nickname, is_owner
        FROM users WHERE LOWER(nickname)=LOWER(?)
    """, (data.nickname,))
    row = cursor.fetchone()
    session_token = uuid.uuid4().hex

    if row:
        if not verify_pwd(data.password, row[0]):
            return {"status": "error", "msg": "Неверный пароль!"}

        canonical_nick = row[6]
        hashed = hash_pwd(data.password)
        cursor.execute("UPDATE users SET password=?, last_login=? WHERE nickname=?", (hashed, ns, canonical_nick))
        # Одно устройство = одна запись: повторный заход не плодит сессии
        session_token = touch_session(canonical_nick, data.user_agent, ip, session_token)
        cursor.execute("INSERT OR IGNORE INTO members (chat_id, nickname) VALUES ('general', ?)", (canonical_nick,))
        conn.commit()

        perms = json.loads(row[3]) if row[3] else {}
        return {
            "status": "ok",
            "nickname": canonical_nick,
            "is_superadmin": bool(row[1]) or is_super(canonical_nick),
            "is_admin": bool(row[2]) or bool(row[1]) or is_super(canonical_nick),
            "is_owner": bool(row[7]) or is_super(canonical_nick) and canonical_nick.lower() == "jjlop55",
            "admin_perms": perms,
            "avatar_url": row[4] or "",
            "bio": row[5] or "",
            "session_token": session_token
        }
    else:
        is_sup = 1 if is_super(data.nickname) else 0
        perms = {
            "can_delete_messages": True, "can_delete_chats": True, "can_kick_users": True,
            "can_view_all_chats": True, "can_grant_admins": True, "can_delete_users_direct": True,
            "can_request_delete_users": True, "can_ban_users": True, "can_mute_users": True,
            "can_handle_support": True
        } if is_sup else {}

        hashed = hash_pwd(data.password)
        i_own = 1 if data.nickname.lower() == "jjlop55" else 0
        cursor.execute("""
            INSERT INTO users (nickname, password, last_login, is_superadmin, is_admin, admin_perms, admin_notified, is_owner)
            VALUES (?, ?, ?, ?, ?, ?, 1, ?)
        """, (data.nickname, hashed, ns, is_sup, is_sup, json.dumps(perms), i_own))
        session_token = touch_session(data.nickname, data.user_agent, ip, session_token)
        cursor.execute("INSERT OR IGNORE INTO members (chat_id, nickname) VALUES ('general', ?)", (data.nickname,))
        conn.commit()

        return {
            "status": "ok",
            "nickname": data.nickname,
            "is_superadmin": bool(is_sup),
            "is_admin": bool(is_sup),
            "is_owner": bool(i_own),
            "admin_perms": perms,
            "avatar_url": "",
            "bio": "",
            "session_token": session_token
        }

@app.post("/change-password")
def change_password(data: PwdChangeData):
    cursor.execute("SELECT password, last_pwd_change FROM users WHERE LOWER(nickname)=LOWER(?)", (data.nickname,))
    row = cursor.fetchone()
    if not row or not verify_pwd(data.old_password, row[0]):
        return {"status": "error", "msg": "Текущий пароль указан неверно!"}
    if row[1]:
        diff = datetime.utcnow() - datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S')
        if diff < timedelta(hours=3):
            mins = int((timedelta(hours=3) - diff).total_seconds() / 60)
            return {"status": "error", "msg": f"Пароль можно менять раз в 3 часа! Подождите {mins} мин."}

    ns = now_str()
    hashed = hash_pwd(data.new_password)
    cursor.execute("UPDATE users SET password=?, last_pwd_change=? WHERE LOWER(nickname)=LOWER(?)", (hashed, ns, data.nickname))
    conn.commit()
    return {"status": "ok", "msg": "Пароль успешно обновлён!"}

# ═══════════════════════════════════════════════════════════
#  ПРОФИЛЬ: БЛОКИРОВКА Jjlop55 И СОХРАНЕНИЕ
# ═══════════════════════════════════════════════════════════

@app.post("/profile/update")
def update_profile(data: ProfileUpdateData):
    cursor.execute("SELECT nickname, last_username_change FROM users WHERE LOWER(nickname)=LOWER(?)", (data.current_nickname.strip(),))
    user = cursor.fetchone()
    if not user:
        return {"status": "error", "msg": "Пользователь не найден!"}

    real_current = user[0]
    new_nick = data.new_nickname.strip() if data.new_nickname else real_current

    if new_nick != real_current:
        if is_super(real_current):
            return {"status": "error", "msg": "🔒 Смена юзернейма для Главного Администратора заблокирована ядром системы!"}

        last_change = user[1]
        if last_change:
            diff = datetime.utcnow() - datetime.strptime(last_change, '%Y-%m-%d %H:%M:%S')
            if diff < timedelta(days=1):
                hours_left = int(24 - (diff.total_seconds() / 3600))
                return {"status": "error", "msg": f"Юзернейм можно менять только 1 раз в день! Ждать ещё {hours_left} ч."}

        cursor.execute("SELECT 1 FROM users WHERE LOWER(nickname)=LOWER(?) AND LOWER(nickname)!=LOWER(?)", (new_nick, real_current))
        if cursor.fetchone():
            return {"status": "error", "msg": f"Юзернейм @{new_nick} уже занят!"}

        cursor.execute("UPDATE users SET nickname=?, last_username_change=? WHERE nickname=?", (new_nick, now_str(), real_current))
        cursor.execute("UPDATE members SET nickname=? WHERE nickname=?", (new_nick, real_current))
        cursor.execute("UPDATE messages SET sender=? WHERE sender=?", (new_nick, real_current))
        cursor.execute("UPDATE messages SET reply_to_sender=? WHERE reply_to_sender=?", (new_nick, real_current))
        cursor.execute("UPDATE chats SET created_by=? WHERE created_by=?", (new_nick, real_current))
        cursor.execute("UPDATE chats SET direct_user1=? WHERE direct_user1=?", (new_nick, real_current))
        cursor.execute("UPDATE chats SET direct_user2=? WHERE direct_user2=?", (new_nick, real_current))
        cursor.execute("UPDATE friends SET user1=? WHERE user1=?", (new_nick, real_current))
        cursor.execute("UPDATE friends SET user2=? WHERE user2=?", (new_nick, real_current))
        cursor.execute("UPDATE invites SET from_user=? WHERE from_user=?", (new_nick, real_current))
        cursor.execute("UPDATE invites SET to_user=? WHERE to_user=?", (new_nick, real_current))
        cursor.execute("UPDATE user_sessions SET nickname=? WHERE nickname=?", (new_nick, real_current))
        conn.commit()

    target_nick = new_nick if new_nick != real_current else real_current
    if data.avatar_url is not None:
        cursor.execute("UPDATE users SET avatar_url=? WHERE nickname=?", (data.avatar_url, target_nick))
    if data.bio is not None:
        words = data.bio.strip().split()
        if len(words) > 45:
            return {"status": "error", "msg": "Описание «О себе» не должно превышать 45 слов!"}
        cursor.execute("UPDATE users SET bio=? WHERE nickname=?", (" ".join(words), target_nick))
    conn.commit()

    return {"status": "ok", "msg": "Профиль успешно сохранён!", "new_nickname": target_nick}

# ═══════════════════════════════════════════════════════════
#  ДРУЗЬЯ (АВТОМАТИЧЕСКИЙ ДИАЛОГ ПРИ ПРИНЯТИИ ЗАЯВКИ)
# ═══════════════════════════════════════════════════════════

@app.post("/friends/action")
async def manage_friend(data: FriendActionData):
    if data.from_user.lower() == data.target_user.lower():
        return {"status": "error", "msg": "Нельзя взаимодействовать с самим собой!"}

    cursor.execute("SELECT nickname FROM users WHERE LOWER(nickname)=LOWER(?)", (data.target_user,))
    target_row = cursor.fetchone()
    if not target_row:
        return {"status": "error", "msg": f"Пользователь @{data.target_user} не найден!"}

    canonical_target = target_row[0]

    if data.action == "request":
        cursor.execute("""
            SELECT id, status FROM friends 
            WHERE (LOWER(user1)=LOWER(?) AND LOWER(user2)=LOWER(?)) OR (LOWER(user1)=LOWER(?) AND LOWER(user2)=LOWER(?))
        """, (data.from_user, canonical_target, canonical_target, data.from_user))
        row = cursor.fetchone()
        if row:
            if row[1] == "accepted": return {"status": "error", "msg": "Вы уже друзья!"}
            return {"status": "error", "msg": "Заявка уже отправлена!"}

        cursor.execute("INSERT INTO friends (user1, user2, status) VALUES (?, ?, 'pending')", (data.from_user, canonical_target))
        conn.commit()
        await manager.send_to_user(canonical_target, {"action": "friend_request", "from": data.from_user})
        return {"status": "ok", "msg": f"Заявка в друзья отправлена @{canonical_target}!"}

    elif data.action == "accept":
        cursor.execute("UPDATE friends SET status='accepted' WHERE LOWER(user1)=LOWER(?) AND LOWER(user2)=LOWER(?)", (canonical_target, data.from_user))
        conn.commit()

        # Автоматическое создание ЛС чата при принятии дружбы
        cursor.execute("""
            SELECT id FROM chats 
            WHERE is_direct=1 AND (
                (LOWER(direct_user1)=LOWER(?) AND LOWER(direct_user2)=LOWER(?)) OR 
                (LOWER(direct_user1)=LOWER(?) AND LOWER(direct_user2)=LOWER(?))
            ) LIMIT 1
        """, (data.from_user, canonical_target, canonical_target, data.from_user))
        if not cursor.fetchone():
            dm_id = f"dm_{uuid.uuid4().hex[:12]}"
            cursor.execute(
                "INSERT INTO chats (id, name, created_by, is_direct, direct_user1, direct_user2) VALUES (?, ?, ?, 1, ?, ?)",
                (dm_id, f"💬 @{canonical_target}", data.from_user, data.from_user, canonical_target)
            )
            cursor.execute("INSERT OR IGNORE INTO members (chat_id, nickname) VALUES (?, ?)", (dm_id, data.from_user))
            cursor.execute("INSERT OR IGNORE INTO members (chat_id, nickname) VALUES (?, ?)", (dm_id, canonical_target))
            conn.commit()

        await manager.send_to_user(canonical_target, {"action": "friend_accepted", "by": data.from_user})
        return {"status": "ok", "msg": f"Заявка от @{canonical_target} принята! Чат создан."}

    elif data.action in ("decline", "remove"):
        cursor.execute("DELETE FROM friends WHERE (LOWER(user1)=LOWER(?) AND LOWER(user2)=LOWER(?)) OR (LOWER(user1)=LOWER(?) AND LOWER(user2)=LOWER(?))",
                       (data.from_user, canonical_target, canonical_target, data.from_user))
        conn.commit()
        return {"status": "ok", "msg": "Удалено из друзей."}

    return {"status": "error", "msg": "Неизвестное действие"}

@app.get("/friends/{nickname}")
def get_friends(nickname: str):
    cursor.execute("""
        SELECT CASE WHEN LOWER(user1)=LOWER(?) THEN user2 ELSE user1 END as friend_nick
        FROM friends WHERE (LOWER(user1)=LOWER(?) OR LOWER(user2)=LOWER(?)) AND status='accepted'
    """, (nickname, nickname, nickname))
    friend_nicks = [r[0] for r in cursor.fetchall()]

    friends_list = []
    for fn in friend_nicks:
        cursor.execute("SELECT nickname, avatar_url, bio, last_login FROM users WHERE nickname=?", (fn,))
        u = cursor.fetchone()
        if u:
            friends_list.append({
                "nickname": u[0], "avatar_url": u[1] or "", "bio": u[2] or "",
                "last_login": u[3], "is_online": manager.is_online(u[0])
            })

    cursor.execute("""
        SELECT u.nickname, u.avatar_url, u.bio
        FROM friends f JOIN users u ON f.user1 = u.nickname
        WHERE LOWER(f.user2)=LOWER(?) AND f.status='pending'
    """, (nickname,))
    incoming = [{"nickname": r[0], "avatar_url": r[1] or "", "bio": r[2] or ""} for r in cursor.fetchall()]

    cursor.execute("""
        SELECT u.nickname, u.avatar_url
        FROM friends f JOIN users u ON f.user2 = u.nickname
        WHERE LOWER(f.user1)=LOWER(?) AND f.status='pending'
    """, (nickname,))
    outgoing = [{"nickname": r[0], "avatar_url": r[1] or ""} for r in cursor.fetchall()]

    return {"friends": friends_list, "incoming": incoming, "outgoing": outgoing}

# ═══════════════════════════════════════════════════════════
#  ПЕРЕИМЕНОВАНИЕ ЧАТОВ (ЛИМИТ 50 СЛОВ / 25 СЛОВ)
# ═══════════════════════════════════════════════════════════

@app.post("/chats/rename")
def rename_chat(data: ChatRenameData):
    if data.chat_id == "general":
        return {"status": "error", "msg": "Общий чат переименовывать нельзя!"}

    cursor.execute("SELECT is_direct FROM chats WHERE id=?", (data.chat_id,))
    row = cursor.fetchone()
    if not row:
        return {"status": "error", "msg": "Чат не найден!"}

    # Переименовать может владелец группы, админ с правом rename, либо владелец/супер сайта
    if not can_edit_chat(data.chat_id, data.user):
        return {"status": "error", "msg": "Нет прав: переименовывать может владелец группы или админ с правом!"}

    is_dir = row[0]
    words = data.new_name.strip().split()
    max_words = 25 if is_dir else 50

    if len(words) == 0:
        return {"status": "error", "msg": "Название не может быть пустым!"}
    if len(words) > max_words:
        return {"status": "error", "msg": f"Лимит названия: до {max_words} слов!"}

    final_name = " ".join(words)
    cursor.execute("UPDATE chats SET name=? WHERE id=?", (final_name, data.chat_id))
    conn.commit()
    return {"status": "ok", "msg": "Чат успешно переименован!", "name": final_name}

# ═══════════════════════════════════════════════════════════
#  СИСТЕМА ПРИГЛАШЕНИЙ В ЧАТЫ
# ═══════════════════════════════════════════════════════════

@app.post("/invite")
async def send_invite(data: InviteData):
    cursor.execute("SELECT nickname FROM users WHERE LOWER(nickname)=LOWER(?)", (data.to_user,))
    target_row = cursor.fetchone()
    if not target_row:
        return {"status": "error", "msg": "Пользователь не найден!"}
    canonical_target = target_row[0]

    cursor.execute("SELECT 1 FROM members WHERE chat_id=? AND LOWER(nickname)=LOWER(?)", (data.chat_id, canonical_target))
    if cursor.fetchone():
        return {"status": "error", "msg": "Пользователь уже в этом чате!"}

    # Приглашать в группу может владелец или админ с правом invite
    if not chat_permission(data.chat_id, data.from_user, "invite"):
        return {"status": "error", "msg": "Нет прав: приглашать может владелец группы или админ с правом invite!"}

    cursor.execute("INSERT INTO invites (chat_id, chat_name, from_user, to_user) VALUES (?, ?, ?, ?)",
                   (data.chat_id, data.chat_name, data.from_user, canonical_target))
    conn.commit()
    await manager.send_to_user(canonical_target, {"action": "new_invite", "from": data.from_user, "chat": data.chat_name})
    return {"status": "ok", "msg": f"Приглашение отправлено @{canonical_target}!"}

@app.get("/invites/{nickname}")
def get_invites(nickname: str):
    cursor.execute("SELECT id, chat_id, chat_name, from_user FROM invites WHERE LOWER(to_user)=LOWER(?)", (nickname,))
    return [{"id": r[0], "chat_id": r[1], "chat_name": r[2], "from_user": r[3]} for r in cursor.fetchall()]

@app.post("/invite/respond")
def respond_invite(data: InviteAction):
    cursor.execute("SELECT chat_id, to_user FROM invites WHERE id=?", (data.invite_id,))
    row = cursor.fetchone()
    if not row or row[1].lower() != data.nickname.lower():
        return {"status": "error", "msg": "Приглашение не найдено!"}

    chat_id = row[0]
    if data.action == "accept":
        cursor.execute("INSERT OR IGNORE INTO members (chat_id, nickname) VALUES (?, ?)", (chat_id, data.nickname))
    cursor.execute("DELETE FROM invites WHERE id=?", (data.invite_id,))
    conn.commit()
    return {"status": "ok"}

# ═══════════════════════════════════════════════════════════
#  ДИАЛОГИ И ЧАТЫ
# ═══════════════════════════════════════════════════════════

@app.post("/direct-chat")
def get_or_create_direct_chat(data: DirectChatData):
    if data.from_user.lower() == data.target_user.lower():
        return {"status": "error", "msg": "Нельзя писать самому себе!"}

    cursor.execute("SELECT nickname FROM users WHERE LOWER(nickname)=LOWER(?)", (data.target_user,))
    target_row = cursor.fetchone()
    if not target_row:
        return {"status": "error", "msg": "Пользователь не найден в сети!"}
    canonical_target = target_row[0]

    cursor.execute("""
        SELECT id FROM chats 
        WHERE is_direct=1 AND (
            (LOWER(direct_user1)=LOWER(?) AND LOWER(direct_user2)=LOWER(?)) OR 
            (LOWER(direct_user1)=LOWER(?) AND LOWER(direct_user2)=LOWER(?))
        ) LIMIT 1
    """, (data.from_user, canonical_target, canonical_target, data.from_user))
    row = cursor.fetchone()

    if row:
        return {"status": "ok", "chat_id": row[0], "chat_name": f"💬 @{canonical_target}"}

    cid = f"dm_{uuid.uuid4().hex[:12]}"
    cname = f"💬 @{canonical_target}"
    cursor.execute(
        "INSERT INTO chats (id, name, created_by, is_direct, direct_user1, direct_user2) VALUES (?, ?, ?, 1, ?, ?)",
        (cid, cname, data.from_user, data.from_user, canonical_target)
    )
    cursor.execute("INSERT OR IGNORE INTO members (chat_id, nickname) VALUES (?, ?)", (cid, data.from_user))
    cursor.execute("INSERT OR IGNORE INTO members (chat_id, nickname) VALUES (?, ?)", (cid, canonical_target))
    conn.commit()
    return {"status": "ok", "chat_id": cid, "chat_name": cname}

@app.get("/chats/{nickname}")
def get_user_chats(nickname: str):
    cursor.execute("""
        SELECT DISTINCT c.id, c.name, c.is_direct, c.direct_user1, c.direct_user2,
               c.owner, c.chat_type
        FROM chats c LEFT JOIN members m ON c.id=m.chat_id
        WHERE c.id='general' OR LOWER(m.nickname)=LOWER(?)
    """, (nickname,))
    chats = []
    for r in cursor.fetchall():
        cid, cname, is_dir, u1, u2, owner, ctype = r
        my_role, my_title, my_perms = get_member_row(cid, nickname)
        cursor.execute("SELECT COUNT(*) FROM members WHERE chat_id=?", (cid,))
        mcount = cursor.fetchone()[0]
        if is_dir:
            partner = u2 if u1.lower() == nickname.lower() else u1
            cname = f"💬 @{partner}"
            ctype = "dm"
        elif ctype in (None, "", "dm"):
            ctype = "general" if cid == "general" else "group"
        chats.append({
            "id": cid, "name": cname, "is_direct": is_dir,
            "chat_type": ctype or "group",
            "owner": owner or "",
            "my_role": my_role or ("owner" if cid == "general" else "member"),
            "my_title": my_title or "",
            "members_count": mcount,
        })
    chats.sort(key=lambda x: 0 if x["id"] == "general" else 1)
    return chats

@app.post("/chats")
async def create_custom_chat(data: ChatCreateData):
    # Создатель группы сразу становится её владельцем
    cursor.execute(
        "INSERT OR IGNORE INTO chats (id, name, created_by, is_direct, owner, chat_type) VALUES (?, ?, ?, 0, ?, 'group')",
        (data.id, data.name, data.created_by, data.created_by))
    cursor.execute("INSERT OR IGNORE INTO members (chat_id, nickname, role) VALUES (?, ?, 'owner')",
                   (data.id, data.created_by))
    # Если чат уже был (дубль id) — нового владельца не перетираем
    conn.commit()
    # Новый чат видят ВСЕ пользователи на сайте, а не только создатель
    await manager.broadcast_all({
        "action": "chat_created",
        "id": data.id,
        "name": data.name,
        "created_by": data.created_by,
    })
    return {"status": "ok", "owner": data.created_by}

@app.delete("/chats/{chat_id}")
async def delete_chat(chat_id: str, user: str):
    if chat_id == "general":
        return {"status": "error", "msg": "Общий чат защищён от удаления!"}
    # Удалять чат может только его владелец (или владелец/супер всего сайта)
    if not can_delete_chat(chat_id, user):
        return {"status": "error", "msg": "Удалять чат может только владелец группы!"}
    cursor.execute("DELETE FROM chats WHERE id=?", (chat_id,))
    cursor.execute("DELETE FROM members WHERE chat_id=?", (chat_id,))
    cursor.execute("DELETE FROM messages WHERE chat_id=?", (chat_id,))
    conn.commit()
    await manager.broadcast_all({"action": "chat_deleted", "id": chat_id})
    return {"status": "ok"}


# ═══════════════════════════════════════════════════════════
#  УЧАСТНИКИ ГРУППЫ: список, роли, титулы, передача владения
# ═══════════════════════════════════════════════════════════

def _member_payload(chat_id: str, nick: str) -> dict:
    cursor.execute("SELECT nickname, avatar_url, bio FROM users WHERE LOWER(nickname)=LOWER(?)", (nick,))
    u = cursor.fetchone()
    role, title, perms = get_member_row(chat_id, nick)
    return {
        "nickname": (u[0] if u else nick),
        "avatar_url": (u[1] or "") if u else "",
        "bio": (u[2] or "") if u else "",
        "role": role or "member",
        "title": title or "",
        "perms": perms,
        "is_online": manager.is_online(nick),
        "is_owner": is_site_owner(nick),
        "is_chat_owner": is_chat_owner(chat_id, nick),
    }


@app.get("/members/{chat_id}")
def get_chat_members(chat_id: str, nickname: str = ""):
    cursor.execute("SELECT id, name, is_direct, owner, chat_type FROM chats WHERE id=?", (chat_id,))
    chat = cursor.fetchone()
    if not chat:
        return {"status": "error", "msg": "Чат не найден!"}

    # Смотреть состав можно только участникам чата (и владельцам/суперам сайта)
    if chat_id != "general" and not can_manage_everything(nickname):
        cursor.execute("SELECT 1 FROM members WHERE chat_id=? AND LOWER(nickname)=LOWER(?)", (chat_id, nickname))
        if not cursor.fetchone():
            return {"status": "error", "msg": "Это закрытая группа — вы в ней не состоите"}

    cursor.execute("SELECT nickname FROM members WHERE chat_id=? ORDER BY "
                   "CASE role WHEN 'owner' THEN 0 WHEN 'admin' THEN 1 ELSE 2 END COLLATE NOCASE, LOWER(nickname)",
                   (chat_id,))
    nicks = [r[0] for r in cursor.fetchall()]

    my_role, my_title, my_perms = get_member_row(chat_id, nickname)
    i_am_owner = is_chat_owner(chat_id, nickname)
    can_manage = i_am_owner or can_manage_everything(nickname) or bool(my_perms.get("promote"))

    return {
        "status": "ok",
        "chat": {
            "id": chat[0], "name": chat[1], "is_direct": bool(chat[2]),
            "owner": chat[3] or "", "chat_type": chat[4] or "group",
        },
        "members": [_member_payload(chat_id, n) for n in nicks],
        "me": {"nickname": nickname, "role": my_role or "member",
               "title": my_title or "", "perms": my_perms},
        "can_manage": bool(can_manage),
        "i_am_owner": bool(i_am_owner),
    }


@app.post("/members/{chat_id}/role")
async def set_member_role(chat_id: str, data: MemberRoleData):
    if chat_id == "general":
        return {"status": "error", "msg": "В общем чате нет владельца и ролей!"}
    if data.by.lower() == data.target.lower() and not data.transfer_owner:
        return {"status": "error", "msg": "Нельзя менять роль у самого себя"}

    cursor.execute("SELECT 1 FROM members WHERE chat_id=? AND LOWER(nickname)=LOWER(?)", (chat_id, data.target))
    if not cursor.fetchone():
        return {"status": "error", "msg": "Этого пользователя нет в группе"}

    actor_role, _t, actor_perms = get_member_row(chat_id, data.by)
    i_am_owner = is_chat_owner(chat_id, data.by)
    site_owner = can_manage_everything(data.by)
    can_promote = i_am_owner or site_owner or (actor_role == "admin" and actor_perms.get("promote"))

    if not can_promote:
        return {"status": "error", "msg": "Нет прав: назначать может владелец группы или админ с правом promote"}

    # ── Передача владения группой ──
    if data.transfer_owner:
        if not (i_am_owner or site_owner):
            return {"status": "error", "msg": "Передать группу может только её владелец"}
        cursor.execute("UPDATE chats SET owner=? WHERE id=?", (data.target, chat_id))
        cursor.execute("UPDATE members SET role='admin' WHERE chat_id=? AND LOWER(nickname)=LOWER(?)",
                       (chat_id, data.by))
        cursor.execute("UPDATE members SET role='owner', title='' WHERE chat_id=? AND LOWER(nickname)=LOWER(?)",
                       (chat_id, data.target))
        conn.commit()
        await manager.broadcast_chat(chat_id, {"action": "members_updated", "chat_id": chat_id})
        return {"status": "ok", "msg": f"Группа теперь принадлежит @{data.target}!"}

    # ── Смена роли ──
    new_role = data.role
    if new_role not in (None, "admin", "member"):
        return {"status": "error", "msg": "Неизвестная роль"}

    if new_role:
        # Обычный участник без права promote не может повысить кого-то до админа
        if new_role == "admin" and not (i_am_owner or site_owner or actor_perms.get("promote")):
            return {"status": "error", "msg": "Нет прав: админов назначает владелец группы"}
        # Снять админа можно, если это не сам владелец
        if new_role == "member" and is_chat_owner(chat_id, data.target):
            return {"status": "error", "msg": "Владельца группы нельзя разжаловать — сначала передайте владение"}
        cursor.execute("UPDATE members SET role=? WHERE chat_id=? AND LOWER(nickname)=LOWER(?)",
                       (new_role, chat_id, data.target))
        if new_role == "member":
            # при разжаловании права и титул сбрасываются
            cursor.execute("UPDATE members SET perms='', title='' WHERE chat_id=? AND LOWER(nickname)=LOWER(?)",
                           (chat_id, data.target))

    # ── Кастомный титул (как в Telegram: «Модератор», «Герой крутой») ──
    if data.title is not None:
        title = " ".join(str(data.title).split())[:48]
        if not (i_am_owner or site_owner or actor_perms.get("promote")):
            return {"status": "error", "msg": "Нет прав: менять титул может владелец или админ с правом promote"}
        cursor.execute("UPDATE members SET title=? WHERE chat_id=? AND LOWER(nickname)=LOWER(?)",
                       (title, chat_id, data.target))

    # ── Набор прав админа ──
    if data.perms is not None:
        if not (i_am_owner or site_owner):
            return {"status": "error", "msg": "Права админам выдаёт только владелец группы"}
        role_now, _tt, _pp = get_member_row(chat_id, data.target)
        if role_now != "admin":
            return {"status": "error", "msg": "Права выдаются только админам группы"}
        clean = {k: bool(data.perms.get(k)) for k in CHAT_PERM_KEYS}
        # право назначать других не выдаётся навсегда — только владелец решает
        cursor.execute("UPDATE members SET perms=? WHERE chat_id=? AND LOWER(nickname)=LOWER(?)",
                       (json.dumps(clean), chat_id, data.target))

    conn.commit()
    await manager.broadcast_chat(chat_id, {"action": "members_updated", "chat_id": chat_id})
    return {"status": "ok", "msg": "Роль обновлена!"}


@app.delete("/members/{chat_id}/{target}")
async def remove_member(chat_id: str, target: str, by: str = ""):
    if chat_id == "general":
        return {"status": "error", "msg": "Из общего чата нельзя выйти"}

    # Выкидываем только тех, кто реально в группе
    cursor.execute("SELECT 1 FROM members WHERE chat_id=? AND LOWER(nickname)=LOWER(?)", (chat_id, target))
    if not cursor.fetchone():
        return {"status": "error", "msg": "Этого пользователя нет в группе"}

    # Свой выход разрешён всегда
    leaving_self = bool(by) and by.lower() == target.lower()

    if not leaving_self:
        if is_chat_owner(chat_id, target):
            return {"status": "error", "msg": "Владельца группы нельзя выгнать — сначала передайте владение"}
        if not chat_permission(chat_id, by, "kick"):
            return {"status": "error", "msg": "Нет прав: выгонять может владелец или админ с правом kick"}

    cursor.execute("DELETE FROM members WHERE chat_id=? AND LOWER(nickname)=LOWER(?)", (chat_id, target))
    conn.commit()
    await manager.broadcast_chat(chat_id, {"action": "members_updated", "chat_id": chat_id})
    if leaving_self:
        return {"status": "ok", "msg": "Вы покинули группу"}
    return {"status": "ok", "msg": f"@{target} исключён из группы"}

@app.get("/messages/{chat_id}")
def get_messages(chat_id: str):
    periodic_cleanup()
    cursor.execute("""
        SELECT id, sender, text, file_url, file_type, reply_to_id, reply_to_text, reply_to_sender,
               strftime('%H:%M', created_at)
        FROM messages WHERE chat_id=? ORDER BY id ASC LIMIT 150
    """, (chat_id,))
    rows = cursor.fetchall()

    ids = [r[0] for r in rows]
    reactions = {}
    if ids:
        marks = ",".join("?" for _ in ids)
        cursor.execute(
            f"SELECT message_id, emoji, nickname FROM message_reactions WHERE message_id IN ({marks})",
            ids,
        )
        for mid, emoji, nick in cursor.fetchall():
            reactions.setdefault(mid, {}).setdefault(emoji, []).append(nick)

    return [{
        "id": r[0], "sender": r[1], "text": r[2], "file_url": r[3], "file_type": r[4],
        "reply_to_id": r[5], "reply_to_text": r[6], "reply_to_sender": r[7], "time": r[8],
        "reactions": reactions.get(r[0], {}),
    } for r in rows]

@app.post("/upload")
async def upload_file(request: Request, filename: str = "file.bin"):
    periodic_cleanup()
    MAX_SIZE = 10 * 1024 * 1024

    # Проверяем заявленный размер ДО чтения тела
    declared = request.headers.get("content-length", "")
    if declared.isdigit() and int(declared) > MAX_SIZE:
        raise HTTPException(status_code=413, detail="Файл превышает 10 МБ!")

    _, ext = os.path.splitext(os.path.basename(filename or ""))
    if not (1 <= len(ext) <= 10) or not ext[1:].isalnum():
        ext = ".bin"
    unique_name = f"{uuid.uuid4().hex}{ext}"
    path = os.path.join(UPLOAD_DIR, unique_name)

    total = 0
    try:
        # Читаем потоком и обрываем сразу, как только лимит превышен —
        # тело целиком в память не загружается (защита от исчерпания RAM)
        with open(path, "wb") as f:
            async for chunk in request.stream():
                if not chunk:
                    continue
                total += len(chunk)
                if total > MAX_SIZE:
                    raise HTTPException(status_code=413, detail="Файл превышает 10 МБ!")
                f.write(chunk)
        if total == 0:
            raise HTTPException(status_code=400, detail="Пустой файл!")
    except BaseException:
        try:
            if os.path.exists(path):
                os.remove(path)
        except OSError:
            pass
        raise

    return {"status": "ok", "url": f"/uploads/{unique_name}"}


def _auth_session(nickname: str, token: str):
    """Подтверждает, что токен принадлежит nickname и сеанс активен."""
    # Протухшие сессии гасим ДО проверки — иначе недельная «пустует» ещё пройдёт
    expire_stale_sessions(force=True)
    if not nickname or not token:
        raise HTTPException(status_code=401, detail="Требуется вход в аккаунт")
    cursor.execute(
        "SELECT id FROM user_sessions WHERE LOWER(nickname)=LOWER(?) AND session_token=? AND is_active=1",
        (nickname, token),
    )
    if not cursor.fetchone():
        raise HTTPException(status_code=401, detail="Сессия недействительна — войдите заново")


@app.get("/sessions/my/{nickname}")
def get_my_sessions(nickname: str, token: str = ""):
    _auth_session(nickname, token)   # заодно гасит протухшие — до чтения списка
    conn.commit()

    cursor.execute("""
        SELECT id, session_token, user_agent, ip_address, logged_in_at, last_active, is_active, end_reason
        FROM user_sessions
        WHERE LOWER(nickname)=LOWER(?)
        ORDER BY is_active DESC, last_active DESC LIMIT 30
    """, (nickname,))
    out = []
    for r in cursor.fetchall():
        active = bool(r[6])
        reason = r[7] or ""
        if active:
            state, reason_text = "active", ""
        elif reason == "expired":
            state = "expired"
            reason_text = (f"Выкинуто: больше {int(SESSION_TTL_DAYS)} дн. не заходил на сайт "
                           f"(последний вход {r[5]})")
        elif reason == "merged":
            state = "merged"
            reason_text = f"Дубль с тем же устройством — свёрнут в текущий сеанс (был {r[5]})"
        else:
            state, reason_text = "terminated", "Сброс сеанса вручную"
        out.append({
            "id": r[0], "user_agent": r[2], "ip_address": r[3],
            "logged_in_at": r[4], "last_active": r[5],
            "is_current": bool(r[1] and r[1] == token),
            "is_active": active,
            "state": state,
            "reason": reason,
            "reason_text": reason_text,
        })
    return out


@app.delete("/sessions/{session_id}")
async def delete_session_artifact(session_id: int, nickname: str = "", token: str = ""):
    """Удалить «артефакт» — сброшенную или заброшенную запись устройства."""
    _auth_session(nickname, token)
    cursor.execute("SELECT nickname, is_active FROM user_sessions WHERE id=?", (session_id,))
    row = cursor.fetchone()
    if not row or str(row[0]).lower() != str(nickname).lower():
        return {"status": "error", "msg": "Сессия не найдена"}
    if row[1]:
        return {"status": "error", "msg": "Активную сессию нельзя удалить — выйдите из неё"}
    cursor.execute("DELETE FROM user_sessions WHERE id=?", (session_id,))
    conn.commit()
    return {"status": "ok", "msg": "Артефакт удалён!"}


@app.post("/sessions/terminate")
async def terminate_session(data: TerminateSessionData):
    _auth_session(data.nickname, data.session_token)
    cursor.execute("SELECT nickname FROM user_sessions WHERE id=? AND is_active=1", (data.target_session_id,))
    row = cursor.fetchone()
    if not row or str(row[0]).lower() != str(data.nickname).lower():
        return {"status": "error", "msg": "Сессия не найдена"}
    cursor.execute("UPDATE user_sessions SET is_active=0, end_reason='manual' WHERE id=?", (data.target_session_id,))
    conn.commit()
    return {"status": "ok", "msg": "Устройство отключено!"}


@app.post("/sessions/terminate-all")
async def terminate_all_sessions(data: TerminateAllSessionsData):
    _auth_session(data.nickname, data.session_token)
    if data.include_current:
        cursor.execute("UPDATE user_sessions SET is_active=0, end_reason='manual' WHERE LOWER(nickname)=LOWER(?)", (data.nickname,))
        conn.commit()
        # Добровольный выход со всех устройств — блокировки входа не накладываем
        await kick_account(data.nickname, "Все сеансы завершены. Войдите заново.", lock=False)
        return {"status": "ok", "msg": "Все сеансы завершены!"}
    cursor.execute(
        "UPDATE user_sessions SET is_active=0, end_reason='manual' WHERE LOWER(nickname)=LOWER(?) AND session_token<>?",
        (data.nickname, data.session_token),
    )
    conn.commit()
    return {"status": "ok", "msg": "Другие устройства отключены!"}

@app.get("/user/info/{nickname}")
def get_user_info(nickname: str, by: str = ""):
    cursor.execute("""SELECT nickname, avatar_url, bio, last_login, is_owner, is_admin, is_superadmin
                      FROM users WHERE LOWER(nickname)=LOWER(?)""", (nickname,))
    u = cursor.fetchone()
    if not u: return {"status": "error"}
    me = u[0]
    by = (by or "").strip()

    # Статус дружбы «кто смотрит» ↔ «кого смотрят»
    friend_status = "none"
    if by and by.lower() != me.lower():
        cursor.execute("""
            SELECT status, user1 FROM friends
            WHERE (LOWER(user1)=LOWER(?) AND LOWER(user2)=LOWER(?))
               OR (LOWER(user1)=LOWER(?) AND LOWER(user2)=LOWER(?))
        """, (by, me, me, by))
        row = cursor.fetchone()
        if row:
            if row[0] == "accepted":
                friend_status = "friends"
            else:
                friend_status = "outgoing" if (row[1] or "").lower() == by.lower() else "incoming"

    # Лесенка: есть ли уже личный чат с этим человеком
    dm_id = None
    if by and by.lower() != me.lower():
        cursor.execute("""
            SELECT id FROM chats WHERE is_direct=1 AND (
                (LOWER(direct_user1)=LOWER(?) AND LOWER(direct_user2)=LOWER(?)) OR
                (LOWER(direct_user1)=LOWER(?) AND LOWER(direct_user2)=LOWER(?)))
            LIMIT 1
        """, (by, me, me, by))
        r = cursor.fetchone()
        dm_id = r[0] if r else None

    # Сколько общих групп
    common = 0
    if by:
        cursor.execute("""
            SELECT COUNT(DISTINCT m1.chat_id)
            FROM members m1 JOIN members m2 ON m1.chat_id = m2.chat_id
            WHERE LOWER(m1.nickname)=LOWER(?) AND LOWER(m2.nickname)=LOWER(?)
        """, (by, me))
        common = cursor.fetchone()[0] or 0

    return {
        "status": "ok", "nickname": me, "avatar_url": u[1] or "",
        "bio": u[2] or "", "last_login": u[3],
        "is_online": manager.is_online(me),
        "is_owner": bool(u[4]), "is_admin": bool(u[5]), "is_superadmin": bool(u[6]),
        "friend_status": friend_status,
        "dm_chat_id": dm_id,
        "common_chats": common,
    }

# ═══════════════════════════════════════════════════════════
#  АДМИНИСТРАТИВНАЯ ПАНЕЛЬ
# ═══════════════════════════════════════════════════════════

@app.get("/admin/users")
def get_admin_users(admin: str, query: str = "", filter_type: str = "all"):
    cursor.execute("SELECT is_superadmin, is_admin FROM users WHERE LOWER(nickname)=LOWER(?)", (admin,))
    row = cursor.fetchone()
    if not row or (not row[0] and not row[1] and not is_super(admin)):
        raise HTTPException(status_code=403, detail="Доступ запрещён")

    ns = now_str()
    cursor.execute("""
        SELECT nickname, created_at, last_login, is_superadmin, is_admin, admin_perms, avatar_url, bio
        FROM users WHERE LOWER(nickname) LIKE ? ORDER BY last_login DESC LIMIT 100
    """, (f"%{query.lower()}%",))

    users_list = []
    for r in cursor.fetchall():
        nick = r[0]
        cursor.execute("SELECT 1 FROM bans WHERE LOWER(target_nick)=LOWER(?) AND (ban_type='permanent' OR expires_at>?)", (nick, ns))
        is_banned = cursor.fetchone() is not None
        cursor.execute("SELECT 1 FROM mutes WHERE LOWER(target_nick)=LOWER(?) AND expires_at>?", (nick, ns))
        is_muted = cursor.fetchone() is not None
        online = manager.is_online(nick)
        is_adm = bool(r[4]) or bool(r[3]) or is_super(nick)

        if filter_type == "online" and not online: continue
        if filter_type == "offline" and online: continue
        if filter_type == "banned" and not is_banned: continue
        if filter_type == "admins" and not is_adm: continue

        users_list.append({
            "nickname": nick, "created_at": r[1], "last_login": r[2],
            "is_superadmin": bool(r[3]) or is_super(nick), "is_admin": is_adm,
            "perms": json.loads(r[5] or '{}'), "avatar_url": r[6] or "", "bio": r[7] or "",
            "is_online": online, "is_banned": is_banned, "is_muted": is_muted
        })

    cursor.execute("SELECT COUNT(*) FROM users")
    total = cursor.fetchone()[0]
    return {"total_users": total, "online_count": len(manager.user_sockets), "users": users_list}

@app.get("/admin/user-sessions")
def admin_get_user_sessions(admin: str, target: str):
    cursor.execute("SELECT is_superadmin, is_admin FROM users WHERE LOWER(nickname)=LOWER(?)", (admin,))
    row = cursor.fetchone()
    if not row or (not row[0] and not row[1] and not is_super(admin)):
        raise HTTPException(status_code=403, detail="Доступ запрещён")

    cursor.execute("""
        SELECT id, user_agent, ip_address, logged_in_at, last_active, is_active 
        FROM user_sessions WHERE LOWER(nickname)=LOWER(?) AND is_active=1 ORDER BY last_active DESC LIMIT 15
    """, (target,))
    return [{
        "id": r[0], "user_agent": r[1], "ip_address": r[2],
        "logged_in_at": r[3], "last_active": r[4], "is_active": bool(r[5])
    } for r in cursor.fetchall()]

@app.post("/admin/kill-session")
async def admin_kill_session(data: TerminateSessionData):
    cursor.execute("SELECT is_superadmin, is_admin FROM users WHERE LOWER(nickname)=LOWER(?)", (data.nickname,))
    row = cursor.fetchone()
    if not row or (not row[0] and not row[1] and not is_super(data.nickname)):
        raise HTTPException(status_code=403, detail="Отказано")

    cursor.execute("SELECT nickname FROM user_sessions WHERE id=?", (data.target_session_id,))
    target = cursor.fetchone()
    if target:
        cursor.execute("UPDATE user_sessions SET is_active=0, end_reason='manual' WHERE id=?", (data.target_session_id,))
        conn.commit()
        # Мгновенно рвём все его вкладки + вход блокируется на 30 секунд
        ok, msg = await kick_account(target[0], "Ваш сеанс был завершён администратором.")
        if not ok:
            return {"status": "error", "msg": msg}
        return {"status": "ok", "msg": f"Сессия юзера @{target[0]} отключена!"}
    return {"status": "error", "msg": "Сессия не найдена"}

@app.get("/admin/groups")
def get_admin_groups(admin: str):
    cursor.execute("SELECT is_superadmin, is_admin FROM users WHERE LOWER(nickname)=LOWER(?)", (admin,))
    row = cursor.fetchone()
    if not row or (not row[0] and not row[1] and not is_super(admin)):
        raise HTTPException(status_code=403, detail="Доступ запрещён")

    cursor.execute("SELECT id, name, created_by, created_at FROM chats")
    res = []
    for cid, cname, cc, cca in cursor.fetchall():
        cursor.execute("SELECT nickname FROM members WHERE chat_id=?", (cid,))
        res.append({"id": cid, "name": cname, "created_by": cc, "created_at": cca,
                    "members": [m[0] for m in cursor.fetchall()]})
    return res

@app.post("/admin/ban")
def ban_user(data: BanData):
    require_admin(data.admin_nick)
    if is_super(data.target_nick):
        return {"status": "error", "msg": "Главного администратора заблокировать невозможно!"}
    expires_at = None
    if data.ban_type == "temporary" and data.duration_hours > 0:
        expires_at = (datetime.utcnow() + timedelta(hours=data.duration_hours)).strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("DELETE FROM bans WHERE LOWER(target_nick)=LOWER(?)", (data.target_nick,))
    cursor.execute("INSERT INTO bans (target_nick, banned_by, reason, ban_type, expires_at) VALUES (?,?,?,?,?)",
                   (data.target_nick, data.admin_nick, data.reason, data.ban_type, expires_at))
    conn.commit()
    return {"status": "ok", "msg": f"@{data.target_nick} заблокирован!"}

@app.post("/admin/unban")
def unban_user(data: BanData):
    require_admin(data.admin_nick)
    cursor.execute("DELETE FROM bans WHERE LOWER(target_nick)=LOWER(?)", (data.target_nick,))
    conn.commit()
    return {"status": "ok", "msg": f"@{data.target_nick} разблокирован."}

@app.get("/admin/bans")
def get_bans(admin: str):
    cursor.execute("SELECT is_superadmin, is_admin FROM users WHERE LOWER(nickname)=LOWER(?)", (admin,))
    row = cursor.fetchone()
    if not row or (not row[0] and not row[1] and not is_super(admin)):
        raise HTTPException(status_code=403, detail="Доступ запрещён")
    periodic_cleanup()
    cursor.execute("SELECT id, target_nick, banned_by, reason, ban_type, expires_at, created_at FROM bans ORDER BY id DESC")
    return [{"id": r[0], "target_nick": r[1], "banned_by": r[2], "reason": r[3],
             "ban_type": r[4], "expires_at": r[5], "created_at": r[6]} for r in cursor.fetchall()]

@app.post("/admin/mute")
def mute_user(data: MuteData):
    require_admin(data.admin_nick)
    exp = (datetime.utcnow() + timedelta(minutes=data.duration_minutes)).strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("DELETE FROM mutes WHERE LOWER(target_nick)=LOWER(?)", (data.target_nick,))
    cursor.execute("INSERT INTO mutes (target_nick, muted_by, reason, duration_minutes, expires_at) VALUES (?,?,?,?,?)",
                   (data.target_nick, data.admin_nick, data.reason, data.duration_minutes, exp))
    conn.commit()
    return {"status": "ok", "msg": f"@{data.target_nick} замьючен на {data.duration_minutes} мин."}

@app.post("/admin/kick")
async def kick_user(data: BanData):
    require_admin(data.admin_nick)
    if is_super(data.target_nick):
        return {"status": "error", "msg": "Нельзя кикнуть Главного Администратора!"}
    # Гасит сессии, рвёт вкладки сразу и запрещает вход на 30 секунд;
    # сам кик — не чаще одного раза в 15 секунд, чтобы нельзя было заморозить аккаунт
    ok, msg = await kick_account(data.target_nick, "Вы были кикнуты администратором.")
    if not ok:
        return {"status": "error", "msg": msg}
    return {"status": "ok", "msg": f"@{data.target_nick} кикнут. {msg}"}

@app.post("/admin/delete-user")
def delete_user_action(data: DeleteUserAction):
    require_admin(data.admin_nick)
    if is_super(data.target_nick):
        return {"status": "error", "msg": "Главного администратора удалить невозможно!"}
    cursor.execute("SELECT is_superadmin, is_admin, admin_perms FROM users WHERE LOWER(nickname)=LOWER(?)", (data.admin_nick,))
    adm = cursor.fetchone()
    if not adm: raise HTTPException(status_code=403, detail="Отказано")
    perms = json.loads(adm[2] or '{}')
    is_sup = bool(adm[0]) or is_super(data.admin_nick)

    if is_sup or perms.get("can_delete_users_direct"):
        cursor.execute("DELETE FROM users WHERE LOWER(nickname)=LOWER(?)", (data.target_nick,))
        cursor.execute("DELETE FROM members WHERE LOWER(nickname)=LOWER(?)", (data.target_nick,))
        cursor.execute("DELETE FROM friends WHERE LOWER(user1)=LOWER(?) OR LOWER(user2)=LOWER(?)", (data.target_nick, data.target_nick))
        cursor.execute("DELETE FROM bans WHERE LOWER(target_nick)=LOWER(?)", (data.target_nick,))
        conn.commit()
        return {"status": "ok", "msg": f"Аккаунт @{data.target_nick} удалён!"}
    elif perms.get("can_request_delete_users"):
        if not data.reason.strip(): return {"status": "error", "msg": "Укажите причину для заявки!"}
        exp = (datetime.utcnow() + timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("INSERT INTO delete_requests (target_nick, requested_by, reason, expires_at) VALUES (?,?,?,?)",
                       (data.target_nick, data.admin_nick, data.reason.strip(), exp))
        conn.commit()
        return {"status": "ok", "msg": f"Заявка на удаление @{data.target_nick} отправлена (срок 7 дней)."}
    return {"status": "error", "msg": "Нет прав на удаление аккаунтов!"}

@app.get("/admin/delete-requests")
def get_delete_requests(admin: str):
    cursor.execute("SELECT is_superadmin FROM users WHERE LOWER(nickname)=LOWER(?)", (admin,))
    row = cursor.fetchone()
    if not row or (not row[0] and not is_super(admin)):
        raise HTTPException(status_code=403, detail="Доступ запрещён")
    periodic_cleanup()
    cursor.execute("SELECT id, target_nick, requested_by, reason, created_at, expires_at FROM delete_requests WHERE status='pending' ORDER BY id DESC")
    return [{"id": r[0], "target_nick": r[1], "requested_by": r[2], "reason": r[3],
             "created_at": r[4], "expires_at": r[5]} for r in cursor.fetchall()]

@app.post("/admin/delete-requests/decision")
def decide_delete_request(data: ReqDecision):
    if not is_super(data.admin_nick): return {"status": "error", "msg": "Только Главный Администратор!"}
    cursor.execute("SELECT target_nick, requested_by FROM delete_requests WHERE id=? AND status='pending'", (data.request_id,))
    req = cursor.fetchone()
    if not req: return {"status": "error", "msg": "Заявка не найдена!"}
    target, requester = req
    if data.action == "approve":
        cursor.execute("UPDATE delete_requests SET status='approved' WHERE id=?", (data.request_id,))
        cursor.execute("DELETE FROM users WHERE LOWER(nickname)=LOWER(?)", (target,))
        cursor.execute("DELETE FROM members WHERE LOWER(nickname)=LOWER(?)", (target,))
        cursor.execute("INSERT INTO notifications (to_user, text) VALUES (?, ?)", (requester, f"Главный администратор одобрил удаление @{target}."))
        conn.commit()
        return {"status": "ok", "msg": f"@{target} удалён!"}
    else:
        cursor.execute("UPDATE delete_requests SET status='rejected' WHERE id=?", (data.request_id,))
        cursor.execute("INSERT INTO notifications (to_user, text) VALUES (?, ?)", (requester, f"Заявка на удаление @{target} отклонена."))
        conn.commit()
        return {"status": "ok", "msg": "Заявка отклонена."}

@app.post("/admin/set-perms")
async def set_admin_permissions(data: AdminPermsData):
    # Выдавать/снимать админ-права может только супер или владелец сайта
    if not can_manage_everything(data.admin_nick):
        raise HTTPException(status_code=403, detail="Доступ запрещён")
    if is_super(data.target_nick): return {"status": "error", "msg": "Нельзя менять права Главного Администратора!"}
    has_any = any(data.perms.values())
    if has_any:
        cursor.execute("UPDATE users SET is_admin=1, admin_perms=?, admin_notified=0, granted_by=?, revoked_by=NULL WHERE LOWER(nickname)=LOWER(?)",
                       (json.dumps(data.perms), data.admin_nick, data.target_nick))
        conn.commit()
        return {"status": "ok", "msg": f"Права для @{data.target_nick} обновлены!"}
    else:
        cursor.execute("UPDATE users SET is_admin=0, admin_perms='{}', admin_notified=1, granted_by=NULL, revoked_by=? WHERE LOWER(nickname)=LOWER(?)",
                       (data.admin_nick, data.target_nick))
        conn.commit()
        await manager.send_to_user(data.target_nick, {"action": "admin_revoked", "revoked_by": data.admin_nick})
        return {"status": "ok", "msg": f"Все права у @{data.target_nick} отозваны!"}

# ═══════════════════════════════════════════════════════════
#  ПОДДЕРЖКА (SUPPORT)
# ═══════════════════════════════════════════════════════════

def _is_support_staff(nick: str) -> bool:
    if is_super(nick): return True
    cursor.execute("SELECT is_superadmin, admin_perms FROM users WHERE LOWER(nickname)=LOWER(?)", (nick,))
    r = cursor.fetchone()
    if not r: return False
    if r[0]: return True
    perms = json.loads(r[1] or '{}')
    return perms.get("can_handle_support", False)

@app.post("/support/ticket")
async def create_support_ticket(data: SupportTicketData):
    if not data.subject.strip() or not data.message.strip():
        return {"status": "error", "msg": "Заполните тему и текст обращения!"}
    cursor.execute("INSERT INTO support_tickets (from_user, subject, message) VALUES (?, ?, ?)",
                   (data.from_user, data.subject.strip(), data.message.strip()))
    ticket_id = cursor.lastrowid
    conn.commit()
    await manager.notify_support_staff({
        "action": "new_support_ticket", "ticket_id": ticket_id,
        "from_user": data.from_user, "subject": data.subject
    })
    return {"status": "ok", "msg": "Обращение отправлено в поддержку!", "ticket_id": ticket_id}

@app.get("/support/tickets/my/{nickname}")
def get_my_tickets(nickname: str):
    cursor.execute("""
        UPDATE support_tickets 
        SET user_viewed_at = CURRENT_TIMESTAMP 
        WHERE LOWER(from_user)=LOWER(?) AND status='answered' AND user_viewed_at IS NULL
    """, (nickname,))
    conn.commit()

    cursor.execute("SELECT id, subject, message, status, created_at, user_viewed_at FROM support_tickets WHERE LOWER(from_user)=LOWER(?) ORDER BY id DESC LIMIT 30", (nickname,))
    tickets = []
    for row in cursor.fetchall():
        tid = row[0]
        cursor.execute("SELECT from_user, message, created_at FROM support_replies WHERE ticket_id=? ORDER BY id ASC", (tid,))
        replies = [{"from_user": r[0], "message": r[1], "created_at": r[2]} for r in cursor.fetchall()]
        tickets.append({
            "id": tid, "subject": row[1], "message": row[2], "status": row[3],
            "created_at": row[4], "user_viewed_at": row[5], "replies": replies
        })
    return tickets

@app.get("/support/tickets/all")
def get_all_tickets(admin: str):
    if not _is_support_staff(admin): raise HTTPException(status_code=403, detail="Доступ запрещён")
    cursor.execute("SELECT id, from_user, subject, message, status, created_at FROM support_tickets ORDER BY id DESC LIMIT 50")
    tickets = []
    for row in cursor.fetchall():
        tid = row[0]
        cursor.execute("SELECT from_user, message, created_at FROM support_replies WHERE ticket_id=? ORDER BY id ASC", (tid,))
        replies = [{"from_user": r[0], "message": r[1], "created_at": r[2]} for r in cursor.fetchall()]
        tickets.append({"id": tid, "from_user": row[1], "subject": row[2], "message": row[3], "status": row[4], "created_at": row[5], "replies": replies})
    return tickets

@app.post("/support/reply")
async def reply_to_ticket(data: SupportReplyData):
    if not _is_support_staff(data.admin_nick): return {"status": "error", "msg": "Нет прав саппорта!"}
    cursor.execute("SELECT from_user, subject FROM support_tickets WHERE id=?", (data.ticket_id,))
    ticket = cursor.fetchone()
    if not ticket: return {"status": "error", "msg": "Обращение не найдено!"}

    cursor.execute("INSERT INTO support_replies (ticket_id, from_user, message) VALUES (?, ?, ?)",
                   (data.ticket_id, data.admin_nick, data.message))
    cursor.execute("UPDATE support_tickets SET status='answered', user_viewed_at=NULL WHERE id=?", (data.ticket_id,))
    conn.commit()

    await manager.send_to_user(ticket[0], {
        "action": "support_reply", "ticket_id": data.ticket_id,
        "from_user": data.admin_nick, "message": data.message
    })
    return {"status": "ok", "msg": "Ответ отправлен!"}

@app.post("/support/delete")
def delete_support_ticket(data: SupportDeleteData):
    if not _is_support_staff(data.admin_nick): return {"status": "error", "msg": "Нет прав саппорта!"}
    cursor.execute("DELETE FROM support_replies WHERE ticket_id=?", (data.ticket_id,))
    cursor.execute("DELETE FROM support_tickets WHERE id=?", (data.ticket_id,))
    conn.commit()
    return {"status": "ok", "msg": f"Тикет #{data.ticket_id} успешно удалён!"}

@app.post("/support/close")
def close_ticket(data: SupportDeleteData):
    cursor.execute("UPDATE support_tickets SET status='closed' WHERE id=?", (data.ticket_id,))
    conn.commit()
    return {"status": "ok", "msg": "Обращение закрыто."}

@app.post("/support/request-perm")
def request_support_perm(data: SupportPermRequestData):
    today = datetime.utcnow().date().isoformat()
    cursor.execute("SELECT 1 FROM support_perm_requests WHERE LOWER(requested_by)=LOWER(?) AND DATE(created_at)=? AND status='pending'",
                   (data.nickname, today))
    if cursor.fetchone(): return {"status": "error", "msg": "Заявка уже подана сегодня! Лимит — 1 раз в сутки."}

    cursor.execute("INSERT INTO support_perm_requests (requested_by) VALUES (?)", (data.nickname,))
    conn.commit()
    return {"status": "ok", "msg": "Запрос отправлен Главному Администратору!"}

@app.get("/support/perm-requests")
def get_support_perm_requests(admin: str):
    if not is_super(admin): raise HTTPException(status_code=403, detail="Только Главный Администратор")
    cursor.execute("SELECT id, requested_by, status, created_at FROM support_perm_requests WHERE status='pending' ORDER BY id DESC")
    return [{"id": r[0], "requested_by": r[1], "status": r[2], "created_at": r[3]} for r in cursor.fetchall()]

@app.post("/support/perm-decision")
async def decide_support_perm(data: SupportPermDecision):
    if not is_super(data.admin_nick): return {"status": "error", "msg": "Только Главный Администратор!"}
    cursor.execute("SELECT requested_by FROM support_perm_requests WHERE id=? AND status='pending'", (data.request_id,))
    row = cursor.fetchone()
    if not row: return {"status": "error", "msg": "Запрос не найден!"}

    nick = row[0]
    cursor.execute("UPDATE support_perm_requests SET status=?, decided_at=?, decided_by=? WHERE id=?",
                   (data.action, now_str(), data.admin_nick, data.request_id))

    if data.action == "approve":
        cursor.execute("SELECT admin_perms FROM users WHERE LOWER(nickname)=LOWER(?)", (nick,))
        perms = json.loads(cursor.fetchone()[0] or '{}')
        perms["can_handle_support"] = True
        cursor.execute("UPDATE users SET is_admin=1, admin_perms=? WHERE LOWER(nickname)=LOWER(?)", (json.dumps(perms), nick))
        conn.commit()
        await manager.send_to_user(nick, {"action": "support_perm_granted"})
        return {"status": "ok", "msg": f"Право на поддержку выдано @{nick}!"}
    else:
        conn.commit()
        return {"status": "ok", "msg": f"Запрос @{nick} отклонён."}

# ═══════════════════════════════════════════════════════════
#  WEBSOCKET
# ═══════════════════════════════════════════════════════════

def can_delete_messages(nick: str) -> bool:
    """Можно ли пользователю удалять ЧУЖИЕ сообщения.

    Свои сообщения удаляет всегда кто угодно — это проверяется отдельно по владельцу.
    """
    if not nick:
        return False
    if is_super(nick):
        return True
    cursor.execute("SELECT is_admin, is_superadmin, admin_perms FROM users WHERE LOWER(nickname)=LOWER(?)", (nick,))
    row = cursor.fetchone()
    if not row:
        return False
    is_admin, is_superadmin, perms_raw = row
    if is_superadmin:
        return True
    try:
        perms = json.loads(perms_raw or "{}")
    except Exception:
        perms = {}
    return bool(perms.get("can_delete_messages")) or bool(is_admin and perms.get("can_delete_messages", True))


@app.websocket("/ws/{chat_id}/{nickname}")
async def websocket_endpoint(websocket: WebSocket, chat_id: str, nickname: str):
    # Сокет тоже требует живую сессию — иначе можно было писать от чужого имени
    token = (websocket.query_params.get("token") or "").strip()
    try:
        _auth_session(nickname, token)
    except Exception:
        await websocket.accept()
        await websocket.close(code=4401)
        return
    await manager.connect(chat_id, websocket, nickname)
    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action", "send")

            if action == "ping":
                token = data.get("session_token")
                if token:
                    cursor.execute("UPDATE user_sessions SET last_active=?, is_active=1 WHERE LOWER(nickname)=LOWER(?) AND session_token=?",
                                   (now_str(), nickname, token))
                    conn.commit()
                await websocket.send_json({"action": "pong"})

            elif action in ("call_offer", "call_answer", "call_ice", "call_reject", "call_end", "call_busy", "call_timeout"):
                target = data.get("target")
                # Звонящий тоже берётся из URL сокета — нельзя представиться чужим именем
                data["caller"] = nickname
                if target and manager.is_online(target):
                    await manager.send_to_user(target, data)
                elif target:
                    await websocket.send_json({"action": "call_offline", "target": target})

            elif action == "send":
                # Отправитель берётся из URL сокета, а не из тела сообщения —
                # иначе любой мог писать от чужого имени
                data["sender"] = nickname

                # Правило ЛС: пока собеседник не в друзьях, можно написать только одно сообщение
                if chat_id.startswith("dm_"):
                    partner = dm_partner(chat_id, nickname)
                    if partner and not is_friend_with(nickname, partner):
                        cursor.execute("SELECT COUNT(*) FROM messages WHERE chat_id=? AND LOWER(sender)=LOWER(?)",
                                       (chat_id, nickname))
                        if (cursor.fetchone()[0] or 0) >= 1:
                            await websocket.send_json({
                                "action": "dm_limit",
                                "partner": partner,
                                "msg": f"Пока @{partner} не у тебя в друзьях — можно написать только одно сообщение. "
                                       f"Отправь запрос в друзья, чтобы общаться без лимита!",
                            })
                            continue

                cursor.execute("""
                    INSERT INTO messages (chat_id, sender, text, file_url, file_type, reply_to_id, reply_to_text, reply_to_sender)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (chat_id, nickname, data.get("text",""), data.get("file_url"), data.get("file_type"),
                      data.get("reply_to_id"), data.get("reply_to_text"), data.get("reply_to_sender")))
                conn.commit()
                data["id"] = cursor.lastrowid
                data["action"] = "new_message"
                await manager.broadcast_chat(chat_id, data)

            elif action == "react":
                mid = data.get("message_id")
                emoji = (data.get("emoji") or "").strip()[:8]
                if mid and emoji:
                    # У одного пользователя на сообщение — одна реакция (как в Телеграме)
                    cursor.execute("DELETE FROM message_reactions WHERE message_id=? AND LOWER(nickname)=LOWER(?)",
                                   (mid, nickname))
                    if not data.get("remove"):
                        cursor.execute("INSERT OR REPLACE INTO message_reactions (message_id, nickname, emoji) VALUES (?, ?, ?)",
                                       (mid, nickname, emoji))
                    conn.commit()
                    cursor.execute("SELECT emoji, nickname FROM message_reactions WHERE message_id=?", (mid,))
                    agg: Dict[str, List[str]] = {}
                    for e, n in cursor.fetchall():
                        agg.setdefault(e, []).append(n)
                    await manager.broadcast_chat(chat_id, {"action": "reactions", "message_id": mid, "reactions": agg})

            elif action == "delete":
                mid = data.get("message_id")
                cursor.execute("SELECT sender FROM messages WHERE id=?", (mid,))
                row = cursor.fetchone()
                if not row:
                    continue
                owner = (row[0] or "").lower()
                # Своё сообщение — можно всегда; чужое — глобальное право
                # can_delete_messages либо чатовое право delete_messages
                if (owner != (nickname or "").lower()
                        and not can_delete_messages(nickname)
                        and not chat_permission(chat_id, nickname, "delete_messages")):
                    await websocket.send_json({"action": "error",
                                               "msg": "Нет прав: можно удалять только свои сообщения"})
                    continue
                cursor.execute("DELETE FROM messages WHERE id=?", (mid,))
                cursor.execute("DELETE FROM message_reactions WHERE message_id=?", (mid,))
                conn.commit()
                await manager.broadcast_chat(chat_id, {"action": "deleted", "message_ids": [mid], "deleted_by": nickname})

    except WebSocketDisconnect:
        manager.disconnect(chat_id, websocket, nickname)
