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
conn = sqlite3.connect("messenger.db", check_same_thread=False, timeout=25.0)
cursor = conn.cursor()
cursor.execute("PRAGMA journal_mode=WAL;")
cursor.execute("PRAGMA busy_timeout=25000;")
conn.commit()

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
]
for tbl, col, ctype in _safe_alters:
    try:
        cursor.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {ctype}")
    except sqlite3.OperationalError:
        pass
conn.commit()

cursor.execute("UPDATE users SET is_superadmin=1, is_admin=1 WHERE LOWER(nickname) IN ('jjlop55','gglo55')")
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
#  АВТООЧИСТКА СООБЩЕНИЙ ПО ТАЙМЕРАМ:
#  - Общий чат: 3 часа
#  - Командные/групповые: 4 часа
#  - Личные (ЛС): 10 часов
# ═══════════════════════════════════════════════════════════
def periodic_cleanup():
    ns = now_str()

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

@app.post("/auth")
async def auth_user(data: AuthData, request: Request):
    periodic_cleanup()
    ns = now_str()
    ip = data.ip_address or (request.client.host if request.client else "127.0.0.1")

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
        SELECT password, is_superadmin, is_admin, admin_perms, avatar_url, bio, nickname 
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
        cursor.execute(
            "INSERT INTO user_sessions (nickname, session_token, user_agent, ip_address, logged_in_at, last_active, is_active) VALUES (?,?,?,?,?,?,1)",
            (canonical_nick, session_token, data.user_agent, ip, ns, ns)
        )
        cursor.execute("INSERT OR IGNORE INTO members (chat_id, nickname) VALUES ('general', ?)", (canonical_nick,))
        conn.commit()

        perms = json.loads(row[3]) if row[3] else {}
        return {
            "status": "ok",
            "nickname": canonical_nick,
            "is_superadmin": bool(row[1]) or is_super(canonical_nick),
            "is_admin": bool(row[2]) or bool(row[1]) or is_super(canonical_nick),
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
        cursor.execute("""
            INSERT INTO users (nickname, password, last_login, is_superadmin, is_admin, admin_perms, admin_notified)
            VALUES (?, ?, ?, ?, ?, ?, 1)
        """, (data.nickname, hashed, ns, is_sup, is_sup, json.dumps(perms)))
        cursor.execute(
            "INSERT INTO user_sessions (nickname, session_token, user_agent, ip_address, logged_in_at, last_active, is_active) VALUES (?,?,?,?,?,?,1)",
            (data.nickname, session_token, data.user_agent, ip, ns, ns)
        )
        cursor.execute("INSERT OR IGNORE INTO members (chat_id, nickname) VALUES ('general', ?)", (data.nickname,))
        conn.commit()

        return {
            "status": "ok",
            "nickname": data.nickname,
            "is_superadmin": bool(is_sup),
            "is_admin": bool(is_sup),
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
        SELECT DISTINCT c.id, c.name, c.is_direct, c.direct_user1, c.direct_user2
        FROM chats c LEFT JOIN members m ON c.id=m.chat_id
        WHERE c.id='general' OR LOWER(m.nickname)=LOWER(?)
    """, (nickname,))
    chats = []
    for r in cursor.fetchall():
        cid, cname, is_dir, u1, u2 = r
        if is_dir:
            partner = u2 if u1.lower() == nickname.lower() else u1
            cname = f"💬 @{partner}"
        chats.append({"id": cid, "name": cname, "is_direct": is_dir})
    chats.sort(key=lambda x: 0 if x["id"] == "general" else 1)
    return chats

@app.post("/chats")
def create_custom_chat(data: ChatCreateData):
    cursor.execute("INSERT OR IGNORE INTO chats (id, name, created_by, is_direct) VALUES (?, ?, ?, 0)", (data.id, data.name, data.created_by))
    cursor.execute("INSERT OR IGNORE INTO members (chat_id, nickname) VALUES (?, ?)", (data.id, data.created_by))
    conn.commit()
    return {"status": "ok"}

@app.delete("/chats/{chat_id}")
def delete_chat(chat_id: str, user: str):
    if chat_id == "general":
        return {"status": "error", "msg": "Общий чат защищён от удаления!"}
    cursor.execute("DELETE FROM chats WHERE id=?", (chat_id,))
    cursor.execute("DELETE FROM members WHERE chat_id=?", (chat_id,))
    cursor.execute("DELETE FROM messages WHERE chat_id=?", (chat_id,))
    conn.commit()
    return {"status": "ok"}

@app.get("/messages/{chat_id}")
def get_messages(chat_id: str):
    periodic_cleanup()
    cursor.execute("""
        SELECT id, sender, text, file_url, file_type, reply_to_id, reply_to_text, reply_to_sender,
               strftime('%H:%M', created_at)
        FROM messages WHERE chat_id=? ORDER BY id ASC LIMIT 150
    """, (chat_id,))
    return [{
        "id": r[0], "sender": r[1], "text": r[2], "file_url": r[3], "file_type": r[4],
        "reply_to_id": r[5], "reply_to_text": r[6], "reply_to_sender": r[7], "time": r[8]
    } for r in cursor.fetchall()]

@app.post("/upload")
async def upload_file(request: Request, filename: str = "file.bin"):
    periodic_cleanup()
    body = await request.body()
    if len(body) > 10485760:
        raise HTTPException(status_code=413, detail="Файл превышает 10 МБ!")
    _, ext = os.path.splitext(filename)
    unique_name = f"{uuid.uuid4().hex}{ext or '.bin'}"
    with open(os.path.join(UPLOAD_DIR, unique_name), "wb") as f:
        f.write(body)
    return {"status": "ok", "url": f"/uploads/{unique_name}"}

@app.get("/user/info/{nickname}")
def get_user_info(nickname: str):
    cursor.execute("SELECT nickname, avatar_url, bio, last_login FROM users WHERE LOWER(nickname)=LOWER(?)", (nickname,))
    u = cursor.fetchone()
    if not u: return {"status": "error"}
    return {
        "status": "ok", "nickname": u[0], "avatar_url": u[1] or "",
        "bio": u[2] or "", "last_login": u[3], "is_online": manager.is_online(u[0])
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
        cursor.execute("UPDATE user_sessions SET is_active=0 WHERE id=?", (data.target_session_id,))
        conn.commit()
        await manager.kick_user(target[0], "Ваш сеанс был завершён администратором.")
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
    exp = (datetime.utcnow() + timedelta(minutes=data.duration_minutes)).strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("DELETE FROM mutes WHERE LOWER(target_nick)=LOWER(?)", (data.target_nick,))
    cursor.execute("INSERT INTO mutes (target_nick, muted_by, reason, duration_minutes, expires_at) VALUES (?,?,?,?,?)",
                   (data.target_nick, data.admin_nick, data.reason, data.duration_minutes, exp))
    conn.commit()
    return {"status": "ok", "msg": f"@{data.target_nick} замьючен на {data.duration_minutes} мин."}

@app.post("/admin/kick")
async def kick_user(data: BanData):
    if is_super(data.target_nick):
        return {"status": "error", "msg": "Нельзя кикнуть Главного Администратора!"}
    await manager.kick_user(data.target_nick)
    return {"status": "ok", "msg": f"@{data.target_nick} кикнут."}

@app.post("/admin/delete-user")
def delete_user_action(data: DeleteUserAction):
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

@app.websocket("/ws/{chat_id}/{nickname}")
async def websocket_endpoint(websocket: WebSocket, chat_id: str, nickname: str):
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
                if target and manager.is_online(target):
                    await manager.send_to_user(target, data)
                elif target:
                    await websocket.send_json({"action": "call_offline", "target": target})

            elif action == "send":
                cursor.execute("""
                    INSERT INTO messages (chat_id, sender, text, file_url, file_type, reply_to_id, reply_to_text, reply_to_sender)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (chat_id, data.get("sender"), data.get("text",""), data.get("file_url"), data.get("file_type"),
                      data.get("reply_to_id"), data.get("reply_to_text"), data.get("reply_to_sender")))
                conn.commit()
                data["id"] = cursor.lastrowid
                data["action"] = "new_message"
                await manager.broadcast_chat(chat_id, data)

            elif action == "delete":
                cursor.execute("DELETE FROM messages WHERE id=?", (data.get("message_id"),))
                conn.commit()
                await manager.broadcast_chat(chat_id, {"action": "deleted", "message_ids": [data.get("message_id")], "deleted_by": data.get("sender")})

    except WebSocketDisconnect:
        manager.disconnect(chat_id, websocket, nickname)
