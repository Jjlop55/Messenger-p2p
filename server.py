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

# Подключение к SQLite в режиме повышенной надёжности (WAL предотвращает краши базы при нагрузке)
conn = sqlite3.connect("messenger.db", check_same_thread=False, timeout=20.0)
cursor = conn.cursor()
cursor.execute("PRAGMA journal_mode=WAL;")
cursor.execute("PRAGMA busy_timeout=20000;")
conn.commit()

# ═══════════════════════════════════════════════════════════
#  ТАБЛИЦЫ БАЗЫ ДАННЫХ
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
CREATE TABLE IF NOT EXISTS reactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER NOT NULL,
    nickname TEXT NOT NULL,
    emoji TEXT NOT NULL,
    UNIQUE(message_id, nickname)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS pinned_messages (
    chat_id TEXT PRIMARY KEY,
    message_id INTEGER,
    pinned_by TEXT,
    message_text TEXT,
    pinned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS support_tickets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_user TEXT NOT NULL,
    subject TEXT NOT NULL,
    message TEXT NOT NULL,
    status TEXT DEFAULT 'open',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

# Безопасное добавление колонок в старые БД
_safe_alters = [
    ("users", "avatar_url", "TEXT DEFAULT ''"),
    ("users", "bio", "TEXT DEFAULT ''"),
    ("users", "last_username_change", "TIMESTAMP"),
    ("chats", "is_direct", "INTEGER DEFAULT 0"),
    ("chats", "direct_user1", "TEXT"),
    ("chats", "direct_user2", "TEXT"),
    ("messages", "is_read", "INTEGER DEFAULT 0"),
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

# Очистка общего чата 4 раза в час (сообщения старше 15 минут) + файлов старше 4 дней
def periodic_cleanup():
    ns = now_str()

    # Очистка общего чата general (каждые 15 минут = 4 раза в час)
    gen_cutoff = (datetime.utcnow() - timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("DELETE FROM messages WHERE chat_id='general' AND created_at < ?", (gen_cutoff,))

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

    # Просроченные заявки
    cursor.execute("SELECT id,target_nick,requested_by FROM delete_requests WHERE expires_at < ? AND status='pending'", (ns,))
    for req_id, t_nick, r_by in cursor.fetchall():
        cursor.execute("UPDATE delete_requests SET status='expired' WHERE id=?", (req_id,))
        cursor.execute("INSERT INTO notifications(to_user,text) VALUES(?,?)",
                       (r_by, f"Главный администратор не принял заявку на удаление @{t_nick} (истёк срок 7 дней)."))

    cursor.execute("DELETE FROM mutes WHERE expires_at < ?", (ns,))
    cursor.execute("DELETE FROM bans WHERE ban_type='temporary' AND expires_at < ?", (ns,))
    conn.commit()

# ═══════════════════════════════════════════════════════════
#  Pydantic Модели
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

class DirectChatData(BaseModel):
    from_user: str
    target_user: str

class ReadChatData(BaseModel):
    chat_id: str
    nickname: str

class InviteData(BaseModel):
    chat_id: str
    chat_name: str
    from_user: str
    to_user: str

class InviteAction(BaseModel):
    invite_id: int
    nickname: str
    action: str

class AdminPermsData(BaseModel):
    admin_nick: str
    target_nick: str
    perms: dict

class DeleteUserAction(BaseModel):
    admin_nick: str
    target_nick: str
    reason: str = ""

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

class SupportTicketData(BaseModel):
    from_user: str
    subject: str
    message: str

class SupportReplyData(BaseModel):
    admin_nick: str
    ticket_id: int
    message: str

# ═══════════════════════════════════════════════════════════
#  CONNECTION MANAGER (Безопасная обработка сбоев сокетов)
# ═══════════════════════════════════════════════════════════

class ConnectionManager:
    def __init__(self):
        self.user_sockets: Dict[str, Set[WebSocket]] = {}
        self.chat_sockets: Dict[str, Set[WebSocket]] = {}

    async def connect(self, chat_id: str, ws: WebSocket, nick: str):
        await ws.accept()
        self.user_sockets.setdefault(nick, set()).add(ws)
        self.chat_sockets.setdefault(chat_id, set()).add(ws)

    def disconnect(self, chat_id: str, ws: WebSocket, nick: str):
        if nick in self.user_sockets:
            self.user_sockets[nick].discard(ws)
            if not self.user_sockets[nick]:
                del self.user_sockets[nick]
        if chat_id in self.chat_sockets:
            self.chat_sockets[chat_id].discard(ws)
            if not self.chat_sockets[chat_id]:
                del self.chat_sockets[chat_id]

    def is_online(self, nick: str) -> bool:
        return nick in self.user_sockets and bool(self.user_sockets[nick])

    def online_users(self) -> List[str]:
        return list(self.user_sockets.keys())

    async def send_to_user(self, nick: str, payload: dict):
        dead = []
        for ws in list(self.user_sockets.get(nick, [])):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for d in dead:
            self.user_sockets.get(nick, set()).discard(d)

    async def broadcast_chat(self, chat_id: str, payload: dict):
        dead = []
        for ws in list(self.chat_sockets.get(chat_id, [])):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for d in dead:
            self.chat_sockets.get(chat_id, set()).discard(d)

    async def kick_user(self, nick: str, reason: str = "Вы были принудительно отключены."):
        for ws in list(self.user_sockets.get(nick, [])):
            try:
                await ws.send_json({"action": "kicked", "msg": reason})
                await ws.close()
            except Exception:
                pass

manager = ConnectionManager()

# ═══════════════════════════════════════════════════════════
#  ЭНДПОИНТЫ АВТОРИЗАЦИИ И ПРОФИЛЯ
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

    # Проверка бана
    cursor.execute(
        "SELECT reason, banned_by, ban_type, expires_at FROM bans WHERE LOWER(target_nick)=LOWER(?)",
        (data.nickname,)
    )
    ban = cursor.fetchone()
    if ban:
        reason, banned_by, ban_type, expires_at = ban
        if ban_type == "permanent" or (ban_type == "temporary" and expires_at and expires_at > ns):
            return {"status": "banned", "msg": f"Аккаунт заблокирован! Причина: {reason}", "expires_at": expires_at}
        else:
            cursor.execute("DELETE FROM bans WHERE LOWER(target_nick)=LOWER(?)", (data.nickname,))
            conn.commit()

    cursor.execute(
        "SELECT password, is_superadmin, is_admin, admin_perms, admin_notified, granted_by, revoked_by, avatar_url, bio "
        "FROM users WHERE nickname=?", (data.nickname,)
    )
    row = cursor.fetchone()
    session_token = uuid.uuid4().hex

    if row:
        if not verify_pwd(data.password, row[0]):
            return {"status": "error", "msg": "Неверный пароль!"}

        hashed = hash_pwd(data.password)
        cursor.execute("UPDATE users SET password=?, last_login=? WHERE nickname=?", (hashed, ns, data.nickname))
        cursor.execute(
            "INSERT INTO user_sessions(nickname,session_token,user_agent,ip_address,logged_in_at,last_active,is_active) "
            "VALUES(?,?,?,?,?,?,1)",
            (data.nickname, session_token, data.user_agent, ip, ns, ns)
        )
        conn.commit()

        perms = json.loads(row[3]) if row[3] else {}
        return {
            "status": "ok", "msg": "logged_in",
            "is_superadmin": bool(row[1]) or is_super(data.nickname),
            "is_admin": bool(row[2]) or bool(row[1]) or is_super(data.nickname),
            "admin_perms": perms,
            "admin_notice": bool(row[4] == 0 and row[2] == 1 and row[1] == 0),
            "granted_by": row[5],
            "revoked_by": row[6],
            "avatar_url": row[7] or "",
            "bio": row[8] or "",
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
        cursor.execute(
            "INSERT INTO users(nickname,password,last_login,is_superadmin,is_admin,admin_perms,admin_notified) "
            "VALUES(?,?,?,?,?,?,1)",
            (data.nickname, hashed, ns, is_sup, is_sup, json.dumps(perms))
        )
        cursor.execute(
            "INSERT INTO user_sessions(nickname,session_token,user_agent,ip_address,logged_in_at,last_active,is_active) "
            "VALUES(?,?,?,?,?,?,1)",
            (data.nickname, session_token, data.user_agent, ip, ns, ns)
        )
        conn.commit()

        return {
            "status": "ok", "msg": "registered",
            "is_superadmin": bool(is_sup), "is_admin": bool(is_sup),
            "admin_perms": perms, "admin_notice": False, "revoked_by": None,
            "avatar_url": "", "bio": "",
            "session_token": session_token
        }

# Обновление профиля: аватар, био, смена юзернейма (раз в 1 день + проверка уникальности)
@app.post("/profile/update")
def update_profile(data: ProfileUpdateData):
    cursor.execute("SELECT nickname, last_username_change FROM users WHERE nickname=?", (data.current_nickname,))
    user = cursor.fetchone()
    if not user:
        return {"status": "error", "msg": "Пользователь не найден!"}

    # Проверка смены ника
    new_nick = data.new_nickname.strip() if data.new_nickname else data.current_nickname
    if new_nick != data.current_nickname:
        # Проверка кулдауна 1 день
        last_change = user[1]
        if last_change:
            diff = datetime.utcnow() - datetime.strptime(last_change, '%Y-%m-%d %H:%M:%S')
            if diff < timedelta(days=1):
                hours_left = int(24 - (diff.total_seconds() / 3600))
                return {"status": "error", "msg": f"Юзернейм можно менять только 1 раз в день! Осталось ждать: {hours_left} ч."}

        # Проверка уникальности
        cursor.execute("SELECT 1 FROM users WHERE LOWER(nickname)=LOWER(?)", (new_nick,))
        if cursor.fetchone():
            return {"status": "error", "msg": f"Юзернейм @{new_nick} уже занят другим пользователем!"}

        # Каскадное обновление юзернейма во всех таблицах
        cursor.execute("UPDATE users SET nickname=?, last_username_change=? WHERE nickname=?", (new_nick, now_str(), data.current_nickname))
        cursor.execute("UPDATE members SET nickname=? WHERE nickname=?", (new_nick, data.current_nickname))
        cursor.execute("UPDATE messages SET sender=? WHERE sender=?", (new_nick, data.current_nickname))
        cursor.execute("UPDATE messages SET reply_to_sender=? WHERE reply_to_sender=?", (new_nick, data.current_nickname))
        cursor.execute("UPDATE chats SET created_by=? WHERE created_by=?", (new_nick, data.current_nickname))
        cursor.execute("UPDATE chats SET direct_user1=? WHERE direct_user1=?", (new_nick, data.current_nickname))
        cursor.execute("UPDATE chats SET direct_user2=? WHERE direct_user2=?", (new_nick, data.current_nickname))
        cursor.execute("UPDATE invites SET from_user=? WHERE from_user=?", (new_nick, data.current_nickname))
        cursor.execute("UPDATE invites SET to_user=? WHERE to_user=?", (new_nick, data.current_nickname))
        cursor.execute("UPDATE user_sessions SET nickname=? WHERE nickname=?", (new_nick, data.current_nickname))
        conn.commit()

    # Обновление аватара и био
    target_nick = new_nick if new_nick != data.current_nickname else data.current_nickname
    if data.avatar_url is not None:
        cursor.execute("UPDATE users SET avatar_url=? WHERE nickname=?", (data.avatar_url, target_nick))
    if data.bio is not None:
        cursor.execute("UPDATE users SET bio=? WHERE nickname=?", (data.bio[:140], target_nick))
    conn.commit()

    return {"status": "ok", "msg": "Профиль успешно сохранён!", "new_nickname": target_nick}

# ═══════════════════════════════════════════════════════════
#  ЛИЧНЫЕ ДИАЛОГИ (DM 1-на-1) И НЕПРОЧИТАННЫЕ СООБЩЕНИЯ
# ═══════════════════════════════════════════════════════════

@app.post("/direct-chat")
def get_or_create_direct_chat(data: DirectChatData):
    if data.from_user == data.target_user:
        return {"status": "error", "msg": "Нельзя создать личный диалог с самим собой!"}

    cursor.execute("SELECT 1 FROM users WHERE nickname=?", (data.target_user,))
    if not cursor.fetchone():
        return {"status": "error", "msg": "Пользователь не найден в сети!"}

    # Ищем существующий личный чат между двумя пользователями
    cursor.execute("""
        SELECT id, name FROM chats
        WHERE is_direct=1 AND (
            (direct_user1=? AND direct_user2=?) OR
            (direct_user1=? AND direct_user2=?)
        ) LIMIT 1
    """, (data.from_user, data.target_user, data.target_user, data.from_user))
    existing = cursor.fetchone()

    if existing:
        return {"status": "ok", "chat_id": existing[0], "chat_name": f"💬 @{data.target_user}"}

    # Создаём новый личный диалог
    chat_id = f"dm_{uuid.uuid4().hex[:12]}"
    chat_name = f"💬 @{data.target_user}"
    cursor.execute(
        "INSERT INTO chats(id, name, created_by, is_direct, direct_user1, direct_user2) VALUES(?,?,?,1,?,?)",
        (chat_id, chat_name, data.from_user, data.from_user, data.target_user)
    )
    cursor.execute("INSERT OR IGNORE INTO members VALUES(?,?)", (chat_id, data.from_user))
    cursor.execute("INSERT OR IGNORE INTO members VALUES(?,?)", (chat_id, data.target_user))
    conn.commit()

    return {"status": "ok", "chat_id": chat_id, "chat_name": chat_name}

@app.get("/unread-counts/{nickname}")
def get_unread_counts(nickname: str):
    # Подсчёт непрочитанных сообщений по чатам, в которых состоит пользователь
    cursor.execute("""
        SELECT m.chat_id, COUNT(msg.id)
        FROM members m
        JOIN messages msg ON m.chat_id = msg.chat_id
        WHERE m.nickname = ? AND msg.sender != ? AND msg.is_read = 0
        GROUP BY m.chat_id
    """, (nickname, nickname))
    res = {r[0]: r[1] for r in cursor.fetchall()}
    return res

@app.post("/messages/read")
def mark_messages_read(data: ReadChatData):
    cursor.execute("""
        UPDATE messages SET is_read=1
        WHERE chat_id=? AND sender!=? AND is_read=0
    """, (data.chat_id, data.nickname))
    conn.commit()
    return {"status": "ok"}

# ═══════════════════════════════════════════════════════════
#  ОСТАЛЬНЫЕ ЭНДПОИНТЫ (ЧАТЫ, ФАЙЛЫ, ПОДДЕРЖКА, АДМИНКА)
# ═══════════════════════════════════════════════════════════

@app.get("/users/all")
def get_all_users_directory(query: str = ""):
    periodic_cleanup()
    cursor.execute("""
        SELECT nickname, created_at, last_login, is_superadmin, is_admin, avatar_url, bio
        FROM users WHERE LOWER(nickname) LIKE ? ORDER BY last_login DESC LIMIT 100
    """, (f"%{query.lower()}%",))
    
    users = []
    for r in cursor.fetchall():
        nick = r[0]
        users.append({
            "nickname": nick,
            "created_at": r[1],
            "last_login": r[2],
            "is_superadmin": bool(r[3]) or is_super(nick),
            "is_admin": bool(r[4]) or bool(r[3]) or is_super(nick),
            "avatar_url": r[5] or "",
            "bio": r[6] or "",
            "is_online": manager.is_online(nick)
        })
    return users

@app.get("/chats/{nickname}")
def get_user_chats(nickname: str):
    cursor.execute("""
        SELECT DISTINCT c.id, c.name, c.is_direct, c.direct_user1, c.direct_user2
        FROM chats c
        JOIN members m ON c.id=m.chat_id WHERE m.nickname=?
    """, (nickname,))
    result = []
    for r in cursor.fetchall():
        cid, cname, is_dir, u1, u2 = r
        # Для личных диалогов динамически подставляем имя собеседника
        if is_dir:
            partner = u2 if u1 == nickname else u1
            cname = f"💬 @{partner}"
        result.append({"id": cid, "name": cname})
    return result

@app.delete("/chats/{chat_id}")
def delete_chat(chat_id: str, user: str):
    if chat_id == "general":
        return {"status": "error", "msg": "Общий чат удалить нельзя!"}
    cursor.execute("SELECT created_by, is_direct FROM chats WHERE id=?", (chat_id,))
    row = cursor.fetchone()
    if not row:
        return {"status": "error", "msg": "Чат не найден!"}

    cursor.execute("SELECT is_superadmin, is_admin, admin_perms FROM users WHERE nickname=?", (user,))
    u = cursor.fetchone()
    can_del = False
    if u:
        perms = json.loads(u[2] or '{}')
        can_del = u[0] or is_super(user) or (u[1] and perms.get("can_delete_chats")) or row[0] == user or row[1] == 1
    if not can_del:
        return {"status": "error", "msg": "У вас нет прав на удаление этого чата!"}

    cursor.execute("DELETE FROM chats WHERE id=?", (chat_id,))
    cursor.execute("DELETE FROM members WHERE chat_id=?", (chat_id,))
    cursor.execute("DELETE FROM messages WHERE chat_id=?", (chat_id,))
    cursor.execute("DELETE FROM invites WHERE chat_id=?", (chat_id,))
    cursor.execute("DELETE FROM pinned_messages WHERE chat_id=?", (chat_id,))
    conn.commit()
    return {"status": "ok"}

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

@app.get("/messages/{chat_id}")
def get_messages(chat_id: str):
    periodic_cleanup()
    cursor.execute("""
        SELECT id,sender,text,file_url,file_type,reply_to_id,reply_to_text,reply_to_sender,
               strftime('%H:%M',created_at),is_edited
        FROM messages WHERE chat_id=? ORDER BY id ASC LIMIT 150
    """, (chat_id,))
    rows = cursor.fetchall()
    return [{
        "id": r[0], "sender": r[1], "text": r[2], "file_url": r[3], "file_type": r[4],
        "reply_to_id": r[5], "reply_to_text": r[6], "reply_to_sender": r[7],
        "time": r[8], "is_edited": bool(r[9]), "reactions": {}
    } for r in rows]

@app.get("/server/monitoring")
def server_monitoring():
    uptime_sec = int(time.time() - SERVER_START_TIME)
    h, rem = divmod(uptime_sec, 3600)
    m, s = divmod(rem, 60)
    cursor.execute("SELECT COUNT(*) FROM users"); tu = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM chats"); tc = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM messages"); tm = cursor.fetchone()[0]
    return {
        "status": "online", "uptime": f"{h}ч {m}м {s}с",
        "online_count": len(manager.user_sockets), "online_users": manager.online_users(),
        "total_users": tu, "total_chats": tc, "total_messages": tm,
        "uploads_mb": 0.5, "total_storage_mb": 1.2, "storage_warning_level": "ok", "messages_until_cleanup": 50000
    }

@app.get("/storage-status")
def storage_status():
    return {"level": "ok", "uploads_mb": 0.5, "total_mb": 1.2, "messages_until_cleanup": 50000}

@app.get("/check-mute/{nickname}")
def check_mute(nickname: str):
    ns = now_str()
    cursor.execute("DELETE FROM mutes WHERE expires_at < ?", (ns,))
    conn.commit()
    cursor.execute("SELECT expires_at, reason FROM mutes WHERE LOWER(target_nick)=LOWER(?) AND expires_at>?", (nickname, ns))
    row = cursor.fetchone()
    if not row:
        return {"is_muted": False, "expires_at": None, "reason": None}
    return {"is_muted": True, "expires_at": row[0], "reason": row[1]}

# ═══════════════════════════════════════════════════════════
#  WEBSOCKET ENDPOINT
# ═══════════════════════════════════════════════════════════

@app.websocket("/ws/{chat_id}/{nickname}")
async def websocket_endpoint(websocket: WebSocket, chat_id: str, nickname: str):
    await manager.connect(chat_id, websocket, nickname)
    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action", "send")

            if action == "ping":
                await websocket.send_json({"action": "pong"})

            # Сигналы WebRTC звонков
            elif action in ("call_offer", "call_answer", "call_ice", "call_reject", "call_end", "call_busy"):
                target = data.get("target")
                if target:
                    if manager.is_online(target):
                        await manager.send_to_user(target, data)
                    else:
                        await websocket.send_json({"action": "call_offline", "target": target})

            elif action == "send":
                sender = data.get("sender")
                cursor.execute(
                    "INSERT INTO messages(chat_id,sender,text,file_url,file_type,reply_to_id,reply_to_text,reply_to_sender) "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    (chat_id, sender, data.get("text",""), data.get("file_url"),
                     data.get("file_type"), data.get("reply_to_id"),
                     data.get("reply_to_text"), data.get("reply_to_sender"))
                )
                conn.commit()
                data["id"] = cursor.lastrowid
                data["action"] = "new_message"
                await manager.broadcast_chat(chat_id, data)

            elif action == "delete":
                msg_id = data.get("message_id")
                cursor.execute("DELETE FROM messages WHERE id=?", (msg_id,))
                conn.commit()
                await manager.broadcast_chat(chat_id, {"action": "deleted", "message_ids": [msg_id], "deleted_by": data.get("sender")})

    except WebSocketDisconnect:
        manager.disconnect(chat_id, websocket, nickname)
