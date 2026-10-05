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

# Подключение к SQLite с защитой от блокировок очереди
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

# Всегда гарантируем наличие Общего чата
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

def periodic_cleanup():
    ns = now_str()
    # Очистка общего чата general 4 раза в час (сообщения старше 15 мин)
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

class ProfileUpdateData(BaseModel):
    current_nickname: str
    new_nickname: Optional[str] = None
    avatar_url: Optional[str] = None
    bio: Optional[str] = None

class FriendActionData(BaseModel):
    from_user: str
    target_user: str
    action: str  # request / accept / decline / remove

class DirectChatData(BaseModel):
    from_user: str
    target_user: str

class ReadChatData(BaseModel):
    chat_id: str
    nickname: str

class ChatCreateData(BaseModel):
    id: str
    name: str
    created_by: str

# ═══════════════════════════════════════════════════════════
#  CONNECTION MANAGER
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
        for ws in list(self.user_sockets.get(nick, [])):
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

    # Проверка банов
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
        SELECT password, is_superadmin, is_admin, admin_perms, avatar_url, bio 
        FROM users WHERE nickname=?
    """, (data.nickname,))
    row = cursor.fetchone()
    session_token = uuid.uuid4().hex

    if row:
        if not verify_pwd(data.password, row[0]):
            return {"status": "error", "msg": "Неверный пароль!"}

        hashed = hash_pwd(data.password)
        cursor.execute("UPDATE users SET password=?, last_login=? WHERE nickname=?", (hashed, ns, data.nickname))
        cursor.execute(
            "INSERT INTO user_sessions (nickname, session_token, user_agent, ip_address, logged_in_at, last_active, is_active) VALUES (?,?,?,?,?,?,1)",
            (data.nickname, session_token, data.user_agent, ip, ns, ns)
        )
        # Добавляем в общий чат
        cursor.execute("INSERT OR IGNORE INTO members (chat_id, nickname) VALUES ('general', ?)", (data.nickname,))
        conn.commit()

        perms = json.loads(row[3]) if row[3] else {}
        return {
            "status": "ok",
            "is_superadmin": bool(row[1]) or is_super(data.nickname),
            "is_admin": bool(row[2]) or bool(row[1]) or is_super(data.nickname),
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
            "is_superadmin": bool(is_sup),
            "is_admin": bool(is_sup),
            "admin_perms": perms,
            "avatar_url": "",
            "bio": "",
            "session_token": session_token
        }

# ═══════════════════════════════════════════════════════════
#  ПРОФИЛЬ (СТРОГИЙ ЗАПРЕТ СМЕНЫ НИКА ДЛЯ ГЛАВНОГО АДМИНА)
# ═══════════════════════════════════════════════════════════

@app.post("/profile/update")
def update_profile(data: ProfileUpdateData):
    cursor.execute("SELECT nickname, last_username_change FROM users WHERE nickname=?", (data.current_nickname,))
    user = cursor.fetchone()
    if not user:
        return {"status": "error", "msg": "Пользователь не найден!"}

    new_nick = data.new_nickname.strip() if data.new_nickname else data.current_nickname

    # СИСТЕМНЫЙ ЗАПРЕТ: Главному администратору смена ника заблокирована намертво
    if new_nick != data.current_nickname:
        if is_super(data.current_nickname):
            return {"status": "error", "msg": "🔒 Смена юзернейма для Главного Администратора заблокирована на уровне ядра системы!"}

        last_change = user[1]
        if last_change:
            diff = datetime.utcnow() - datetime.strptime(last_change, '%Y-%m-%d %H:%M:%S')
            if diff < timedelta(days=1):
                hours_left = int(24 - (diff.total_seconds() / 3600))
                return {"status": "error", "msg": f"Юзернейм можно менять только 1 раз в день! Ждать ещё {hours_left} ч."}

        cursor.execute("SELECT 1 FROM users WHERE LOWER(nickname)=LOWER(?)", (new_nick,))
        if cursor.fetchone():
            return {"status": "error", "msg": f"Юзернейм @{new_nick} уже занят!"}

        # Каскадное обновление юзернейма
        cursor.execute("UPDATE users SET nickname=?, last_username_change=? WHERE nickname=?", (new_nick, now_str(), data.current_nickname))
        cursor.execute("UPDATE members SET nickname=? WHERE nickname=?", (new_nick, data.current_nickname))
        cursor.execute("UPDATE messages SET sender=? WHERE sender=?", (new_nick, data.current_nickname))
        cursor.execute("UPDATE messages SET reply_to_sender=? WHERE reply_to_sender=?", (new_nick, data.current_nickname))
        cursor.execute("UPDATE chats SET created_by=? WHERE created_by=?", (new_nick, data.current_nickname))
        cursor.execute("UPDATE chats SET direct_user1=? WHERE direct_user1=?", (new_nick, data.current_nickname))
        cursor.execute("UPDATE chats SET direct_user2=? WHERE direct_user2=?", (new_nick, data.current_nickname))
        cursor.execute("UPDATE friends SET user1=? WHERE user1=?", (new_nick, data.current_nickname))
        cursor.execute("UPDATE friends SET user2=? WHERE user2=?", (new_nick, data.current_nickname))
        cursor.execute("UPDATE user_sessions SET nickname=? WHERE nickname=?", (new_nick, data.current_nickname))
        conn.commit()

    target_nick = new_nick if new_nick != data.current_nickname else data.current_nickname
    if data.avatar_url is not None:
        cursor.execute("UPDATE users SET avatar_url=? WHERE nickname=?", (data.avatar_url, target_nick))
    if data.bio is not None:
        # Лимит 45 слов
        words = data.bio.strip().split()
        if len(words) > 45:
            return {"status": "error", "msg": "Описание «О себе» не должно превышать 45 слов!"}
        cursor.execute("UPDATE users SET bio=? WHERE nickname=?", (" ".join(words), target_nick))
    conn.commit()

    return {"status": "ok", "msg": "Профиль успешно обновлён!", "new_nickname": target_nick}

# ═══════════════════════════════════════════════════════════
#  СИСТЕМА ДРУЗЕЙ В СТИЛЕ DISCORD
# ═══════════════════════════════════════════════════════════

@app.post("/friends/action")
async def manage_friend(data: FriendActionData):
    if data.from_user == data.target_user:
        return {"status": "error", "msg": "Нельзя взаимодействовать с самим собой!"}

    cursor.execute("SELECT 1 FROM users WHERE nickname=?", (data.target_user,))
    if not cursor.fetchone():
        return {"status": "error", "msg": f"Пользователь @{data.target_user} не найден!"}

    if data.action == "request":
        # Проверяем, нет ли уже дружбы или заявки
        cursor.execute("""
            SELECT id, status FROM friends 
            WHERE (user1=? AND user2=?) OR (user1=? AND user2=?)
        """, (data.from_user, data.target_user, data.target_user, data.from_user))
        row = cursor.fetchone()
        if row:
            if row[1] == "accepted":
                return {"status": "error", "msg": "Вы уже друзья!"}
            return {"status": "error", "msg": "Заявка уже отправлена!"}

        cursor.execute("INSERT INTO friends (user1, user2, status) VALUES (?, ?, 'pending')", (data.from_user, data.target_user))
        conn.commit()
        await manager.send_to_user(data.target_user, {"action": "friend_request", "from": data.from_user})
        return {"status": "ok", "msg": f"Заявка в друзья отправлена @{data.target_user}!"}

    elif data.action == "accept":
        cursor.execute("UPDATE friends SET status='accepted' WHERE user1=? AND user2=?", (data.target_user, data.from_user))
        conn.commit()
        await manager.send_to_user(data.target_user, {"action": "friend_accepted", "by": data.from_user})
        return {"status": "ok", "msg": f"Заявка от @{data.target_user} принята!"}

    elif data.action in ("decline", "remove"):
        cursor.execute("""
            DELETE FROM friends 
            WHERE (user1=? AND user2=?) OR (user1=? AND user2=?)
        """, (data.from_user, data.target_user, data.target_user, data.from_user))
        conn.commit()
        return {"status": "ok", "msg": "Удалено из друзей."}

    return {"status": "error", "msg": "Неизвестное действие"}

@app.get("/friends/{nickname}")
def get_friends(nickname: str):
    # Принятые друзья
    cursor.execute("""
        SELECT CASE WHEN user1=? THEN user2 ELSE user1 END as friend_nick
        FROM friends 
        WHERE (user1=? OR user2=?) AND status='accepted'
    """, (nickname, nickname, nickname))
    friend_nicks = [r[0] for r in cursor.fetchall()]

    friends_list = []
    for fn in friend_nicks:
        cursor.execute("SELECT nickname, avatar_url, bio, last_login FROM users WHERE nickname=?", (fn,))
        u = cursor.fetchone()
        if u:
            friends_list.append({
                "nickname": u[0],
                "avatar_url": u[1] or "",
                "bio": u[2] or "",
                "last_login": u[3],
                "is_online": manager.is_online(u[0])
            })

    # Входящие заявки
    cursor.execute("""
        SELECT u.nickname, u.avatar_url, u.bio
        FROM friends f
        JOIN users u ON f.user1 = u.nickname
        WHERE f.user2=? AND f.status='pending'
    """, (nickname,))
    incoming = [{"nickname": r[0], "avatar_url": r[1] or "", "bio": r[2] or ""} for r in cursor.fetchall()]

    # Исходящие заявки
    cursor.execute("""
        SELECT u.nickname, u.avatar_url
        FROM friends f
        JOIN users u ON f.user2 = u.nickname
        WHERE f.user1=? AND f.status='pending'
    """, (nickname,))
    outgoing = [{"nickname": r[0], "avatar_url": r[1] or ""} for r in cursor.fetchall()]

    return {"friends": friends_list, "incoming": incoming, "outgoing": outgoing}

# ═══════════════════════════════════════════════════════════
#  ДИАЛОГИ И ЧАТЫ
# ═══════════════════════════════════════════════════════════

@app.post("/direct-chat")
def get_or_create_direct_chat(data: DirectChatData):
    if data.from_user == data.target_user:
        return {"status": "error", "msg": "Нельзя писать самому себе!"}

    cursor.execute("""
        SELECT id FROM chats 
        WHERE is_direct=1 AND (
            (direct_user1=? AND direct_user2=?) OR 
            (direct_user1=? AND direct_user2=?)
        ) LIMIT 1
    """, (data.from_user, data.target_user, data.target_user, data.from_user))
    row = cursor.fetchone()

    if row:
        return {"status": "ok", "chat_id": row[0], "chat_name": f"💬 @{data.target_user}"}

    cid = f"dm_{uuid.uuid4().hex[:12]}"
    cname = f"💬 @{data.target_user}"
    cursor.execute(
        "INSERT INTO chats (id, name, created_by, is_direct, direct_user1, direct_user2) VALUES (?, ?, ?, 1, ?, ?)",
        (cid, cname, data.from_user, data.from_user, data.target_user)
    )
    cursor.execute("INSERT OR IGNORE INTO members (chat_id, nickname) VALUES (?, ?)", (cid, data.from_user))
    cursor.execute("INSERT OR IGNORE INTO members (chat_id, nickname) VALUES (?, ?)", (cid, data.target_user))
    conn.commit()

    return {"status": "ok", "chat_id": cid, "chat_name": cname}

@app.get("/chats/{nickname}")
def get_user_chats(nickname: str):
    # Общий чат ВСЕГДА идёт первым
    cursor.execute("""
        SELECT DISTINCT c.id, c.name, c.is_direct, c.direct_user1, c.direct_user2
        FROM chats c
        LEFT JOIN members m ON c.id=m.chat_id
        WHERE c.id='general' OR m.nickname=?
    """, (nickname,))
    
    chats = []
    for r in cursor.fetchall():
        cid, cname, is_dir, u1, u2 = r
        if is_dir:
            partner = u2 if u1 == nickname else u1
            cname = f"💬 @{partner}"
        chats.append({"id": cid, "name": cname})

    # Сортируем: general на 1-м месте
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
    cursor.execute("SELECT created_by, is_direct FROM chats WHERE id=?", (chat_id,))
    row = cursor.fetchone()
    if not row:
        return {"status": "error", "msg": "Чат не найден!"}

    cursor.execute("DELETE FROM chats WHERE id=?", (chat_id,))
    cursor.execute("DELETE FROM members WHERE chat_id=?", (chat_id,))
    cursor.execute("DELETE FROM messages WHERE chat_id=?", (chat_id,))
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
        SELECT id, sender, text, file_url, file_type, reply_to_id, reply_to_text, reply_to_sender,
               strftime('%H:%M', created_at)
        FROM messages WHERE chat_id=? ORDER BY id ASC LIMIT 150
    """, (chat_id,))
    return [{
        "id": r[0], "sender": r[1], "text": r[2], "file_url": r[3], "file_type": r[4],
        "reply_to_id": r[5], "reply_to_text": r[6], "reply_to_sender": r[7], "time": r[8]
    } for r in cursor.fetchall()]

@app.get("/user/info/{nickname}")
def get_user_info(nickname: str):
    cursor.execute("SELECT nickname, avatar_url, bio, last_login FROM users WHERE nickname=?", (nickname,))
    u = cursor.fetchone()
    if not u:
        return {"status": "error"}
    return {
        "status": "ok",
        "nickname": u[0],
        "avatar_url": u[1] or "",
        "bio": u[2] or "",
        "last_login": u[3],
        "is_online": manager.is_online(u[0])
    }

# ═══════════════════════════════════════════════════════════
#  WEBSOCKET ENDPOINT (WebRTC Видео, Демонстрация экрана, Пинг)
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

            # Сигналы WebRTC (аудио, камера, демка экрана, качество связи)
            elif action in ("call_offer", "call_answer", "call_ice", "call_reject", "call_end", "call_busy", "network_warning"):
                target = data.get("target")
                if target:
                    if manager.is_online(target):
                        await manager.send_to_user(target, data)
                    else:
                        await websocket.send_json({"action": "call_offline", "target": target})

            elif action == "send":
                sender = data.get("sender")
                cursor.execute("""
                    INSERT INTO messages (chat_id, sender, text, file_url, file_type, reply_to_id, reply_to_text, reply_to_sender)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (chat_id, sender, data.get("text",""), data.get("file_url"), data.get("file_type"),
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
