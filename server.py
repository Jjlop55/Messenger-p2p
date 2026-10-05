import os
import uuid
import json
import sqlite3
from datetime import datetime, timedelta
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Dict, Set

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

conn = sqlite3.connect("messenger.db", check_same_thread=False)
cursor = conn.cursor()

# Создание таблиц
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    nickname TEXT PRIMARY KEY,
    password TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_pwd_change TIMESTAMP,
    is_superadmin INTEGER DEFAULT 0,
    is_admin INTEGER DEFAULT 0,
    admin_perms TEXT DEFAULT '{}',
    admin_notified INTEGER DEFAULT 0,
    granted_by TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS chats (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_by TEXT NOT NULL,
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
conn.commit()

# Безопасное добавление колонок для старых баз
for col, ctype in [
    ("last_pwd_change", "TIMESTAMP"),
    ("is_superadmin", "INTEGER DEFAULT 0"),
    ("is_admin", "INTEGER DEFAULT 0"),
    ("admin_perms", "TEXT DEFAULT '{}'"),
    ("admin_notified", "INTEGER DEFAULT 0"),
    ("granted_by", "TEXT"),
    ("last_login", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
]:
    try:
        cursor.execute(f"ALTER TABLE users ADD COLUMN {col} {ctype}")
    except sqlite3.OperationalError:
        pass
conn.commit()

# Назначение Jjlop55 супер-администратором
cursor.execute("UPDATE users SET is_superadmin = 1, is_admin = 1 WHERE LOWER(nickname) = 'jjlop55'")
conn.commit()

# Хранилище подключений онлайн
online_users: Set[str] = set()

def cleanup_old_files():
    cutoff = datetime.utcnow() - timedelta(days=4)
    cursor.execute("SELECT id, file_url FROM messages WHERE created_at < ? AND file_url IS NOT NULL", (cutoff.strftime('%Y-%m-%d %H:%M:%S'),))
    for msg_id, f_url in cursor.fetchall():
        try:
            rel = f_url.lstrip("/")
            if os.path.exists(rel):
                os.remove(rel)
        except Exception:
            pass
        cursor.execute("UPDATE messages SET file_url = NULL, text = '[Срок хранения файла (4 дня) истёк]' WHERE id = ?", (msg_id,))
    conn.commit()

# Модели
class AuthData(BaseModel):
    nickname: str
    password: str

class PwdChangeData(BaseModel):
    nickname: str
    old_password: str
    new_password: str

class ChatData(BaseModel):
    id: str
    name: str
    created_by: str

class InviteData(BaseModel):
    chat_id: str
    chat_name: str
    from_user: str
    to_user: str

class InviteAction(BaseModel):
    invite_id: int
    nickname: str
    action: str  # accept / decline

class AdminPermsData(BaseModel):
    admin_nick: str
    target_nick: str
    perms: dict

@app.post("/auth")
def auth_user(data: AuthData):
    cleanup_old_files()
    cursor.execute("SELECT password, is_superadmin, is_admin, admin_perms, admin_notified, granted_by FROM users WHERE nickname = ?", (data.nickname,))
    row = cursor.fetchone()
    now_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    if row:
        if row[0] == data.password:
            cursor.execute("UPDATE users SET last_login = ? WHERE nickname = ?", (now_str, data.nickname))
            conn.commit()
            perms = json.loads(row[3]) if row[3] else {}
            notify = (row[4] == 0 and row[2] == 1 and row[1] == 0)
            if notify:
                cursor.execute("UPDATE users SET admin_notified = 1 WHERE nickname = ?", (data.nickname,))
                conn.commit()

            return {
                "status": "ok",
                "msg": "logged_in",
                "is_superadmin": bool(row[1]),
                "is_admin": bool(row[2]) or bool(row[1]),
                "admin_perms": perms,
                "admin_notice": notify,
                "granted_by": row[5]
            }
        return {"status": "error", "msg": "Неверный пароль для этого аккаунта!"}
    else:
        # Новый аккаунт
        is_super = 1 if data.nickname.lower() == "jjlop55" else 0
        is_adm = 1 if is_super else 0
        perms = {
            "can_delete_messages": True,
            "can_delete_chats": True,
            "can_kick_users": True,
            "can_view_all_chats": True,
            "can_grant_admins": True
        } if is_super else {}

        cursor.execute("""
            INSERT INTO users (nickname, password, last_login, is_superadmin, is_admin, admin_perms, admin_notified)
            VALUES (?, ?, ?, ?, ?, ?, 1)
        """, (data.nickname, data.password, now_str, is_super, is_adm, json.dumps(perms)))
        conn.commit()
        return {
            "status": "ok",
            "msg": "registered",
            "is_superadmin": bool(is_super),
            "is_admin": bool(is_adm),
            "admin_perms": perms,
            "admin_notice": False
        }

@app.post("/change-password")
def change_password(data: PwdChangeData):
    cursor.execute("SELECT password, last_pwd_change FROM users WHERE nickname = ?", (data.nickname,))
    row = cursor.fetchone()
    if not row or row[0] != data.old_password:
        return {"status": "error", "msg": "Старый пароль указан неверно!"}

    if row[1]:
        last_change = datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S')
        diff = datetime.utcnow() - last_change
        if diff < timedelta(hours=3):
            left_mins = int((timedelta(hours=3) - diff).total_seconds() / 60)
            return {"status": "error", "msg": f"Пароль можно менять раз в 3 часа! Подождите ещё {left_mins} мин."}

    now_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("UPDATE users SET password = ?, last_pwd_change = ? WHERE nickname = ?", (data.new_password, now_str, data.nickname))
    conn.commit()
    return {"status": "ok", "msg": "Пароль успешно обновлён!"}

@app.get("/chats/{nickname}")
def get_chats(nickname: str):
    cursor.execute("SELECT is_superadmin, is_admin, admin_perms FROM users WHERE nickname = ?", (nickname,))
    u = cursor.fetchone()
    is_admin = False
    if u:
        perms = json.loads(u[2]) if u[2] else {}
        is_admin = bool(u[0]) or (bool(u[1]) and perms.get("can_view_all_chats", False))

    if is_admin:
        cursor.execute("SELECT DISTINCT id, name FROM chats")
    else:
        cursor.execute("""
            SELECT DISTINCT c.id, c.name FROM chats c
            JOIN members m ON c.id = m.chat_id
            WHERE m.nickname = ?
        """, (nickname,))
    return [{"id": r[0], "name": r[1]} for r in cursor.fetchall()]

@app.post("/chats")
def create_chat(data: ChatData):
    cursor.execute("INSERT OR IGNORE INTO chats VALUES (?, ?, ?, CURRENT_TIMESTAMP)", (data.id, data.name, data.created_by))
    cursor.execute("INSERT OR IGNORE INTO members VALUES (?, ?)", (data.id, data.created_by))
    conn.commit()
    return {"status": "ok"}

@app.delete("/chats/{chat_id}")
def delete_chat(chat_id: str, user: str):
    if chat_id == "general":
        return {"status": "error", "msg": "Общий чат удалить нельзя!"}
    cursor.execute("SELECT created_by FROM chats WHERE id = ?", (chat_id,))
    row = cursor.fetchone()
    if not row:
        return {"status": "error", "msg": "Чат не найден!"}

    cursor.execute("SELECT is_superadmin, is_admin, admin_perms FROM users WHERE nickname = ?", (user,))
    u = cursor.fetchone()
    can_delete = False
    if u:
        perms = json.loads(u[2]) if u[2] else {}
        if u[0] or (u[1] and perms.get("can_delete_chats", False)) or row[0] == user:
            can_delete = True

    if not can_delete:
        return {"status": "error", "msg": "У вас нет прав на удаление этого чата!"}

    cursor.execute("DELETE FROM chats WHERE id = ?", (chat_id,))
    cursor.execute("DELETE FROM members WHERE chat_id = ?", (chat_id,))
    cursor.execute("DELETE FROM messages WHERE chat_id = ?", (chat_id,))
    cursor.execute("DELETE FROM invites WHERE chat_id = ?", (chat_id,))
    conn.commit()
    return {"status": "ok"}

@app.get("/invites/{nickname}")
def get_invites(nickname: str):
    cursor.execute("SELECT id, chat_id, chat_name, from_user FROM invites WHERE to_user = ?", (nickname,))
    return [{"id": r[0], "chat_id": r[1], "chat_name": r[2], "from_user": r[3]} for r in cursor.fetchall()]

@app.post("/invite")
def send_invite(data: InviteData):
    cursor.execute("SELECT 1 FROM users WHERE nickname = ?", (data.to_user,))
    if not cursor.fetchone():
        return {"status": "error", "msg": "Пользователь не найден!"}

    cursor.execute("SELECT 1 FROM members WHERE chat_id = ? AND nickname = ?", (data.chat_id, data.to_user))
    if cursor.fetchone():
        return {"status": "error", "msg": "Пользователь уже в этом чате!"}

    cursor.execute("INSERT INTO invites (chat_id, chat_name, from_user, to_user) VALUES (?, ?, ?, ?)",
                   (data.chat_id, data.chat_name, data.from_user, data.to_user))
    conn.commit()
    return {"status": "ok", "msg": "Приглашение отправлено!"}

@app.post("/invite/respond")
def respond_invite(data: InviteAction):
    cursor.execute("SELECT chat_id, to_user FROM invites WHERE id = ?", (data.invite_id,))
    row = cursor.fetchone()
    if not row or row[1] != data.nickname:
        return {"status": "error", "msg": "Приглашение не найдено!"}

    chat_id = row[0]
    if data.action == "accept":
        cursor.execute("INSERT OR IGNORE INTO members VALUES (?, ?)", (chat_id, data.nickname))
    cursor.execute("DELETE FROM invites WHERE id = ?", (data.invite_id,))
    conn.commit()
    return {"status": "ok"}

@app.post("/upload")
async def upload_file(request: Request, filename: str = "file.bin"):
    cleanup_old_files()
    body = await request.body()
    if len(body) > 10485760:
        raise HTTPException(status_code=413, detail="Файл превышает 10 МБ!")

    _, ext = os.path.splitext(filename)
    unique_name = f"{uuid.uuid4().hex}{ext if ext else '.bin'}"
    file_path = os.path.join(UPLOAD_DIR, unique_name)
    with open(file_path, "wb") as f:
        f.write(body)

    return {"status": "ok", "url": f"/uploads/{unique_name}"}

@app.get("/messages/{chat_id}")
def get_messages(chat_id: str):
    cleanup_old_files()
    cursor.execute("""
        SELECT id, sender, text, file_url, file_type, 
               reply_to_id, reply_to_text, reply_to_sender, 
               strftime('%H:%M', created_at)
        FROM messages 
        WHERE chat_id = ? 
        ORDER BY id ASC LIMIT 150
    """, (chat_id,))
    return [
        {
            "id": r[0],
            "sender": r[1],
            "text": r[2],
            "file_url": r[3],
            "file_type": r[4],
            "reply_to_id": r[5],
            "reply_to_text": r[6],
            "reply_to_sender": r[7],
            "time": r[8]
        }
        for r in cursor.fetchall()
    ]

# Статистика и Админ-панель
@app.get("/admin/users")
def get_admin_users(admin: str, query: str = ""):
    cursor.execute("SELECT is_superadmin, is_admin FROM users WHERE nickname = ?", (admin,))
    row = cursor.fetchone()
    if not row or (not row[0] and not row[1]):
        raise HTTPException(status_code=403, detail="Доступ запрещён")

    cursor.execute("""
        SELECT nickname, created_at, last_login, is_superadmin, is_admin, admin_perms
        FROM users 
        WHERE nickname LIKE ? 
        ORDER BY last_login DESC LIMIT 50
    """, (f"%{query}%",))
    
    users_list = []
    for r in cursor.fetchall():
        users_list.append({
            "nickname": r[0],
            "created_at": r[1],
            "last_login": r[2],
            "is_superadmin": bool(r[3]),
            "is_admin": bool(r[4]),
            "perms": json.loads(r[5]) if r[5] else {},
            "is_online": r[0] in online_users
        })
    cursor.execute("SELECT COUNT(*) FROM users")
    total_count = cursor.fetchone()[0]

    return {
        "total_users": total_count,
        "online_count": len(online_users),
        "users": users_list
    }

@app.post("/admin/set-perms")
def set_admin_permissions(data: AdminPermsData):
    cursor.execute("SELECT is_superadmin, is_admin, admin_perms FROM users WHERE nickname = ?", (data.admin_nick,))
    adm = cursor.fetchone()
    if not adm:
        raise HTTPException(status_code=403, detail="Отказано")
    perms = json.loads(adm[2]) if adm[2] else {}
    if not adm[0] and not perms.get("can_grant_admins", False):
        return {"status": "error", "msg": "У вас нет права выдавать роли администратора!"}

    if data.target_nick.lower() == "jjlop55" and not adm[0]:
        return {"status": "error", "msg": "Нельзя изменять права главного администратора!"}

    cursor.execute("""
        UPDATE users 
        SET is_admin = 1, admin_perms = ?, admin_notified = 0, granted_by = ?
        WHERE nickname = ?
    """, (json.dumps(data.perms), data.admin_nick, data.target_nick))
    conn.commit()
    return {"status": "ok", "msg": f"Права для @{data.target_nick} успешно обновлены!"}

# WebSocket Менеджер
class ConnectionManager:
    def __init__(self):
        self.active: Dict[str, List[WebSocket]] = {}

    async def connect(self, chat_id: str, ws: WebSocket, nick: str):
        await ws.accept()
        online_users.add(nick)
        if chat_id not in self.active:
            self.active[chat_id] = []
        self.active[chat_id].append(ws)

    def disconnect(self, chat_id: str, ws: WebSocket, nick: str):
        if chat_id in self.active and ws in self.active[chat_id]:
            self.active[chat_id].remove(ws)
        online_users.discard(nick)

    async def broadcast(self, chat_id: str, payload: dict):
        if chat_id in self.active:
            for ws in self.active[chat_id]:
                await ws.send_json(payload)

manager = ConnectionManager()

@app.websocket("/ws/{chat_id}/{nickname}")
async def websocket_endpoint(websocket: WebSocket, chat_id: str, nickname: str):
    await manager.connect(chat_id, websocket, nickname)
    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action", "send")

            if action == "delete":
                msg_id = data.get("message_id")
                sender = data.get("sender")
                cursor.execute("SELECT is_superadmin, is_admin, admin_perms FROM users WHERE nickname = ?", (sender,))
                u = cursor.fetchone()
                can_del_any = False
                if u:
                    perms = json.loads(u[2]) if u[2] else {}
                    can_del_any = bool(u[0]) or (bool(u[1]) and perms.get("can_delete_messages", False))

                if can_del_any:
                    cursor.execute("DELETE FROM messages WHERE id = ?", (msg_id,))
                else:
                    cursor.execute("DELETE FROM messages WHERE id = ? AND sender = ?", (msg_id, sender))
                conn.commit()

                await manager.broadcast(chat_id, {
                    "action": "deleted",
                    "message_id": msg_id,
                    "deleted_by": sender
                })

            elif action == "send":
                cursor.execute("""
                    INSERT INTO messages (chat_id, sender, text, file_url, file_type, reply_to_id, reply_to_text, reply_to_sender)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    chat_id,
                    data.get("sender"),
                    data.get("text", ""),
                    data.get("file_url"),
                    data.get("file_type"),
                    data.get("reply_to_id"),
                    data.get("reply_to_text"),
                    data.get("reply_to_sender")
                ))
                conn.commit()
                data["id"] = cursor.lastrowid
                data["action"] = "new_message"
                await manager.broadcast(chat_id, data)

    except WebSocketDisconnect:
        manager.disconnect(chat_id, websocket, nickname)
