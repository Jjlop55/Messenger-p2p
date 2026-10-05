import os
import sys
import time
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

SERVER_START_TIME = time.time()
conn = sqlite3.connect("messenger.db", check_same_thread=False)
cursor = conn.cursor()

# Базовые таблицы
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
    granted_by TEXT,
    revoked_by TEXT
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

# Заявки на удаление аккаунтов (7 дней)
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

# Оповещения пользователям
cursor.execute("""
CREATE TABLE IF NOT EXISTS notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    to_user TEXT NOT NULL,
    text TEXT NOT NULL,
    is_read INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
conn.commit()

# Безопасное добавление колонок
for col, ctype in [
    ("last_pwd_change", "TIMESTAMP"),
    ("is_superadmin", "INTEGER DEFAULT 0"),
    ("is_admin", "INTEGER DEFAULT 0"),
    ("admin_perms", "TEXT DEFAULT '{}'"),
    ("admin_notified", "INTEGER DEFAULT 0"),
    ("granted_by", "TEXT"),
    ("revoked_by", "TEXT"),
    ("last_login", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
]:
    try:
        cursor.execute(f"ALTER TABLE users ADD COLUMN {col} {ctype}")
    except sqlite3.OperationalError:
        pass
conn.commit()

# Назначение Главного Администратора
cursor.execute("UPDATE users SET is_superadmin = 1, is_admin = 1 WHERE LOWER(nickname) IN ('jjlop55', 'gglo55')")
conn.commit()

def is_super(nick: str) -> bool:
    return nick.lower() in ("jjlop55", "gglo55")

# Очистка файлов старше 4 дней и просроченных заявок на удаление (7 дней)
def periodic_cleanup():
    cutoff_files = datetime.utcnow() - timedelta(days=4)
    cursor.execute("SELECT id, file_url FROM messages WHERE created_at < ? AND file_url IS NOT NULL", (cutoff_files.strftime('%Y-%m-%d %H:%M:%S'),))
    for msg_id, f_url in cursor.fetchall():
        try:
            rel = f_url.lstrip("/")
            if os.path.exists(rel):
                os.remove(rel)
        except Exception:
            pass
        cursor.execute("UPDATE messages SET file_url = NULL, text = '[Срок хранения файла (4 дня) истёк]' WHERE id = ?", (msg_id,))
    
    # Проверка просроченных заявок (7 дней)
    now_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("SELECT id, target_nick, requested_by FROM delete_requests WHERE expires_at < ? AND status = 'pending'", (now_str,))
    for req_id, t_nick, r_by in cursor.fetchall():
        cursor.execute("UPDATE delete_requests SET status = 'expired' WHERE id = ?", (req_id,))
        cursor.execute("""
            INSERT INTO notifications (to_user, text)
            VALUES (?, ?)
        """, (r_by, f"Главный администратор не принял вашу заявку на удаление @{t_nick} (истёк 7-дневный срок рассмотрения)."))
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
    action: str

class AdminPermsData(BaseModel):
    admin_nick: str
    target_nick: str
    perms: dict

class DeleteUserAction(BaseModel):
    admin_nick: str
    target_nick: str
    reason: str = ""

class ReqDecision(BaseModel):
    admin_nick: str
    request_id: int
    action: str  # approve / reject

# WebSocket Connection Tracker
class ConnectionManager:
    def __init__(self):
        self.user_sockets: Dict[str, Set[WebSocket]] = {}
        self.chat_sockets: Dict[str, Set[WebSocket]] = {}

    async def connect(self, chat_id: str, ws: WebSocket, nick: str):
        await ws.accept()
        if nick not in self.user_sockets:
            self.user_sockets[nick] = set()
        self.user_sockets[nick].add(ws)

        if chat_id not in self.chat_sockets:
            self.chat_sockets[chat_id] = set()
        self.chat_sockets[chat_id].add(ws)

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
        return nick in self.user_sockets and len(self.user_sockets[nick]) > 0

    async def send_to_user(self, nick: str, payload: dict):
        if nick in self.user_sockets:
            for ws in list(self.user_sockets[nick]):
                try:
                    await ws.send_json(payload)
                except Exception:
                    pass

    async def broadcast_chat(self, chat_id: str, payload: dict):
        if chat_id in self.chat_sockets:
            for ws in list(self.chat_sockets[chat_id]):
                try:
                    await ws.send_json(payload)
                except Exception:
                    pass

manager = ConnectionManager()

@app.post("/auth")
def auth_user(data: AuthData):
    periodic_cleanup()
    cursor.execute("""
        SELECT password, is_superadmin, is_admin, admin_perms, admin_notified, granted_by, revoked_by 
        FROM users WHERE nickname = ?
    """, (data.nickname,))
    row = cursor.fetchone()
    now_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    if row:
        if row[0] == data.password:
            cursor.execute("UPDATE users SET last_login = ? WHERE nickname = ?", (now_str, data.nickname))
            conn.commit()

            perms = json.loads(row[3]) if row[3] else {}
            notify_granted = (row[4] == 0 and row[2] == 1 and row[1] == 0)
            revoked_from = row[6]

            if notify_granted:
                cursor.execute("UPDATE users SET admin_notified = 1 WHERE nickname = ?", (data.nickname,))
                conn.commit()
            if revoked_from:
                cursor.execute("UPDATE users SET revoked_by = NULL WHERE nickname = ?", (data.nickname,))
                conn.commit()

            return {
                "status": "ok",
                "msg": "logged_in",
                "is_superadmin": bool(row[1]) or is_super(data.nickname),
                "is_admin": bool(row[2]) or bool(row[1]) or is_super(data.nickname),
                "admin_perms": perms,
                "admin_notice": notify_granted,
                "granted_by": row[5],
                "revoked_by": revoked_from
            }
        return {"status": "error", "msg": "Неверный пароль для этого аккаунта!"}
    else:
        is_sup = 1 if is_super(data.nickname) else 0
        perms = {
            "can_delete_messages": True,
            "can_delete_chats": True,
            "can_kick_users": True,
            "can_view_all_chats": True,
            "can_grant_admins": True,
            "can_delete_users_direct": True,
            "can_request_delete_users": True
        } if is_sup else {}

        cursor.execute("""
            INSERT INTO users (nickname, password, last_login, is_superadmin, is_admin, admin_perms, admin_notified)
            VALUES (?, ?, ?, ?, ?, ?, 1)
        """, (data.nickname, data.password, now_str, is_sup, is_sup, json.dumps(perms)))
        conn.commit()
        return {
            "status": "ok",
            "msg": "registered",
            "is_superadmin": bool(is_sup),
            "is_admin": bool(is_sup),
            "admin_perms": perms,
            "admin_notice": False,
            "revoked_by": None
        }

@app.post("/change-password")
def change_password(data: PwdChangeData):
    cursor.execute("SELECT password, last_pwd_change FROM users WHERE nickname = ?", (data.nickname,))
    row = cursor.fetchone()
    if not row or row[0] != data.old_password:
        return {"status": "error", "msg": "Текущий пароль указан неверно!"}

    if row[1]:
        diff = datetime.utcnow() - datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S')
        if diff < timedelta(hours=3):
            mins = int((timedelta(hours=3) - diff).total_seconds() / 60)
            return {"status": "error", "msg": f"Пароль можно менять раз в 3 часа! Подождите {mins} мин."}

    now_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("UPDATE users SET password = ?, last_pwd_change = ? WHERE nickname = ?", (data.new_password, now_str, data.nickname))
    conn.commit()
    return {"status": "ok", "msg": "Пароль успешно обновлён!"}

# Получение списка СВОИХ чатов (строго где пользователь участник)
@app.get("/chats/{nickname}")
def get_user_chats(nickname: str):
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
    can_del = False
    if u:
        perms = json.loads(u[2]) if u[2] else {}
        if u[0] or is_super(user) or (u[1] and perms.get("can_delete_chats", False)) or row[0] == user:
            can_del = True

    if not can_del:
        return {"status": "error", "msg": "У вас нет прав на удаление этого чата!"}

    cursor.execute("DELETE FROM chats WHERE id = ?", (chat_id,))
    cursor.execute("DELETE FROM members WHERE chat_id = ?", (chat_id,))
    cursor.execute("DELETE FROM messages WHERE chat_id = ?", (chat_id,))
    cursor.execute("DELETE FROM invites WHERE chat_id = ?", (chat_id,))
    conn.commit()
    return {"status": "ok"}

# Приглашения
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
        return {"status": "error", "msg": "Пользователь уже находится в этом чате!"}

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
    # При отклонении строго удаляем инвайт, не добавляя в members
    cursor.execute("DELETE FROM invites WHERE id = ?", (data.invite_id,))
    conn.commit()
    return {"status": "ok"}

# Загрузка файлов
@app.post("/upload")
async def upload_file(request: Request, filename: str = "file.bin"):
    periodic_cleanup()
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
    periodic_cleanup()
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

# Оповещения
@app.get("/notifications/{nickname}")
def get_notifications(nickname: str):
    cursor.execute("SELECT id, text FROM notifications WHERE to_user = ? AND is_read = 0", (nickname,))
    rows = cursor.fetchall()
    cursor.execute("UPDATE notifications SET is_read = 1 WHERE to_user = ?", (nickname,))
    conn.commit()
    return [{"id": r[0], "text": r[1]} for r in rows]

# Мониторинг сервера для всех пользователей
@app.get("/server/monitoring")
def server_monitoring():
    uptime_sec = int(time.time() - SERVER_START_TIME)
    h, rem = divmod(uptime_sec, 3600)
    m, s = divmod(rem, 60)
    uptime_fmt = f"{h}ч {m}м {s}с"

    # Подсчет размера uploads
    uploads_bytes = 0
    for root, _, files in os.walk(UPLOAD_DIR):
        for f in files:
            uploads_bytes += os.path.getsize(os.path.join(root, f))
    uploads_mb = round(uploads_bytes / (1024 * 1024), 2)

    db_size_kb = round(os.path.getsize("messenger.db") / 1024, 1) if os.path.exists("messenger.db") else 0

    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM chats")
    total_chats = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM messages")
    total_msgs = cursor.fetchone()[0]

    return {
        "status": "online",
        "uptime": uptime_fmt,
        "online_count": len(manager.user_sockets),
        "total_users": total_users,
        "total_chats": total_chats,
        "total_messages": total_msgs,
        "uploads_mb": uploads_mb,
        "db_size_kb": db_size_kb
    }

# Админ-панель: пользователи
@app.get("/admin/users")
def get_admin_users(admin: str, query: str = ""):
    cursor.execute("SELECT is_superadmin, is_admin FROM users WHERE nickname = ?", (admin,))
    row = cursor.fetchone()
    if not row or (not row[0] and not row[1] and not is_super(admin)):
        raise HTTPException(status_code=403, detail="Доступ запрещён")

    cursor.execute("""
        SELECT nickname, created_at, last_login, is_superadmin, is_admin, admin_perms
        FROM users 
        WHERE nickname LIKE ? 
        ORDER BY last_login DESC LIMIT 60
    """, (f"%{query}%",))
    
    users_list = []
    for r in cursor.fetchall():
        users_list.append({
            "nickname": r[0],
            "created_at": r[1],
            "last_login": r[2],
            "is_superadmin": bool(r[3]) or is_super(r[0]),
            "is_admin": bool(r[4]) or bool(r[3]) or is_super(r[0]),
            "perms": json.loads(r[5]) if r[5] else {},
            "is_online": manager.is_online(r[0])
        })

    cursor.execute("SELECT COUNT(*) FROM users")
    total_count = cursor.fetchone()[0]

    return {
        "total_users": total_count,
        "online_count": len(manager.user_sockets),
        "users": users_list
    }

# Админ-панель: все группы и их участники
@app.get("/admin/groups")
def get_admin_groups(admin: str):
    cursor.execute("SELECT is_superadmin, is_admin FROM users WHERE nickname = ?", (admin,))
    row = cursor.fetchone()
    if not row or (not row[0] and not row[1] and not is_super(admin)):
        raise HTTPException(status_code=403, detail="Доступ запрещён")

    cursor.execute("SELECT id, name, created_by, created_at FROM chats")
    chats = cursor.fetchall()
    groups_list = []

    for cid, cname, ccreator, ccreated in chats:
        cursor.execute("SELECT nickname FROM members WHERE chat_id = ?", (cid,))
        members = [m[0] for m in cursor.fetchall()]
        groups_list.append({
            "id": cid,
            "name": cname,
            "created_by": ccreator,
            "created_at": ccreated,
            "members": members
        })
    return groups_list

# Назначение или снятие прав
@app.post("/admin/set-perms")
async def set_admin_permissions(data: AdminPermsData):
    cursor.execute("SELECT is_superadmin, is_admin, admin_perms FROM users WHERE nickname = ?", (data.admin_nick,))
    adm = cursor.fetchone()
    if not adm:
        raise HTTPException(status_code=403, detail="Отказано")
    perms_adm = json.loads(adm[2]) if adm[2] else {}
    if not adm[0] and not is_super(data.admin_nick) and not perms_adm.get("can_grant_admins", False):
        return {"status": "error", "msg": "У вас нет права изменять роли администраторов!"}

    if is_super(data.target_nick):
        return {"status": "error", "msg": "Нельзя изменять права Главного Администратора!"}

    # Проверяем, есть ли хоть одна галочка
    has_any = any(data.perms.values())
    if has_any:
        cursor.execute("""
            UPDATE users 
            SET is_admin = 1, admin_perms = ?, admin_notified = 0, granted_by = ?, revoked_by = NULL
            WHERE nickname = ?
        """, (json.dumps(data.perms), data.admin_nick, data.target_nick))
        conn.commit()
        return {"status": "ok", "msg": f"Права администратора для @{data.target_nick} успешно обновлены!"}
    else:
        # Лишение ВСЕХ прав
        cursor.execute("""
            UPDATE users 
            SET is_admin = 0, admin_perms = '{}', admin_notified = 1, granted_by = NULL, revoked_by = ?
            WHERE nickname = ?
        """, (data.admin_nick, data.target_nick))
        conn.commit()

        # Мгновенное оповещение по WebSocket
        await manager.send_to_user(data.target_nick, {
            "action": "admin_revoked",
            "revoked_by": data.admin_nick
        })
        return {"status": "ok", "msg": f"Все администраторские права у @{data.target_nick} полностью отозваны!"}

# Удаление аккаунта / Подача заявки на удаление
@app.post("/admin/delete-user")
def delete_user_action(data: DeleteUserAction):
    if is_super(data.target_nick):
        return {"status": "error", "msg": "Главного администратора удалить невозможно!"}

    cursor.execute("SELECT is_superadmin, is_admin, admin_perms FROM users WHERE nickname = ?", (data.admin_nick,))
    adm = cursor.fetchone()
    if not adm:
        raise HTTPException(status_code=403, detail="Отказано")

    is_super_admin = bool(adm[0]) or is_super(data.admin_nick)
    perms = json.loads(adm[2]) if adm[2] else {}

    can_direct = is_super_admin or perms.get("can_delete_users_direct", False)
    can_request = perms.get("can_request_delete_users", False)

    if can_direct:
        # Прямое мгновенное удаление
        cursor.execute("DELETE FROM users WHERE nickname = ?", (data.target_nick,))
        cursor.execute("DELETE FROM members WHERE nickname = ?", (data.target_nick,))
        cursor.execute("DELETE FROM invites WHERE to_user = ? OR from_user = ?", (data.target_nick, data.target_nick))
        conn.commit()
        return {"status": "ok", "msg": f"Аккаунт @{data.target_nick} успешно удалён навсегда!"}

    elif can_request:
        if not data.reason.strip():
            return {"status": "error", "msg": "Укажите причину для подачи заявки на удаление!"}

        expires = datetime.utcnow() + timedelta(days=7)
        cursor.execute("""
            INSERT INTO delete_requests (target_nick, requested_by, reason, expires_at)
            VALUES (?, ?, ?, ?)
        """, (data.target_nick, data.admin_nick, data.reason.strip(), expires.strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()
        return {"status": "ok", "msg": f"Заявка на удаление @{data.target_nick} отправлена главному администратору (срок рассмотрения: 7 дней)."}

    else:
        return {"status": "error", "msg": "У вас нет прав на удаление аккаунтов!"}

# Список заявок на удаление
@app.get("/admin/delete-requests")
def get_delete_requests(admin: str):
    cursor.execute("SELECT is_superadmin, is_admin FROM users WHERE nickname = ?", (admin,))
    row = cursor.fetchone()
    if not row or (not row[0] and not is_super(admin)):
        raise HTTPException(status_code=403, detail="Доступ запрещён")

    periodic_cleanup()
    cursor.execute("""
        SELECT id, target_nick, requested_by, reason, created_at, expires_at
        FROM delete_requests 
        WHERE status = 'pending'
        ORDER BY id DESC
    """)
    return [{
        "id": r[0],
        "target_nick": r[1],
        "requested_by": r[2],
        "reason": r[3],
        "created_at": r[4],
        "expires_at": r[5]
    } for r in cursor.fetchall()]

# Одобрение / отклонение заявки главным администратором
@app.post("/admin/delete-requests/decision")
def decide_delete_request(data: ReqDecision):
    if not is_super(data.admin_nick):
        return {"status": "error", "msg": "Принимать решения по заявкам может только Главный Администратор!"}

    cursor.execute("SELECT target_nick, requested_by FROM delete_requests WHERE id = ? AND status = 'pending'", (data.request_id,))
    req = cursor.fetchone()
    if not req:
        return {"status": "error", "msg": "Заявка не найдена или уже обработана!"}

    target, requester = req[0], req[1]
    if data.action == "approve":
        cursor.execute("UPDATE delete_requests SET status = 'approved' WHERE id = ?", (data.request_id,))
        cursor.execute("DELETE FROM users WHERE nickname = ?", (target,))
        cursor.execute("DELETE FROM members WHERE nickname = ?", (target,))
        cursor.execute("""
            INSERT INTO notifications (to_user, text)
            VALUES (?, ?)
        """, (requester, f"Главный администратор одобрил вашу заявку: аккаунт @{target} был удалён."))
        conn.commit()
        return {"status": "ok", "msg": f"Заявка одобрена: @{target} удалён!"}
    else:
        cursor.execute("UPDATE delete_requests SET status = 'rejected' WHERE id = ?", (data.request_id,))
        cursor.execute("""
            INSERT INTO notifications (to_user, text)
            VALUES (?, ?)
        """, (requester, f"Главный администратор отклонил вашу заявку на удаление @{target}."))
        conn.commit()
        return {"status": "ok", "msg": "Заявка отклонена."}

# WebSocket Endpoint с Heartbeat и мультиудалением
@app.websocket("/ws/{chat_id}/{nickname}")
async def websocket_endpoint(websocket: WebSocket, chat_id: str, nickname: str):
    await manager.connect(chat_id, websocket, nickname)
    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action", "send")

            if action == "ping":
                await websocket.send_json({"action": "pong"})

            elif action == "delete":
                msg_id = data.get("message_id")
                sender = data.get("sender")
                cursor.execute("SELECT is_superadmin, is_admin, admin_perms FROM users WHERE nickname = ?", (sender,))
                u = cursor.fetchone()
                can_del_any = False
                if u:
                    perms = json.loads(u[2]) if u[2] else {}
                    can_del_any = bool(u[0]) or is_super(sender) or (bool(u[1]) and perms.get("can_delete_messages", False))

                if can_del_any:
                    cursor.execute("DELETE FROM messages WHERE id = ?", (msg_id,))
                else:
                    cursor.execute("DELETE FROM messages WHERE id = ? AND sender = ?", (msg_id, sender))
                conn.commit()

                await manager.broadcast_chat(chat_id, {
                    "action": "deleted",
                    "message_ids": [msg_id],
                    "deleted_by": sender
                })

            elif action == "delete_batch":
                msg_ids = data.get("message_ids", [])
                sender = data.get("sender")
                cursor.execute("SELECT is_superadmin, is_admin, admin_perms FROM users WHERE nickname = ?", (sender,))
                u = cursor.fetchone()
                can_del_any = bool(u[0]) or is_super(sender) or (bool(u[1]) and json.loads(u[2] or '{}').get("can_delete_messages", False)) if u else False

                for mid in msg_ids:
                    if can_del_any:
                        cursor.execute("DELETE FROM messages WHERE id = ?", (mid,))
                    else:
                        cursor.execute("DELETE FROM messages WHERE id = ? AND sender = ?", (mid, sender))
                conn.commit()

                await manager.broadcast_chat(chat_id, {
                    "action": "deleted",
                    "message_ids": msg_ids,
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
                await manager.broadcast_chat(chat_id, data)

    except WebSocketDisconnect:
        manager.disconnect(chat_id, websocket, nickname)
