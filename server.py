import os
import uuid
import sqlite3
from datetime import datetime, timedelta
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Dict

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

# Таблицы БД
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    nickname TEXT PRIMARY KEY,
    password TEXT NOT NULL
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS chats (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_by TEXT NOT NULL
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

# Очистка файлов старше 4 дней
def cleanup_old_files():
    cutoff = datetime.utcnow() - timedelta(days=4)
    cursor.execute("""
        SELECT id, file_url FROM messages 
        WHERE created_at < ? AND file_url IS NOT NULL
    """, (cutoff.strftime('%Y-%m-%d %H:%M:%S'),))
    old_files = cursor.fetchall()
    for msg_id, f_url in old_files:
        try:
            rel_path = f_url.lstrip("/")
            if os.path.exists(rel_path):
                os.remove(rel_path)
        except Exception:
            pass
        cursor.execute("UPDATE messages SET file_url = NULL, text = '[Срок хранения файла (4 дня) истёк]' WHERE id = ?", (msg_id,))
    conn.commit()

class AuthData(BaseModel):
    nickname: str
    password: str

class ChatData(BaseModel):
    id: str
    name: str
    created_by: str

class InviteData(BaseModel):
    chat_id: str
    nickname: str

@app.post("/auth")
def auth_user(data: AuthData):
    cursor.execute("SELECT password FROM users WHERE nickname = ?", (data.nickname,))
    row = cursor.fetchone()
    if row:
        if row[0] == data.password:
            return {"status": "ok", "msg": "logged_in"}
        # Четкое уведомление о неверном пароле
        return {"status": "error", "msg": "Неверный пароль для этого никнейма!"}
    else:
        cursor.execute("INSERT INTO users VALUES (?, ?)", (data.nickname, data.password))
        conn.commit()
        return {"status": "ok", "msg": "registered"}

@app.get("/chats/{nickname}")
def get_chats(nickname: str):
    cursor.execute("""
        SELECT DISTINCT c.id, c.name FROM chats c
        JOIN members m ON c.id = m.chat_id
        WHERE m.nickname = ?
    """, (nickname,))
    return [{"id": r[0], "name": r[1]} for r in cursor.fetchall()]

@app.post("/chats")
def create_chat(data: ChatData):
    cursor.execute("INSERT OR IGNORE INTO chats VALUES (?, ?, ?)", (data.id, data.name, data.created_by))
    cursor.execute("INSERT OR IGNORE INTO members VALUES (?, ?)", (data.id, data.created_by))
    conn.commit()
    return {"status": "ok"}

@app.post("/invite")
def invite_user(data: InviteData):
    cursor.execute("SELECT 1 FROM users WHERE nickname = ?", (data.nickname,))
    if not cursor.fetchone():
        return {"status": "error", "msg": "Пользователь не найден!"}
    cursor.execute("INSERT OR IGNORE INTO members VALUES (?, ?)", (data.chat_id, data.nickname))
    conn.commit()
    return {"status": "ok"}

@app.post("/upload")
async def upload_file(request: Request, filename: str = "file.bin"):
    cleanup_old_files()
    body = await request.body()
    # Строгий лимит 10 МБ
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

# Менеджер сокетов
class ConnectionManager:
    def __init__(self):
        self.active: Dict[str, List[WebSocket]] = {}

    async def connect(self, chat_id: str, ws: WebSocket):
        await ws.accept()
        if chat_id not in self.active:
            self.active[chat_id] = []
        self.active[chat_id].append(ws)

    def disconnect(self, chat_id: str, ws: WebSocket):
        if chat_id in self.active and ws in self.active[chat_id]:
            self.active[chat_id].remove(ws)

    async def broadcast(self, chat_id: str, payload: dict):
        if chat_id in self.active:
            for ws in self.active[chat_id]:
                await ws.send_json(payload)

manager = ConnectionManager()

@app.websocket("/ws/{chat_id}")
async def websocket_endpoint(websocket: WebSocket, chat_id: str):
    await manager.connect(chat_id, websocket)
    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action", "send")

            if action == "delete":
                msg_id = data.get("message_id")
                sender = data.get("sender")
                # Удаляем из БД
                cursor.execute("DELETE FROM messages WHERE id = ? AND sender = ?", (msg_id, sender))
                conn.commit()
                # Рассылаем оповещение об удалении всем активным пользователям комнаты
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
                msg_id = cursor.lastrowid
                data["id"] = msg_id
                data["action"] = "new_message"
                await manager.broadcast(chat_id, data)

    except WebSocketDisconnect:
        manager.disconnect(chat_id, websocket)
