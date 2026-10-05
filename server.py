import os
import uuid
import sqlite3
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

# Папка для загрузки файлов и голосовых
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)
app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")

# Инициализация базы данных
conn = sqlite3.connect("messenger.db", check_same_thread=False)
cursor = conn.cursor()

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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
conn.commit()

# Безопасное добавление колонок для медиафайлов, если таблица уже существовала
try:
    cursor.execute("ALTER TABLE messages ADD COLUMN file_url TEXT")
except sqlite3.OperationalError:
    pass
try:
    cursor.execute("ALTER TABLE messages ADD COLUMN file_type TEXT")
except sqlite3.OperationalError:
    pass
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
        return {"status": "error", "msg": "Неверный пароль!"}
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
    body = await request.body()
    # Лимит строго до 10 МБ (10 * 1024 * 1024 байт)
    if len(body) > 10485760:
        raise HTTPException(status_code=413, detail="Файл превышает лимит 10 МБ!")

    _, ext = os.path.splitext(filename)
    unique_name = f"{uuid.uuid4().hex}{ext if ext else '.bin'}"
    file_path = os.path.join(UPLOAD_DIR, unique_name)

    with open(file_path, "wb") as f:
        f.write(body)

    return {"status": "ok", "url": f"/uploads/{unique_name}"}

@app.get("/messages/{chat_id}")
def get_messages(chat_id: str):
    cursor.execute("""
        SELECT sender, text, file_url, file_type, strftime('%H:%M', created_at)
        FROM messages 
        WHERE chat_id = ? 
        ORDER BY id ASC LIMIT 100
    """, (chat_id,))
    return [
        {
            "sender": r[0],
            "text": r[1],
            "file_url": r[2],
            "file_type": r[3],
            "time": r[4]
        }
        for r in cursor.fetchall()
    ]

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
        cursor.execute("""
            INSERT INTO messages (chat_id, sender, text, file_url, file_type)
            VALUES (?, ?, ?, ?, ?)
        """, (chat_id, payload.get("sender"), payload.get("text", ""), payload.get("file_url"), payload.get("file_type")))
        conn.commit()

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
            await manager.broadcast(chat_id, data)
    except WebSocketDisconnect:
        manager.disconnect(chat_id, websocket)
