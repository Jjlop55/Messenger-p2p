import sqlite3
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
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

# Инициализация базы данных SQLite
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
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

@app.get("/messages/{chat_id}")
def get_messages(chat_id: str):
    cursor.execute("SELECT sender, text FROM messages WHERE chat_id = ? ORDER BY id ASC LIMIT 100", (chat_id,))
    return [{"sender": r[0], "text": r[1]} for r in cursor.fetchall()]

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

    async def broadcast(self, chat_id: str, sender: str, text: str):
        cursor.execute("INSERT INTO messages (chat_id, sender, text) VALUES (?, ?, ?)", (chat_id, sender, text))
        conn.commit()
        if chat_id in self.active:
            for ws in self.active[chat_id]:
                await ws.send_json({"sender": sender, "text": text})

manager = ConnectionManager()

@app.websocket("/ws/{chat_id}")
async def websocket_endpoint(websocket: WebSocket, chat_id: str):
    await manager.connect(chat_id, websocket)
    try:
        while True:
            data = await websocket.receive_json()
            await manager.broadcast(chat_id, data["sender"], data["text"])
    except WebSocketDisconnect:
        manager.disconnect(chat_id, websocket)
