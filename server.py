import os
import time
import uuid
import json
import gzip
import base64
import atexit
import shutil
import tempfile
import random
import asyncio
import sqlite3
import hashlib
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
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

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if os.environ.get("VERCEL") or os.environ.get("VERCEL_ENV"):
    DATA_DIR = os.path.join(tempfile.gettempdir(), "utopia_data")
else:
    DATA_DIR = os.environ.get("DATA_DIR") or ("/data" if os.path.isdir("/data") and os.access("/data", os.W_OK) else BASE_DIR)
os.makedirs(DATA_DIR, exist_ok=True)
UPLOAD_DIR = os.path.join(DATA_DIR, "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)
app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")
BOYKISSER_DIR = os.path.join(BASE_DIR, "boykisser")
if os.path.isdir(BOYKISSER_DIR):
    app.mount("/boykisser", StaticFiles(directory=BOYKISSER_DIR), name="boykisser")
MUSIC_DIR = os.path.join(BASE_DIR, "music")
os.makedirs(MUSIC_DIR, exist_ok=True)
app.mount("/music", StaticFiles(directory=MUSIC_DIR), name="music")


def delete_upload_file(file_url: str) -> bool:
    """Файл больше никому не нужен (сообщение/чат удалены) — убираем его с диска."""
    if not file_url:
        return False
    try:
        fname = os.path.basename(str(file_url).strip().replace("\\", "/"))
        if not fname:
            return False
        path = os.path.join(UPLOAD_DIR, fname)
        if os.path.isfile(path):
            os.remove(path)
            return True
    except Exception:
        pass
    return False

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

_DB_PATH = os.path.join(DATA_DIR, "messenger.db")
_thread_local = threading.local()
_all_thread_conns: List[sqlite3.Connection] = []
_all_conns_lock = threading.Lock()
_last_gist_hash = ""


def _gist_backup_cfg():
    gid = os.environ.get("BACKUP_GIST_ID", "").strip()
    tok = os.environ.get("BACKUP_GITHUB_TOKEN", "").strip()
    return gid, tok


def _pack_uploads_b64(max_total_bytes: int = 18 * 1024 * 1024) -> str:
    """Упаковывает активные (не удалённые) файлы из UPLOAD_DIR в gzip+base64 JSON."""
    bundle = {}
    total = 0
    if not os.path.isdir(UPLOAD_DIR):
        return ""
    for fname in sorted(os.listdir(UPLOAD_DIR)):
        if fname.startswith("."):
            continue
        fp = os.path.join(UPLOAD_DIR, fname)
        if not os.path.isfile(fp):
            continue
        sz = os.path.getsize(fp)
        if sz <= 0 or total + sz > max_total_bytes:
            continue
        try:
            with open(fp, "rb") as f:
                bundle[fname] = base64.b64encode(f.read()).decode("ascii")
            total += sz
        except Exception:
            pass
    if not bundle:
        return ""
    raw_json = json.dumps(bundle, ensure_ascii=True).encode("utf-8")
    return base64.b64encode(gzip.compress(raw_json, compresslevel=6)).decode("ascii")


def _restore_uploads_from_b64(b64_str: str) -> int:
    """Восстанавливает файлы и фотки в UPLOAD_DIR из снапшота."""
    if not b64_str:
        return 0
    try:
        raw_json = gzip.decompress(base64.b64decode(b64_str))
        bundle = json.loads(raw_json.decode("utf-8"))
        os.makedirs(UPLOAD_DIR, exist_ok=True)
        restored = 0
        for fname, fb64 in bundle.items():
            safe_name = os.path.basename(str(fname))
            if not safe_name or safe_name.startswith("."):
                continue
            dst = os.path.join(UPLOAD_DIR, safe_name)
            if not os.path.exists(dst):
                with open(dst, "wb") as f:
                    f.write(base64.b64decode(fb64))
                restored += 1
        return restored
    except Exception as e:
        print("[DB] Ошибка восстановления uploads:", e)
        return 0


def _restore_db_from_gist_if_needed():
    """Если БД ещё не существует (свежий деплой), восстанавливаем её и uploads из приватного GitHub Gist."""
    gid, tok = _gist_backup_cfg()
    if not gid or not tok:
        return
    if os.path.exists(_DB_PATH) and os.path.getsize(_DB_PATH) > 4096:
        return
    try:
        req = urllib.request.Request(
            f"https://api.github.com/gists/{gid}",
            headers={"Authorization": f"Bearer {tok}", "Accept": "application/vnd.github+json", "User-Agent": "UtopiaServer"},
        )
        with urllib.request.urlopen(req, timeout=25) as resp:
            gist = json.loads(resp.read().decode("utf-8"))
        files = gist.get("files") or {}
        fobj = files.get("messenger_db.gz.b64")
        if fobj and fobj.get("content"):
            raw = gzip.decompress(base64.b64decode(fobj["content"]))
            if raw.startswith(b"SQLite format 3\x00"):
                tmp = _DB_PATH + ".restore"
                with open(tmp, "wb") as f:
                    f.write(raw)
                os.replace(tmp, _DB_PATH)
                print("[DB] Восстановлено из GitHub Gist:", len(raw), "байт")
        uobj = files.get("uploads_bundle.json.gz.b64")
        if uobj and uobj.get("content"):
            cnt = _restore_uploads_from_b64(uobj["content"])
            print("[DB] Восстановлено файлов uploads:", cnt)
    except Exception as e:
        print("[DB] Не удалось восстановить из Gist:", e)


def _export_consistent_db_bytes() -> bytes:
    """Создаёт консистентный снимок SQLite WAL-базы через sqlite3.backup()."""
    fd, tmp_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        src = sqlite3.connect(_DB_PATH, timeout=25.0)
        dst = sqlite3.connect(tmp_path)
        src.backup(dst)
        dst.close()
        src.close()
        with open(tmp_path, "rb") as f:
            return f.read()
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


def _backup_db_to_gist(force: bool = False):
    """Сохраняет снимок БД и активных фоток/файлов в приватный GitHub Gist."""
    global _last_gist_hash
    gid, tok = _gist_backup_cfg()
    if not gid or not tok or not os.path.exists(_DB_PATH):
        return
    try:
        raw = _export_consistent_db_bytes()
        uploads_b64 = _pack_uploads_b64()
        h = hashlib.sha256(raw + uploads_b64.encode("ascii")).hexdigest()
        if not force and h == _last_gist_hash:
            return
        b64 = base64.b64encode(gzip.compress(raw, compresslevel=6)).decode("ascii")
        gist_files = {"messenger_db.gz.b64": {"content": b64}}
        if uploads_b64:
            gist_files["uploads_bundle.json.gz.b64"] = {"content": uploads_b64}
        payload = json.dumps({"files": gist_files}).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.github.com/gists/{gid}",
            data=payload,
            method="PATCH",
            headers={
                "Authorization": f"Bearer {tok}",
                "Accept": "application/vnd.github+json",
                "Content-Type": "application/json",
                "User-Agent": "UtopiaServer",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status < 300:
                _last_gist_hash = h
    except Exception as e:
        print("[DB] Ошибка автобэкапа в Gist:", e)


def _start_gist_backup_loop():
    gid, tok = _gist_backup_cfg()
    if not gid or not tok:
        return
    atexit.register(lambda: _backup_db_to_gist(force=False))
    def _loop():
        while True:
            time.sleep(60)
            _backup_db_to_gist(force=False)
    threading.Thread(target=_loop, daemon=True).start()


_restore_db_from_gist_if_needed()


def _new_connection():
    c = sqlite3.connect(_DB_PATH, check_same_thread=False, timeout=25.0, isolation_level=None)
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("PRAGMA busy_timeout=25000;")
    c.execute("PRAGMA synchronous=NORMAL;")
    with _all_conns_lock:
        _all_thread_conns.append(c)
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
CREATE TABLE IF NOT EXISTS ban_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_nick TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    reason TEXT NOT NULL,
    ban_type TEXT DEFAULT 'permanent',
    duration_hours REAL DEFAULT 0,
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
CREATE TABLE IF NOT EXISTS chat_reads (
    chat_id TEXT NOT NULL,
    nickname TEXT NOT NULL,
    last_read_id INTEGER DEFAULT 0,
    PRIMARY KEY (chat_id, nickname)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS clicker_grants (
    nickname TEXT PRIMARY KEY,
    bonus_coins INTEGER DEFAULT 0,
    granted_by TEXT DEFAULT '',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

# Игровой зал: статистика игрока по играм и история 1х1-матчей (для таблиц лидеров)
cursor.execute("""
CREATE TABLE IF NOT EXISTS game_stats (
    nickname TEXT NOT NULL,
    game TEXT NOT NULL,
    wins INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    draws INTEGER DEFAULT 0,
    matches INTEGER DEFAULT 0,
    PRIMARY KEY (nickname, game)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS game_matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game TEXT NOT NULL,
    player1 TEXT NOT NULL,
    player2 TEXT NOT NULL,
    winner TEXT DEFAULT '',
    score TEXT DEFAULT '',
    played_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

# Чёрный список удалённых аккаунтов: такой ник нельзя занять повторно
cursor.execute("""
CREATE TABLE IF NOT EXISTS deleted_users (
    nickname TEXT PRIMARY KEY,
    deleted_by TEXT DEFAULT '',
    reason TEXT DEFAULT '',
    deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

# Настройки пользователя (синхронизируются между устройствами)
cursor.execute("""
CREATE TABLE IF NOT EXISTS user_settings (
    nickname TEXT PRIMARY KEY,
    settings_json TEXT DEFAULT '{}',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

# Игровая статистика профиля: PvE-матчи (с ботами), статистика кликера.
# Онлайн-матчи уже лежат в game_stats; сюда пишет клиент своё состояние.
cursor.execute("""
CREATE TABLE IF NOT EXISTS profile_game_stats (
    nickname TEXT PRIMARY KEY,
    pve_json TEXT DEFAULT '{}',
    clicker_json TEXT DEFAULT '{}',
    coins INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    # Служебные сообщения (мьют, бан, звонки) — не редактируются и не имеют автора
    ("messages", "is_system", "INTEGER DEFAULT 0"),
    # Discord/Telegram каналы, серверы, темы и закреплённые сообщения
    ("chats", "topic", "TEXT DEFAULT ''"),
    ("chats", "server_name", "TEXT DEFAULT ''"),
    ("chats", "channel_kind", "TEXT DEFAULT 'text'"),
    ("chats", "pinned_msg_id", "INTEGER DEFAULT NULL"),
    # Серверная экономика (валюты и купленное оружие Green Valley по аккаунту)
    ("profile_game_stats", "weapons_json", "TEXT DEFAULT '[0, 7]'"),
    ("profile_game_stats", "currencies_json", "TEXT DEFAULT '{}'"),
    ("users", "fav_track", "TEXT DEFAULT ''"),
]
for tbl, col, ctype in _safe_alters:
    try:
        cursor.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {ctype}")
    except sqlite3.OperationalError:
        pass

cursor.execute("""
    CREATE TABLE IF NOT EXISTS music_tracks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fandom TEXT NOT NULL DEFAULT 'gamer',
        title TEXT NOT NULL,
        artist TEXT DEFAULT '',
        url TEXT NOT NULL,
        added_by TEXT DEFAULT '',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS music_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nickname TEXT NOT NULL,
        fandom TEXT NOT NULL DEFAULT 'gamer',
        url TEXT NOT NULL,
        note TEXT NOT NULL,
        status TEXT DEFAULT 'pending',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
""")

DEFAULT_MUSIC_SEED = [
    ("chill", "SONGFORMYDEATH", "YXREMI", "/music/yxremi_songformydeath.mp3"),
    ("chill", "ural mountains", "prodwhiteball, prodbyjane", "/music/prodbyjane_ural_mountains.mp3"),
    ("chill", "pepsi p (prod. STAKILLAZ)", "KAMZ0NER", "/music/kamz0ner_pepsi_p.mp3"),
    ("chill", "ENTITY.096", "KLEAVE, SXCREDMANE", "/music/kleave_entity096.mp3"),
    ("chill", "LANCA NA MAO (Super Slowed)", "OTVphonk, DJ FZ DA ZN", "/music/otvphonk_lanca_na_mao.mp3"),
    ("chill", "DARK AGE FUNK", "DJ AURAXÉ, DR FUNKO", "/music/dj_auraxe_dark_age_funk.mp3"),
    ("chill", "MONTAGEM SANTA FE 2 (Slowed)", "qaraqshy", "/music/qaraqshy_montagem_santa_fe_2.mp3"),
    ("chill", "Edgy (Slowed)", "luvwillow", "/music/luvwillow_edgy_slowed.mp3"),
]
for _f, _t, _a, _u in DEFAULT_MUSIC_SEED:
    cursor.execute("SELECT 1 FROM music_tracks WHERE url=? LIMIT 1", (_u,))
    if not cursor.fetchone():
        cursor.execute(
            "INSERT INTO music_tracks (fandom, title, artist, url, added_by) VALUES (?, ?, ?, ?, 'admin')",
            (_f, _t, _a, _u),
        )
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
    return bool(nick) and str(nick).lower() in ("jjlop55", "gglo55")

def get_admin_perms(nick: str) -> dict:
    """Сырые права админа из БД ({} для обычного юзера)."""
    if not nick:
        return {}
    cursor.execute("SELECT is_superadmin, admin_perms FROM users WHERE LOWER(nickname)=LOWER(?)", (nick,))
    row = cursor.fetchone()
    if not row:
        return {}
    if row[0] or is_super(nick):
        return {"__super__": True}
    try:
        p = json.loads(row[1] or "{}")
    except Exception:
        p = {}
    return p if isinstance(p, dict) else {}

def is_assistant(nick: str) -> bool:
    """Помощник главного админа: все права супера, кроме действий над самим супером."""
    return bool(get_admin_perms(nick).get("can_assistant"))

def has_perm(nick: str, perm: str) -> bool:
    """Строгая проверка конкретного права. Супер и помощник — всегда True."""
    p = get_admin_perms(nick)
    if p.get("__super__"):
        return True
    if p.get("can_assistant"):
        return True
    return bool(p.get(perm))


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


# ── Бот-помощник: живёт в личном чате bot_<ник> ─────────────────────
# Ник бота, внешний ИИ (бесплатный, без ключа) для свободных вопросов.
BOT_NICK = os.environ.get("BOT_NICK", "🤖 Utopia")
AI_API_URL = os.environ.get("AI_API_URL", "https://text.pollinations.ai/")
AI_API_KEY = os.environ.get("AI_API_KEY", "").strip()
AI_MODEL = os.environ.get("AI_MODEL", "").strip()
AI_TIMEOUT = int(os.environ.get("AI_TIMEOUT", "45"))
AI_HISTORY = 8          # сколько последних сообщений чата отдаём ИИ как контекст
AI_MAX_QUESTION = 1000  # длина вопроса
AI_MAX_ANSWER = 3000    # длина ответа

BOT_INTRO = (
    "Привет! Я Utopia — бот-помощник, я живу прямо в этом чате.\n"
    "Знаю все функции мессенджера и объясню, как ими пользоваться.\n"
    "Спроси меня, например: «как позвонить?», «как работают голосовые каналы?», «где настройки панели?», «какие есть игры?»\n"
    "Или напиши «функции» — покажу весь список."
)

# Справочник функций: desc — это описание, которое бот выдаёт пользователю,
# short — однострочник для списка, keys — слова-триггеры для подбора темы.
FEATURES = [
    {"id": "calls", "emoji": "📞", "title": "Аудио, видео и групповые звонки", "short": "1 на 1 и созвоны на несколько человек",
     "desc": "Кнопка 📞 в шапке чата зовёт собеседника (в ЛС — сразу, в группе/общем — выбор из списка друзей или по нику). "
             "Во время звонка доступны: 🎙️ Микрофон (не глушит собеседника), 🎧 Звук (режим тишины без звука входящих), "
             "📹 Камера, 🖥️ Демка экрана, ➕ Добавить ещё участников в общий созвон и выбор динамиков. "
             "Сигнализация работает через отдельный фоновый канал, поэтому переключение чатов больше не рвёт звонок! "
             "Если нет микрофона, можно подключиться в режиме прослушивания (нужен https://).",
     "keys": ["звонок", "звонки", "позвон", "аудиозвонок", "микрофон", "камер", "видеозвонок", "дозвон", "демк", "экран"]},
    {"id": "voice_channels", "emoji": "🔊", "title": "Голосовые каналы (Discord)", "short": "комнаты с живым голосом без обрыва при смене чата",
     "desc": "Создай канал с типом «🔊 Голосовой канал» через кнопку «+ Канал». Нажми «🔊 Войти в голос» — сразу "
             "включится живая аудиосвязь со всеми участниками в комнате, с индикаторами речи, кнопками 🎙️ Микр и 🎧 Звук. "
             "Можно свободно переключаться в другие текстовые чаты или ЛС — голос продолжит работать в фоне, а внизу "
             "будет висеть компактная панель управления.",
     "keys": ["голосов", "войс", "комнат", "дискорд", "discord", "audio", "канал голос"]},
    {"id": "groups", "emoji": "👥", "title": "Группы, каналы и роли", "short": "свои чаты, #каналы, роли и права",
     "desc": "Кнопка «+ Канал» создаёт 👥 Группу, # Текстовый канал или 🔊 Голосовой канал с темой. Создатель — владелец. "
             "Можно назначать админов, выдавать точечные права (переименование, приглашение, кик, удаление сообщений), "
             "ставить титулы и передавать владение. Пригласить участника можно кнопкой «+ Пригласить» — входящее "
             "приглашение чётко укажет, куда вас зовут (в голосовой канал, текстовый канал или группу).",
     "keys": ["групп", "канал", "роль", "роли", "админ", "владел", "права", "титул", "приглас", "участник", "кик"]},
    {"id": "interface", "emoji": "🎨", "title": "Настройка интерфейса и панели", "short": "профиль и кнопки сверху, сбоку или везде",
     "desc": "В Настройках ⚙ есть переключатель «Расположение профиля и кнопок»: можно оставить аватарку и кнопки "
             "только сверху в шапке, убрать на боковую панель (слева или справа) либо показывать и там, и там. "
             "А справа у любого чата кнопка 👤 открывает Discord-панель участников с онлайн-статусом и голосовой активностью.",
     "keys": ["интерфейс", "панел", "кнопк", "аватар", "сбоку", "справа", "сверху", "расположен", "меню", "тема"]},
    {"id": "friends", "emoji": "🤝", "title": "Друзья и личные сообщения", "short": "заявки, архив и лимит ЛС",
     "desc": "Кнопка 🤝 открывает друзей: кто в сети, заявки и поиск. Пока заявка не принята, незнакомцу можно отправить "
             "ровно одно сообщение — после дружбы лимит снимается. ЛС можно переименовать только для себя или убрать в 📦 Архив.",
     "keys": ["друз", "друг", "заявк", "лс", "личн", "собеседник", "лимит", "архив"]},
    {"id": "files", "emoji": "📎", "title": "Файлы, фото и голосовые", "short": "вложения до 10 МБ, фото с приближением",
     "desc": "Скрепка 📎 прикрепляет файл, картинку или документ до 10 МБ (фото открываются на весь экран по клику). "
             "Кнопка 🎙 записывает голосовое сообщение до 5 минут (можно тянуть вверх для фиксации или влево для отмены), "
             "а 🗣️ диктует текст голосом прямо в строку ввода. Файлы автоматически чистятся через 4 дня.",
     "keys": ["файл", "фото", "картинк", "скрепк", "прикреп", "голосов", "дикт", "удален", "вложен"]},
    {"id": "reactions", "emoji": "😀", "title": "Реакции, ответы, закреп и поиск", "short": "смайлы, редактирование, 📌 и 🔍",
     "desc": "ПКМ или долгое нажатие на сообщение открывает меню: поставить реакцию, ответить цитатой, "
             "редактировать своё сообщение, закрепить важное в шапке 📌 или удалить. Кнопка 🔍 в шапке ищет по истории чата.",
     "keys": ["реакц", "смайл", "ответ", "цитат", "эмодзи", "закреп", "поиск", "редакт"]},
    {"id": "sessions", "emoji": "📱", "title": "Устройства и сессии", "short": "активные входы и сброс",
     "desc": "Кнопка 📱 показывает устройства, с которых выполнен вход: активные и заброшенные сессии (7 дней без активности), "
             "IP и время входа. Можно сбросить чужое устройство или выйти со всех сразу.",
     "keys": ["сесси", "устройств", "вход", "ip", "кикнуть", "разлогин"]},
    {"id": "notifications", "emoji": "🔔", "title": "Уведомления и звук", "short": "системные пуши и тоны",
     "desc": "В Настройках ⚙ можно отдельно включать уведомления о новых сообщениях и звук, когда вкладка свёрнута. "
             "Кнопка «Разрешить системные уведомления» включает пуш-сообщения браузера с переходом прямо в нужный чат.",
     "keys": ["уведомлен", "пуш", "звук", "настройк", "оповещен", "notifications"]},
    {"id": "support", "emoji": "🎧", "title": "Поддержка", "short": "тикеты и ответы саппорта",
     "desc": "Кнопка 🎧 открывает службу поддержки: тикет можно отправить даже до входа в аккаунт (просто впиши свой ник). "
             "Ответ хранится 4 часа после прочтения.",
     "keys": ["поддержк", "тикет", "обращен", "жалоб", "проблем", "help"]},
    {"id": "admin", "emoji": "⚡", "title": "Админ-панель", "short": "баны, мьюты, монеты, права",
     "desc": "Кнопка ⚡ (видна админам): управление пользователями (бан/мьют/кик/выдача монет/удаление), группами "
             "(передача владения кнопкой 👑), списком блокировок, саппортом и заявками.",
     "keys": ["админ", "панел", "бан", "мьют", "заглушить", "права админа"]},
    {"id": "cleanup", "emoji": "🧹", "title": "Автоочистка", "short": "сообщения живут недолго",
     "desc": "Сообщения хранятся временно: общий чат — 3 часа, группы и каналы — 4 часа, ЛС — 10 часов. "
             "Вложения удаляются с диска одновременно с сообщениями.",
     "keys": ["очистк", "удаляется", "истек", "срок", "хранятся", "истори"]},
    {"id": "monitoring", "emoji": "📊", "title": "Мониторинг сервера", "short": "аптайм, онлайн и память",
     "desc": "Кнопка 📊 показывает статус сервиса, время безотказной работы, число пользователей онлайн и место на диске.",
     "keys": ["мониторинг", "сервер", "пинг", "статус", "задержк", "онлайн"]},
    {"id": "games", "emoji": "🎮", "title": "Игровой зал, Дуэли и Кликер", "short": "5 игр, лобби, лидеры и 👆 Кликер",
     "desc": "Кнопка 🎮 открывает Игровой зал: ⚔️ Утопия (2D-арена), 🌿 Green Valley (3D с ботами и PvP), "
             "✊ Камень-ножницы-бумага, ❌ Крестики-нолики и 🚢 Морской бой. Через профиль можно бросить прямую ⚔️ Дуэль другу! "
             "А кнопка 👆 открывает Кликер с прокачками, скинами, сундуком и обменом на общие 💰 игровые монеты.",
     "keys": ["игр", "игров", "игра", "соперник", "поиск", "лидер", "таблиц", "утопи", "green", "кликер", "дуэл", "монет", "крестик", "морской"]},
    {"id": "bot", "emoji": "🤖", "title": "Бот-помощник (это я)", "short": "справки по всем функциям",
     "desc": "Этот чат. Пиши вопрос обычными словами или жми быстрые кнопки внизу — подскажу любую функцию по шагам.",
     "keys": ["бот", "помощник", "кто ты", "что умеешь", "функции", "список"]},
]


def _ai_http(payload: dict) -> str:
    """Синхронный POST к текстовому API. Выполняется в отдельном потоке."""
    headers = {"Content-Type": "application/json", "Accept": "text/plain, application/json"}
    if AI_API_KEY:
        headers["Authorization"] = "Bearer " + AI_API_KEY
    req = urllib.request.Request(AI_API_URL, data=json.dumps(payload).encode("utf-8"),
                                 headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=AI_TIMEOUT) as resp:
        raw = resp.read().decode("utf-8", "replace")

    # Ответ может быть и простым текстом, и openai-совместимым JSON
    try:
        parsed = json.loads(raw)
    except Exception:
        parsed = None
    if isinstance(parsed, dict):
        choices = parsed.get("choices")
        if isinstance(choices, list) and choices and isinstance(choices[0], dict):
            msg = choices[0].get("message")
            if isinstance(msg, dict) and msg.get("content"):
                return str(msg["content"])
        for key in ("text", "content", "response", "message"):
            val = parsed.get(key)
            if isinstance(val, str) and val.strip():
                return val
    return raw


async def ask_ai(question: str, history: List[tuple]) -> str:
    """Свободный вопрос во внешний ИИ. history — (sender, text) по возрастанию."""
    messages = [{"role": "system", "content":
                 "Ты — дружелюбный помощник внутри мессенджера Utopia Messenger. "
                 "Отвечай коротко, по-русски, без использования разметки markdown."}]
    for sender, text in history:
        if sender == BOT_NICK:
            messages.append({"role": "assistant", "content": text})
        else:
            messages.append({"role": "user", "content": f"{sender}: {text}"})
    messages.append({"role": "user", "content": question})

    payload: dict = {"messages": messages}
    if AI_MODEL:
        payload["model"] = AI_MODEL
    try:
        answer = await asyncio.wait_for(
            asyncio.to_thread(_ai_http, payload), timeout=AI_TIMEOUT + 10)
    except Exception:
        return ""

    answer = (answer or "").strip()
    if len(answer) > AI_MAX_ANSWER:
        answer = answer[:AI_MAX_ANSWER] + "…"
    return answer


def _bot_history(chat_id: str) -> List[tuple]:
    cursor.execute("SELECT sender, text FROM messages WHERE chat_id=? ORDER BY id DESC LIMIT ?",
                   (chat_id, AI_HISTORY))
    rows = cursor.fetchall()[::-1]
    return [(r[0] or "", (r[1] or "")[:400]) for r in rows]


async def post_bot_message(chat_id: str, text: str):
    cursor.execute("INSERT INTO messages (chat_id, sender, text) VALUES (?, ?, ?)",
                   (chat_id, BOT_NICK, text))
    conn.commit()
    mid = cursor.lastrowid
    await manager.broadcast_chat_event(chat_id, {
        "action": "new_message", "id": mid, "chat_id": chat_id,
        "sender": BOT_NICK, "text": text,
        "time": datetime.now().strftime("%H:%M"),
    })


def bot_features_list() -> str:
    lines = [f"{f['emoji']} {f['title']} — {f['short']}." for f in FEATURES]
    return ("Вот всё, что умеет Utopia:\n" + "\n".join(lines) +
            f"\n\nВсего функций: {len(FEATURES)}. Напиши тему — расскажу подробно.")


def bot_match(text: str):
    """Подбирает функцию по словам из вопроса. Возвращает dict или None."""
    low = (text or "").lower()
    best, best_hits = None, 0
    for f in FEATURES:
        hits = sum(1 for k in f["keys"] if k in low)
        if hits > best_hits:
            best, best_hits = f, hits
    return best


async def bot_reply(chat_id: str, nickname: str, text: str):
    """Ответ бота в личном чате bot_<ник>."""
    q = (text or "").strip()
    low = q.lower()
    if not q:
        answer = BOT_INTRO
    elif len(low) <= 40 and any(w in low for w in ("привет", "здравств", "хай", "hello", "добр")):
        answer = f"Привет, @{nickname}!\n\n" + BOT_INTRO
    elif len(low) <= 60 and any(w in low for w in ("что умеешь", "что ты умеешь", "функции", "список",
                                                   "помощь", "справка", "help", "возможности")):
        answer = bot_features_list()
    else:
        feature = bot_match(q)
        if feature:
            answer = f"{feature['emoji']} {feature['title']}\n\n{feature['desc']}"
        else:
            ai = await ask_ai(q[:AI_MAX_QUESTION], _bot_history(chat_id))
            if not ai:
                answer = ("Не нашёл такой темы в справочнике, а внешний ИИ сейчас недоступен.\n\n"
                          + bot_features_list())
            else:
                answer = ai
    await post_bot_message(chat_id, answer)


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


def mute_info(nick: str):
    """Активный мьют: сколько минут осталось и почему. None — не замьючен."""
    if not nick:
        return None
    cursor.execute(
        "SELECT reason, duration_minutes, expires_at FROM mutes WHERE LOWER(target_nick)=LOWER(?) AND expires_at>?",
        (nick, now_str()))
    row = cursor.fetchone()
    if not row:
        return None
    reason, minutes, expires = row
    try:
        secs = (datetime.strptime(expires, '%Y-%m-%d %H:%M:%S') - datetime.utcnow()).total_seconds()
    except Exception:
        secs = float((minutes or 1) * 60)
    if secs <= 0:
        return None
    return {"reason": (reason or "").strip(), "minutes": max(1, int(secs // 60)), "expires": expires}


async def post_system_message(chat_id: str, text: str) -> int:
    """Служебное сообщение чата (мьют/бан) — видят все участники, автора нет."""
    cursor.execute("INSERT INTO messages (chat_id, sender, text, is_system) VALUES (?,?,?,1)",
                   (chat_id, "Система", text))
    conn.commit()
    mid = cursor.lastrowid
    await manager.broadcast_chat_event(chat_id, {
        "action": "new_message", "id": mid, "chat_id": chat_id,
        "sender": "Система", "text": text,
        "time": datetime.now().strftime("%H:%M"), "is_system": True,
    })
    return mid


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


def _purge_files_for(where: str, args: tuple):
    """Удаляет с диска файлы сообщений, которые сейчас будут вычищены из чата."""
    cursor.execute(f"SELECT file_url FROM messages WHERE ({where}) AND file_url IS NOT NULL", args)
    for (f_url,) in cursor.fetchall():
        delete_upload_file(f_url)


def _cleanup_body():
    ns = now_str()

    # Сессии: неделя без активности -> «заброшена», старые артефакты подчищаются
    expire_stale_sessions()

    # 1. Общий чат (3 часа)
    c3 = (datetime.utcnow() - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
    _purge_files_for("chat_id='general' AND created_at < ?", (c3,))
    cursor.execute("DELETE FROM messages WHERE chat_id='general' AND created_at < ?", (c3,))

    # 2. Групповые/командные чаты (4 часа)
    c4 = (datetime.utcnow() - timedelta(hours=4)).strftime('%Y-%m-%d %H:%M:%S')
    _purge_files_for("chat_id IN (SELECT id FROM chats WHERE is_direct=0 AND id!='general') AND created_at < ?", (c4,))
    cursor.execute("""
        DELETE FROM messages 
        WHERE chat_id IN (SELECT id FROM chats WHERE is_direct=0 AND id!='general')
          AND created_at < ?
    """, (c4,))

    # 3. Личные диалоги и чат с ботом-помощником (10 часов)
    c10 = (datetime.utcnow() - timedelta(hours=10)).strftime('%Y-%m-%d %H:%M:%S')
    bot_cond = "(chat_id IN (SELECT id FROM chats WHERE is_direct=1) OR chat_id LIKE 'bot!_%' ESCAPE '!')"
    _purge_files_for(f"{bot_cond} AND created_at < ?", (c10,))
    cursor.execute(f"""
        DELETE FROM messages 
        WHERE {bot_cond}
          AND created_at < ?
    """, (c10,))

    # Файлы старше 4 дней (сообщение остаётся, файл уходит)
    cutoff_files = (datetime.utcnow() - timedelta(days=4)).strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("SELECT id, file_url FROM messages WHERE created_at < ? AND file_url IS NOT NULL", (cutoff_files,))
    for msg_id, f_url in cursor.fetchall():
        delete_upload_file(f_url)
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

    # Просроченные заявки на бан
    cursor.execute("SELECT id, target_nick, requested_by FROM ban_requests WHERE expires_at < ? AND status='pending'", (ns,))
    for req_id, t_nick, r_by in cursor.fetchall():
        cursor.execute("UPDATE ban_requests SET status='expired' WHERE id=?", (req_id,))
        cursor.execute("INSERT INTO notifications (to_user, text) VALUES (?, ?)",
                       (r_by, f"Заявка на бан @{t_nick} истекла (срок 7 дней)."))

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
    silent: bool = False  # звонковая группа: не broadcast_all всему сайту
    chat_type: str = "group"      # 'group' | 'channel' | 'voice'
    server_name: str = ""
    topic: str = ""
    channel_kind: str = "text"    # 'text' | 'voice'

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

class GiveCoinsData(BaseModel):
    admin_nick: str
    target_nick: str
    amount: int

class ClickerSyncData(BaseModel):
    nickname: str
    session_token: str

class ProfileGameStatsData(BaseModel):
    nickname: str
    session_token: str
    pve_json: dict = {}
    clicker_json: dict = {}

class EconomyMigrateData(BaseModel):
    nickname: str
    session_token: str
    clicker_json: Optional[dict] = None
    weapons: Optional[List[int]] = None
    pve_json: Optional[dict] = None

class EconomyClickerSaveData(BaseModel):
    nickname: str
    session_token: str
    clicker_json: dict = {}
    client_id: str = ""

class EconomyConvertData(BaseModel):
    nickname: str
    session_token: str
    clicker_json: Optional[dict] = None

class EconomyWeaponBuyData(BaseModel):
    nickname: str
    session_token: str
    weapon_idx: int

class EconomyPveRecordData(BaseModel):
    nickname: str
    session_token: str
    game_key: str
    won: bool

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

class AdminBroadcastData(BaseModel):
    admin_nick: str
    message: str
    post_to_general: bool = False

class AdminRestoreDeletedData(BaseModel):
    admin_nick: str
    target_nick: str

class VoiceRoomActionData(BaseModel):
    nickname: str
    session_token: str = ""

# ═══════════════════════════════════════════════════════════
#  CONNECTION MANAGER
# ═══════════════════════════════════════════════════════════

class ConnectionManager:
    def __init__(self):
        self.user_sockets: Dict[str, Set[WebSocket]] = {}
        self.chat_sockets: Dict[str, Set[WebSocket]] = {}
        self.signal_sockets: Dict[str, Set[WebSocket]] = {}
        self.voice_socket_rooms: Dict[WebSocket, Set[str]] = {}

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

    async def connect_signal(self, ws: WebSocket, nick: str):
        await ws.accept()
        canonical = nick.lower()
        self.signal_sockets.setdefault(canonical, set()).add(ws)

    def disconnect_signal(self, ws: WebSocket, nick: str) -> Set[str]:
        canonical = nick.lower()
        if canonical in self.signal_sockets:
            self.signal_sockets[canonical].discard(ws)
            if not self.signal_sockets[canonical]:
                del self.signal_sockets[canonical]
        return self.voice_socket_rooms.pop(ws, set())

    def bind_voice_socket(self, ws: WebSocket, chat_id: str):
        if ws is None or not chat_id:
            return
        self.voice_socket_rooms.setdefault(ws, set()).add(chat_id)

    def unbind_voice_socket(self, ws: WebSocket, chat_id: str = ""):
        if ws is None:
            return
        if not chat_id:
            self.voice_socket_rooms.pop(ws, None)
            return
        s = self.voice_socket_rooms.get(ws)
        if s:
            s.discard(chat_id)
            if not s:
                self.voice_socket_rooms.pop(ws, None)

    def is_online(self, nick: str) -> bool:
        canonical = (nick or "").strip().lower()
        if not canonical:
            return False
        return bool(self.user_sockets.get(canonical)) or bool(self.signal_sockets.get(canonical))

    def online_users(self) -> List[str]:
        return list(set(self.user_sockets.keys()) | set(self.signal_sockets.keys()))

    async def send_to_user(self, nick: str, payload: dict):
        canonical = (nick or "").strip().lower()
        for ws in list(self.user_sockets.get(canonical, [])):
            try:
                await ws.send_json(payload)
            except Exception:
                pass

    async def send_signal_to_user(self, nick: str, payload: dict, exclude_ws: Optional[WebSocket] = None, allow_chat_fallback: bool = True):
        """Доставка сигнализации звонка/голоса без дублирования, в первую очередь в signalWs."""
        canonical = (nick or "").strip().lower()
        all_sig = self.signal_sockets.get(canonical, set())
        if all_sig:
            targets = [ws for ws in all_sig if ws is not exclude_ws]
        elif allow_chat_fallback:
            targets = [ws for ws in self.user_sockets.get(canonical, set()) if ws is not exclude_ws]
        else:
            targets = []
        for ws in list(targets):
            try:
                await ws.send_json(payload)
            except Exception:
                pass

    async def broadcast_signal_or_chat_all(self, payload: dict):
        """Шлёт по 1 разу каждому онлайн-юзеру: в signalWs (не рвётся при switchChat), иначе в userWs."""
        for canonical in self.online_users():
            await self.send_signal_to_user(canonical, payload)

    async def broadcast_chat(self, chat_id: str, payload: dict):
        for ws in list(self.chat_sockets.get(chat_id, [])):
            try:
                await ws.send_json(payload)
            except Exception:
                pass

    def _chat_participant_lows(self, chat_id: str, include_nick: str = "") -> List[str]:
        if chat_id == "general":
            users = set(self.user_sockets.keys())
        elif chat_id.startswith("bot_"):
            target = chat_id[4:].strip().lower()
            users = {target} if target else set()
        else:
            cursor.execute("SELECT nickname FROM members WHERE chat_id=?", (chat_id,))
            users = {str(r[0]).lower() for r in cursor.fetchall() if r[0]}
        if include_nick:
            users.add(include_nick.lower())
        return list(users)

    async def broadcast_chat_event(self, chat_id: str, payload: dict, sender_nick: str = ""):
        """Рассылает событие в открытый чат И на все остальные устройства/вкладки участников (включая свои)."""
        chat_socks = self.chat_sockets.get(chat_id, set())
        delivered = set()
        for ws in list(chat_socks):
            try:
                await ws.send_json(payload)
                delivered.add(ws)
            except Exception:
                pass
        for u_low in self._chat_participant_lows(chat_id, include_nick=sender_nick):
            for ws in list(self.user_sockets.get(u_low, set())):
                if ws in delivered:
                    continue
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
        for ws in list(self.signal_sockets.get(canonical, [])):
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
            if is_sup or is_super(nick) or is_assistant(nick) or perms.get("can_handle_support", False):
                await self.send_to_user(nick, payload)

manager = ConnectionManager()

async def _notify_admins_about_request(text: str):
    """Оповещение суперов и помощников о новой заявке: WS + запись в notifications."""
    cursor.execute("SELECT nickname FROM users WHERE is_superadmin=1 OR is_admin=1")
    rows = cursor.fetchall()
    for (nick,) in rows:
        if is_super(nick) or is_assistant(nick):
            await manager.send_to_user(nick, {"action": "admin_notify", "msg": text})
            cursor.execute("INSERT INTO notifications (to_user, text) VALUES (?, ?)", (nick, text))
    conn.commit()

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

    # Сначала бан: забаненный при входе видит причину и срок, а не «кик-лок»
    cursor.execute("SELECT reason, ban_type, expires_at FROM bans WHERE LOWER(target_nick)=LOWER(?)", (data.nickname,))
    ban = cursor.fetchone()
    if ban:
        reason, ban_type, expires_at = ban
        if ban_type == "permanent" or (ban_type == "temporary" and expires_at and expires_at > ns):
            if ban_type == "permanent":
                srok = "навсегда"
            else:
                left = datetime.strptime(expires_at, '%Y-%m-%d %H:%M:%S') - datetime.utcnow()
                if left.total_seconds() < 0:
                    left = timedelta(0)
                local_exp = datetime.now() + left
                hh, mm = int(left.total_seconds() // 3600), int((left.total_seconds() % 3600) // 60)
                srok = f"до {local_exp.strftime('%d.%m.%Y %H:%M')} (осталось {hh} ч. {mm} мин.)"
            return {"status": "banned",
                    "msg": f"Аккаунт заблокирован! Причина: {reason}. Срок бана: {srok}. Войти нельзя."}
        else:
            cursor.execute("DELETE FROM bans WHERE LOWER(target_nick)=LOWER(?)", (data.nickname,))
            conn.commit()

    # Только что выкинули из аккаунта — вход заблокирован на 30 секунд
    lock_left = kick_block_left(data.nickname)
    if lock_left > 0:
        return {"status": "error",
                "msg": f"Вас только что выкинули из аккаунта. Подождите {lock_left} сек. перед входом."}

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
        # Регистрация: пустой аккаунт создать нельзя, ник и пароль — от 3 символов
        nick = (data.nickname or "").strip()
        pwd = data.password or ""
        if len(nick) < 3:
            return {"status": "error", "msg": "Ник должен быть от 3 символов!"}
        if len(nick) > 8:
            return {"status": "error", "msg": "Ник должен быть не длиннее 8 символов!"}
        if len(pwd.strip()) < 3:
            return {"status": "error", "msg": "Пароль должен быть от 3 символов!"}
        if is_deleted_nick(nick):
            return {"status": "error",
                    "msg": "Этот ник занят: аккаунт с таким никнеймом был удалён администрацией и больше не регистрируется."}
        if nick.lower() in ("система", "system"):
            return {"status": "error", "msg": "Этот ник зарезервирован системой."}
        data.nickname = nick

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
async def update_profile(data: ProfileUpdateData):
    cursor.execute("SELECT nickname, last_username_change FROM users WHERE LOWER(nickname)=LOWER(?)", (data.current_nickname.strip(),))
    user = cursor.fetchone()
    if not user:
        return {"status": "error", "msg": "Пользователь не найден!"}

    real_current = user[0]
    new_nick = data.new_nickname.strip() if data.new_nickname else real_current

    if new_nick != real_current:
        if is_super(real_current):
            return {"status": "error", "msg": "🔒 Смена юзернейма для Главного Администратора заблокирована ядром системы!"}
        if len(new_nick) < 3:
            return {"status": "error", "msg": "Юзернейм должен быть от 3 символов!"}
        if len(new_nick) > 8:
            return {"status": "error", "msg": "Юзернейм должен быть не длиннее 8 символов!"}

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
        cursor.execute("UPDATE chats SET owner=? WHERE owner=?", (new_nick, real_current))
        cursor.execute("UPDATE chats SET direct_user1=? WHERE direct_user1=?", (new_nick, real_current))
        cursor.execute("UPDATE chats SET direct_user2=? WHERE direct_user2=?", (new_nick, real_current))
        cursor.execute("UPDATE friends SET user1=? WHERE user1=?", (new_nick, real_current))
        cursor.execute("UPDATE friends SET user2=? WHERE user2=?", (new_nick, real_current))
        cursor.execute("UPDATE invites SET from_user=? WHERE from_user=?", (new_nick, real_current))
        cursor.execute("UPDATE invites SET to_user=? WHERE to_user=?", (new_nick, real_current))
        cursor.execute("UPDATE user_sessions SET nickname=? WHERE nickname=?", (new_nick, real_current))
        cursor.execute("UPDATE profile_game_stats SET nickname=? WHERE LOWER(nickname)=LOWER(?)", (new_nick, real_current))
        cursor.execute("UPDATE game_stats SET nickname=? WHERE LOWER(nickname)=LOWER(?)", (new_nick, real_current))
        cursor.execute("UPDATE user_settings SET nickname=? WHERE LOWER(nickname)=LOWER(?)", (new_nick, real_current))
        cursor.execute("UPDATE chat_reads SET nickname=? WHERE LOWER(nickname)=LOWER(?)", (new_nick, real_current))
        cursor.execute("UPDATE message_reactions SET nickname=? WHERE LOWER(nickname)=LOWER(?)", (new_nick, real_current))
        cursor.execute("UPDATE clicker_grants SET nickname=? WHERE LOWER(nickname)=LOWER(?)", (new_nick, real_current))
        cursor.execute("UPDATE bans SET target_nick=? WHERE LOWER(target_nick)=LOWER(?)", (new_nick, real_current))
        cursor.execute("UPDATE mutes SET target_nick=? WHERE LOWER(target_nick)=LOWER(?)", (new_nick, real_current))
        cursor.execute("UPDATE support_tickets SET from_user=? WHERE LOWER(from_user)=LOWER(?)", (new_nick, real_current))
        cursor.execute("UPDATE notifications SET to_user=? WHERE LOWER(to_user)=LOWER(?)", (new_nick, real_current))
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

    cursor.execute("SELECT avatar_url, bio FROM users WHERE nickname=?", (target_nick,))
    pr = cursor.fetchone()
    await manager.broadcast_all({
        "action": "profile_updated",
        "old_nickname": real_current,
        "nickname": target_nick,
        "avatar_url": (pr[0] or "") if pr else (data.avatar_url or ""),
        "bio": (pr[1] or "") if pr else (data.bio or ""),
    })
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
        await manager.send_to_user(data.from_user, {"action": "friends_updated"})
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

        # Обе стороны и все их устройства узнают сразу — без перезагрузки
        await manager.send_to_user(canonical_target, {"action": "friend_accepted", "by": data.from_user})
        await manager.send_to_user(data.from_user, {"action": "friend_accepted", "by": canonical_target})
        await manager.send_to_user(canonical_target, {"action": "chats_updated"})
        await manager.send_to_user(data.from_user, {"action": "chats_updated"})
        return {"status": "ok", "msg": f"Заявка от @{canonical_target} принята! Чат создан."}

    elif data.action in ("decline", "remove"):
        cursor.execute("DELETE FROM friends WHERE (LOWER(user1)=LOWER(?) AND LOWER(user2)=LOWER(?)) OR (LOWER(user1)=LOWER(?) AND LOWER(user2)=LOWER(?))",
                       (data.from_user, canonical_target, canonical_target, data.from_user))
        conn.commit()
        if data.action == "remove":
            # Удаление из друзей = единственная возможность удалить ЛС-чат:
            # находим dm-чат этой пары и удаляем его целиком
            cursor.execute("""
                SELECT id FROM chats
                WHERE is_direct=1 AND (
                    (LOWER(direct_user1)=LOWER(?) AND LOWER(direct_user2)=LOWER(?)) OR
                    (LOWER(direct_user1)=LOWER(?) AND LOWER(direct_user2)=LOWER(?)))
            """, (data.from_user, canonical_target, canonical_target, data.from_user))
            for (dm_id,) in cursor.fetchall():
                _purge_files_for("chat_id=?", (dm_id,))
                cursor.execute("DELETE FROM chats WHERE id=?", (dm_id,))
                cursor.execute("DELETE FROM members WHERE chat_id=?", (dm_id,))
                cursor.execute("DELETE FROM messages WHERE chat_id=?", (dm_id,))
                conn.commit()
                await manager.broadcast_all({"action": "chat_deleted", "id": dm_id})
        await manager.send_to_user(canonical_target, {"action": "friends_updated"})
        await manager.send_to_user(data.from_user, {"action": "friends_updated"})
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
async def rename_chat(data: ChatRenameData):
    if data.chat_id == "general":
        return {"status": "error", "msg": "Общий чат переименовывать нельзя!"}

    cursor.execute("SELECT is_direct FROM chats WHERE id=?", (data.chat_id,))
    row = cursor.fetchone()
    if not row:
        return {"status": "error", "msg": "Чат не найден!"}

    # ЛС переименовывается только локально (на клиенте) — серверное переименование запрещено
    if row[0]:
        return {"status": "error", "msg": "ЛС переименовывается только для себя (локально на вашем устройстве)!"}

    # Переименовать может владелец группы, админ с правом rename, либо владелец/супер сайта
    if not can_edit_chat(data.chat_id, data.user):
        return {"status": "error", "msg": "Нет прав: переименовывать может владелец группы или админ с правом!"}

    words = data.new_name.strip().split()
    if len(words) == 0:
        return {"status": "error", "msg": "Название не может быть пустым!"}
    if len(words) > 50:
        return {"status": "error", "msg": "Лимит названия: до 50 слов!"}

    final_name = " ".join(words)
    cursor.execute("UPDATE chats SET name=? WHERE id=?", (final_name, data.chat_id))
    conn.commit()
    await manager.broadcast_chat_event(data.chat_id, {
        "action": "chat_renamed", "chat_id": data.chat_id, "name": final_name
    })
    return {"status": "ok", "msg": "Чат успешно переименован!", "name": final_name}

# ═══════════════════════════════════════════════════════════
#  СИСТЕМА ПРИГЛАШЕНИЙ В ЧАТЫ
# ═══════════════════════════════════════════════════════════

def _chat_kind_accusative(chat_type: str, channel_kind: str) -> str:
    if channel_kind == "voice" or chat_type == "voice":
        return "голосовой канал"
    if chat_type == "channel":
        return "текстовый канал"
    return "группу"

@app.post("/invite")
async def send_invite(data: InviteData):
    if data.chat_id == "general" or str(data.chat_id).startswith(("dm_", "bot_")):
        return {"status": "error", "msg": "В этот чат нельзя отправить приглашение"}

    cursor.execute("SELECT nickname FROM users WHERE LOWER(nickname)=LOWER(?)", (data.to_user,))
    target_row = cursor.fetchone()
    if not target_row:
        return {"status": "error", "msg": "Пользователь не найден!"}
    canonical_target = target_row[0]
    canonical_from = _canonical_nick(data.from_user)

    cursor.execute("SELECT name, chat_type, channel_kind, is_direct FROM chats WHERE id=?", (data.chat_id,))
    chat_row = cursor.fetchone()
    if not chat_row or chat_row[3]:
        return {"status": "error", "msg": "Чат не найден или является личным"}
    real_name = chat_row[0] or data.chat_name
    ctype = chat_row[1] or "group"
    ch_kind = chat_row[2] or ("voice" if ctype == "voice" else "text")

    cursor.execute("SELECT 1 FROM members WHERE chat_id=? AND LOWER(nickname)=LOWER(?)", (data.chat_id, canonical_target))
    if cursor.fetchone():
        return {"status": "error", "msg": "Пользователь уже в этом чате!"}

    # Приглашать в группу/канал может владелец или админ с правом invite
    if not chat_permission(data.chat_id, canonical_from, "invite"):
        return {"status": "error", "msg": "Нет прав: приглашать может владелец или админ с правом invite!"}

    cursor.execute("SELECT id FROM invites WHERE chat_id=? AND LOWER(to_user)=LOWER(?)", (data.chat_id, canonical_target))
    if cursor.fetchone():
        return {"status": "ok", "msg": f"Приглашение в {_chat_kind_accusative(ctype, ch_kind)} уже отправлено @{canonical_target}!"}

    cursor.execute("INSERT INTO invites (chat_id, chat_name, from_user, to_user) VALUES (?, ?, ?, ?)",
                   (data.chat_id, real_name, canonical_from, canonical_target))
    conn.commit()
    payload = {
        "action": "new_invite",
        "from": canonical_from,
        "chat": real_name,
        "chat_id": data.chat_id,
        "chat_type": ctype,
        "channel_kind": ch_kind,
    }
    await manager.send_to_user(canonical_target, payload)
    # На случай если получатель прямо сейчас переключает activeWs
    if not manager.user_sockets.get(canonical_target.lower()):
        await manager.send_signal_to_user(canonical_target, payload)
    return {"status": "ok", "msg": f"Приглашение в {_chat_kind_accusative(ctype, ch_kind)} отправлено @{canonical_target}!"}

@app.get("/invites/{nickname}")
def get_invites(nickname: str):
    cursor.execute("""
        SELECT i.id, i.chat_id, COALESCE(c.name, i.chat_name), i.from_user,
               COALESCE(c.chat_type, 'group'), COALESCE(c.channel_kind, 'text'),
               COALESCE(c.topic, ''), c.id
        FROM invites i
        LEFT JOIN chats c ON i.chat_id = c.id
        WHERE LOWER(i.to_user)=LOWER(?)
    """, (nickname,))
    rows = cursor.fetchall()
    orphan_ids = [r[0] for r in rows if not r[7]]
    if orphan_ids:
        marks = ",".join("?" for _ in orphan_ids)
        cursor.execute(f"DELETE FROM invites WHERE id IN ({marks})", orphan_ids)
        conn.commit()
    return [{
        "id": r[0], "chat_id": r[1], "chat_name": r[2], "from_user": r[3],
        "chat_type": r[4] or "group", "channel_kind": r[5] or ("voice" if r[4] == "voice" else "text"),
        "topic": r[6] or "",
    } for r in rows if r[7]]

@app.post("/invite/respond")
async def respond_invite(data: InviteAction):
    cursor.execute("SELECT chat_id, to_user FROM invites WHERE id=?", (data.invite_id,))
    row = cursor.fetchone()
    if not row or row[1].lower() != data.nickname.lower():
        return {"status": "error", "msg": "Приглашение не найдено!"}

    chat_id = row[0]
    canonical_user = _canonical_nick(data.nickname)
    cursor.execute("SELECT 1 FROM chats WHERE id=?", (chat_id,))
    chat_exists = cursor.fetchone() is not None
    if data.action == "accept" and chat_exists:
        cursor.execute("INSERT OR IGNORE INTO members (chat_id, nickname) VALUES (?, ?)", (chat_id, canonical_user))
    cursor.execute("DELETE FROM invites WHERE id=?", (data.invite_id,))
    conn.commit()
    await manager.send_to_user(canonical_user, {"action": "invites_updated"})
    if data.action == "accept" and chat_exists:
        await manager.send_to_user(canonical_user, {"action": "chats_updated"})
        await manager.broadcast_chat_event(chat_id, {"action": "members_updated", "chat_id": chat_id})
    elif data.action == "accept" and not chat_exists:
        return {"status": "error", "msg": "Этот чат уже удалён"}
    return {"status": "ok"}

# ═══════════════════════════════════════════════════════════
#  ДИАЛОГИ И ЧАТЫ
# ═══════════════════════════════════════════════════════════

@app.post("/direct-chat")
async def get_or_create_direct_chat(data: DirectChatData):
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
    await manager.send_to_user(data.from_user, {"action": "chats_updated", "chat_id": cid})
    await manager.send_to_user(canonical_target, {"action": "chats_updated", "chat_id": cid})
    return {"status": "ok", "chat_id": cid, "chat_name": cname}

_voice_rooms: Dict[str, List[str]] = {}  # chat_id -> list of active voice participants
_voice_states: Dict[str, Dict[str, dict]] = {}  # chat_id -> { low_nick: {nickname, muted, deafened} }

def _voice_room_states(chat_id: str) -> List[dict]:
    st_map = _voice_states.get(chat_id) or {}
    out = []
    for u in _voice_rooms.get(chat_id, []):
        low = u.lower()
        st = st_map.get(low) or {"nickname": u, "muted": False, "deafened": False}
        out.append({"nickname": u, "muted": bool(st.get("muted")), "deafened": bool(st.get("deafened"))})
    return out

async def _broadcast_voice_room(chat_id: str):
    payload = {
        "action": "voice_room_updated",
        "chat_id": chat_id,
        "users": list(_voice_rooms.get(chat_id, [])),
        "states": _voice_room_states(chat_id),
    }
    await manager.broadcast_all(payload)
    # Доставляем также в signalWs на случай, если user прямо сейчас переключает chatWs
    for u_low, socks in list(manager.signal_sockets.items()):
        if u_low not in manager.user_sockets:
            for ws in list(socks):
                try:
                    await ws.send_json(payload)
                except Exception:
                    pass

def _remove_user_from_voice_room(chat_id: str, nick: str) -> bool:
    low = (nick or "").strip().lower()
    if not low or chat_id not in _voice_rooms:
        return False
    before = _voice_rooms.get(chat_id, [])
    after = [u for u in before if u.lower() != low]
    changed = len(after) != len(before)
    if after:
        _voice_rooms[chat_id] = after
    else:
        _voice_rooms.pop(chat_id, None)
    st_map = _voice_states.get(chat_id)
    if st_map and low in st_map:
        st_map.pop(low, None)
        if not st_map:
            _voice_states.pop(chat_id, None)
        changed = True
    return changed

async def _leave_all_voice_rooms_for(nick: str, except_chat_id: str = ""):
    for vcid in list(_voice_rooms.keys()):
        if except_chat_id and vcid == except_chat_id:
            continue
        if _remove_user_from_voice_room(vcid, nick):
            await _broadcast_voice_room(vcid)

def _get_pinned_preview(chat_id: str, pinned_id) -> Optional[dict]:
    if not pinned_id:
        return None
    cursor.execute("SELECT id, sender, text, file_type FROM messages WHERE id=? AND chat_id=?", (pinned_id, chat_id))
    r = cursor.fetchone()
    if not r:
        return None
    return {"id": r[0], "sender": r[1] or "", "text": (r[2] or "")[:140], "file_type": r[3] or ""}

@app.get("/chats/{nickname}")
def get_user_chats(nickname: str):
    cursor.execute("""
        SELECT DISTINCT c.id, c.name, c.is_direct, c.direct_user1, c.direct_user2,
               c.owner, c.chat_type, c.topic, c.server_name, c.channel_kind, c.pinned_msg_id
        FROM chats c LEFT JOIN members m ON c.id=m.chat_id
        WHERE c.id='general' OR LOWER(m.nickname)=LOWER(?)
    """, (nickname,))
    chats = []
    for r in cursor.fetchall():
        cid, cname, is_dir, u1, u2, owner, ctype, topic, srv_name, ch_kind, pinned_id = r
        my_role, my_title, my_perms = get_member_row(cid, nickname)
        cursor.execute("SELECT COUNT(*) FROM members WHERE chat_id=?", (cid,))
        mcount = cursor.fetchone()[0]
        # Счётчик непрочитанных: чужие сообщения новее последнего прочитанного
        cursor.execute("SELECT last_read_id FROM chat_reads WHERE chat_id=? AND LOWER(nickname)=LOWER(?)",
                       (cid, nickname))
        rr = cursor.fetchone()
        last_read = rr[0] if rr else 0
        cursor.execute("SELECT COUNT(*) FROM messages WHERE chat_id=? AND id>? AND LOWER(sender)!=LOWER(?)",
                       (cid, last_read, nickname))
        unread = cursor.fetchone()[0]
        if is_dir:
            partner = u2 if (u1 or "").lower() == nickname.lower() else u1
            cname = f"💬 @{partner}"
            ctype = "dm"
        elif ctype in (None, "", "dm"):
            ctype = "general" if cid == "general" else "group"
        chats.append({
            "id": cid, "name": cname, "is_direct": is_dir,
            "chat_type": ctype or "group",
            "topic": topic or "",
            "server_name": srv_name or "",
            "channel_kind": ch_kind or ("voice" if ctype == "voice" else "text"),
            "pinned_msg": _get_pinned_preview(cid, pinned_id),
            "voice_users": list(_voice_rooms.get(cid, [])),
            "voice_states": _voice_room_states(cid),
            "owner": owner or "",
            "my_role": my_role or ("owner" if cid == "general" else "member"),
            "my_title": my_title or "",
            "members_count": mcount,
            "direct_user1": u1 or "",
            "direct_user2": u2 or "",
            "unread": unread,
        })
    chats.sort(key=lambda x: 0 if x["id"] == "general" else 1)
    # Личный чат с ботом-помощником — виртуальная запись, сообщения лежат в bot_<ник>
    bot_id = f"bot_{nickname}"
    chats.insert(1 if chats and chats[0]["id"] == "general" else 0, {
        "id": bot_id, "name": "🤖 Бот-помощник", "is_direct": 1,
        "chat_type": "bot", "topic": "Справочник и ИИ-помощник", "server_name": "",
        "channel_kind": "text", "pinned_msg": None, "voice_users": [],
        "owner": "", "my_role": "member", "my_title": "",
        "members_count": 1, "unread": 0,
    })
    return chats

@app.post("/chats/{chat_id}/read")
async def mark_chat_read(chat_id: str, user: str):
    """Чат открыт — сбрасываем красный счётчик непрочитанных (на всех устройствах юзера)."""
    cursor.execute("SELECT COALESCE(MAX(id), 0) FROM messages WHERE chat_id=?", (chat_id,))
    last = cursor.fetchone()[0]
    cursor.execute("""INSERT INTO chat_reads (chat_id, nickname, last_read_id) VALUES (?,?,?)
                      ON CONFLICT(chat_id, nickname) DO UPDATE SET last_read_id=excluded.last_read_id""",
                   (chat_id, user, last))
    conn.commit()
    await manager.send_to_user(user, {"action": "chat_read", "chat_id": chat_id})
    return {"status": "ok", "unread": 0}

@app.get("/features")
def list_features():
    """Справка по функциям — тот же источник, что отвечает бот-помощник."""
    return [{"id": f["id"], "emoji": f["emoji"], "title": f["title"],
             "short": f["short"], "desc": f["desc"]} for f in FEATURES]

@app.post("/chats")
async def create_custom_chat(data: ChatCreateData):
    ctype = data.chat_type if data.chat_type in ("group", "channel", "voice") else "group"
    ch_kind = "voice" if (ctype == "voice" or data.channel_kind == "voice") else "text"
    topic = " ".join((data.topic or "").split())[:120]
    srv = " ".join((data.server_name or "").split())[:40]
    # Создатель группы сразу становится её владельцем
    cursor.execute(
        "INSERT OR IGNORE INTO chats (id, name, created_by, is_direct, owner, chat_type, topic, server_name, channel_kind) VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?)",
        (data.id, data.name, data.created_by, data.created_by, ctype, topic, srv, ch_kind))
    cursor.execute("INSERT OR IGNORE INTO members (chat_id, nickname, role) VALUES (?, ?, 'owner')",
                   (data.id, data.created_by))
    # Если чат уже был (дубль id) — нового владельца не перетираем
    conn.commit()
    # Новый чат видят ВСЕ пользователи на сайте, а не только создатель
    # (для звонковой группы silent=True — она не должна мелькать в общем списке)
    if not data.silent:
        await manager.broadcast_all({
            "action": "chat_created",
            "id": data.id,
            "name": data.name,
            "chat_type": ctype,
            "channel_kind": ch_kind,
            "created_by": data.created_by,
        })
    return {"status": "ok", "owner": data.created_by, "chat_type": ctype, "channel_kind": ch_kind}

@app.post("/voice/{chat_id}/join")
async def join_voice_room(chat_id: str, data: VoiceRoomActionData):
    cursor.execute("""
        SELECT id, name, chat_type, channel_kind FROM chats WHERE id=?
    """, (chat_id,))
    crow = cursor.fetchone()
    if not crow:
        return {"status": "error", "msg": "Голосовой канал не найден"}
    nick = _canonical_nick(data.nickname)
    if not nick:
        return {"status": "error", "msg": "Укажите ник"}
    if data.session_token:
        try:
            _auth_session(nick, data.session_token)
        except Exception:
            return {"status": "error", "msg": "Сессия недействительна"}
    await _leave_all_voice_rooms_for(nick, except_chat_id=chat_id)
    peers = _voice_rooms.setdefault(chat_id, [])
    if not any(p.lower() == nick.lower() for p in peers):
        peers.append(nick)
    st_map = _voice_states.setdefault(chat_id, {})
    st_map[nick.lower()] = {"nickname": nick, "muted": False, "deafened": False}
    await _broadcast_voice_room(chat_id)
    return {"status": "ok", "users": list(peers), "states": _voice_room_states(chat_id)}

@app.post("/voice/{chat_id}/leave")
async def leave_voice_room(chat_id: str, request: Request):
    nick = ""
    try:
        raw = await request.body()
        if raw:
            body = json.loads(raw.decode("utf-8"))
            nick = (body.get("nickname") or "").strip()
    except Exception:
        nick = ""
    if not nick:
        return {"status": "ok", "users": list(_voice_rooms.get(chat_id, []))}
    nick = _canonical_nick(nick)
    if chat_id == "__all__":
        await _leave_all_voice_rooms_for(nick)
        return {"status": "ok", "users": []}
    _remove_user_from_voice_room(chat_id, nick)
    await _broadcast_voice_room(chat_id)
    return {"status": "ok", "users": list(_voice_rooms.get(chat_id, [])), "states": _voice_room_states(chat_id)}

@app.delete("/chats/{chat_id}")
async def delete_chat(chat_id: str, user: str):
    if chat_id == "general":
        return {"status": "error", "msg": "Общий чат защищён от удаления!"}
    # Удалять чат может только его владелец (или владелец/супер всего сайта)
    if not can_delete_chat(chat_id, user):
        return {"status": "error", "msg": "Удалять чат может только владелец группы!"}
    # Файлы всех сообщений чата больше никому не нужны — убираем с диска
    _purge_files_for("chat_id=?", (chat_id,))
    cursor.execute("DELETE FROM chats WHERE id=?", (chat_id,))
    cursor.execute("DELETE FROM members WHERE chat_id=?", (chat_id,))
    cursor.execute("DELETE FROM messages WHERE chat_id=?", (chat_id,))
    cursor.execute("DELETE FROM invites WHERE chat_id=?", (chat_id,))
    conn.commit()
    had_voice = bool(_voice_rooms.pop(chat_id, None))
    _voice_states.pop(chat_id, None)
    if had_voice:
        await _broadcast_voice_room(chat_id)
    await manager.broadcast_all({"action": "chat_deleted", "id": chat_id})
    return {"status": "ok"}

@app.post("/chats/{chat_id}/clear")
async def clear_chat(chat_id: str, user: str):
    # Очистка ленты: для ЛС — любой участник, для групп — как удаление чата
    cursor.execute("SELECT is_direct FROM chats WHERE id=?", (chat_id,))
    row = cursor.fetchone()
    if not row:
        return {"status": "error", "msg": "Чат не найден"}
    if row[0]:
        cursor.execute("SELECT 1 FROM members WHERE chat_id=? AND LOWER(nickname)=LOWER(?)", (chat_id, user))
        if not cursor.fetchone():
            return {"status": "error", "msg": "Это не ваш чат"}
    elif not can_delete_chat(chat_id, user):
        return {"status": "error", "msg": "Очищать может только владелец группы!"}
    _purge_files_for("chat_id=?", (chat_id,))
    cursor.execute("DELETE FROM messages WHERE chat_id=?", (chat_id,))
    conn.commit()
    await manager.broadcast_chat_event(chat_id, {"action": "chat_cleared", "id": chat_id, "chat_id": chat_id})
    return {"status": "ok", "msg": "История очищена"}


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
        await manager.broadcast_chat_event(chat_id, {"action": "members_updated", "chat_id": chat_id})
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
    await manager.broadcast_chat_event(chat_id, {"action": "members_updated", "chat_id": chat_id})
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
    if _remove_user_from_voice_room(chat_id, target):
        await _broadcast_voice_room(chat_id)
    await manager.broadcast_chat_event(chat_id, {"action": "members_updated", "chat_id": chat_id})
    await manager.send_to_user(target, {"action": "chats_updated"})
    if leaving_self:
        return {"status": "ok", "msg": "Вы покинули группу"}
    return {"status": "ok", "msg": f"@{target} исключён из группы"}

@app.get("/messages/{chat_id}")
def get_messages(chat_id: str):
    periodic_cleanup()
    cursor.execute("""
        SELECT id, sender, text, file_url, file_type, reply_to_id, reply_to_text, reply_to_sender,
               strftime('%H:%M', created_at), is_system, COALESCE(is_edited, 0)
        FROM messages WHERE chat_id=? ORDER BY id DESC LIMIT 150
    """, (chat_id,))
    rows = list(reversed(cursor.fetchall()))

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
        "reactions": reactions.get(r[0], {}), "is_system": bool(r[9]), "is_edited": bool(r[10]),
    } for r in rows]

@app.get("/messages/{chat_id}/search")
def search_chat_messages(chat_id: str, q: str = "", limit: int = 30):
    term = (q or "").strip()
    if not term:
        return []
    cursor.execute("""
        SELECT id, sender, text, strftime('%H:%M', created_at), is_system
        FROM messages
        WHERE chat_id=? AND LOWER(COALESCE(text,'')) LIKE ?
        ORDER BY id DESC LIMIT ?
    """, (chat_id, f"%{term.lower()}%", max(1, min(limit, 50))))
    return [{
        "id": r[0], "sender": r[1] or "", "text": r[2] or "", "time": r[3] or "", "is_system": bool(r[4])
    } for r in cursor.fetchall()]

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
    cursor.execute("""SELECT nickname, avatar_url, bio, last_login, is_owner, is_admin, is_superadmin, COALESCE(fav_track, '')
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

    # Баланс кликера (монеты) — для профиля
    coins = 0
    try:
        cursor.execute("SELECT coins FROM profile_game_stats WHERE LOWER(nickname)=LOWER(?)", (me,))
        cr = cursor.fetchone()
        coins = int(cr[0] or 0) if cr else 0
    except Exception:
        coins = 0

    return {
        "status": "ok", "nickname": me, "avatar_url": u[1] or "",
        "bio": u[2] or "", "last_login": u[3],
        "is_online": manager.is_online(me),
        "is_owner": bool(u[4]), "is_admin": bool(u[5]), "is_superadmin": bool(u[6]),
        "friend_status": friend_status,
        "dm_chat_id": dm_id,
        "common_chats": common,
        "coins": coins,
        "fav_track": u[7] or "",
    }

# ── НАСТРОЙКИ ПОЛЬЗОВАТЕЛЯ (синхронизация между устройствами) ──
@app.get("/user/settings/{nickname}")
def get_user_settings(nickname: str):
    cursor.execute("SELECT settings_json FROM user_settings WHERE LOWER(nickname)=LOWER(?)", (nickname,))
    row = cursor.fetchone()
    if row:
        try:
            import json as _json
            return {"status": "ok", "settings": _json.loads(row[0] or "{}")}
        except Exception:
            return {"status": "ok", "settings": {}}
    return {"status": "ok", "settings": {}}

@app.post("/user/settings/{nickname}")
async def save_user_settings(nickname: str, data: dict = None):
    import json as _json
    if not data: return {"status": "error", "msg": "Нет данных"}
    settings = data.get("settings", {})
    # Ограничиваем размер — не храним мегабайты
    s = _json.dumps(settings, ensure_ascii=False)
    if len(s) > 50000:
        return {"status": "error", "msg": "Настройки слишком большие"}
    cursor.execute("""INSERT INTO user_settings (nickname, settings_json, updated_at)
                      VALUES (?, ?, datetime('now'))
                      ON CONFLICT(nickname) DO UPDATE SET settings_json=excluded.settings_json, updated_at=datetime('now')""",
                   (nickname, s))
    conn.commit()
    await manager.send_to_user(nickname, {"action": "settings_updated", "settings": settings})
    return {"status": "ok"}

# ═══════════════════════════════════════════════════════════
#  АДМИНИСТРАТИВНАЯ ПАНЕЛЬ
# ═══════════════════════════════════════════════════════════

@app.get("/admin/users")
def get_admin_users(admin: str, query: str = "", filter_type: str = "all"):
    cursor.execute("SELECT is_superadmin, is_admin, is_owner FROM users WHERE LOWER(nickname)=LOWER(?)", (admin,))
    row = cursor.fetchone()
    if not row or (not row[0] and not row[1] and not row[2] and not is_super(admin)):
        raise HTTPException(status_code=403, detail="Доступ запрещён")

    ns = now_str()
    cursor.execute("""
        SELECT u.nickname, u.created_at, u.last_login, u.is_superadmin, u.is_admin, u.admin_perms,
               u.avatar_url, u.bio, COALESCE(g.coins, 0), COALESCE(u.is_owner, 0)
        FROM users u LEFT JOIN profile_game_stats g ON LOWER(g.nickname)=LOWER(u.nickname)
        WHERE LOWER(u.nickname) LIKE ? ORDER BY u.last_login DESC LIMIT 150
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

        try:
            perms_dict = json.loads(r[5] or '{}')
        except Exception:
            perms_dict = {}
        users_list.append({
            "nickname": nick, "created_at": r[1], "last_login": r[2],
            "is_owner": bool(r[9]) or nick.lower() == "jjlop55",
            "is_superadmin": bool(r[3]) or is_super(nick), "is_admin": is_adm,
            "perms": perms_dict, "avatar_url": r[6] or "", "bio": r[7] or "",
            "is_online": online, "is_banned": is_banned, "is_muted": is_muted,
            "coins": int(r[8] or 0),
            "assistant": bool(perms_dict.get("can_assistant"))
        })

    cursor.execute("SELECT COUNT(*) FROM users")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM bans WHERE ban_type='permanent' OR expires_at>?", (ns,))
    active_bans = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM mutes WHERE expires_at>?", (ns,))
    active_mutes = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM support_tickets WHERE status='open'")
    open_tickets = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM delete_requests WHERE status='pending'")
    pend_del = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM ban_requests WHERE status='pending'")
    pend_ban = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM support_perm_requests WHERE status='pending'")
    pend_sup = cursor.fetchone()[0]
    return {
        "total_users": total,
        "online_count": len(manager.online_users()),
        "active_bans": active_bans,
        "active_mutes": active_mutes,
        "open_tickets": open_tickets,
        "pending_requests": pend_del + pend_ban + pend_sup,
        "users": users_list,
    }

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
    # Строгая проверка: сессию можно рвать только с правом на кик
    if not has_perm(data.nickname, "can_kick_users"):
        return {"status": "error", "msg": "Нет права «Кик пользователей»!"}

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
    cursor.execute("SELECT is_superadmin, is_admin, is_owner FROM users WHERE LOWER(nickname)=LOWER(?)", (admin,))
    row = cursor.fetchone()
    if not row or (not row[0] and not row[1] and not row[2] and not is_super(admin)):
        raise HTTPException(status_code=403, detail="Доступ запрещён")

    cursor.execute("SELECT id, name, created_by, created_at, is_direct, chat_type, topic, channel_kind FROM chats")
    res = []
    for cid, cname, cc, cca, is_dir, ctype, topic, ch_kind in cursor.fetchall():
        cursor.execute("SELECT nickname, role FROM members WHERE chat_id=?", (cid,))
        roles = {r[0]: (r[1] or "member") for r in cursor.fetchall()}
        cursor.execute("SELECT COUNT(*) FROM messages WHERE chat_id=?", (cid,))
        msg_cnt = cursor.fetchone()[0]
        owner = next((n for n, rl in roles.items() if rl == "owner"), None)
        res.append({
            "id": cid, "name": cname, "created_by": cc, "created_at": cca,
            "is_direct": bool(is_dir), "chat_type": ctype or "group",
            "topic": topic or "", "channel_kind": ch_kind or "text",
            "msg_count": msg_cnt,
            "members": list(roles.keys()), "roles": roles, "owner": owner
        })
    return res

@app.get("/admin/chat-messages")
def admin_view_chat_messages(admin: str, chat_id: str, limit: int = 80):
    require_admin(admin)
    if not (can_manage_everything(admin) or is_assistant(admin) or has_perm(admin, "can_view_all_chats")):
        raise HTTPException(status_code=403, detail="Нет права «Просмотр всех чатов» (can_view_all_chats)")
    cursor.execute("""
        SELECT id, sender, text, file_url, file_type, strftime('%d.%m %H:%M', created_at), is_system
        FROM messages WHERE chat_id=? ORDER BY id DESC LIMIT ?
    """, (chat_id, max(1, min(limit, 150))))
    rows = list(reversed(cursor.fetchall()))
    return [{
        "id": r[0], "sender": r[1] or "", "text": r[2] or "",
        "file_url": r[3] or "", "file_type": r[4] or "",
        "time": r[5] or "", "is_system": bool(r[6])
    } for r in rows]

@app.post("/admin/broadcast")
async def admin_broadcast_announcement(data: AdminBroadcastData):
    if not (can_manage_everything(data.admin_nick) or is_assistant(data.admin_nick)):
        raise HTTPException(status_code=403, detail="Доступ только для Главного Администратора или помощника")
    text = (data.message or "").strip()[:500]
    if not text:
        return {"status": "error", "msg": "Введите текст объявления!"}
    await manager.broadcast_all({
        "action": "admin_broadcast",
        "from": data.admin_nick,
        "msg": f"📢 Объявление от @{data.admin_nick}: {text}",
    })
    if data.post_to_general:
        await post_system_message("general", f"📢 ОБЪЯВЛЕНИЕ (@{data.admin_nick}): {text}")
    return {"status": "ok", "msg": "Объявление доставлено всем пользователям в сети!"}

@app.post("/admin/restore-deleted")
def admin_restore_deleted(data: AdminRestoreDeletedData):
    if not (can_manage_everything(data.admin_nick) or is_assistant(data.admin_nick)):
        raise HTTPException(status_code=403, detail="Только Главный Администратор или помощник")
    target = (data.target_nick or "").strip()
    if not target:
        return {"status": "error", "msg": "Укажите ник!"}
    cursor.execute("DELETE FROM deleted_users WHERE LOWER(nickname)=LOWER(?)", (target,))
    conn.commit()
    return {"status": "ok", "msg": f"Ник @{target} разблокирован для повторной регистрации."}

@app.post("/admin/ban")
async def ban_user(data: BanData):
    require_admin(data.admin_nick)
    # Ни один админ (включая супера) не может забанить сам себя
    if data.admin_nick.strip().lower() == data.target_nick.strip().lower():
        return {"status": "error", "msg": "Нельзя забанить самого себя!"}
    if is_super(data.target_nick):
        return {"status": "error", "msg": "Главного администратора заблокировать невозможно!"}

    # Помощник баниет сам (все права супера); обычный админ с can_ban_users —
    # только ЗАЯВКА на бан, решение принимает супер (/admin/ban-requests/decision)
    sup = is_super(data.admin_nick) or is_assistant(data.admin_nick)
    if not sup:
        if not has_perm(data.admin_nick, "can_ban_users"):
            return {"status": "error", "msg": "Нет права «Блокировка (бан)» — его должен выдать Главный Администратор!"}
        exp = (datetime.utcnow() + timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("""INSERT INTO ban_requests
            (target_nick, requested_by, reason, ban_type, duration_hours, expires_at)
            VALUES (?,?,?,?,?,?,?)""",
            (data.target_nick, data.admin_nick, data.reason, data.ban_type or 'permanent',
             float(data.duration_hours or 0), exp))
        conn.commit()
        await _notify_admins_about_request(
            f"🚫 Заявка на бан @{data.target_nick} от @{data.admin_nick}: {data.reason or 'без причины'}")
        return {"status": "ok", "msg": f"Заявка на бан @{data.target_nick} отправлена суперу (срок 7 дней).", "request": True}

    expires_at = None
    if data.ban_type == "temporary" and data.duration_hours > 0:
        expires_at = (datetime.utcnow() + timedelta(hours=data.duration_hours)).strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("DELETE FROM bans WHERE LOWER(target_nick)=LOWER(?)", (data.target_nick,))
    cursor.execute("INSERT INTO bans (target_nick, banned_by, reason, ban_type, expires_at) VALUES (?,?,?,?,?)",
                   (data.target_nick, data.admin_nick, data.reason, data.ban_type, expires_at))
    conn.commit()
    # Забаненный сразу выкидывается со всех устройств и роняет открытые вкладки
    await kick_account(data.target_nick,
                       f"Вы заблокированы! Причина: {data.reason or 'нарушение правил'}.", lock=False)
    return {"status": "ok", "msg": f"@{data.target_nick} заблокирован и отключён со всех устройств!"}

@app.post("/admin/unban")
def unban_user(data: BanData):
    require_admin(data.admin_nick)
    if not has_perm(data.admin_nick, "can_ban_users"):
        return {"status": "error", "msg": "Нет права «Блокировка (бан)»!"}
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

@app.post("/admin/unmute")
async def unmute_user(data: BanData):
    """Снятие мута (кнопка «Размьютить» в супер-панели)."""
    require_admin(data.admin_nick)
    if not has_perm(data.admin_nick, "can_mute_users"):
        return {"status": "error", "msg": "Нет права «Заглушение (мьют)»!"}
    cursor.execute("DELETE FROM mutes WHERE LOWER(target_nick)=LOWER(?)", (data.target_nick,))
    conn.commit()
    await manager.send_to_user(data.target_nick, {
        "action": "unmuted",
        "msg": f"🔊 Мут снят администратором @{data.admin_nick}.",
    })
    return {"status": "ok", "msg": f"@{data.target_nick} размьючен."}


@app.get("/admin/mutes")
def get_mutes(admin: str):
    """Список активных мутов (для супер-панели: кто замучен)."""
    if not (is_super(admin) or is_assistant(admin)):
        cursor.execute("SELECT is_superadmin, is_admin, admin_perms FROM users WHERE LOWER(nickname)=LOWER(?)", (admin,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=403, detail="Доступ запрещён")
        perms = json.loads(row[2] or '{}')
        if not (row[1] and perms.get("can_mute_users")):
            raise HTTPException(status_code=403, detail="Доступ запрещён")
    periodic_cleanup()
    cursor.execute("""SELECT target_nick, muted_by, reason, duration_minutes, expires_at
                      FROM mutes WHERE expires_at > ? ORDER BY id DESC""",
                   (datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),))
    return [{"target_nick": r[0], "muted_by": r[1], "reason": r[2],
             "duration_minutes": r[3], "expires_at": r[4]} for r in cursor.fetchall()]


@app.get("/admin/perm-grants")
def get_perm_grants(admin: str):
    """Выданные права: супер и помощник смотрят, кому что выдано."""
    if not (is_super(admin) or is_assistant(admin)):
        raise HTTPException(status_code=403, detail="Доступ запрещён")
    cursor.execute("""SELECT nickname, is_admin, is_superadmin, admin_perms FROM users
                      WHERE is_admin=1 OR is_superadmin=1 OR admin_perms IS NOT NULL
                      ORDER BY is_superadmin DESC, nickname""")
    out = []
    for r in cursor.fetchall():
        try:
            perms = json.loads(r[3] or '{}')
        except Exception:
            perms = {}
        if r[2] or perms:
            out.append({"nickname": r[0], "is_admin": bool(r[1]), "is_superadmin": bool(r[2]), "perms": perms})
    return out


@app.post("/admin/mute")
async def mute_user(data: MuteData):
    require_admin(data.admin_nick)
    # Строгая проверка права: без can_mute_users мьютить нельзя (супер/помощник — можно)
    if not has_perm(data.admin_nick, "can_mute_users"):
        return {"status": "error", "msg": "Нет права «Заглушение (мьют)» — его должен выдать Главный Администратор!"}
    # Нельзя замутить самого себя
    if data.admin_nick.strip().lower() == data.target_nick.strip().lower():
        return {"status": "error", "msg": "Нельзя замутить самого себя!"}
    # Обычный админ: мут максимум 5 часов (300 мин), без продления.
    # Супер и помощник — без лимитов.
    sup = is_super(data.admin_nick) or is_assistant(data.admin_nick)
    duration = int(data.duration_minutes or 0)
    if not sup:
        if duration > 300:
            return {"status": "error", "msg": "Без супер-прав мут максимум 5 часов (300 мин)!"}
        # Продление запрещено: если уже замучен — отказ
        cursor.execute("SELECT expires_at FROM mutes WHERE LOWER(target_nick)=LOWER(?)", (data.target_nick,))
        cur = cursor.fetchone()
        if cur and cur[0]:
            try:
                left = datetime.strptime(cur[0], '%Y-%m-%d %H:%M:%S') - datetime.utcnow()
                if left.total_seconds() > 0:
                    return {"status": "error", "msg": "Пользователь уже замучен — продление мута запрещено!"}
            except Exception:
                pass
    if duration <= 0:
        return {"status": "error", "msg": "Укажите длительность мута!"}
    exp = (datetime.utcnow() + timedelta(minutes=duration)).strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("DELETE FROM mutes WHERE LOWER(target_nick)=LOWER(?)", (data.target_nick,))
    cursor.execute("INSERT INTO mutes (target_nick, muted_by, reason, duration_minutes, expires_at) VALUES (?,?,?,?,?)",
                   (data.target_nick, data.admin_nick, data.reason, duration, exp))
    conn.commit()

    reason_txt = (data.reason or "").strip()
    # Все личные диалоги замьюченного получают служебное уведомление
    cursor.execute("""
        SELECT DISTINCT m.chat_id FROM members m JOIN chats c ON c.id = m.chat_id
        WHERE LOWER(m.nickname)=LOWER(?) AND c.is_direct=1
    """, (data.target_nick,))
    dm_chats = [r[0] for r in cursor.fetchall()]
    note = (f"🔇 Пользователь @{data.target_nick} замучен на {duration} мин. по правилам"
            + (f": {reason_txt}" if reason_txt else "") + ".")
    for cid in dm_chats:
        await post_system_message(cid, note)

    # Сам мьюченный узнаёт об этом сразу (даже если не в сети — увидит тостом при открытии)
    await manager.send_to_user(data.target_nick, {
        "action": "muted",
        "msg": f"🔇 Вас замьючили на {duration} мин. по правилам"
               + (f": {reason_txt}" if reason_txt else "") + ". Писать нельзя.",
        "minutes": duration, "reason": reason_txt, "expires": exp,
    })
    return {"status": "ok", "msg": f"@{data.target_nick} замьючен на {duration} мин."}

@app.post("/admin/kick")
async def kick_user(data: BanData):
    require_admin(data.admin_nick)
    # Строгая проверка права: без can_kick_users кикать нельзя
    if not has_perm(data.admin_nick, "can_kick_users"):
        return {"status": "error", "msg": "Нет права «Кик» — его должен выдать Главный Администратор!"}
    # Себя кикнуть нельзя (даже суперу) — иначе выкинешь сам себя
    if data.admin_nick.strip().lower() == data.target_nick.strip().lower():
        return {"status": "error", "msg": "Нельзя кикнуть самого себя!"}
    if is_super(data.target_nick):
        return {"status": "error", "msg": "Нельзя кикнуть Главного Администратора!"}
    # Гасит сессии, рвёт вкладки сразу и запрещает вход на 30 секунд;
    # сам кик — не чаще одного раза в 15 секунд, чтобы нельзя было заморозить аккаунт
    ok, msg = await kick_account(data.target_nick, "Вы были кикнуты администратором.")
    if not ok:
        return {"status": "error", "msg": msg}
    return {"status": "ok", "msg": f"@{data.target_nick} кикнут. {msg}"}

def note_deleted_user(nick: str, by: str, reason: str = ""):
    """Аккаунт удалён — ник уходит в чёрный список и больше не регистрируется."""
    cursor.execute("INSERT OR REPLACE INTO deleted_users (nickname, deleted_by, reason) VALUES (?,?,?)",
                   (nick, by or "", (reason or "").strip()[:300]))


def is_deleted_nick(nick: str) -> bool:
    cursor.execute("SELECT 1 FROM deleted_users WHERE LOWER(nickname)=LOWER(?)", (nick,))
    return cursor.fetchone() is not None


@app.post("/admin/delete-user")
async def delete_user_action(data: DeleteUserAction):
    require_admin(data.admin_nick)
    # Удалить самого себя нельзя ни при каких правах
    if data.admin_nick.strip().lower() == data.target_nick.strip().lower():
        return {"status": "error", "msg": "Нельзя удалить самому себе аккаунт!"}
    if is_super(data.target_nick):
        return {"status": "error", "msg": "Главного администратора удалить невозможно!"}
    cursor.execute("SELECT is_superadmin, is_admin, admin_perms FROM users WHERE LOWER(nickname)=LOWER(?)", (data.admin_nick,))
    adm = cursor.fetchone()
    if not adm: raise HTTPException(status_code=403, detail="Отказано")
    perms = json.loads(adm[2] or '{}')
    is_sup = bool(adm[0]) or is_super(data.admin_nick) or is_assistant(data.admin_nick)

    if is_sup or perms.get("can_delete_users_direct"):
        cursor.execute("DELETE FROM users WHERE LOWER(nickname)=LOWER(?)", (data.target_nick,))
        cursor.execute("DELETE FROM members WHERE LOWER(nickname)=LOWER(?)", (data.target_nick,))
        cursor.execute("DELETE FROM friends WHERE LOWER(user1)=LOWER(?) OR LOWER(user2)=LOWER(?)", (data.target_nick, data.target_nick))
        cursor.execute("DELETE FROM bans WHERE LOWER(target_nick)=LOWER(?)", (data.target_nick,))
        cursor.execute("DELETE FROM mutes WHERE LOWER(target_nick)=LOWER(?)", (data.target_nick,))
        note_deleted_user(data.target_nick, data.admin_nick, data.reason)
        conn.commit()
        # Удалённый сразу вылетает из аккаунта
        await kick_account(data.target_nick, "Ваш аккаунт был удалён администратором.", lock=False)
        return {"status": "ok", "msg": f"Аккаунт @{data.target_nick} удалён! Ник больше нельзя занять."}
    elif perms.get("can_request_delete_users"):
        if not data.reason.strip(): return {"status": "error", "msg": "Укажите причину для заявки!"}
        exp = (datetime.utcnow() + timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("INSERT INTO delete_requests (target_nick, requested_by, reason, expires_at) VALUES (?,?,?,?)",
                       (data.target_nick, data.admin_nick, data.reason.strip(), exp))
        conn.commit()
        await _notify_admins_about_request(
            f"🗑 Заявка на удаление @{data.target_nick} от @{data.admin_nick}: {data.reason.strip()}")
        return {"status": "ok", "msg": f"Заявка на удаление @{data.target_nick} отправлена (срок 7 дней)."}
    return {"status": "error", "msg": "Нет прав на удаление аккаунтов!"}

@app.get("/admin/delete-requests")
def get_delete_requests(admin: str):
    if not (is_super(admin) or is_assistant(admin)):
        raise HTTPException(status_code=403, detail="Доступ запрещён")
    periodic_cleanup()
    cursor.execute("SELECT id, target_nick, requested_by, reason, created_at, expires_at FROM delete_requests WHERE status='pending' ORDER BY id DESC")
    return [{"id": r[0], "target_nick": r[1], "requested_by": r[2], "reason": r[3],
             "created_at": r[4], "expires_at": r[5]} for r in cursor.fetchall()]

@app.get("/admin/deleted")
def get_deleted_users(admin: str):
    """Стопка удалённых аккаунтов: их ники больше нельзя занять."""
    cursor.execute("SELECT is_superadmin, is_admin FROM users WHERE LOWER(nickname)=LOWER(?)", (admin,))
    row = cursor.fetchone()
    if not row or (not row[0] and not row[1] and not is_super(admin)):
        raise HTTPException(status_code=403, detail="Доступ запрещён")
    cursor.execute("SELECT nickname, deleted_by, reason, deleted_at FROM deleted_users ORDER BY deleted_at DESC LIMIT 200")
    return [{"nickname": r[0], "deleted_by": r[1], "reason": r[2], "deleted_at": r[3]}
            for r in cursor.fetchall()]

@app.get("/admin/ban-requests")
def get_ban_requests(admin: str):
    """Заявки на бан: решают супер и помощник."""
    if not (is_super(admin) or is_assistant(admin)):
        raise HTTPException(status_code=403, detail="Доступ запрещён")
    periodic_cleanup()
    cursor.execute("""SELECT id, target_nick, requested_by, reason, ban_type, duration_hours, created_at
                      FROM ban_requests WHERE status='pending' ORDER BY id DESC""")
    return [{"id": r[0], "target_nick": r[1], "requested_by": r[2], "reason": r[3],
             "ban_type": r[4], "duration_hours": r[5], "created_at": r[6]} for r in cursor.fetchall()]


@app.post("/admin/ban-requests/decision")
async def decide_ban_request(data: ReqDecision):
    if not (is_super(data.admin_nick) or is_assistant(data.admin_nick)):
        return {"status": "error", "msg": "Только Главный Администратор или его помощник!"}
    cursor.execute("""SELECT target_nick, requested_by, reason, ban_type, duration_hours
                      FROM ban_requests WHERE id=? AND status='pending'""", (data.request_id,))
    req = cursor.fetchone()
    if not req:
        return {"status": "error", "msg": "Заявка не найдена!"}
    target, requester, req_reason, ban_type, hours = req
    if data.action == "approve":
        if is_super(target):
            return {"status": "error", "msg": "Главного администратора заблокировать невозможно!"}
        cursor.execute("UPDATE ban_requests SET status='approved' WHERE id=?", (data.request_id,))
        expires_at = None
        if ban_type == "temporary" and float(hours or 0) > 0:
            expires_at = (datetime.utcnow() + timedelta(hours=float(hours))).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("DELETE FROM bans WHERE LOWER(target_nick)=LOWER(?)", (target,))
        cursor.execute("INSERT INTO bans (target_nick, banned_by, reason, ban_type, expires_at) VALUES (?,?,?,?,?)",
                       (target, requester, req_reason, ban_type, expires_at))
        conn.commit()
        await kick_account(target,
                           f"Вы заблокированы! Причина: {req_reason or 'нарушение правил'}.", lock=False)
        text = f"Заявка на бан @{target} одобрена."
        cursor.execute("INSERT INTO notifications (to_user, text) VALUES (?, ?)", (requester, text))
        conn.commit()
        await manager.send_to_user(requester, {"action": "notify", "msg": text})
        return {"status": "ok", "msg": f"@{target} заблокирован по заявке @{requester}."}
    cursor.execute("UPDATE ban_requests SET status='rejected' WHERE id=?", (data.request_id,))
    text = f"Заявка на бан @{target} отклонена."
    cursor.execute("INSERT INTO notifications (to_user, text) VALUES (?, ?)", (requester, text))
    conn.commit()
    await manager.send_to_user(requester, {"action": "notify", "msg": text})
    return {"status": "ok", "msg": "Заявка отклонена."}


@app.post("/admin/delete-requests/decision")
async def decide_delete_request(data: ReqDecision):
    if not (is_super(data.admin_nick) or is_assistant(data.admin_nick)):
        return {"status": "error", "msg": "Только Главный Администратор или его помощник!"}
    cursor.execute("SELECT target_nick, requested_by, reason FROM delete_requests WHERE id=? AND status='pending'", (data.request_id,))
    req = cursor.fetchone()
    if not req: return {"status": "error", "msg": "Заявка не найдена!"}
    target, requester, req_reason = req
    if data.action == "approve":
        cursor.execute("UPDATE delete_requests SET status='approved' WHERE id=?", (data.request_id,))
        cursor.execute("DELETE FROM users WHERE LOWER(nickname)=LOWER(?)", (target,))
        cursor.execute("DELETE FROM members WHERE LOWER(nickname)=LOWER(?)", (target,))
        cursor.execute("DELETE FROM bans WHERE LOWER(target_nick)=LOWER(?)", (target,))
        note_deleted_user(target, data.admin_nick, req_reason)
        text = f"Удаление @{target} одобрено."
        cursor.execute("INSERT INTO notifications (to_user, text) VALUES (?, ?)", (requester, text))
        conn.commit()
        await kick_account(target, "Ваш аккаунт был удалён администратором.", lock=False)
        await manager.send_to_user(requester, {"action": "notify", "msg": text})
        return {"status": "ok", "msg": f"@{target} удалён! Ник больше нельзя занять."}
    else:
        cursor.execute("UPDATE delete_requests SET status='rejected' WHERE id=?", (data.request_id,))
        text = f"Заявка на удаление @{target} отклонена."
        cursor.execute("INSERT INTO notifications (to_user, text) VALUES (?, ?)", (requester, text))
        conn.commit()
        await manager.send_to_user(requester, {"action": "notify", "msg": text})
        return {"status": "ok", "msg": "Заявка отклонена."}

@app.post("/admin/set-perms")
async def set_admin_permissions(data: AdminPermsData):
    # Выдавать/снимать админ-права: супер, владелец сайта, помощник
    # или админ с конкретным правом can_grant_admins
    if not (can_manage_everything(data.admin_nick) or is_assistant(data.admin_nick)
            or has_perm(data.admin_nick, "can_grant_admins")):
        raise HTTPException(status_code=403, detail="Доступ запрещён")
    if is_super(data.target_nick): return {"status": "error", "msg": "Нельзя менять права Главного Администратора!"}
    # Роль «помощник» может выдать только супер/владелец
    if data.perms.get("can_assistant") and not can_manage_everything(data.admin_nick):
        return {"status": "error", "msg": "Роль «Помощник» выдаёт только Главный Администратор!"}
    # Помощник получает все права автоматически
    if data.perms.get("can_assistant"):
        for k in ("can_delete_messages", "can_delete_chats", "can_kick_users", "can_view_all_chats",
                  "can_grant_admins", "can_delete_users_direct", "can_request_delete_users",
                  "can_ban_users", "can_mute_users", "can_handle_support", "can_give_coins",
                  "can_manage_everything"):
            data.perms[k] = True
    has_any = any(data.perms.values())
    if has_any:
        cursor.execute("UPDATE users SET is_admin=1, admin_perms=?, admin_notified=0, granted_by=?, revoked_by=NULL WHERE LOWER(nickname)=LOWER(?)",
                       (json.dumps(data.perms), data.admin_nick, data.target_nick))
        conn.commit()
        # Живое оповещение: выдаваемый сразу узнаёт о правах (без перезагрузки)
        await manager.send_to_user(data.target_nick, {
            "action": "admin_granted", "granted_by": data.admin_nick,
            "perms": data.perms, "assistant": bool(data.perms.get("can_assistant"))})
        text = f"Вы получили права администратора от @{data.admin_nick}!"
        if data.perms.get("can_assistant"):
            text = f"@{data.admin_nick} назначил вас помощником главного администратора!"
        await manager.send_to_user(data.target_nick, {"action": "notify", "msg": text})
        cursor.execute("INSERT INTO notifications (to_user, text) VALUES (?, ?)", (data.target_nick, text))
        conn.commit()
        return {"status": "ok", "msg": f"Права для @{data.target_nick} обновлены!"}
    else:
        cursor.execute("UPDATE users SET is_admin=0, admin_perms='{}', admin_notified=1, granted_by=NULL, revoked_by=? WHERE LOWER(nickname)=LOWER(?)",
                       (data.admin_nick, data.target_nick))
        conn.commit()
        await manager.send_to_user(data.target_nick, {"action": "admin_revoked", "revoked_by": data.admin_nick})
        return {"status": "ok", "msg": f"Все права у @{data.target_nick} отозваны!"}

import math

GV_WEAPON_COSTS = {
    1: 2000,   # АВТОМАТ
    2: 3500,   # ДРОБОВИК
    4: 1500,   # НОЖИ
    5: 8000,   # СНАЙПЕРКА
    6: 12000,  # РПГ
}
GV_DEFAULT_WEAPONS = [0, 7]

CLICKER_UPGRADE_CFG = {
    "click":    {"base": 40,    "mult": 1.75},
    "auto":     {"base": 100,   "mult": 1.75},
    "double":   {"base": 350,   "mult": 1.9},
    "combo":    {"base": 800,   "mult": 2.1},
    "crit":     {"base": 2500,  "mult": 2.2},
    "prestige": {"base": 15000, "mult": 2.8},
}
VALID_CLICKER_SKINS = {
    "skin_gold", "skin_diamond", "skin_fire", "skin_shadow", "skin_rainbow", "skin_cosmic"
}


def _canonical_nick(nick: str) -> str:
    n = (nick or "").strip()
    if not n:
        return ""
    cursor.execute("SELECT nickname FROM users WHERE LOWER(nickname)=LOWER(?)", (n,))
    r = cursor.fetchone()
    return r[0] if r and r[0] else n


def _default_clicker_state() -> dict:
    return {
        "score": 0,
        "total": 0,
        "levels": {},
        "prestige": 0,
        "skins": [],
        "selectedSkin": None,
        "chestBase": 2500,
        "chestOpens": 0,
    }


def _sanitize_clicker(raw) -> dict:
    if not isinstance(raw, dict):
        return _default_clicker_state()
    def _int(v, lo, hi, default=0):
        try:
            x = int(float(v))
        except (TypeError, ValueError):
            return default
        return max(lo, min(hi, x))

    lv_in = raw.get("levels") if isinstance(raw.get("levels"), dict) else {}
    levels = {}
    for uid in CLICKER_UPGRADE_CFG:
        if uid == "prestige":
            continue
        val = _int(lv_in.get(uid, 0), 0, 200, 0)
        if val > 0:
            levels[uid] = val

    prestige = max(
        _int(raw.get("prestige", 0), 0, 50, 0),
        _int(lv_in.get("prestige", 0), 0, 50, 0),
    )
    if prestige > 0:
        levels["prestige"] = prestige

    skins = []
    for s in (raw.get("skins") or []):
        if isinstance(s, str) and s in VALID_CLICKER_SKINS and s not in skins:
            skins.append(s)

    sel = raw.get("selectedSkin")
    if not isinstance(sel, str) or sel not in skins:
        sel = None

    return {
        "score": _int(raw.get("score", 0), 0, 10**9, 0),
        "total": _int(raw.get("total", 0), 0, 10**10, 0),
        "levels": levels,
        "prestige": prestige,
        "skins": skins,
        "selectedSkin": sel,
        "chestBase": _int(raw.get("chestBase", 2500), 2500, 10**7, 2500),
        "chestOpens": _int(raw.get("chestOpens", 0), 0, 10**6, 0),
    }


def _sanitize_weapons(raw) -> List[int]:
    out = set(GV_DEFAULT_WEAPONS)
    if isinstance(raw, list):
        for x in raw:
            try:
                idx = int(x)
                if 0 <= idx <= 7:
                    out.add(idx)
            except (TypeError, ValueError):
                pass
    return sorted(out)


def _calc_clicker_reset_reward(clk: dict) -> int:
    score = max(0, int(clk.get("score") or 0))
    levels = clk.get("levels") or {}
    spent = 0
    for uid, cfg in CLICKER_UPGRADE_CFG.items():
        if uid == "prestige":
            continue
        lvl = max(0, min(200, int(levels.get(uid) or 0)))
        for i in range(lvl):
            spent += math.ceil(cfg["base"] * (cfg["mult"] ** i))
    prestige = max(0, int(clk.get("prestige") or levels.get("prestige") or 0))
    raw_pool = score + int(spent * 0.35)
    if raw_pool < 5:
        return 0
    return (raw_pool // 5) + prestige * 50


def _save_economy_row(canon: str, pve: dict, clk: dict, coins: int, weapons: List[int], currencies: dict):
    currencies = dict(currencies or {})
    currencies["v2"] = True
    currencies["coins"] = int(coins)
    currencies["clicks"] = int(clk.get("score") or 0)
    currencies["prestige"] = int(clk.get("prestige") or 0)
    cursor.execute("DELETE FROM profile_game_stats WHERE LOWER(nickname)=LOWER(?) AND nickname<>?", (canon, canon))
    cursor.execute("""
        INSERT INTO profile_game_stats (nickname, pve_json, clicker_json, coins, weapons_json, currencies_json, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(nickname) DO UPDATE SET
          pve_json=excluded.pve_json,
          clicker_json=excluded.clicker_json,
          coins=excluded.coins,
          weapons_json=excluded.weapons_json,
          currencies_json=excluded.currencies_json,
          updated_at=CURRENT_TIMESTAMP
    """, (
        canon,
        json.dumps(pve or {}, ensure_ascii=False),
        json.dumps(clk or _default_clicker_state(), ensure_ascii=False),
        max(0, int(coins)),
        json.dumps(weapons or GV_DEFAULT_WEAPONS),
        json.dumps(currencies, ensure_ascii=False),
    ))
    conn.commit()


def _get_or_init_economy(nick: str) -> dict:
    canon = _canonical_nick(nick)
    cursor.execute("""
        SELECT nickname, pve_json, clicker_json, coins, weapons_json, currencies_json, updated_at
        FROM profile_game_stats WHERE LOWER(nickname)=LOWER(?) ORDER BY updated_at DESC LIMIT 1
    """, (canon,))
    row = cursor.fetchone()

    changed = False
    if row:
        try:
            pve = json.loads(row[1] or "{}")
            if not isinstance(pve, dict):
                pve = {}
        except Exception:
            pve = {}
        try:
            raw_clk = json.loads(row[2] or "{}")
            if not isinstance(raw_clk, dict):
                raw_clk = {}
        except Exception:
            raw_clk = {}
        old_admin = 0
        try:
            old_admin = max(0, int(raw_clk.get("adminCoins") or 0))
        except Exception:
            old_admin = 0
        clk = _sanitize_clicker(raw_clk)
        coins = max(0, int(row[3] or 0))
        try:
            weapons = _sanitize_weapons(json.loads(row[4] or "[0, 7]"))
        except Exception:
            weapons = list(GV_DEFAULT_WEAPONS)
        try:
            currencies = json.loads(row[5] or "{}")
            if not isinstance(currencies, dict):
                currencies = {}
        except Exception:
            currencies = {}
        updated = row[6]
        if row[0] != canon:
            changed = True
        if not currencies.get("v2"):
            # Миграция v2: adminCoins -> coins, при этом прежние набитые монеты не теряются
            if old_admin > 0 and coins < old_admin:
                coins = old_admin
            currencies["v2"] = True
            changed = True
    else:
        pve = {}
        clk = _default_clicker_state()
        coins = 0
        weapons = list(GV_DEFAULT_WEAPONS)
        currencies = {"v2": True}
        updated = now_str()
        changed = True

    # Автоприменение ожидающих грантов из clicker_grants прямо на серверный баланс coins
    cursor.execute("SELECT bonus_coins FROM clicker_grants WHERE LOWER(nickname)=LOWER(?)", (canon,))
    gr = cursor.fetchone()
    if gr and gr[0]:
        bonus = int(gr[0])
        coins = max(0, coins + bonus)
        cursor.execute("UPDATE clicker_grants SET bonus_coins=0, updated_at=CURRENT_TIMESTAMP WHERE LOWER(nickname)=LOWER(?)", (canon,))
        changed = True

    if changed:
        _save_economy_row(canon, pve, clk, coins, weapons, currencies)

    currencies = dict(currencies)
    currencies["coins"] = coins
    currencies["clicks"] = clk["score"]
    currencies["prestige"] = clk["prestige"]

    return {
        "nickname": canon,
        "coins": coins,
        "clicker": clk,
        "weapons": weapons,
        "pve": pve,
        "currencies": currencies,
        "updated_at": updated,
    }


async def _broadcast_economy(nick: str, client_id: str = ""):
    st = _get_or_init_economy(nick)
    payload = {"action": "economy_sync", "economy": st, "client_id": client_id}
    await manager.send_to_user(st["nickname"], payload)
    await _gsend(st["nickname"], payload)


@app.post("/admin/give-coins")
async def admin_give_coins(data: GiveCoinsData):
    """Выдача/забор игровых монет из админ-панели. Требует права can_give_coins."""
    require_admin(data.admin_nick)
    if not (has_perm(data.admin_nick, "can_give_coins")
            or can_manage_everything(data.admin_nick) or is_assistant(data.admin_nick)):
        return {"status": "error", "msg": "Нет права «Выдача/забор монет» — его должен выдать Главный Администратор!"}
    if not data.target_nick:
        return {"status": "error", "msg": "Укажите юзера!"}
    try:
        amount = int(data.amount)
    except Exception:
        return {"status": "error", "msg": "Некорректная сумма!"}
    if amount == 0:
        return {"status": "error", "msg": "Сумма не может быть нулевой!"}
    if amount > 10**9 or amount < -10**9:
        return {"status": "error", "msg": "Сумма слишком большая!"}

    canon = _canonical_nick(data.target_nick)
    st = _get_or_init_economy(canon)
    cur_coins = int(st["coins"])
    if amount < 0 and cur_coins < abs(amount):
        return {"status": "error",
                "msg": f"У @{canon} всего {cur_coins} 💰 — забрать {abs(amount)} нельзя!"}

    new_coins = max(0, cur_coins + amount)
    _save_economy_row(canon, st["pve"], st["clicker"], new_coins, st["weapons"], st["currencies"])

    cursor.execute("SELECT 1 FROM clicker_grants WHERE LOWER(nickname)=LOWER(?)", (canon,))
    if cursor.fetchone():
        cursor.execute("UPDATE clicker_grants SET bonus_coins=0, granted_by=?, updated_at=CURRENT_TIMESTAMP WHERE LOWER(nickname)=LOWER(?)",
                       (data.admin_nick, canon))
    else:
        cursor.execute("INSERT INTO clicker_grants (nickname, bonus_coins, granted_by) VALUES (?,0,?)",
                       (canon, data.admin_nick))
    conn.commit()

    await _broadcast_economy(canon)
    note = (f"⚡ Админ @{data.admin_nick} выдал вам {amount} 💰!" if amount > 0
            else f"💸 Админ @{data.admin_nick} забрал {abs(amount)} 💰.")
    await manager.send_to_user(canon, {"action": "notify", "msg": note})

    if amount > 0:
        return {"status": "ok", "msg": f"@{canon} получает {amount} 💰 (баланс: {new_coins} 💰)."}
    return {"status": "ok", "msg": f"У @{canon} забрано {abs(amount)} 💰 (баланс: {new_coins} 💰)."}


@app.get("/admin/clicker-coins")
def admin_get_clicker_coins(admin: str, target: str):
    cursor.execute("SELECT is_superadmin, is_admin, admin_perms FROM users WHERE LOWER(nickname)=LOWER(?)", (admin,))
    row = cursor.fetchone()
    if not row or (not row[0] and not row[1] and not is_super(admin)):
        raise HTTPException(status_code=403, detail="Доступ запрещён")
    if not (row[0] or is_super(admin) or is_assistant(admin)
            or json.loads(row[2] or '{}').get("can_give_coins")):
        return {"error": "Нет права «Выдача/забор монет»"}
    st = _get_or_init_economy(target)
    cursor.execute("SELECT granted_by FROM clicker_grants WHERE LOWER(nickname)=LOWER(?)", (target,))
    r = cursor.fetchone()
    return {"bonus_coins": st["coins"], "coins": st["coins"], "granted_by": (r[0] if r else "")}


@app.get("/economy/state/{nickname}")
def economy_get_state(nickname: str, token: str = ""):
    _auth_session(nickname, token)
    st = _get_or_init_economy(nickname)
    return {"status": "ok", "economy": st}


@app.post("/economy/migrate")
async def economy_migrate(data: EconomyMigrateData):
    """Однократная миграция старых локальных данных (если серверная запись ещё пустая)."""
    _auth_session(data.nickname, data.session_token)
    st = _get_or_init_economy(data.nickname)
    canon = st["nickname"]
    clk = st["clicker"]
    coins = st["coins"]
    weapons = list(st["weapons"])
    pve = dict(st["pve"])
    changed = False

    if data.weapons and set(weapons) == set(GV_DEFAULT_WEAPONS):
        merged_w = _sanitize_weapons(weapons + list(data.weapons))
        if merged_w != weapons:
            weapons = merged_w
            changed = True

    if data.clicker_json and clk["score"] == 0 and clk["total"] == 0 and not clk["levels"] and not clk["skins"]:
        raw_c = data.clicker_json or {}
        try:
            old_adm = max(0, int(raw_c.get("adminCoins") or 0))
        except Exception:
            old_adm = 0
        if old_adm > 0 and coins == 0:
            coins = old_adm
            changed = True
        san = _sanitize_clicker(raw_c)
        if san["score"] > 0 or san["total"] > 0 or san["levels"] or san["skins"] or san["prestige"] > 0:
            clk = san
            changed = True

    if data.pve_json and not pve and isinstance(data.pve_json, dict):
        pve = data.pve_json
        changed = True

    if changed:
        _save_economy_row(canon, pve, clk, coins, weapons, st["currencies"])
        await _broadcast_economy(canon)
        st = _get_or_init_economy(canon)

    return {"status": "ok", "economy": st}


@app.post("/economy/clicker/save")
async def economy_clicker_save(data: EconomyClickerSaveData):
    """Сохраняет прогресс кликера (клики/уровни/скины) и синхронизирует все устройства."""
    _auth_session(data.nickname, data.session_token)
    st = _get_or_init_economy(data.nickname)
    canon = st["nickname"]
    san = _sanitize_clicker(data.clicker_json)
    _save_economy_row(canon, st["pve"], san, st["coins"], st["weapons"], st["currencies"])
    await _broadcast_economy(canon, client_id=data.client_id or "")
    return {"status": "ok", "economy": _get_or_init_economy(canon)}


@app.post("/economy/clicker/convert")
async def economy_clicker_convert(data: EconomyConvertData):
    """Обнуляет клики и уровни кликера, начисляя монеты на игровой баланс аккаунта."""
    _auth_session(data.nickname, data.session_token)
    st = _get_or_init_economy(data.nickname)
    canon = st["nickname"]
    clk = st["clicker"]

    # Учитываем последние несохранённые клики клиента, если он передал актуальное состояние
    if isinstance(data.clicker_json, dict):
        cli = _sanitize_clicker(data.clicker_json)
        if cli["total"] >= clk["total"] and (cli["score"] - clk["score"]) <= 5000000:
            clk = cli

    reward = _calc_clicker_reset_reward(clk)
    if reward <= 0:
        return {"status": "error", "msg": "Сначала натапайте хотя бы 5 кликов!", "reward": 0}

    new_coins = int(st["coins"]) + reward
    new_clk = {
        "score": 0,
        "total": 0,
        "levels": ({"prestige": clk["prestige"]} if clk["prestige"] > 0 else {}),
        "prestige": clk["prestige"],
        "skins": clk["skins"],
        "selectedSkin": clk["selectedSkin"],
        "chestBase": clk["chestBase"],
        "chestOpens": 0,
    }
    _save_economy_row(canon, st["pve"], new_clk, new_coins, st["weapons"], st["currencies"])
    await _broadcast_economy(canon)
    return {"status": "ok", "reward": reward, "coins": new_coins, "economy": _get_or_init_economy(canon)}


@app.post("/economy/weapon/buy")
async def economy_weapon_buy(data: EconomyWeaponBuyData):
    """Покупка оружия Green Valley за серверный баланс монет."""
    _auth_session(data.nickname, data.session_token)
    st = _get_or_init_economy(data.nickname)
    canon = st["nickname"]
    idx = int(data.weapon_idx)
    if idx in st["weapons"]:
        return {"status": "ok", "already": True, "economy": st}
    if idx not in GV_WEAPON_COSTS:
        return {"status": "error", "msg": "Неизвестное оружие"}
    cost = GV_WEAPON_COSTS[idx]
    if st["coins"] < cost:
        return {"status": "error", "msg": f"Недостаточно монет (нужно {cost} 💰)", "economy": st}
    new_coins = st["coins"] - cost
    new_weapons = sorted(set(st["weapons"]) | {idx})
    _save_economy_row(canon, st["pve"], st["clicker"], new_coins, new_weapons, st["currencies"])
    await _broadcast_economy(canon)
    return {"status": "ok", "weapon_idx": idx, "coins": new_coins, "economy": _get_or_init_economy(canon)}


@app.post("/economy/pve/record")
async def economy_pve_record(data: EconomyPveRecordData):
    """Атомарно записывает итог PvE-матча на сервер."""
    _auth_session(data.nickname, data.session_token)
    key = (data.game_key or "").strip()
    if key not in ("greenu_match", "utopia_duel"):
        return {"status": "error", "msg": "Неизвестный режим"}
    st = _get_or_init_economy(data.nickname)
    canon = st["nickname"]
    pve = dict(st["pve"] or {})
    cur = pve.get(key)
    if not isinstance(cur, dict):
        cur = {"wins": 0, "losses": 0, "matches": 0}
    cur["matches"] = int(cur.get("matches") or 0) + 1
    if data.won:
        cur["wins"] = int(cur.get("wins") or 0) + 1
    else:
        cur["losses"] = int(cur.get("losses") or 0) + 1
    pve[key] = cur
    _save_economy_row(canon, pve, st["clicker"], st["coins"], st["weapons"], st["currencies"])
    await _broadcast_economy(canon)
    return {"status": "ok", "economy": _get_or_init_economy(canon)}


@app.post("/clicker/sync")
async def clicker_sync(data: ClickerSyncData):
    """Обратная совместимость: возвращает текущий серверный баланс монет (бонусы применяются сразу)."""
    _auth_session(data.nickname, data.session_token)
    st = _get_or_init_economy(data.nickname)
    return {"status": "ok", "bonus": 0, "coins": st["coins"], "economy": st}


@app.post("/profile/gamestats/sync")
async def profile_gamestats_sync(data: ProfileGameStatsData):
    """Обратная совместимость: НЕ затирает сервер пустыми локальными данными и НЕ трогает coins."""
    _auth_session(data.nickname, data.session_token)
    st = _get_or_init_economy(data.nickname)
    canon = st["nickname"]
    return {"status": "ok", "coins": st["coins"], "economy": st}


@app.get("/profile/gamestats/{nickname}")
def profile_gamestats_get(nickname: str):
    """Публичная игровая статистика для вкладки профиля (видят все, редактировать нельзя)."""
    st = _get_or_init_economy(nickname)
    online = _stats_payload(st["nickname"])
    return {
        "status": "ok",
        "nickname": st["nickname"],
        "pve": st["pve"],
        "clicker": st["clicker"],
        "coins": st["coins"],
        "weapons": st["weapons"],
        "currencies": st["currencies"],
        "online": online,
        "updated_at": st["updated_at"],
    }

# ═══════════════════════════════════════════════════════════
#  ПОДДЕРЖКА (SUPPORT)
# ═══════════════════════════════════════════════════════════

def _is_support_staff(nick: str) -> bool:
    if is_super(nick) or is_assistant(nick): return True
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
    nick = (data.from_user or "").strip()
    if not nick:
        return {"status": "error", "msg": "Укажите никнейм — на него придёт ответ!"}
    if len(nick) > 32:
        return {"status": "error", "msg": "Слишком длинный никнейм!"}
    cursor.execute("INSERT INTO support_tickets (from_user, subject, message) VALUES (?, ?, ?)",
                   (nick, data.subject.strip(), data.message.strip()))
    ticket_id = cursor.lastrowid
    conn.commit()
    await manager.notify_support_staff({
        "action": "new_support_ticket", "ticket_id": ticket_id,
        "from_user": nick, "subject": data.subject
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
    if not _is_support_staff(data.admin_nick):
        return {"status": "error", "msg": "Нет прав саппорта!"}
    cursor.execute("UPDATE support_tickets SET status='closed' WHERE id=?", (data.ticket_id,))
    conn.commit()
    return {"status": "ok", "msg": "Обращение закрыто."}

@app.post("/support/request-perm")
async def request_support_perm(data: SupportPermRequestData):
    today = datetime.utcnow().date().isoformat()
    cursor.execute("SELECT 1 FROM support_perm_requests WHERE LOWER(requested_by)=LOWER(?) AND DATE(created_at)=? AND status='pending'",
                   (data.nickname, today))
    if cursor.fetchone(): return {"status": "error", "msg": "Заявка уже подана сегодня! Лимит — 1 раз в сутки."}

    cursor.execute("INSERT INTO support_perm_requests (requested_by) VALUES (?)", (data.nickname,))
    conn.commit()
    await _notify_admins_about_request(f"🙋 @{data.nickname} просит право «Помощник поддержки».")
    return {"status": "ok", "msg": "Запрос отправлен Главному Администратору!"}

@app.get("/support/perm-requests")
def get_support_perm_requests(admin: str):
    if not (is_super(admin) or is_assistant(admin)):
        raise HTTPException(status_code=403, detail="Только Главный Администратор")
    cursor.execute("SELECT id, requested_by, status, created_at FROM support_perm_requests WHERE status='pending' ORDER BY id DESC")
    return [{"id": r[0], "requested_by": r[1], "status": r[2], "created_at": r[3]} for r in cursor.fetchall()]

@app.post("/support/perm-decision")
async def decide_support_perm(data: SupportPermDecision):
    if not (is_super(data.admin_nick) or is_assistant(data.admin_nick)):
        return {"status": "error", "msg": "Только Главный Администратор или его помощник!"}
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
        # Живо: клиент сразу подхватывает право без перезагрузки
        await manager.send_to_user(nick, {"action": "admin_granted", "granted_by": data.admin_nick,
                                          "perms": perms, "assistant": False})
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


# ══════════════════════════════════════════════════════════════
#  ИГРОВОЙ ЗАЛ: поиск соперника, три игры, статистика и лидеры
# ══════════════════════════════════════════════════════════════
GAMES = {
    "rps": "✊ Камень-ножницы-бумага",
    "ttt": "❌ Крестики-нолики",
    "sea": "🚢 Морской бой",
    "utopia": "⚔️ Утопия (2D арена)",
    "greenu": "🌿 Green Valley (3D)",
}
RPS_TARGET = 3  # до скольких побед идёт матч

_queues: Dict[str, List[str]] = {g: [] for g in GAMES}
_queue_game: Dict[str, str] = {}       # ник -> игра, в которой он ищет соперника
_matches: Dict[int, dict] = {}         # match_id -> состояние матча
_match_seq = 0
_game_sockets: Dict[str, WebSocket] = {}


def _gsock_find(nick: str) -> Optional[str]:
    if not nick:
        return None
    if nick in _game_sockets:
        return nick
    low = nick.lower()
    for k in list(_game_sockets.keys()):
        if k.lower() == low:
            return k
    return None


async def _gsend(nick: str, payload: dict):
    key = _gsock_find(nick)
    ws = _game_sockets.get(key) if key else None
    if not ws:
        return
    try:
        await ws.send_json(payload)
    except Exception:
        pass


def _lobby_state() -> dict:
    playing = {g: 0 for g in GAMES}
    for m in _matches.values():
        if not m.get("over") and m.get("game") in playing:
            playing[m["game"]] += 1
    return {
        "action": "lobby_state",
        "online": len(manager.online_users()),
        "queue": {g: len(_queues[g]) for g in GAMES},
        "playing": playing,
    }


async def _lobby_broadcast():
    state = _lobby_state()
    for nick in list(_game_sockets):
        await _gsend(nick, state)


def _sea_place() -> List[List[int]]:
    """Случайная расстановка 10 кораблей (4/3/3/2/2/2/1/1/1/1) на поле 10×10.
    Корабли НЕ соприкасаются — даже по диагонали (классические правила)."""
    while True:
        taken = set()
        ships: List[List[int]] = []
        ok = True
        for size in (4, 3, 3, 2, 2, 2, 1, 1, 1, 1):
            placed = False
            for _try in range(800):
                horiz = random.random() < 0.5
                r, c = random.randrange(10), random.randrange(10)
                cells = []
                for i in range(size):
                    rr = r if horiz else r + i
                    cc = c + i if horiz else c
                    if rr > 9 or cc > 9:
                        cells = []
                        break
                    cells.append(rr * 10 + cc)
                if len(cells) != size:
                    continue
                # Окрестность корабля (включая диагонали) должна быть пуста
                bad = False
                for cell in cells:
                    cr, cc2 = divmod(cell, 10)
                    for dr in (-1, 0, 1):
                        for dc in (-1, 0, 1):
                            rr, rc = cr + dr, cc2 + dc
                            if 0 <= rr <= 9 and 0 <= rc <= 9 and (rr * 10 + rc) in taken:
                                bad = True
                                break
                        if bad:
                            break
                    if bad:
                        break
                if bad:
                    continue
                taken.update(cells)
                ships.append(cells)
                placed = True
                break
            if not placed:
                ok = False
                break
        if ok and len(ships) == 10:
            return ships


def _sea_validate(ships_raw) -> List[List[int]]:
    """Проверка ручной расстановки: 10 кораблей 4/3/3/2/2/2/1/1/1/1,
    прямые линии, в границах, без соприкосновений (вкл. диагонали)."""
    try:
        ships = [[int(c) for c in cells] for cells in ships_raw]
    except (TypeError, ValueError):
        raise ValueError("Неверный формат расстановки")
    if sorted(len(c) for c in ships) != [1, 1, 1, 1, 2, 2, 2, 3, 3, 4]:
        raise ValueError("Нужно 10 кораблей: 4, 3, 3, 2, 2, 2 и четыре одиночных")
    taken = set()
    for cells in ships:
        if len(set(cells)) != len(cells):
            raise ValueError("Клетки повторяются")
        if any(c < 0 or c > 99 for c in cells):
            raise ValueError("Корабль за пределами поля")
        rows = {c // 10 for c in cells}
        cols = {c % 10 for c in cells}
        if not (len(rows) == 1 or len(cols) == 1):
            raise ValueError("Корабль должен стоять по прямой")
        # непрерывность линии
        if len(rows) == 1:
            r = next(iter(rows))
            expect = list(range(r * 10 + min(cols), r * 10 + min(cols) + len(cells)))
            if sorted(cells) != expect:
                raise ValueError("Клетки корабля должны идти подряд")
        else:
            c0 = next(iter(cols))
            expect = list(range(min(rows) * 10 + c0, (min(rows) + len(cells)) * 10 + c0, 10))
            if sorted(cells) != expect:
                raise ValueError("Клетки корабля должны идти подряд")
        # соприкосновение с уже поставленными (вкл. диагонали)
        for cell in cells:
            cr, cc = divmod(cell, 10)
            for dr in (-1, 0, 1):
                for dc in (-1, 0, 1):
                    rr, rc = cr + dr, cc + dc
                    if 0 <= rr <= 9 and 0 <= rc <= 9 and (rr * 10 + rc) in taken:
                        raise ValueError("Корабли не должны соприкасаться (в том числе по диагонали)")
        taken.update(cells)
    return ships


def _init_match(game: str, p1: str, p2: str) -> dict:
    state: dict = {}
    if game == "rps":
        state = {"scores": {p1: 0, p2: 0}, "choices": {}, "round": 1, "log": []}
    elif game == "ttt":
        state = {"board": [""] * 9, "marks": {p1: "X", p2: "O"}, "turn": p1}
    elif game == "sea":
        state = {
            "phase": "placing",              # placing -> battle
            "ships": {p1: None, p2: None},   # каждый ставит сам
            "ready": {p1: False, p2: False},
            "shots": {p1: {}, p2: {}},       # клетка -> 'hit'/'miss'
            "turn": p1,
            "last": None,                    # последний выстрел: {"cell","hit","by"}
        }
    elif game in ("utopia", "greenu"):
        # реалтайм-арена: сервер релеит стейт, цвета назначаются случайно
        import random as _rnd
        colors = _rnd.choice([("red", "blue"), ("blue", "red")])
        state = {
            "phase": "fighting",
            "hp": {p1: 100, p2: 100},
            "pos": {},
            "colors": {p1: colors[0], p2: colors[1]},
            "last_state": {},
        }
    return {"id": None, "game": game, "p1": p1, "p2": p2, "over": False, "state": state}


def _public_state(m: dict, nick: str) -> dict:
    """Приватное состояние матча для конкретного игрока (без чужих секретов)."""
    game, st, opp = m["game"], m["state"], (m["p2"] if nick == m["p1"] else m["p1"])
    base = {"action": "game_state", "id": m["id"], "game": game,
            "you": nick, "opp": opp, "over": m["over"]}
    if game in ("utopia", "greenu"):
        colors = st.get("colors", {})
        base.update({"my_color": colors.get(nick, "red"), "opp_color": colors.get(opp, "blue")})
    if game == "rps":
        base.update({
            "scores": st["scores"], "round": st["round"], "log": st["log"],
            "my_choice": st["choices"].get(nick, ""),
            "opp_sent": opp in st["choices"],
        })
    elif game == "ttt":
        base.update({"board": st["board"], "my_mark": st["marks"][nick],
                     "opp_mark": st["marks"][opp], "turn": st["turn"]})
    elif game == "sea":
        phase = st.get("phase", "battle")
        my_shots = st["shots"][nick]           # мои выстрелы по сопернику
        foe_shots = st["shots"][opp]           # выстрелы соперника по мне
        my_ship_cells = [c for cells in (st["ships"].get(nick) or []) for c in cells]
        foe_ships = st["ships"].get(opp) or []
        base.update({
            "phase": phase,
            "my_ready": bool(st["ready"][nick]),
            "foe_ready": bool(st["ready"][opp]),
            "last": st.get("last"),
            "turn": st["turn"], "my_ships": my_ship_cells,
            "my_field": foe_shots, "foe_field": my_shots,
            "my_sunk": [c for cells in (st["ships"].get(nick) or [])
                        if all(foe_shots.get(str(c)) == "hit" for c in cells) for c in cells],
            "foe_sunk": [c for cells in foe_ships
                         if all(my_shots.get(str(c)) == "hit" for c in cells) for c in cells],
            "foe_ship_count": len(foe_ships),
        })
    return base


async def _send_state(m: dict):
    for nick in (m["p1"], m["p2"]):
        await _gsend(nick, _public_state(m, nick))


def _stats_payload(nick: str) -> dict:
    cursor.execute("SELECT game, wins, losses, draws, matches FROM game_stats WHERE LOWER(nickname)=LOWER(?)",
                   (nick,))
    return {r[0]: {"wins": r[1], "losses": r[2], "draws": r[3], "matches": r[4]}
            for r in cursor.fetchall()}


async def _finish_match(m: dict, winner: Optional[str], reason: str = ""):
    if m.get("over"):
        return
    m["over"] = True
    p1, p2, game = m["p1"], m["p2"], m["game"]
    score = ""
    if game == "rps":
        score = f"{m['state']['scores'][p1]}:{m['state']['scores'][p2]}"
    if winner == p1:
        pairs = ((p1, 1, 0, 0), (p2, 0, 1, 0))
    elif winner == p2:
        pairs = ((p1, 0, 1, 0), (p2, 1, 0, 0))
    else:  # ничья
        pairs = ((p1, 0, 0, 1), (p2, 0, 0, 1))
    for nick, w, l, d in pairs:
        cursor.execute("""
            INSERT INTO game_stats (nickname, game, wins, losses, draws, matches)
            VALUES (?, ?, ?, ?, ?, 1)
            ON CONFLICT(nickname, game) DO UPDATE SET
              wins = wins + excluded.wins,
              losses = losses + excluded.losses,
              draws = draws + excluded.draws,
              matches = matches + 1
        """, (nick, game, w, l, d))
    cursor.execute("INSERT INTO game_matches (game, player1, player2, winner, score) VALUES (?,?,?,?,?)",
                   (game, p1, p2, winner or "", score))
    conn.commit()

    over = {"action": "game_over", "id": m["id"], "game": game,
            "winner": winner, "you": None, "reason": reason, "score": score,
            "stats": {p1: _stats_payload(p1), p2: _stats_payload(p2)}}
    for nick in (p1, p2):
        final_st = _public_state(m, nick)
        if game == "sea" and m.get("state", {}).get("ships"):
            opp_n = p2 if nick == p1 else p1
            final_st["foe_ships"] = [c for cells in (m["state"]["ships"].get(opp_n) or []) for c in cells]
        payload = dict(over, you=nick, you_won=(winner == nick),
                       opp=(p2 if nick == p1 else p1), final_state=final_st)
        await _gsend(nick, payload)
    _matches.pop(m["id"], None)
    await _lobby_broadcast()


async def _create_match(game: str, a: str, b: str):
    global _match_seq
    _match_seq += 1
    m = _init_match(game, a, b)
    m["id"] = _match_seq
    m["created_at"] = time.time()  # grace-период: forfeit не засчитывается первые 15 сек
    _matches[m["id"]] = m
    for nick in (a, b):
        colors = m["state"].get("colors", {})
        await _gsend(nick, {"action": "game_found", "id": m["id"], "game": game,
                            "game_title": GAMES[game], "opp": (b if nick == a else a),
                            "my_color": colors.get(nick, "red"), "opp_color": colors.get(b if nick == a else a, "blue")})
    await _send_state(m)
    await _lobby_broadcast()


# ═══════════════════════════════════════════════════════════════
#  ПРИВАТНЫЕ ЛОББИ (комнаты) — создание, инвайты, команды, боты
# ═══════════════════════════════════════════════════════════════
_rooms: Dict[str, dict] = {}   # room_id -> лобби


def _user_avatar(nick: str) -> str:
    cursor.execute("SELECT avatar_url FROM users WHERE nickname=?", (nick,))
    row = cursor.fetchone()
    return (row[0] or "") if row else ""


def _room_payload(room: dict) -> dict:
    return {
        "action": "room_state",
        "id": room["id"],
        "game": room["game"],
        "host": room["host"],
        "members": [
            {"nick": m["nick"], "team": m["team"],
             "is_host": m["nick"] == room["host"], "avatar_url": m.get("avatar_url", "")}
            for m in room["members"]
        ],
        "bots": dict(room.get("bots", {"blue": 0, "red": 0})),
    }


async def _room_broadcast(room: dict):
    payload = _room_payload(room)
    for m in room["members"]:
        await _gsend(m["nick"], dict(payload, you=m["nick"]))


def _room_of(nick: str):
    for room in _rooms.values():
        if any(m["nick"] == nick for m in room["members"]):
            return room
    return None


async def _room_member_left(nick: str):
    """Выход/отключение игрока из лобби (админка переходит первому)."""
    room = _room_of(nick)
    if not room:
        return
    room["members"] = [m for m in room["members"] if m["nick"] != nick]
    if not room["members"]:
        _rooms.pop(room["id"], None)
        return
    if room["host"] == nick:
        room["host"] = room["members"][0]["nick"]
    await _room_broadcast(room)


async def _finish_co_op(m: dict, won: bool):
    """Итог co-op лобби-матча: оба игрока получают одинаковый результат."""
    if m.get("over"):
        return
    m["over"] = True
    p1, p2, game = m["p1"], m["p2"], m["game"]
    for nick, w, l in ((p1, 1 if won else 0, 0 if won else 1), (p2, 1 if won else 0, 0 if won else 1)):
        cursor.execute("""
            INSERT INTO game_stats (nickname, game, wins, losses, draws, matches)
            VALUES (?, ?, ?, ?, 0, 1)
            ON CONFLICT(nickname, game) DO UPDATE SET
              wins = wins + excluded.wins,
              losses = losses + excluded.losses,
              matches = matches + 1
        """, (nick, game, w, l))
    cursor.execute("INSERT INTO game_matches (game, player1, player2, winner, score) VALUES (?,?,?,?,?)",
                   (game, p1, p2, (p1 if won else ""), "coop"))
    conn.commit()
    for nick in (p1, p2):
        await _gsend(nick, {"action": "game_over", "id": m["id"], "game": game,
                            "winner": (p1 if won else None), "you": nick, "you_won": won,
                            "reason": "co_op", "score": "coop",
                            "stats": {nick: _stats_payload(nick)}})
    _matches.pop(m["id"], None)
    await _lobby_broadcast()


async def _drop_queue(nick: str):
    g = _queue_game.pop(nick, None)
    if g and nick in _queues[g]:
        _queues[g].remove(nick)
        await _gsend(nick, {"action": "unqueued"})


async def _handle_queue(nick: str, game: str):
    if game not in GAMES:
        await _gsend(nick, {"action": "error", "msg": "Неизвестная игра"})
        return
    if any(mid for mid, mm in _matches.items() if not mm["over"] and nick in (mm["p1"], mm["p2"])):
        await _gsend(nick, {"action": "error", "msg": "Вы уже в игре — доиграйте матч!"})
        return
    if _queue_game.get(nick) == game:
        await _gsend(nick, {"action": "queued", "game": game, "title": GAMES[game]})
        return
    await _drop_queue(nick)
    rival = next((n for n in _queues[game] if n.lower() != nick.lower() and _gsock_find(n)), None)
    if rival is None:
        _queues[game].append(nick)
        _queue_game[nick] = game
        await _gsend(nick, {"action": "queued", "game": game, "title": GAMES[game]})
    else:
        _queues[game].remove(rival)
        _queue_game.pop(rival, None)
        await _create_match(game, rival, nick)
    await _lobby_broadcast()


_TTT_LINES = [(0, 1, 2), (3, 4, 5), (6, 7, 8), (0, 3, 6), (1, 4, 7), (2, 5, 8), (0, 4, 8), (2, 4, 6)]


async def _handle_place_ships(nick: str, mid, data: dict):
    """Морской бой: игрок присылает свою расстановку (или просит авто)."""
    try:
        mid = int(mid)
    except (TypeError, ValueError):
        return
    m = _matches.get(mid)
    if not m or m["over"]:
        await _gsend(nick, {"action": "error", "msg": "Матч уже закончен или не найден"})
        return
    if nick not in (m["p1"], m["p2"]):
        return
    if m["game"] != "sea":
        return
    st = m["state"]
    if st.get("phase") != "placing":
        await _gsend(nick, {"action": "error", "msg": "Расстановка уже завершена"})
        return
    if data.get("auto"):
        ships = _sea_place()
    else:
        try:
            ships = _sea_validate(data.get("ships"))
        except ValueError as e:
            await _gsend(nick, {"action": "error", "msg": f"Расстановка не подходит: {e}"})
            return
    st["ships"][nick] = ships
    st["ready"][nick] = True
    if all(st["ready"].values()):
        st["phase"] = "battle"
        st["turn"] = m["p1"]
    await _send_state(m)


async def _handle_move(nick: str, mid, data: dict):
    try:
        mid = int(mid)
    except (TypeError, ValueError):
        return
    m = _matches.get(mid)
    if not m or m["over"]:
        await _gsend(nick, {"action": "error", "msg": "Матч уже закончен или не найден"})
        return
    if nick not in (m["p1"], m["p2"]):
        return
    game, st, opp = m["game"], m["state"], (m["p2"] if nick == m["p1"] else m["p1"])

    if game == "rps":
        choice = (data.get("choice") or "").lower()
        if choice not in ("rock", "paper", "scissors"):
            await _gsend(nick, {"action": "error", "msg": "Выберите камень, ножницы или бумагу"})
            return
        if st["choices"].get(nick):
            return
        st["choices"][nick] = choice
        if len(st["choices"]) < 2:
            await _send_state(m)
            return
        a, b = st["choices"][m["p1"]], st["choices"][m["p2"]]
        if a == b:
            res = 0
        else:
            beats = {"rock": "scissors", "scissors": "paper", "paper": "rock"}
            res = 1 if beats[a] == b else 2
        if res:
            st["scores"][m["p1"] if res == 1 else m["p2"]] += 1
        st["log"].append({"round": st["round"], "a": a, "b": b,
                          "winner": ("" if res == 0 else (m["p1"] if res == 1 else m["p2"]))})
        st["round"] += 1
        st["choices"] = {}
        s1, s2 = st["scores"][m["p1"]], st["scores"][m["p2"]]
        if s1 >= RPS_TARGET or s2 >= RPS_TARGET:
            await _send_state(m)
            await _finish_match(m, m["p1"] if s1 > s2 else m["p2"])
            return
        await _send_state(m)
        return

    if game == "ttt":
        if st["turn"] != nick:
            await _gsend(nick, {"action": "error", "msg": "Сейчас ход соперника"})
            return
        try:
            cell = int(data.get("cell"))
        except (TypeError, ValueError):
            return
        if not 0 <= cell <= 8 or st["board"][cell]:
            await _gsend(nick, {"action": "error", "msg": "Клетка занята"})
            return
        st["board"][cell] = st["marks"][nick]
        win_line = next((ln for ln in _TTT_LINES
                         if all(st["board"][i] == st["marks"][nick] for i in ln)), None)
        if win_line:
            await _send_state(m)
            await _finish_match(m, nick)
            return
        if all(st["board"]):
            await _send_state(m)
            await _finish_match(m, None)
            return
        st["turn"] = opp
        await _send_state(m)
        return

    if game == "sea":
        if st.get("phase") != "battle":
            await _gsend(nick, {"action": "error", "msg": "Соперник ещё расставляет корабли"})
            return
        if st["turn"] != nick:
            await _gsend(nick, {"action": "error", "msg": "Сейчас ход соперника"})
            return
        try:
            cell = int(data.get("cell"))
        except (TypeError, ValueError):
            return
        if not 0 <= cell <= 99:
            return
        shots = st["shots"][nick]
        if str(cell) in shots:
            await _gsend(nick, {"action": "error", "msg": "В эту клетку уже стреляли"})
            return
        hit = any(cell in cells for cells in st["ships"][opp])
        shots[str(cell)] = "hit" if hit else "miss"
        st["last"] = {"cell": cell, "hit": bool(hit), "by": nick}
        all_cells = [c for cells in st["ships"][opp] for c in cells]
        if all(str(c) in shots for c in all_cells):
            await _send_state(m)
            await _finish_match(m, nick)
            return
        # Классические правила: попал — ходишь ещё, промах — ход соперника
        if not hit:
            st["turn"] = opp
        await _send_state(m)


@app.get("/games/lobby")
def games_lobby():
    """Счётчики зала: онлайн, кто ищет, кто играет, сколько матчей сыграно."""
    totals = {g: 0 for g in GAMES}
    for (g, n) in cursor.execute(
            "SELECT game, COUNT(*) FROM game_matches GROUP BY game").fetchall():
        if g in totals:
            totals[g] = n
    players = {g: 0 for g in GAMES}
    for (g, n) in cursor.execute(
            "SELECT game, COUNT(DISTINCT nickname) FROM game_stats GROUP BY game").fetchall():
        if g in players:
            players[g] = n
    return {
        "online": len(manager.online_users()),
        "queue": {g: len(_queues[g]) for g in GAMES},
        "playing": {g: sum(1 for m in _matches.values()
                           if m["game"] == g and not m["over"]) for g in GAMES},
        "totals": totals,
        "players": players,
        "games": [{"id": g, "title": t} for g, t in GAMES.items()],
    }


@app.get("/games/leaderboard")
def games_leaderboard(game: str = "all", limit: int = 10):
    """Таблица лидеров: сначала по числу сыгранных 1х1 матчей, потом по победам.
    Удалённые аккаунты (нет в users) и активно забаненные в топ не попадают."""
    ns = now_str()
    where = """
        WHERE EXISTS (SELECT 1 FROM users u WHERE LOWER(u.nickname)=LOWER(gs.nickname))
          AND NOT EXISTS (SELECT 1 FROM bans b WHERE LOWER(b.target_nick)=LOWER(gs.nickname)
                          AND (b.ban_type='permanent' OR b.expires_at>?))
    """
    if game in GAMES:
        rows = cursor.execute(f"""
            SELECT gs.nickname, gs.wins, gs.losses, gs.draws, gs.matches
            FROM game_stats gs {where} AND gs.game=?
            ORDER BY gs.matches DESC, gs.wins DESC LIMIT ?
        """, (ns, game, limit)).fetchall()
    else:
        rows = cursor.execute(f"""
            SELECT gs.nickname, SUM(gs.wins), SUM(gs.losses), SUM(gs.draws), SUM(gs.matches)
            FROM game_stats gs {where} GROUP BY LOWER(gs.nickname)
            ORDER BY SUM(gs.matches) DESC, SUM(gs.wins) DESC LIMIT ?
        """, (ns, limit)).fetchall()
    return [{"rank": i + 1, "nickname": r[0], "wins": r[1] or 0, "losses": r[2] or 0,
             "draws": r[3] or 0, "matches": r[4] or 0}
            for i, r in enumerate(rows)]


@app.get("/games/stats/{nickname}")
def games_stats(nickname: str):
    return {"status": "ok", "nickname": nickname, "stats": _stats_payload(nickname)}


@app.websocket("/ws/games/{nickname}")
async def games_websocket(websocket: WebSocket, nickname: str):
    """Игровой сокет: очередь, ходы, счётчики зала. Требует живую сессию."""
    global _match_seq
    token = (websocket.query_params.get("token") or "").strip()
    try:
        _auth_session(nickname, token)
    except Exception:
        await websocket.accept()
        await websocket.close(code=4401)
        return
    # Приводим ник к каноническому из БД (чтобы Jjlop55 и jjlop55 не разъезжались)
    cursor.execute("SELECT nickname FROM users WHERE LOWER(nickname)=LOWER(?)", (nickname,))
    crow = cursor.fetchone()
    if crow and crow[0]:
        nickname = crow[0]
    await websocket.accept()
    old = _game_sockets.get(nickname)
    if old and old is not websocket:
        try:
            await old.close(code=4402)
        except Exception:
            pass
    _game_sockets[nickname] = websocket
    await websocket.send_json(_lobby_state())
    await websocket.send_json({"action": "my_stats", "stats": _stats_payload(nickname)})
    try:
        await websocket.send_json({"action": "economy_sync", "economy": _get_or_init_economy(nickname)})
    except Exception:
        pass
    await _lobby_broadcast()
    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")
            if action == "queue":
                await _handle_queue(nickname, (data.get("game") or "").strip())
            elif action == "unqueue":
                await _drop_queue(nickname)
                await _lobby_broadcast()
            elif action == "move":
                await _handle_move(nickname, data.get("match_id"), data.get("data") or {})
            elif action == "place_ships":
                await _handle_place_ships(nickname, data.get("match_id"), data)
            elif action == "utopia_state":
                # Утопия / Green Valley: клиент шлёт свой стейт + выстрелы/урон, сервер релеит сопернику
                try:
                    mid = int(data.get("match_id"))
                except (TypeError, ValueError):
                    continue
                m = _matches.get(mid)
                if not m or m["over"] or m["game"] not in ("utopia", "greenu") or nickname not in (m["p1"], m["p2"]):
                    continue
                opp = m["p2"] if nickname == m["p1"] else m["p1"]
                payload = data.get("state") or {}
                m["state"].setdefault("last_state", {})[nickname] = payload
                m["state"].setdefault("hp", {m["p1"]: 100, m["p2"]: 100})
                m["state"].setdefault("pos", {})
                # сохраняем hp/alive и pos для статистики
                if "hp" in payload:
                    try:
                        m["state"]["hp"][nickname] = max(0, min(100, float(payload["hp"])))
                    except (TypeError, ValueError):
                        pass
                if "alive" in payload:
                    if not payload["alive"]:
                        m["state"]["hp"][nickname] = 0
                    elif "hp" not in payload:
                        m["state"]["hp"][nickname] = 100
                if "pos" in payload:
                    m["state"]["pos"][nickname] = payload["pos"]
                # если кто-то умер — матч окончен (в co-op — только когда пали ОБА)
                hp = m["state"]["hp"]
                if m["state"].get("co_op"):
                    if hp.get(m["p1"], 100) <= 0 and hp.get(m["p2"], 100) <= 0:
                        await _finish_co_op(m, False)
                        continue
                else:
                    if hp.get(m["p1"], 100) <= 0 or hp.get(m["p2"], 100) <= 0:
                        winner = m["p1"] if hp.get(m["p2"], 100) <= 0 else m["p2"]
                        await _finish_match(m, winner, "kill")
                        continue
                # релеим сопернику (включая выстрелы, hit_dmg, открытие дверей и т.д.)
                await _gsend(opp, {"action": "utopia_state", "id": mid, "from": nickname, "state": payload})
            elif action == "forfeit":
                try:
                    mid = int(data.get("match_id"))
                except (TypeError, ValueError):
                    continue
                m = _matches.get(mid)
                if m and not m["over"] and nickname in (m["p1"], m["p2"]):
                    opp = m["p2"] if nickname == m["p1"] else m["p1"]
                    await _finish_match(m, opp, "forfeit")
            elif action == "lobby":
                await websocket.send_json(_lobby_state())
            elif action == "room_create":
                # создать приватное лобби (сам — в красную команду, боты по умолчанию)
                game = (data.get("game") or "").strip()
                if game not in GAMES:
                    game = "greenu"
                existing = _room_of(nickname)
                if existing:
                    await _room_broadcast(existing)
                    continue
                import random as _rnd_room
                rid = f"{_rnd_room.randint(100, 999)}-{_rnd_room.randint(10, 99)}"
                while rid in _rooms:
                    rid = f"{_rnd_room.randint(100, 999)}-{_rnd_room.randint(10, 99)}"
                _rooms[rid] = {
                    "id": rid, "game": game, "host": nickname,
                    "members": [{"nick": nickname, "team": "red", "avatar_url": _user_avatar(nickname)}],
                    "bots": {"blue": 2, "red": 1},
                }
                await _room_broadcast(_rooms[rid])
            elif action == "room_invite":
                room = _room_of(nickname)
                target = (data.get("target") or "").strip()
                if not room or not target:
                    continue
                if nickname != room["host"]:
                    await _gsend(nickname, {"action": "error", "msg": "Приглашать может только создатель лобби"})
                    continue
                if target.lower() == nickname.lower():
                    continue
                target_online = _gsock_find(target)
                if not target_online:
                    await _gsend(nickname, {"action": "error", "msg": f"@{target} не в сети"})
                    continue
                if any(m["nick"].lower() == target_online.lower() for m in room["members"]):
                    await _gsend(nickname, {"action": "error", "msg": f"@{target_online} уже в лобби"})
                    continue
                if len(room["members"]) >= 2:
                    await _gsend(nickname, {"action": "error", "msg": "Лобби заполнено (макс. 2 игрока)"})
                    continue
                await _gsend(target_online, {"action": "room_invite", "from": nickname,
                                       "room_id": room["id"],
                                       "game_title": GAMES.get(room["game"], room["game"])})
                await _gsend(nickname, {"action": "room_invited", "to": target_online})
            elif action == "room_join":
                rid = (data.get("room_id") or "").strip()
                room = _rooms.get(rid)
                if not room:
                    await _gsend(nickname, {"action": "error", "msg": "Лобби не найдено"})
                    continue
                if any(m["nick"] == nickname for m in room["members"]):
                    await _room_broadcast(room)
                    continue
                if len(room["members"]) >= 2:
                    await _gsend(nickname, {"action": "error", "msg": "Лобби заполнено (макс. 2 игрока)"})
                    continue
                blue_n = sum(1 for m in room["members"] if m["team"] == "blue")
                red_n = len(room["members"]) - blue_n
                team = "blue" if blue_n <= red_n else "red"
                room["members"].append({"nick": nickname, "team": team, "avatar_url": _user_avatar(nickname)})
                await _room_broadcast(room)
            elif action == "room_leave":
                await _room_member_left(nickname)
            elif action == "room_swap":
                # создатель перекидывает игрока в другую команду
                room = _room_of(nickname)
                target = (data.get("target") or "").strip()
                if not room or room["host"].lower() != nickname.lower() or not target:
                    continue
                for m in room["members"]:
                    if m["nick"].lower() == target.lower():
                        m["team"] = "blue" if m["team"] == "red" else "red"
                await _room_broadcast(room)
            elif action == "room_kick":
                # создатель выкидывает игрока из лобби
                room = _room_of(nickname)
                target = (data.get("target") or "").strip()
                if not room or room["host"].lower() != nickname.lower() or not target or target.lower() == nickname.lower():
                    continue
                kicked_m = next((m for m in room["members"] if m["nick"].lower() == target.lower()), None)
                if not kicked_m:
                    continue
                room["members"] = [m for m in room["members"] if m["nick"].lower() != target.lower()]
                await _gsend(kicked_m["nick"], {"action": "room_kicked", "by": nickname})
                await _room_broadcast(room)
            elif action == "room_bots":
                # включить/выключить ботов в команде (0..3 минус игроки)
                room = _room_of(nickname)
                if not room or room["host"].lower() != nickname.lower():
                    continue
                team = data.get("team")
                if team not in ("blue", "red"):
                    continue
                count = data.get("count")
                if not isinstance(count, int) or count < 0 or count > 3:
                    continue
                players_in = sum(1 for m in room["members"] if m["team"] == team)
                room.setdefault("bots", {"blue": 0, "red": 0})
                room["bots"][team] = max(0, min(count, 3 - players_in))
                await _room_broadcast(room)
            elif action == "room_start":
                # создатель запускает матч: 2 человека — PvP/co-op + боты по конфигу
                room = _room_of(nickname)
                if not room or room["host"].lower() != nickname.lower():
                    await _gsend(nickname, {"action": "error", "msg": "Только создатель может начать"})
                    continue
                humans = room["members"]
                if not humans:
                    continue
                for h in humans:
                    await _drop_queue(h["nick"])  # из лобби — не в очередь одновременно
                bots_cfg = dict(room.get("bots", {"blue": 0, "red": 0}))
                if len(humans) == 1:
                    # одиночная лобби-игра — без серверного матча, только конфиг ботов
                    _rooms.pop(room["id"], None)
                    await _gsend(nickname, {"action": "room_solo_start", "id": 0,
                                            "game": room["game"], "opp": "",
                                            "my_color": humans[0]["team"],
                                            "lobby_bots": bots_cfg})
                    continue
                p1, p2 = humans[0]["nick"], humans[1]["nick"]
                t1, t2 = humans[0]["team"], humans[1]["team"]
                co_op = (t1 == t2)
                _match_seq += 1
                m = _init_match(room["game"] if room["game"] in GAMES else "greenu", p1, p2)
                m["id"] = _match_seq
                m["created_at"] = time.time()
                m["state"]["colors"] = {p1: t1, p2: t2}   # цвета из лобби, не случайные
                m["state"]["co_op"] = co_op
                _matches[m["id"]] = m
                for nick, opp, myt, ot in ((p1, p2, t1, t2), (p2, p1, t2, t1)):
                    await _gsend(nick, {"action": "game_found", "id": m["id"], "game": m["game"],
                                        "game_title": GAMES.get(m["game"], m["game"]), "opp": opp,
                                        "my_color": myt, "opp_color": ot,
                                        "lobby_bots": bots_cfg, "co_op": co_op})
                _rooms.pop(room["id"], None)
                await _lobby_broadcast()
            elif action == "room_finish":
                # co-op: ход сообщает итог (враги-боты зачищены / поражение)
                try:
                    mid = int(data.get("match_id"))
                except (TypeError, ValueError):
                    continue
                m = _matches.get(mid)
                if m and not m["over"] and m["state"].get("co_op") and nickname in (m["p1"], m["p2"]):
                    await _finish_co_op(m, bool(data.get("won")))
            elif action == "duel_challenge":
                # бросить дуэль конкретному игроку
                target = (data.get("target") or "").strip()
                game = (data.get("game") or "").strip()
                target_online = _gsock_find(target)
                if target_online and game in GAMES and target_online.lower() != nickname.lower():
                    # отправляем вызов цели
                    await _gsend(target_online, {
                        "action": "duel_request",
                        "from": nickname,
                        "game": game,
                        "game_title": GAMES[game],
                    })
                    await websocket.send_json({"action": "duel_sent", "to": target_online, "game_title": GAMES.get(game, game)})
                elif target and game in GAMES:
                    await websocket.send_json({"action": "error", "msg": f"@{target} сейчас не в сети (игровой сокет оффлайн)"})
            elif action == "duel_accept":
                # принять дуэль — создаём матч напрямую
                challenger = (data.get("from") or "").strip()
                game = (data.get("game") or "").strip()
                chal_key = _gsock_find(challenger)
                self_key = _gsock_find(nickname)
                if challenger and game in GAMES:
                    # оба должны быть онлайн
                    if chal_key and self_key:
                        # убираем из очередей
                        await _drop_queue(chal_key)
                        await _drop_queue(self_key)
                        await _create_match(game, chal_key, self_key)
                    else:
                        await _gsend(nickname, {"action": "error", "msg": "Соперник оффлайн"})
            elif action == "duel_decline":
                challenger = (data.get("from") or "").strip()
                chal_key = _gsock_find(challenger) or challenger
                if chal_key:
                    await _gsend(chal_key, {
                        "action": "duel_declined",
                        "by": nickname,
                    })
            elif action == "stats":
                await websocket.send_json({"action": "my_stats", "stats": _stats_payload(nickname)})
            elif action == "ping":
                await websocket.send_json({"action": "pong"})
    except WebSocketDisconnect:
        pass
    finally:
        # КРИТИЧНО: если это старый сокет (вытеснен новым из iframe utopia/greenu с кодом 4402),
        # НЕЛЬЗЯ сбрасывать очередь, лобби и активные матчи нового сокета!
        if _game_sockets.get(nickname) is websocket:
            _game_sockets.pop(nickname, None)
            await _drop_queue(nickname)
            await _room_member_left(nickname)
            # Противник не будет ждать вечно — закрытый игрок проигрывает
            # Но grace-период 15 сек: если игрок отключился сразу после создания матча — не forfeit
            now = time.time()
            for m in list(_matches.values()):
                if not m["over"] and nickname in (m["p1"], m["p2"]):
                    created = m.get("created_at", 0)
                    if now - created < 15:
                        # grace-период — просто закрываем матч без штрафа
                        m["over"] = True
                        _matches.pop(m["id"], None)
                        continue
                    opp = m["p2"] if nickname == m["p1"] else m["p1"]
                    await _finish_match(m, opp, "forfeit")
            await _lobby_broadcast()


SIGNAL_ACTIONS = (
    "call_offer", "call_answer", "call_ice", "call_reject", "call_end",
    "call_busy", "call_timeout", "call_video_state", "call_mute_state",
    "call_reaction", "call_chat", "call_sfx",
    "voice_offer", "voice_answer", "voice_ice",
)
CALL_SFX_IDS = ("bell", "horn", "bonk", "clap")

async def _handle_signal_message(websocket: WebSocket, nickname: str, data: dict):
    action = data.get("action")
    if action in SIGNAL_ACTIONS:
        target = (data.get("target") or "").strip()
        # Отправитель строго из проверенного сокета — подделать чужой ник нельзя
        data["caller"] = nickname
        if action == "call_reaction":
            emoji = str(data.get("emoji") or "").strip()[:12]
            if not emoji:
                return
            data["emoji"] = emoji
        elif action == "call_chat":
            text = str(data.get("text") or "").strip()[:500]
            if not text:
                return
            data["text"] = text
        elif action == "call_sfx":
            sfx_id = str(data.get("sfx") or "").strip()
            if sfx_id not in CALL_SFX_IDS:
                return
            data["sfx"] = sfx_id
        if action in ("call_answer", "call_reject", "call_end"):
            # Останавливаем входящий звонок на остальных открытых вкладках/устройствах этого юзера
            await manager.send_signal_to_user(
                nickname,
                {"action": "call_sync_stop", "by_action": action, "partner": target},
                exclude_ws=websocket,
                allow_chat_fallback=False,
            )
        if not target:
            return
        target_canon = _canonical_nick(target)
        if manager.is_online(target_canon):
            await manager.send_signal_to_user(target_canon, data)
        elif action == "call_offer":
            await websocket.send_json({"action": "call_offline", "target": target_canon})
        return

    if action == "voice_join":
        cid = (data.get("chat_id") or "").strip()
        if not cid:
            return
        await _leave_all_voice_rooms_for(nickname, except_chat_id=cid)
        peers = _voice_rooms.setdefault(cid, [])
        if not any(p.lower() == nickname.lower() for p in peers):
            peers.append(nickname)
        st_map = _voice_states.setdefault(cid, {})
        st_map[nickname.lower()] = {
            "nickname": nickname,
            "muted": bool(data.get("muted", False)),
            "deafened": bool(data.get("deafened", False)),
        }
        manager.unbind_voice_socket(websocket)
        manager.bind_voice_socket(websocket, cid)
        await _broadcast_voice_room(cid)
        return

    if action == "voice_leave":
        cid = (data.get("chat_id") or "").strip()
        manager.unbind_voice_socket(websocket, cid)
        if cid:
            if _remove_user_from_voice_room(cid, nickname):
                await _broadcast_voice_room(cid)
        else:
            await _leave_all_voice_rooms_for(nickname)
        return

    if action == "voice_state":
        cid = (data.get("chat_id") or "").strip()
        if not cid:
            return
        st_map = _voice_states.setdefault(cid, {})
        if nickname.lower() in st_map or any(p.lower() == nickname.lower() for p in _voice_rooms.get(cid, [])):
            st_map[nickname.lower()] = {
                "nickname": nickname,
                "muted": bool(data.get("muted", False)),
                "deafened": bool(data.get("deafened", False)),
            }
            manager.bind_voice_socket(websocket, cid)
            await _broadcast_voice_room(cid)


@app.get("/webrtc/ice")
def get_webrtc_ice_config():
    """Список ICE-серверов: живые STUN + опциональный TURN из env (если задан на сервере)."""
    servers = [
        {"urls": "stun:stun.l.google.com:19302"},
        {"urls": "stun:stun1.l.google.com:19302"},
        {"urls": "stun:stun2.l.google.com:19302"},
        {"urls": "stun:stun.cloudflare.com:3478"},
    ]
    turn_urls = [u.strip() for u in (os.environ.get("TURN_URLS") or "").split(",") if u.strip()]
    turn_user = (os.environ.get("TURN_USERNAME") or "").strip()
    turn_cred = (os.environ.get("TURN_CREDENTIAL") or "").strip()
    if turn_urls:
        entry = {"urls": turn_urls if len(turn_urls) > 1 else turn_urls[0]}
        if turn_user:
            entry["username"] = turn_user
        if turn_cred:
            entry["credential"] = turn_cred
        servers.append(entry)
    return {"status": "ok", "iceServers": servers}


@app.websocket("/ws/signal/{nickname}")
async def signal_websocket(websocket: WebSocket, nickname: str):
    """Постоянный канал WebRTC-сигнализации и голосовых комнат (не рвётся при переключении чатов)."""
    token = (websocket.query_params.get("token") or "").strip()
    try:
        _auth_session(nickname, token)
    except Exception:
        await websocket.accept()
        await websocket.close(code=4401)
        return
    nickname = _canonical_nick(nickname)
    await manager.connect_signal(websocket, nickname)
    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action", "")
            if action == "ping":
                tok = data.get("session_token") or token
                if tok:
                    cursor.execute("UPDATE user_sessions SET last_active=?, is_active=1 WHERE LOWER(nickname)=LOWER(?) AND session_token=?",
                                   (now_str(), nickname, tok))
                    conn.commit()
                await websocket.send_json({"action": "pong"})
            elif action in SIGNAL_ACTIONS or action in ("voice_join", "voice_leave", "voice_state"):
                await _handle_signal_message(websocket, nickname, data)
    except WebSocketDisconnect:
        pass
    finally:
        bound_rooms = manager.disconnect_signal(websocket, nickname)
        if not manager.is_online(nickname):
            await _leave_all_voice_rooms_for(nickname)
        elif bound_rooms:
            for cid in bound_rooms:
                if _remove_user_from_voice_room(cid, nickname):
                    await _broadcast_voice_room(cid)


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
    nickname = _canonical_nick(nickname)
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

            elif action in SIGNAL_ACTIONS or action in ("voice_join", "voice_leave", "voice_state"):
                await _handle_signal_message(websocket, nickname, data)

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

                # Мьют: замьюченный не может писать никуда — ни в ЛС, ни в группы
                mu = mute_info(nickname)
                if mu:
                    if data.get("file_url"):
                        delete_upload_file(data.get("file_url"))
                    await websocket.send_json({
                        "action": "error",
                        "msg": (f"🔇 Вы замучены на {mu['minutes']} мин. по правилам"
                                + (f": {mu['reason']}" if mu["reason"] else "")
                                + ". Писать нельзя."),
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
                data["chat_id"] = chat_id
                if not data.get("time"):
                    data["time"] = datetime.now().strftime("%H:%M")
                # Мгновенно доставляем и в открытый чат, и на все остальные устройства/вкладки участников и самого отправителя
                await manager.broadcast_chat_event(chat_id, data, sender_nick=nickname)

                # Личный чат с ботом-помощником: он отвечает отдельной задачей
                if chat_id.startswith("bot_"):
                    asyncio.create_task(bot_reply(chat_id, nickname, data.get("text") or ""))

            elif action == "typing":
                await manager.broadcast_chat(chat_id, {
                    "action": "typing",
                    "chat_id": chat_id,
                    "sender": nickname,
                })

            elif action == "edit":
                mid = data.get("message_id")
                new_text = (data.get("text") or "").strip()[:2000]
                try:
                    mid = int(mid)
                except (TypeError, ValueError):
                    continue
                if not new_text:
                    await websocket.send_json({"action": "error", "msg": "Текст не может быть пустым"})
                    continue
                cursor.execute("SELECT sender, chat_id, is_system FROM messages WHERE id=?", (mid,))
                row = cursor.fetchone()
                if not row or (row[1] or "") != chat_id or row[2]:
                    continue
                if (row[0] or "").lower() != nickname.lower():
                    await websocket.send_json({"action": "error", "msg": "Редактировать можно только свои сообщения"})
                    continue
                cursor.execute("UPDATE messages SET text=?, is_edited=1 WHERE id=?", (new_text, mid))
                conn.commit()
                await manager.broadcast_chat_event(chat_id, {
                    "action": "edited",
                    "chat_id": chat_id,
                    "message_id": mid,
                    "text": new_text,
                }, sender_nick=nickname)

            elif action in ("pin", "unpin"):
                if chat_id.startswith("bot_"):
                    continue
                can_pin = chat_id.startswith("dm_") or can_edit_chat(chat_id, nickname) or chat_permission(chat_id, nickname, "delete_messages") or can_manage_everything(nickname)
                if chat_id == "general" and not (can_manage_everything(nickname) or is_assistant(nickname)):
                    can_pin = False
                if not can_pin:
                    await websocket.send_json({"action": "error", "msg": "Нет прав на закрепление сообщений"})
                    continue
                if action == "unpin":
                    cursor.execute("UPDATE chats SET pinned_msg_id=NULL WHERE id=?", (chat_id,))
                    conn.commit()
                    await manager.broadcast_chat_event(chat_id, {"action": "pinned", "chat_id": chat_id, "pinned_msg": None}, sender_nick=nickname)
                else:
                    try:
                        mid = int(data.get("message_id"))
                    except (TypeError, ValueError):
                        continue
                    cursor.execute("UPDATE chats SET pinned_msg_id=? WHERE id=?", (mid, chat_id))
                    conn.commit()
                    await manager.broadcast_chat_event(chat_id, {
                        "action": "pinned",
                        "chat_id": chat_id,
                        "pinned_msg": _get_pinned_preview(chat_id, mid),
                    }, sender_nick=nickname)

            elif action == "react":
                mid = data.get("message_id")
                emoji = (data.get("emoji") or "").strip()[:8]
                try:
                    mid = int(mid)
                except (TypeError, ValueError):
                    mid = None
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
                    await manager.broadcast_chat_event(chat_id, {
                        "action": "reactions", "chat_id": chat_id, "message_id": mid, "reactions": agg
                    }, sender_nick=nickname)

            elif action == "delete":
                mid = data.get("message_id")
                try:
                    mid = int(mid)
                except (TypeError, ValueError):
                    continue
                cursor.execute("SELECT sender, chat_id, file_url FROM messages WHERE id=?", (mid,))
                row = cursor.fetchone()
                if not row:
                    continue
                owner = (row[0] or "").lower()
                if (row[1] or "") != chat_id:
                    await websocket.send_json({"action": "error",
                                               "msg": "Это сообщение из другого чата"})
                    continue
                # Своё сообщение — можно всегда; чужое — глобальное право
                # can_delete_messages либо чатовое право delete_messages
                owns_bot = chat_id.lower() == f"bot_{nickname}".lower()
                if (owner != (nickname or "").lower()
                        and not owns_bot
                        and not can_delete_messages(nickname)
                        and not chat_permission(chat_id, nickname, "delete_messages")):
                    await websocket.send_json({"action": "error",
                                               "msg": "Нет прав: можно удалять только свои сообщения"})
                    continue
                cursor.execute("DELETE FROM messages WHERE id=?", (mid,))
                cursor.execute("DELETE FROM message_reactions WHERE message_id=?", (mid,))
                conn.commit()
                # Файл (фото/голос/вложение) убираем с диска — в чате его больше нет
                delete_upload_file(row[2])
                await manager.broadcast_chat_event(chat_id, {
                    "action": "deleted", "chat_id": chat_id, "message_ids": [mid], "deleted_by": nickname
                }, sender_nick=nickname)

    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(chat_id, websocket, nickname)
        if not manager.is_online(nickname):
            await _leave_all_voice_rooms_for(nickname)


# ── Ручной и авто-бэкап БД (защита чатов и переписок при редеплое) ──
_start_gist_backup_loop()


@app.get("/admin/db/backup")
def admin_db_backup(admin: str):
    cursor.execute("SELECT is_superadmin, is_owner FROM users WHERE LOWER(nickname)=LOWER(?)", (admin,))
    row = cursor.fetchone()
    if not row or (not row[0] and not row[1] and not is_super(admin)):
        raise HTTPException(status_code=403, detail="Доступ запрещён")
    raw = _export_consistent_db_bytes()
    fd, tmp_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    with open(tmp_path, "wb") as f:
        f.write(raw)
    return FileResponse(tmp_path, filename="messenger_backup.db", media_type="application/octet-stream")


@app.post("/admin/db/restore")
async def admin_db_restore(admin: str, request: Request):
    cursor.execute("SELECT is_superadmin, is_owner FROM users WHERE LOWER(nickname)=LOWER(?)", (admin,))
    row = cursor.fetchone()
    if not row or (not row[0] and not row[1] and not is_super(admin)):
        raise HTTPException(status_code=403, detail="Доступ запрещён")
    raw = await request.body()
    if not raw.startswith(b"SQLite format 3\x00"):
        raise HTTPException(status_code=400, detail="Файл не является базой SQLite")
    with _all_conns_lock:
        for c_item in _all_thread_conns:
            try:
                c_item.close()
            except Exception:
                pass
        _all_thread_conns.clear()
    _thread_local.conn = None
    _thread_local.cursor = None
    for ext in ("-wal", "-shm"):
        p_ext = _DB_PATH + ext
        if os.path.exists(p_ext):
            try:
                os.remove(p_ext)
            except Exception:
                pass
    tmp = _DB_PATH + ".upload"
    with open(tmp, "wb") as f:
        f.write(raw)
    os.replace(tmp, _DB_PATH)
    _backup_db_to_gist(force=True)
    return {"status": "ok", "size": len(raw), "msg": "База восстановлена"}


# ── Музыкальные плейлисты (для геймеров / для фембоев), предложка 1 раз в неделю, любимый трек профиля ──
@app.get("/music/tracks")
def get_music_tracks():
    cursor.execute("SELECT id, fandom, title, artist, url, added_by, created_at FROM music_tracks ORDER BY id DESC")
    rows = cursor.fetchall()
    tracks = [
        {"id": r[0], "fandom": r[1], "title": r[2], "artist": r[3] or "", "url": r[4], "added_by": r[5] or "", "created_at": r[6]}
        for r in rows
    ]
    return {"status": "ok", "tracks": tracks}


@app.post("/music/request")
async def submit_music_request(data: dict):
    nick = (data.get("nickname") or "").strip()
    fandom = (data.get("fandom") or "gamer").strip().lower()
    if fandom not in ("gamer", "femboy", "chill"):
        fandom = "gamer"
    url = (data.get("url") or "").strip()
    note = (data.get("note") or "").strip()
    if not nick:
        raise HTTPException(status_code=400, detail="Не указан никнейм")
    if not url or not (url.startswith("http://") or url.startswith("https://") or url.startswith("/")):
        raise HTTPException(status_code=400, detail="Нужна корректная ссылка на трек (http:// или https://)")
    if len(note) > 300:
        raise HTTPException(status_code=400, detail="Сообщение не должно превышать 300 символов")
    if not note:
        raise HTTPException(status_code=400, detail="Напишите короткое сообщение (до 300 символов)")

    # Лимит: 1 заявка в 7 дней на аккаунт
    cursor.execute(
        "SELECT created_at FROM music_requests WHERE LOWER(nickname)=LOWER(?) ORDER BY id DESC LIMIT 1",
        (nick,),
    )
    last_row = cursor.fetchone()
    if last_row and last_row[0] and not is_super(nick):
        try:
            last_dt = datetime.strptime(str(last_row[0])[:19], "%Y-%m-%d %H:%M:%S")
            diff = datetime.utcnow() - last_dt
            if diff.total_seconds() < 7 * 86400:
                days_left = max(1, int((7 * 86400 - diff.total_seconds()) // 86400) + 1)
                return {"status": "error", "msg": f"Предлагать трек можно раз в неделю! Подождите ещё ~{days_left} дн."}
        except Exception:
            pass

    cursor.execute(
        "INSERT INTO music_requests (nickname, fandom, url, note, status) VALUES (?, ?, ?, ?, 'pending')",
        (nick, fandom, url, note),
    )
    conn.commit()
    return {"status": "ok", "msg": "Заявка отправлена админу!"}


def _check_music_admin(admin: str):
    cursor.execute("SELECT is_superadmin, is_admin, is_owner FROM users WHERE LOWER(nickname)=LOWER(?)", (admin,))
    row = cursor.fetchone()
    if not row or (not row[0] and not row[1] and not row[2] and not is_super(admin)):
        raise HTTPException(status_code=403, detail="Доступ только для админа")


@app.get("/music/requests")
def list_music_requests(admin: str):
    _check_music_admin(admin)
    cursor.execute("SELECT id, nickname, fandom, url, note, status, created_at FROM music_requests ORDER BY id DESC LIMIT 60")
    rows = cursor.fetchall()
    return {
        "status": "ok",
        "requests": [
            {"id": r[0], "nickname": r[1], "fandom": r[2], "url": r[3], "note": r[4], "status": r[5], "created_at": r[6]}
            for r in rows
        ],
    }


@app.post("/music/admin/add")
async def admin_add_music_track(data: dict):
    admin = (data.get("admin") or "").strip()
    _check_music_admin(admin)
    fandom = (data.get("fandom") or "gamer").strip().lower()
    if fandom not in ("gamer", "femboy", "chill"):
        fandom = "gamer"
    title = (data.get("title") or "").strip()[:120]
    artist = (data.get("artist") or "").strip()[:80]
    url = (data.get("url") or "").strip()
    req_id = data.get("request_id")
    if not title or not url:
        raise HTTPException(status_code=400, detail="Укажите название трека и ссылку")
    cursor.execute(
        "INSERT INTO music_tracks (fandom, title, artist, url, added_by) VALUES (?, ?, ?, ?, ?)",
        (fandom, title, artist, url, admin),
    )
    if req_id:
        cursor.execute("UPDATE music_requests SET status='approved' WHERE id=?", (int(req_id),))
    conn.commit()
    return {"status": "ok"}


@app.post("/music/admin/delete")
async def admin_delete_music_track(data: dict):
    admin = (data.get("admin") or "").strip()
    _check_music_admin(admin)
    tid = int(data.get("track_id") or 0)
    cursor.execute("DELETE FROM music_tracks WHERE id=?", (tid,))
    conn.commit()
    return {"status": "ok"}


@app.post("/music/admin/reject")
async def admin_reject_music_request(data: dict):
    admin = (data.get("admin") or "").strip()
    _check_music_admin(admin)
    rid = int(data.get("request_id") or 0)
    cursor.execute("UPDATE music_requests SET status='rejected' WHERE id=?", (rid,))
    conn.commit()
    return {"status": "ok"}


@app.post("/user/fav-track")
async def set_user_fav_track(data: dict):
    nick = (data.get("nickname") or "").strip()
    fav = (data.get("fav_track") or "").strip()[:140]
    if not nick:
        raise HTTPException(status_code=400, detail="Не указан никнейм")
    cursor.execute("UPDATE users SET fav_track=? WHERE LOWER(nickname)=LOWER(?)", (fav, nick))
    conn.commit()
    return {"status": "ok", "fav_track": fav}


# ── Статические маршруты страниц (мессенджер, игры) ──
@app.get("/")
def serve_index():
    path = os.path.join(BASE_DIR, "index.html")
    if os.path.isfile(path):
        return FileResponse(path)
    return {"status": "ok", "msg": "Utopia Server Running"}


@app.get("/greenu.html")
def serve_greenu():
    path = os.path.join(BASE_DIR, "greenu.html")
    if os.path.isfile(path):
        return FileResponse(path)
    raise HTTPException(status_code=404, detail="greenu.html not found")


@app.get("/utopia.html")
def serve_utopia():
    path = os.path.join(BASE_DIR, "utopia.html")
    if os.path.isfile(path):
        return FileResponse(path)
    raise HTTPException(status_code=404, detail="utopia.html not found")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


