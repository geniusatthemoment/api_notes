from contextlib import asynccontextmanager
import json
import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text

try:
    from redis.sentinel import Sentinel
except ImportError:
    Sentinel = None

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://myuser:strongpass@127.0.0.1:5432/myappdb",
)

REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "StrongRedisPassword")
REDIS_MASTER_NAME = os.getenv("REDIS_MASTER_NAME", "mymaster")

SENTINELS = [
    (os.getenv("SENTINEL_1_HOST", "127.0.0.1"), int(os.getenv("SENTINEL_1_PORT", "26379"))),
    (os.getenv("SENTINEL_2_HOST", "127.0.0.1"), int(os.getenv("SENTINEL_2_PORT", "26380"))),
    (os.getenv("SENTINEL_3_HOST", "127.0.0.1"), int(os.getenv("SENTINEL_3_PORT", "26381"))),
]

NOTE_TTL_SECONDS = 300
NOTES_LIST_TTL_SECONDS = 120

engine = create_engine(DATABASE_URL, pool_pre_ping=True)


class NoteCreate(BaseModel):
    text: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS notes (
                id SERIAL PRIMARY KEY,
                text TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """))

    app.state.redis = None

    if Sentinel is not None:
        try:
            sentinel = Sentinel(
                SENTINELS,
                socket_timeout=0.5,
                decode_responses=True,
            )
            redis_master = sentinel.master_for(
                REDIS_MASTER_NAME,
                socket_timeout=0.5,
                decode_responses=True,
                password=REDIS_PASSWORD,
            )
            redis_master.ping()
            app.state.redis = redis_master
            print("Redis connected")
        except Exception as e:
            print(f"Redis unavailable: {e}")
            app.state.redis = None

    yield
    engine.dispose()


app = FastAPI(title="FastAPI + Postgres + Redis", lifespan=lifespan)


def note_key(note_id: int) -> str:
    return f"cache:note:{note_id}"


def notes_list_key(limit: int) -> str:
    return f"cache:notes:list:{limit}"


def safe_redis_get(key: str):
    r = app.state.redis
    if r is None:
        return None
    try:
        return r.get(key)
    except Exception:
        return None


def safe_redis_set(key: str, value: str, ttl: int):
    r = app.state.redis
    if r is None:
        return
    try:
        r.set(key, value, ex=ttl)
    except Exception:
        pass


def safe_redis_delete(*keys: str):
    r = app.state.redis
    if r is None:
        return
    try:
        if keys:
            r.delete(*keys)
    except Exception:
        pass


@app.get("/health")
def health():
    try:
        with engine.begin() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    return {
        "status": "ok",
        "db": "ok",
        "redis": "ok" if app.state.redis is not None else "degraded",
    }


@app.post("/notes")
def create_note(body: NoteCreate):
    with engine.begin() as conn:
        row = conn.execute(
            text("INSERT INTO notes(text) VALUES (:t) RETURNING id, text, created_at"),
            {"t": body.text},
        ).mappings().first()

    result = dict(row)
    safe_redis_set(note_key(result["id"]), json.dumps(result, default=str), NOTE_TTL_SECONDS)

    for lim in (10, 20, 50, 100):
        safe_redis_delete(notes_list_key(lim))

    return result


@app.get("/notes")
def list_notes(limit: int = 50):
    key = notes_list_key(limit)
    cached = safe_redis_get(key)
    if cached is not None:
        return json.loads(cached)

    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT id, text, created_at FROM notes ORDER BY id DESC LIMIT :lim"),
            {"lim": limit},
        ).mappings().all()

    result = [dict(r) for r in rows]
    safe_redis_set(key, json.dumps(result, default=str), NOTES_LIST_TTL_SECONDS)
    return result


@app.get("/notes/{note_id}")
def get_note(note_id: int):
    key = note_key(note_id)
    cached = safe_redis_get(key)
    if cached is not None:
        return json.loads(cached)

    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT id, text, created_at FROM notes WHERE id = :id"),
            {"id": note_id},
        ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Not found")

    result = dict(row)
    safe_redis_set(key, json.dumps(result, default=str), NOTE_TTL_SECONDS)
    return result


@app.delete("/notes/{note_id}")
def delete_note(note_id: int):
    with engine.begin() as conn:
        row = conn.execute(
            text("DELETE FROM notes WHERE id = :id RETURNING id"),
            {"id": note_id},
        ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Not found")

    safe_redis_delete(note_key(note_id))
    for lim in (10, 20, 50, 100):
        safe_redis_delete(notes_list_key(lim))

    return {"deleted": note_id}

#new_commit
