"""SQLite 持久层：建表、连接与建库初始化。

使用 WAL 以支持“一个写进程 + 多个读进程”的并发访问；
写操作通过 busy_timeout 等待而不是立刻报 SQLITE_BUSY。
"""
from __future__ import annotations

import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    task_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    available_at REAL NOT NULL,
    leased_at REAL,
    lease_expires_at REAL,
    lease_owner TEXT,
    last_error TEXT,
    last_failure_type TEXT,
    result TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_dispatch
    ON jobs (status, available_at);
CREATE INDEX IF NOT EXISTS idx_jobs_lease
    ON jobs (status, lease_expires_at);
"""


async def init_schema(conn: aiosqlite.Connection) -> None:
    """创建表与索引（幂等）。"""
    await conn.executescript(SCHEMA)
    await conn.commit()


async def connect(db_path: str) -> aiosqlite.Connection:
    """打开数据库连接：WAL、外键、忙等，并确保表结构就绪。"""
    conn = await aiosqlite.connect(db_path, timeout=30.0)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA foreign_keys=ON")
    await conn.execute("PRAGMA busy_timeout=30000")
    await conn.execute("PRAGMA synchronous=NORMAL")
    await init_schema(conn)
    return conn
