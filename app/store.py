"""SQLite 持久化层。

设计要点：
- 进程内单连接 + asyncio.Lock 串行化所有写事务，配合 SQLite 单条
  ``UPDATE ... WHERE id=(SELECT ... LIMIT 1)`` 完成"抢占式认领"：
  多个 worker / 多个进程同时抢时，数据库行锁保证只有一个 UPDATE 命中。
- 认领同时携带租约（leased_by + lease_expires_at），所有终态/回退写回
  都带 ``leased_by`` 作为 fencing token，旧执行者无法覆盖新执行者的结果。
- 崩溃进程遗留的 running 任务：在租约到期后由 claim_next 原子重领；
  reset_stale_leases 也可作为显式运维入口。
"""

import asyncio
import json
import sqlite3
import uuid
from collections.abc import Iterable
from contextlib import asynccontextmanager
from typing import Any


class IdempotencyConflict(Exception):
    """同一幂等键被用于不同的任务内容。"""


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT UNIQUE,
    type TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL,
    result TEXT,
    last_error TEXT,
    error_kind TEXT,
    run_after REAL NOT NULL,
    run_after_ts REAL,
    leased_by TEXT,
    lease_expires_at REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    created_ts REAL NOT NULL,
    updated_ts REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_dispatch ON jobs(status, run_after);
CREATE TABLE IF NOT EXISTS effects (
    job_id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    created_at REAL NOT NULL
);
"""


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    d["payload"] = json.loads(d["payload"])
    d["result"] = json.loads(d["result"]) if d["result"] is not None else None
    return d


class JobStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._conn: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()

    # ---- 连接管理 ----------------------------------------------------------
    async def connect(self) -> None:
        # isolation_level=None：关闭 sqlite3 模块的隐式事务，事务边界全部由
        # _tx() 显式管理，避免协程被 cancel 时遗留隐式事务污染同一连接。
        conn = sqlite3.connect(
            self.db_path, check_same_thread=False, isolation_level=None
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.executescript(SCHEMA)
        self._conn = conn

    async def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        assert self._conn is not None, "JobStore.connect() 尚未调用"
        return self._conn

    @asynccontextmanager
    async def _tx(self):
        async with self._lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield self.conn
                self.conn.execute("COMMIT")
            except BaseException:
                self.conn.execute("ROLLBACK")
                raise

    # ---- 提交 / 查询 --------------------------------------------------------
    async def create_job(
        self,
        *,
        type_: str,
        payload: dict[str, Any],
        max_retries: int,
        idempotency_key: str | None,
        now: float,
        wall_now: float,
    ) -> tuple[dict[str, Any], bool]:
        """返回 (job, deduplicated)。内容冲突时抛 IdempotencyConflict。"""
        payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        async with self._tx() as conn:
            if idempotency_key is not None:
                existing = conn.execute(
                    "SELECT * FROM jobs WHERE idempotency_key=?",
                    (idempotency_key,),
                ).fetchone()
                if existing is not None:
                    old = _row_to_dict(existing)
                    if old["type"] != type_ or json.dumps(
                        old["payload"], separators=(",", ":"), sort_keys=True
                    ) != payload_json:
                        raise IdempotencyConflict(idempotency_key)
                    return old, True
            job_id = uuid.uuid4().hex
            conn.execute(
                "INSERT INTO jobs(id, idempotency_key, type, payload, status, "
                "max_retries, run_after, created_at, updated_at, created_ts, updated_ts) "
                "VALUES (?,?,?,?, 'pending', ?, ?, ?, ?, ?, ?)",
                (job_id, idempotency_key, type_, payload_json, max_retries,
                 now, now, now, wall_now, wall_now),
            )
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            return _row_to_dict(row), False

    async def get_job(self, job_id: str) -> dict[str, Any] | None:
        async with self._lock:
            row = self.conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            return _row_to_dict(row) if row is not None else None

    async def list_jobs(self, statuses: Iterable[str] | None = None) -> list[dict[str, Any]]:
        async with self._lock:
            if statuses is None:
                rows = self.conn.execute("SELECT * FROM jobs ORDER BY created_ts").fetchall()
            else:
                statuses = list(statuses)
                placeholders = ",".join("?" * len(statuses))
                rows = self.conn.execute(
                    f"SELECT * FROM jobs WHERE status IN ({placeholders}) ORDER BY created_ts",
                    statuses,
                ).fetchall()
            return [_row_to_dict(r) for r in rows]

    # ---- 调度：认领 / 心跳 / 回写 -------------------------------------------
    async def reset_stale_leases(self, now: float, *, force: bool = False) -> list[str]:
        """把遗留的 running 任务放回待认领队列。

        默认（force=False）只回收**租约已过期**的 running——这是安全的，
        不会干扰持有效租约的同伴。正常启动无需调用：claim_next 自身就能在
        租约到期时原子重领。本方法保留为运维/测试的显式入口；
        force=True 会无条件回收全部 running，仅适合"确认本服务是唯一实例"
        的维护窗口，多实例下使用会造成重复执行。
        """
        async with self._tx() as conn:
            if force:
                rows = conn.execute("SELECT id FROM jobs WHERE status='running'").fetchall()
                ids = [r["id"] for r in rows]
                if ids:
                    conn.execute(
                        "UPDATE jobs SET status='pending', leased_by=NULL, "
                        "lease_expires_at=NULL, run_after=?, updated_at=? "
                        "WHERE status='running'",
                        (now, now),
                    )
                return ids
            rows = conn.execute(
                "SELECT id FROM jobs WHERE status='running' "
                "AND lease_expires_at IS NOT NULL AND lease_expires_at<?",
                (now,),
            ).fetchall()
            ids = [r["id"] for r in rows]
            if ids:
                conn.execute(
                    "UPDATE jobs SET status='pending', leased_by=NULL, "
                    "lease_expires_at=NULL, run_after=?, updated_at=? "
                    "WHERE status='running' "
                    "AND lease_expires_at IS NOT NULL AND lease_expires_at<?",
                    (now, now, now),
                )
            return ids

    async def claim_next(self, worker_id: str, lease_seconds: float, now: float):
        """原子认领一个可执行任务，返回 (job, reclaimed) 或 None。

        候选：pending/retrying 且 run_after 已到；或 running 租约已过期
        （僵死任务安全重领）。
        """
        async with self._tx() as conn:
            row = conn.execute(
                "SELECT * FROM jobs WHERE "
                "(status IN ('pending','retrying') AND run_after<=?) "
                "OR (status='running' AND lease_expires_at IS NOT NULL AND lease_expires_at<?) "
                "ORDER BY run_after, created_ts LIMIT 1",
                (now, now),
            ).fetchone()
            if row is None:
                return None
            job = _row_to_dict(row)
            reclaimed = job["status"] == "running"
            conn.execute(
                "UPDATE jobs SET status='running', leased_by=?, "
                "lease_expires_at=?, attempts=attempts+1, updated_at=? WHERE id=?",
                (worker_id, now + lease_seconds, now, job["id"]),
            )
            job["status"] = "running"
            job["leased_by"] = worker_id
            job["lease_expires_at"] = now + lease_seconds
            job["attempts"] += 1
            job["updated_at"] = now
            return job, reclaimed

    async def heartbeat(self, job_id: str, worker_id: str, lease_seconds: float, now: float) -> bool:
        async with self._lock:
            cur = self.conn.execute(
                "UPDATE jobs SET lease_expires_at=?, updated_at=? "
                "WHERE id=? AND leased_by=? AND status='running'",
                (now + lease_seconds, now, job_id, worker_id),
            )
            return cur.rowcount == 1

    async def _finish(
        self,
        conn: sqlite3.Connection,
        job_id: str,
        worker_id: str,
        *,
        status: str,
        now: float,
        result: str | None = None,
        last_error: str | None = None,
        error_kind: str | None = None,
        run_after: float | None = None,
    ) -> bool:
        cur = conn.execute(
            "UPDATE jobs SET status=?, result=COALESCE(?, result), "
            "last_error=?, error_kind=?, "
            "run_after=COALESCE(?, run_after), "
            "run_after_ts=COALESCE(CAST(? AS REAL), run_after_ts, ?), "
            "leased_by=NULL, lease_expires_at=NULL, updated_at=?, "
            "updated_ts=CAST(strftime('%s','now') AS REAL) "
            "WHERE id=? AND leased_by=? AND status='running'",
            (status, result, last_error, error_kind, run_after,
             run_after, now, now, job_id, worker_id),
        )
        return cur.rowcount == 1

    async def mark_succeeded(self, job_id: str, worker_id: str, result: Any, now: float) -> bool:
        result_json = json.dumps(result, separators=(",", ":"), sort_keys=True)
        async with self._tx() as conn:
            return await self._finish(
                conn, job_id, worker_id, status="succeeded", now=now, result=result_json
            )

    async def mark_retry(
        self,
        job_id: str,
        worker_id: str,
        *,
        error: str,
        error_kind: str,
        run_after: float,
        now: float,
    ) -> bool:
        async with self._tx() as conn:
            return await self._finish(
                conn, job_id, worker_id, status="retrying", now=now,
                last_error=error, error_kind=error_kind, run_after=run_after,
            )

    async def mark_failed(
        self, job_id: str, worker_id: str, *, error: str, error_kind: str, now: float
    ) -> bool:
        async with self._tx() as conn:
            return await self._finish(
                conn, job_id, worker_id, status="failed", now=now,
                last_error=error, error_kind=error_kind,
            )

    # ---- 外部副作用去重（供处理器调用）--------------------------------------
    async def record_effect(self, job_id: str, label: str, now: float) -> int:
        """以 job_id 为主键记录一次副作用；重复执行不会产生多行。返回该任务累计次数。"""
        async with self._tx() as conn:
            conn.execute(
                "INSERT INTO effects(job_id, label, created_at) VALUES (?, ?, ?) "
                "ON CONFLICT(job_id) DO NOTHING",
                (job_id, label, now),
            )
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM effects WHERE job_id=?", (job_id,)
            ).fetchone()
            return int(row["c"])

    async def list_effects(self) -> list[dict[str, Any]]:
        async with self._lock:
            rows = self.conn.execute("SELECT * FROM effects ORDER BY created_at").fetchall()
            return [dict(r) for r in rows]
