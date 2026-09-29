"""测试夹具：每个用例独立的临时 SQLite 文件 + 队列。"""
from __future__ import annotations

import logging
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO)


@pytest.fixture
async def make_queue(tmp_path):
    from app.db import connect
    from app.queue import JobQueue

    conns = []

    async def _make(
        max_attempts: int = 3,
        base_delay: float = 0.01,
        lease_seconds: float = 10.0,
        known=None,
    ):
        db_path = str(tmp_path / f"test-{len(conns)}.db")
        conn = await connect(db_path)
        conns.append(conn)
        queue = JobQueue(
            conn,
            known_task_types=known
            or {"demo.echo", "demo.flaky", "demo.permanent_fail"},
            max_attempts=max_attempts,
            base_delay=base_delay,
            lease_seconds=lease_seconds,
        )
        return queue, conn, db_path

    yield _make

    for conn in conns:
        try:
            await conn.close()
        except Exception:  # noqa: BLE001
            pass
