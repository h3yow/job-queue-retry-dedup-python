"""测试公共夹具：临时 SQLite、加快的调度参数、ASGI HTTP 客户端。"""

import asyncio
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from app.config import Settings


def fast_settings(db_path: str, *, lease: float = 0.6, workers: int = 2) -> Settings:
    return Settings(
        db_path=db_path,
        worker_count=workers,
        lease_seconds=lease,
        max_retries=3,
        base_backoff_seconds=0.02,
        backoff_multiplier=2.0,
        max_backoff_seconds=0.2,
        poll_interval_seconds=0.01,
        max_payload_bytes=2048,
    )


@pytest.fixture
def settings(tmp_path) -> Settings:
    return fast_settings(str(tmp_path / "jobs.db"))


@pytest_asyncio.fixture
async def client(settings) -> AsyncIterator[httpx.AsyncClient]:
    from main import create_app

    app = create_app(settings)
    transport = httpx.ASGITransport(app=app)
    async with (
        httpx.AsyncClient(transport=transport, base_url="http://test") as c,
        app.router.lifespan_context(app),
    ):
        yield c


async def wait_status(client, job_id, expected, timeout=5.0):
    deadline = asyncio.get_running_loop().time() + timeout
    last = None
    while asyncio.get_running_loop().time() < deadline:
        r = await client.get(f"/api/v1/jobs/{job_id}")
        assert r.status_code == 200
        last = r.json()
        if isinstance(expected, (set, tuple, list)):
            if last["status"] in expected:
                return last
        elif last["status"] == expected:
            return last
        await asyncio.sleep(0.02)
    raise AssertionError(f"job {job_id} 未到达 {expected}, 最后状态={last}")


def submit(client, type_: str, payload=None, key=None, raw_body=None, headers=None):
    url = "/api/v1/jobs"
    h = dict(headers or {})
    if key is not None:
        h["Idempotency-Key"] = key
    if raw_body is not None:
        return client.post(url, content=raw_body, headers={**h, "Content-Type": "application/json"})
    return client.post(url, json={"type": type_, "payload": payload or {}}, headers=h)
