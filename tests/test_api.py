"""API + Worker 集成：幂等提交响应、拒绝原因、端到端执行、重试、死信。"""
from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

import main as main_module

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def client(tmp_path):
    # demo.flaky 的计数是进程级单例，用例间重置
    main_module.demo_flaky._calls = 0  # type: ignore[attr-defined]
    db_path = str(tmp_path / "api.db")
    app = main_module.create_app(
        db_path=db_path,
        worker_count=1,
        max_attempts=3,
        base_delay=0.02,
        lease_seconds=30,
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://t") as ac:
        async with app.router.lifespan_context(app):
            yield ac


async def _wait_status(ac, job_id, expected, timeout_s=3.0):
    deadline = asyncio.get_event_loop().time() + timeout_s
    last = None
    while asyncio.get_event_loop().time() < deadline:
        r = await ac.get(f"/jobs/{job_id}")
        last = r.json()["status"]
        if last == expected:
            return r.json()
        await asyncio.sleep(0.02)
    raise AssertionError(f"任务 {job_id} 未在 {timeout_s}s 内到达 {expected}，最后状态 {last}")


async def test_submit_and_execute_success(client):
    r = await client.post(
        "/jobs",
        json={"task_type": "demo.echo", "payload": '{"v":42}', "idempotency_key": "k1"},
    )
    assert r.status_code == 201
    body = r.json()
    assert body["duplicated"] is False
    job = await _wait_status(client, body["job"]["id"], "succeeded")
    assert "42" in job["result"]


async def test_duplicate_submit_returns_distinguishable_result(client):
    payload = {"task_type": "demo.echo", "payload": '{"v":1}', "idempotency_key": "dup"}
    r1 = await client.post("/jobs", json=payload)
    r2 = await client.post("/jobs", json=payload)
    assert r1.status_code == 201 and r1.json()["duplicated"] is False
    assert r2.status_code == 200
    b2 = r2.json()
    assert b2["duplicated"] is True
    assert b2["job"]["id"] == r1.json()["job"]["id"]


async def test_idempotency_key_header(client):
    r = await client.post(
        "/jobs",
        json={"task_type": "demo.echo", "payload": '{}'},
        headers={"Idempotency-Key": "hdr-key"},
    )
    assert r.status_code == 201
    r2 = await client.get("/jobs-by-key/hdr-key")
    assert r2.status_code == 200 and r2.json()["idempotency_key"] == "hdr-key"


async def test_reject_reasons_are_distinct(client):
    # 缺少幂等键
    r = await client.post("/jobs", json={"task_type": "demo.echo", "payload": "{}"})
    assert r.status_code == 400 and r.json()["reason"] == "invalid_payload"

    # 未知任务类型
    r = await client.post(
        "/jobs", json={"task_type": "nope", "payload": "{}", "idempotency_key": "x"}
    )
    assert r.status_code == 400 and r.json()["reason"] == "unknown_task"

    # payload 超大
    r = await client.post(
        "/jobs",
        json={
            "task_type": "demo.echo",
            "payload": "x" * (64 * 1024 + 1),
            "idempotency_key": "y",
        },
    )
    assert r.status_code == 422 and r.json()["reason"] == "payload_too_large"


async def test_flaky_retries_then_succeeds(client):
    main_module.demo_flaky._calls = 0  # type: ignore[attr-defined]
    r = await client.post(
        "/jobs",
        json={
            "task_type": "demo.flaky",
            "payload": '{"fail_times":2}',
            "idempotency_key": "flaky",
        },
    )
    job_id = r.json()["job"]["id"]
    final = await _wait_status(client, job_id, "succeeded", timeout_s=5)
    assert final["attempts"] == 3
    assert final["last_error"] is None or "boom" not in (final["last_error"] or "")

    dead = (await client.get("/dead-letter")).json()
    assert all(j["id"] != job_id for j in dead)


async def test_permanent_failure_goes_to_dead_letter(client):
    r = await client.post(
        "/jobs",
        json={
            "task_type": "demo.permanent_fail",
            "payload": "{}",
            "idempotency_key": "perm",
        },
    )
    job_id = r.json()["job"]["id"]
    final = await _wait_status(client, job_id, "failed")
    assert final["attempts"] == 1  # 永久失败不重试
    assert final["last_failure_type"] == "permanent"
    assert "永久" in final["last_error"]

    dead = (await client.get("/dead-letter")).json()
    assert any(j["id"] == job_id and j["last_error"] for j in dead)
