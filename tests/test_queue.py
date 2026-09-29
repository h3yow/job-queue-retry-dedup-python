"""队列层：重复提交、拒绝原因、重试退避、死信、原子认领与租约重认领。"""
from __future__ import annotations

import asyncio

import pytest

from app.models import FailureType, JobStatus, RejectReason, SubmitError

pytestmark = pytest.mark.asyncio


async def test_submit_idempotent_same_job(make_queue):
    queue, conn, _ = await make_queue()
    job1, created1 = await queue.submit("demo.echo", '{"x":1}', "key-1")
    job2, created2 = await queue.submit("demo.echo", '{"x":1}', "key-1")
    assert created1 is True
    assert created2 is False
    assert job1.id == job2.id

    count = (await conn.execute_fetchall("SELECT COUNT(*) c FROM jobs"))[0]["c"]
    assert count == 1
    await conn.close()


async def test_concurrent_submit_same_key_only_one_row(make_queue):
    queue, conn, _ = await make_queue(base_delay=10)
    results = await asyncio.gather(
        *[queue.submit("demo.echo", '{"x":1}', "race-key") for _ in range(10)]
    )
    created = [r[1] for r in results]
    ids = {r[0].id for r in results}
    assert sum(created) == 1
    assert len(ids) == 1
    rows = await conn.execute_fetchall("SELECT COUNT(*) c FROM jobs")
    assert rows[0]["c"] == 1
    await conn.close()


async def test_submit_rejection_reasons_distinct(make_queue):
    queue, conn, _ = await make_queue()

    with pytest.raises(SubmitError) as e1:
        await queue.submit("unknown.task", '{}', "k")
    assert e1.value.reason is RejectReason.UNKNOWN_TASK

    with pytest.raises(SubmitError) as e2:
        await queue.submit("demo.echo", "", "k")
    assert e2.value.reason is RejectReason.INVALID_PAYLOAD

    with pytest.raises(SubmitError) as e3:
        await queue.submit("demo.echo", "x" * (64 * 1024 + 1), "k")
    assert e3.value.reason is RejectReason.PAYLOAD_TOO_LARGE
    await conn.close()


async def test_transient_retry_then_success(make_queue):
    queue, conn, _ = await make_queue(max_attempts=3, base_delay=0.005)
    job, _ = await queue.submit("demo.flaky", '{"fail_times":2}', "k")

    # 第 1 次：临时失败 -> 回到 PENDING，排期后重试
    claimed = await queue.claim_next("w1")
    assert claimed.id == job.id and claimed.attempts == 1
    st = await queue.fail(claimed.id, "w1", FailureType.TRANSIENT.value, "boom-1")
    assert st == JobStatus.PENDING.value
    j = await queue.get(job.id)
    assert j.last_error == "boom-1" and j.attempts == 1

    # 未到退避时间，不应被认领
    assert await queue.claim_next("w1") is None
    j = await queue.get(job.id)
    await asyncio.sleep(max(0.0, j.available_at - __import__("time").time()) + 0.01)

    # 第 2 次失败、第 3 次成功
    claimed = await queue.claim_next("w1")
    assert claimed.attempts == 2
    await queue.fail(claimed.id, "w1", FailureType.TRANSIENT.value, "boom-2")
    await asyncio.sleep(0.02)
    claimed = await queue.claim_next("w1")
    assert claimed.attempts == 3
    assert await queue.complete(claimed.id, "w1", "done") is True
    j = await queue.get(job.id)
    assert j.status is JobStatus.SUCCEEDED and j.result == "done"
    assert await queue.list_failed() == []
    await conn.close()


async def test_permanent_failure_immediately_dead(make_queue):
    queue, conn, _ = await make_queue()
    job, _ = await queue.submit("demo.permanent_fail", "{}", "k")
    claimed = await queue.claim_next("w1")
    st = await queue.fail(claimed.id, "w1", FailureType.PERMANENT.value, "永久坏")
    assert st == JobStatus.FAILED.value
    # 永久失败后不应再被认领
    assert await queue.claim_next("w1") is None
    j = await queue.get(job.id)
    assert j.status is JobStatus.FAILED
    assert j.last_error == "永久坏"
    assert j.last_failure_type is FailureType.PERMANENT
    dead = await queue.list_failed()
    assert [d.id for d in dead] == [job.id]
    await conn.close()


async def test_retries_exhausted_enters_dead_letter(make_queue):
    queue, conn, _ = await make_queue(max_attempts=2, base_delay=0.005)
    job, _ = await queue.submit("demo.flaky", '{"fail_times":99}', "k")
    c1 = await queue.claim_next("w1")
    await queue.fail(c1.id, "w1", FailureType.TRANSIENT.value, "e1")
    await asyncio.sleep(0.02)
    c2 = await queue.claim_next("w1")
    assert c2.attempts == 2
    st = await queue.fail(c2.id, "w1", FailureType.TRANSIENT.value, "e2")
    assert st == JobStatus.FAILED.value
    j = await queue.get(job.id)
    assert j.status is JobStatus.FAILED and j.last_error == "e2"
    assert await queue.claim_next("w1") is None
    await conn.close()


async def test_concurrent_claim_only_one_winner(make_queue):
    queue, conn, _ = await make_queue()
    await queue.submit("demo.echo", '{}', "only-one")
    winners = await asyncio.gather(
        *[queue.claim_next(f"w{i}") for i in range(8)]
    )
    owners_ok = [w for w in winners if w is not None]
    assert len(owners_ok) == 1
    await conn.close()


async def test_expired_lease_is_reclaimed_and_stale_report_rejected(make_queue):
    queue, conn, db_path = await make_queue(lease_seconds=0.05)
    job, _ = await queue.submit("demo.echo", '{}', "stale-owner")

    old = await queue.claim_next("old-worker")
    assert old is not None
    # 租约未过期时：别人抢不到，旧持有者有效
    assert await queue.claim_next("new-worker") is None
    await asyncio.sleep(0.07)
    # 租约过期后：被新执行者重认领
    again = await queue.claim_next("new-worker")
    assert again is not None and again.id == job.id
    # 旧执行者迟到的回报必须被丢弃，不能覆盖新执行者
    assert await queue.complete(job.id, "old-worker", "stale-result") is False
    assert await queue.complete(job.id, "new-worker", "fresh-result") is True
    j = await queue.get(job.id)
    assert j.status is JobStatus.SUCCEEDED and j.result == "fresh-result"
    await conn.close()


async def test_recover_pending_after_restart(make_queue):
    queue, conn, db_path = await make_queue(lease_seconds=100)
    job, _ = await queue.submit("demo.echo", '{}', "crash-me")
    await queue.claim_next("dead-worker")
    # 模拟进程被杀：直接关闭连接，任务停留在 RUNNING 且租约未过期
    await conn.close()

    # 新进程打开同一个库并执行启动恢复
    from app.db import connect

    conn2 = await connect(db_path)
    from app.queue import JobQueue

    queue2 = JobQueue(conn2, known_task_types={"demo.echo"}, lease_seconds=100)
    n = await queue2.recover_pending()
    assert n == 1
    claimed = await queue2.claim_next("new-worker")
    assert claimed is not None and claimed.id == job.id
    assert claimed.attempts == 2  # 认领次数累加，未丢任务
    await conn2.close()
