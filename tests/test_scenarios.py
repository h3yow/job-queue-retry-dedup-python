"""端到端关键场景：
- 重复提交不产生重复执行副作用；
- 服务重启后执行中的任务不丢、不被重复执行完；
- 多个执行者并发抢同一任务只有一个真正推进；
- 租约卡死的任务可被安全重认领。
"""
from __future__ import annotations

import asyncio
import json
import os

import pytest

from app.db import connect
from app.executors import ExecutorRegistry
from app.models import FailureType, JobStatus, TaskExecutionError
from app.queue import JobQueue
from app.worker import Worker

pytestmark = pytest.mark.asyncio


async def _drain(queue, executors, workers: list[Worker], expect: dict[str, int],
                 timeout_s: float = 5.0) -> dict[str, int]:
    """驱动若干 worker 直到所有任务终结，返回每种终结状态的计数。"""
    for w in workers:
        await w.start()
    deadline = asyncio.get_event_loop().time() + timeout_s
    final: dict[str, int] = {}
    while asyncio.get_event_loop().time() < deadline:
        rows = await queue.conn.execute_fetchall(
            "SELECT status, COUNT(*) c FROM jobs GROUP BY status"
        )
        counts = {r["status"]: r["c"] for r in rows}
        if not (set(counts) & {"pending", "running"}):
            final = counts
            break
        await asyncio.sleep(0.01)
    for w in workers:
        await w.stop()
    return final


async def test_duplicate_submit_has_single_side_effect(make_queue):
    """同幂等键重复提交 N 次：只建一条任务、副作用只发生一次。"""
    queue, conn, _ = await make_queue(base_delay=10)
    side_effects = {"calls": 0}

    async def handler(payload: str) -> str:
        side_effects["calls"] += 1
        return "ok"

    reg = ExecutorRegistry()
    reg.register("demo.echo", handler)
    queue.known_task_types = {"demo.echo"}

    results = await asyncio.gather(
        *[queue.submit("demo.echo", "{}", "same-business-op") for _ in range(5)]
    )
    assert sum(1 for _, created in results if created) == 1

    workers = [Worker(queue, reg, worker_id=f"w{i}") for i in range(3)]
    final = await _drain(queue, reg, workers, {JobStatus.SUCCEEDED.value: 1})
    assert final.get(JobStatus.SUCCEEDED.value) == 1
    assert side_effects["calls"] == 1
    await conn.close()


async def test_restart_recovers_running_job_without_loss_or_dup(make_queue):
    """进程在任务 RUNNING 时被杀：重启恢复后任务恰好再执行一次并成功。"""
    queue, conn, db_path = await make_queue(lease_seconds=100, base_delay=0.01)
    attempts_seen: list[int] = []

    async def handler(payload: str) -> str:
        j = await queue.get(job_id)
        attempts_seen.append(j.attempts)
        return "recovered-ok"

    reg = ExecutorRegistry()
    reg.register("demo.echo", handler)

    job, _ = await queue.submit("demo.echo", "{}", "op-restart")
    job_id = job.id
    await queue.claim_next("old-process-worker")  # 旧进程已认领，随即崩溃
    await conn.close()  # 模拟进程被杀：没有任何回报

    # —— 新进程：同一数据库文件、新连接、新 worker ——
    conn2 = await connect(db_path)
    queue.conn = conn2  # handler 里读状态用
    queue2 = JobQueue(conn2, known_task_types={"demo.echo"}, lease_seconds=100)
    recovered = await queue2.recover_pending()
    assert recovered == 1

    worker = Worker(queue2, reg, worker_id="new-process-worker")
    await worker.start()

    deadline = asyncio.get_event_loop().time() + 3
    while asyncio.get_event_loop().time() < deadline:
        j = await queue2.get(job_id)
        if j.status is JobStatus.SUCCEEDED:
            break
        await asyncio.sleep(0.01)
    await worker.stop()

    j = await queue2.get(job_id)
    assert j.status is JobStatus.SUCCEEDED
    assert j.result == "recovered-ok"
    assert attempts_seen == [2]  # 第一次认领是崩溃进程（未执行 handler），恢复后恰好执行 1 次
    await conn2.close()


async def test_concurrent_workers_only_one_advances_each_job(make_queue):
    """20 条任务 × 5 个并发执行者：每条任务恰好被执行一次。"""
    queue, conn, _ = await make_queue(base_delay=10, lease_seconds=100)
    execution_records: dict[str, int] = {}
    record_lock = asyncio.Lock()

    async def handler(payload: str) -> str:
        data = json.loads(payload)
        key = data["key"]
        async with record_lock:
            assert key not in execution_records, f"任务 {key} 被重复执行！"
            execution_records[key] = 1
        await asyncio.sleep(0)  # 让出事件循环，制造真实交错
        return "ok"

    reg = ExecutorRegistry()
    reg.register("demo.echo", handler)
    queue.known_task_types = {"demo.echo"}

    for i in range(20):
        await queue.submit("demo.echo", json.dumps({"key": f"k{i}"}), f"k{i}")

    workers = [Worker(queue, reg, worker_id=f"w{i}") for i in range(5)]
    final = await _drain(queue, reg, workers, {JobStatus.SUCCEEDED.value: 20})
    assert final.get(JobStatus.SUCCEEDED.value) == 20
    assert len(execution_records) == 20
    await conn.close()


async def test_stuck_lease_is_reclaimed_without_double_finish(make_queue):
    """任务被认领后执行者卡死（无回报）：租约过期后被别人安全接管，
    旧执行者迟到的成功回报不能再生效。"""
    queue, conn, _ = await make_queue(lease_seconds=0.05, base_delay=10)
    await queue.submit("demo.echo", "{}", "stuck")

    stuck_claim = await queue.claim_next("stuck-worker")
    assert stuck_claim is not None
    await asyncio.sleep(0.08)  # 租约过期

    rescue = await queue.claim_next("rescue-worker")
    assert rescue is not None and rescue.id == stuck_claim.id
    assert rescue.attempts == 2

    # 卡死的执行者突然“醒来”并回报 —— 必须被 fencing 拒绝
    stale_ok = await queue.complete(stuck_claim.id, "stuck-worker", "stale")
    assert stale_ok is False
    stale_fail = await queue.fail(
        stuck_claim.id, "stuck-worker", FailureType.TRANSIENT.value, "late"
    )
    assert stale_fail == JobStatus.RUNNING.value  # 回报无效，状态未改变

    j = await queue.get(stuck_claim.id)
    assert j.status is JobStatus.RUNNING and j.lease_owner == "rescue-worker"

    assert await queue.complete(stuck_claim.id, "rescue-worker", "rescued") is True
    j = await queue.get(stuck_claim.id)
    assert j.status is JobStatus.SUCCEEDED and j.result == "rescued"
    await conn.close()
