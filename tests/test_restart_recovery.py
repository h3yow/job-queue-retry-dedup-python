"""进程重启恢复：执行中的任务不丢、不被重复执行完。"""

import asyncio
import time

from app.config import Settings
from app.queue import WorkerPool
from app.store import JobStore


async def test_running_job_recovered_after_restart_and_runs_once(tmp_path, caplog):
    db = str(tmp_path / "jobs.db")
    settings = Settings(
        db_path=db, worker_count=1, lease_seconds=0.4, max_retries=3,
        base_backoff_seconds=0.02, backoff_multiplier=2.0,
        max_backoff_seconds=0.1, poll_interval_seconds=0.01,
    )

    # 进程 A：连接、提交、启动 worker 并等待任务被认领（running），随后"硬终止"
    store_a = JobStore(db)
    await store_a.connect()
    job, _ = await store_a.create_job(
        type_="record_effect", payload={"label": "ship", "delay_before": 0.3},
        max_retries=3, idempotency_key="restart-1",
        now=time.time(), wall_now=0.0,
    )
    pool_a = WorkerPool(store_a, settings)
    await pool_a.start()
    deadline = asyncio.get_running_loop().time() + 3.0
    while asyncio.get_running_loop().time() < deadline:
        j = await store_a.get_job(job["id"])
        if j["status"] == "running" and j["leased_by"] is not None:
            break
        await asyncio.sleep(0.01)
    else:
        raise AssertionError("任务未被进程 A 认领")
    # 模拟进程崩溃：硬杀 worker 协程，running 记录原样遗留（不走优雅停机）
    await pool_a.crash()
    await store_a.close()  # 旧进程的连接随之消失

    # 进程 B：重新打开同一个 DB 并启动
    store_b = JobStore(db)
    await store_b.connect()
    mid = await store_b.get_job(job["id"])
    assert mid["status"] == "running" and mid["leased_by"] is not None, "崩溃前应为 running 遗留态"
    pool_b = WorkerPool(store_b, settings)
    await pool_b.start()

    try:
        deadline = asyncio.get_running_loop().time() + 5.0
        final = None
        while asyncio.get_running_loop().time() < deadline:
            final = await store_b.get_job(job["id"])
            if final["status"] == "succeeded":
                break
            await asyncio.sleep(0.02)
        assert final is not None and final["status"] == "succeeded", final
        # 副作用只允许一行（以 job_id 去重）
        effects = await store_b.list_effects()
        assert [e["job_id"] for e in effects] == [job["id"]]
        assert final["result"]["effect_count"] == 1
    finally:
        await pool_b.stop()
        await store_b.close()

    # 恢复动作必须在日志中可解释：到期后被重领一次
    assert any("claim job_id=" in r.message and "reclaimed=True" in r.message
               for r in caplog.records), [r.message for r in caplog.records]


async def test_restart_requeues_multiple_running_and_loses_none(tmp_path):
    db = str(tmp_path / "jobs.db")
    settings = Settings(
        db_path=db, worker_count=3, lease_seconds=5.0, max_retries=2,
        base_backoff_seconds=0.02, poll_interval_seconds=0.01,
    )
    s = JobStore(db)
    await s.connect()
    now = asyncio.get_running_loop().time()
    ids = []
    for i in range(3):
        j, _ = await s.create_job(
            type_="echo", payload={"i": i}, max_retries=2,
            idempotency_key=f"r-{i}", now=now, wall_now=0.0,
        )
        ids.append(j["id"])

    # 手工伪造"被死掉的进程认领但租约尚未到期"的崩溃现场
    async with s._tx() as conn:
        conn.execute(
            "UPDATE jobs SET status='running', leased_by='dead-proc', "
            "lease_expires_at=?, attempts=1 WHERE id IN (?,?,?)",
            (now - 1.0, *ids),
        )
    await s.close()  # 旧进程消失（甚至没等租约过期）

    # 新进程启动：不立即回收；到期租约由认领循环原子重领（reclaimed=True）
    s2 = JobStore(db)
    await s2.connect()
    pool = WorkerPool(s2, settings)
    await pool.start()
    try:
        deadline = asyncio.get_running_loop().time() + 5.0
        while asyncio.get_running_loop().time() < deadline:
            jobs = await s2.list_jobs()
            if all(j["status"] == "succeeded" for j in jobs):
                break
            await asyncio.sleep(0.02)
        jobs = {j["id"]: j for j in await s2.list_jobs()}
        assert all(jobs[i]["status"] == "succeeded" for i in ids), jobs
        assert all(jobs[i]["attempts"] == 2 for i in ids), "崩溃前 attempt=1，恢复后应再跑一次"
        assert len(await s2.list_effects()) == 0
    finally:
        await pool.stop()
        await s2.close()
