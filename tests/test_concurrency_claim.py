"""并发抢占：同一任务只能被一个执行者推进；僵死租约安全重领；fencing 防旧写覆盖。"""

import asyncio
import time

from app.config import Settings
from app.queue import WorkerPool
from app.store import JobStore


def stall_settings(db: str, *, workers: int, lease: float = 0.3) -> Settings:
    return Settings(
        db_path=db, worker_count=workers, lease_seconds=lease, max_retries=3,
        base_backoff_seconds=0.02, backoff_multiplier=2.0,
        max_backoff_seconds=0.1, poll_interval_seconds=0.01,
    )


async def test_only_one_worker_claims_each_job(tmp_path, caplog):
    """多 worker + 多任务：每个任务恰好被 claim 一次（首轮无重领）。"""
    db = str(tmp_path / "jobs.db")
    settings = stall_settings(db, workers=4)
    s = JobStore(db)
    await s.connect()
    now = time.time()
    n = 8
    for i in range(n):
        await s.create_job(
            type_="echo", payload={"i": i}, max_retries=2,
            idempotency_key=f"c-{i}", now=now, wall_now=0.0,
        )
    pool = WorkerPool(s, settings)
    await pool.start()
    try:
        deadline = asyncio.get_running_loop().time() + 5.0
        while asyncio.get_running_loop().time() < deadline:
            jobs = await s.list_jobs()
            if all(j["status"] == "succeeded" for j in jobs):
                break
            await asyncio.sleep(0.02)
        jobs = await s.list_jobs()
        assert len(jobs) == n
        assert all(j["status"] == "succeeded" and j["attempts"] == 1 for j in jobs)
        # 每个任务有且只有一行 claim 日志，且没有任何重领
        claim_lines = [r.message for r in caplog.records if r.message.startswith("claim job_id=")]
        assert len(claim_lines) == n, claim_lines
        assert all("reclaimed=False" in m for m in claim_lines)
    finally:
        await pool.stop()
        await s.close()


async def test_stalled_lease_is_reclaimed_and_finishes_once(tmp_path, caplog):
    """stall 任务不续租，租约到期后由另一 worker 重领；用 flaky+延迟的等价任务验证完成。

    为能观察"重领后跑完"，这里使用 record_effect + delay_before 制造执行窗口，
    并用不续租的方式让首个执行者的租约过期：注册一个 no-heartbeat 且慢的任务类型。
    """
    from app import handlers

    @handlers.register("slow_no_hb_x", heartbeat=False)
    async def slow_no_hb(payload, ctx):
        # 不续租：首次执行长时间卡死；重领后的执行(ctx.attempt>1)快速完成。
        # 无论哪个执行者，外部副作用只允许记录一次。
        await asyncio.sleep(payload["sleep"] if ctx.attempt == 1 else 0.02)
        await ctx.record_effect("late")
        return "late-success"

    db = str(tmp_path / "jobs.db")
    settings = stall_settings(db, workers=3, lease=0.25)
    s = JobStore(db)
    await s.connect()
    now = time.time()
    job, _ = await s.create_job(
        type_="slow_no_hb_x", payload={"sleep": 1.0}, max_retries=0,
        idempotency_key="stall-1", now=now, wall_now=0.0,
    )
    pool = WorkerPool(s, settings)
    await pool.start()
    try:
        # 等待：w1 认领(attempt1) → 租约过期 → w2 重领(attempt2) → w2 先于 w1 完成(0.15s)
        deadline = asyncio.get_running_loop().time() + 6.0
        final = None
        while asyncio.get_running_loop().time() < deadline:
            final = await s.get_job(job["id"])
            if final["status"] == "succeeded":
                break
            await asyncio.sleep(0.02)
        assert final is not None and final["status"] == "succeeded", final
        assert final["attempts"] == 2, "应经历一次重领"
        # 等 w1 的"晚到结果"也跑完，确认旧执行者没有把状态/结果改坏
        await asyncio.sleep(1.0)
        after = await s.get_job(job["id"])
        assert after["status"] == "succeeded"
        assert after["result"] == "late-success"
        # 副作用行只有一条（record_effect 以 job_id 去重，INSERT OR IGNORE）
        effects = await s.list_effects()
        assert len(effects) == 1 and effects[0]["job_id"] == job["id"]

        claim_lines = [r.message for r in caplog.records if r.message.startswith("claim job_id=")]
        assert any("reclaimed=True" in m for m in claim_lines), claim_lines
        # 旧执行者的晚到结果被 fencing 拒绝，不得覆盖新执行者
        assert any("success_write_lost" in r.message for r in caplog.records), \
            [r.message for r in caplog.records if "write_lost" in r.message]
    finally:
        await pool.stop()
        await s.close()
    handlers._REGISTRY.pop("slow_no_hb_x", None)


async def test_heartbeat_worker_aborts_after_lease_stolen(tmp_path, caplog):
    """带心跳的慢任务：租约被他人从外部抢走后，心跳续租失败 → 看门狗中止旧执行。"""
    from app import handlers

    @handlers.register("slow_hb_x", heartbeat=True)
    async def slow_hb(payload, ctx):
        await asyncio.sleep(5.0)  # 正常心跳下一直跑；被抢走后应被中止
        await ctx.record_effect("hb")
        return "ok"

    db = str(tmp_path / "jobs.db")
    settings = stall_settings(db, workers=1, lease=0.3)  # 心跳间隔 0.1s
    s = JobStore(db)
    await s.connect()
    now = time.time()
    job, _ = await s.create_job(
        type_="slow_hb_x", payload={}, max_retries=0,
        idempotency_key="hb-1", now=now, wall_now=0.0,
    )
    pool = WorkerPool(s, settings)
    await pool.start()
    try:
        # 等 w0 认领并进入执行
        deadline = asyncio.get_running_loop().time() + 3.0
        while asyncio.get_running_loop().time() < deadline:
            j = await s.get_job(job["id"])
            if j["status"] == "running" and j["leased_by"]:
                break
            await asyncio.sleep(0.01)
        thief = "thief-node"
        loop_now = time.time()
        # 外部"抢走"租约：换持有者（旧 worker 的心跳 UPDATE 将 0 行命中）
        async with s._tx() as conn:
            conn.execute(
                "UPDATE jobs SET leased_by=?, lease_expires_at=? WHERE id=?",
                (thief, loop_now + 999, job["id"]),
            )
        # 等待旧执行者被看门狗中止
        deadline = loop_now + 3.0
        while asyncio.get_running_loop().time() < deadline:
            if any("lease_lost" in r.message and job["id"] in r.message for r in caplog.records):
                break
            await asyncio.sleep(0.02)
        assert any("lease_lost" in r.message for r in caplog.records)
        assert any("abort job_id=" in r.message for r in caplog.records)
        await asyncio.sleep(0.3)
        # 任务未被旧执行者标记成功，且无副作用落库
        j = await s.get_job(job["id"])
        assert j["status"] == "running" and j["leased_by"] == thief
        assert await s.list_effects() == []
    finally:
        await pool.stop()
        await s.close()
    handlers._REGISTRY.pop("slow_hb_x", None)
