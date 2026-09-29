"""跨进程抢占：两个独立进程的 worker 同时消费同一 DB，任务不得被执行多次。"""

import asyncio
import os
import subprocess
import sys
import time

from app.store import JobStore

WORKER_SCRIPT = os.path.join(os.path.dirname(__file__), "_multiproc_worker.py")


async def test_two_processes_consume_jobs_without_duplicate_execution(tmp_path):
    db = str(tmp_path / "jobs.db")
    n = 12

    async with asyncio.timeout(20):
        # 预置任务：带轻微执行耗时，制造真正的争抢窗口
        s = JobStore(db)
        await s.connect()
        now = time.time()
        for i in range(n):
            await s.create_job(
                type_="record_effect",
                payload={"label": f"mp-{i}", "delay_before": 0.05},
                max_retries=2, idempotency_key=f"mp-{i}",
                now=now, wall_now=0.0,
            )
        await s.close()

        env = {**os.environ, "PYTHONPATH": os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}
        procs = [
            subprocess.Popen(  # noqa: ASYNC220  故意启动独立 OS 进程，且仅创建不阻塞等待
                [sys.executable, WORKER_SCRIPT, "--db", db, "--seconds", "3",
                 "--workers", "3", "--lease", "0.5"],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env,
                text=True,
            )
            for _ in range(2)
        ]
        outputs = []
        for p in procs:
            await asyncio.to_thread(p.wait)
            outputs.append(p.stdout.read() if p.stdout else "")
        for i, p in enumerate(procs):
            assert p.returncode == 0, f"worker 进程 {i} 异常退出:\n{outputs[i]}"

        s2 = JobStore(db)
        await s2.connect()
        try:
            jobs = await s2.list_jobs()
            assert len(jobs) == n
            assert all(j["status"] == "succeeded" for j in jobs), \
                [(j["id"], j["status"], j["last_error"]) for j in jobs]
            # 每个任务首轮就跑完，没有重领/重试
            assert all(j["attempts"] == 1 for j in jobs), \
                [j["attempts"] for j in jobs]
            # 副作用去重表：恰好每个任务一行
            effects = await s2.list_effects()
            assert len(effects) == n, [e["job_id"] for e in effects]
        finally:
            await s2.close()
