"""后台 worker 池。

每个 worker 在循环中原子认领一个任务后执行：
- 声明需要心跳的处理器：看门狗周期续租，租约失效（返回 False）则中止执行；
- 成功 → mark_succeeded（带 leased_by fencing）；
- 临时故障 → 按指数退避置为 retrying，超过 max_retries 则进失败区；
- 永久故障 → 直接 failed；
- 旧执行者的任何写回都会因 fencing 条件不匹配而落空，不会覆盖新执行者。
"""

import asyncio
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from app.config import Settings
from app.handlers import (
    JobContext,
    classify_error,
    get_handler,
)
from app.logging_setup import get_logger
from app.store import JobStore


def default_clock() -> float:
    # 用墙上时钟：调度时刻需要跨进程、跨重启可比（monotonic 各进程基准不同）。
    return time.time()


class WorkerPool:
    def __init__(
        self,
        store: JobStore,
        settings: Settings,
        *,
        clock: Callable[[], float] = default_clock,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._store = store
        self._settings = settings
        self._clock = clock
        self._sleep = sleep
        self._tasks: list[asyncio.Task[None]] = []
        self._stopping = asyncio.Event()
        # 被看门狗判定租约失效而主动中止的 job_id（这类 CancelledError 可吞掉，
        # worker 继续跑；来自 stop/crash 的外部取消必须上抛以结束 worker）
        self._lease_aborted: set[str] = set()
        self.instance_id = uuid.uuid4().hex[:8]

    async def start(self) -> None:
        # 注意：启动时不无条件清空 running 租约。
        # 多进程/多实例并发启动时，无法判断持有未过期租约的对端是"死掉的旧进程"
        # 还是"正在执行的同伴"；安全做法是等租约到期后由 claim_next 的
        # expired-running 分支原子重领（日志中标记 reclaimed=True）。
        self._stopping.clear()
        self._tasks = [
            asyncio.create_task(self._run(i), name=f"worker-{self.instance_id}-{i}")
            for i in range(self._settings.worker_count)
        ]
        get_logger().info(
            "worker_pool started instance=%s workers=%d",
            self.instance_id, self._settings.worker_count,
        )

    async def stop(self) -> None:
        self._stopping.set()
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
        self._tasks = []
        get_logger().info("worker_pool stopped instance=%s", self.instance_id)

    async def crash(self) -> None:
        """模拟进程被硬杀：立即取消所有 worker，不回写任何任务状态。

        running 记录（含 leased_by / 未到期租约）原样遗留在持久层，
        供重启恢复测试使用。
        """
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001,S110  崩溃模拟：吞掉 worker 内一切残留异常
                pass
        self._tasks = []
        get_logger().warning("worker_pool CRASHED instance=%s (running leases left behind)", self.instance_id)

    # ------------------------------------------------------------------
    async def _run(self, index: int) -> None:
        worker_id = f"{self.instance_id}-w{index}"
        log = get_logger()
        while not self._stopping.is_set():
            try:
                claimed = await self._store.claim_next(
                    worker_id, self._settings.lease_seconds, self._clock()
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("worker=%s claim_error", worker_id)
                await self._sleep(self._settings.poll_interval_seconds)
                continue
            if claimed is None:
                try:
                    await asyncio.wait_for(
                        self._stopping.wait(), timeout=self._settings.poll_interval_seconds
                    )
                except TimeoutError:
                    pass
                continue
            job, reclaimed = claimed
            await self._execute(worker_id, job, reclaimed)

    async def _execute(self, worker_id: str, job: dict[str, Any], reclaimed: bool) -> None:
        log = get_logger()
        jid, jtype = job["id"], job["type"]
        log.info(
            "claim job_id=%s type=%s worker=%s attempt=%d reclaimed=%s",
            jid, jtype, worker_id, job["attempts"], reclaimed,
        )
        entry = get_handler(jtype)
        if entry is None:
            await self._store.mark_failed(
                jid, worker_id,
                error=f"未知任务类型: {jtype}", error_kind="permanent", now=self._clock(),
            )
            log.error("fail job_id=%s worker=%s kind=permanent reason=unknown_type", jid, worker_id)
            return

        ctx = JobContext(
            job_id=jid,
            attempt=job["attempts"],
            record_effect=lambda label: self._store.record_effect(jid, label, self._clock()),
        )
        run = asyncio.create_task(entry.func(job["payload"], ctx))
        hb: asyncio.Task[None] | None = None
        if entry.heartbeat:
            hb = asyncio.create_task(self._heartbeat(worker_id, jid, run))

        try:
            result = await run
        except asyncio.CancelledError:
            if self._stopping.is_set() or jid not in self._lease_aborted:
                log.warning("cancel job_id=%s worker=%s reason=shutdown_or_crash", jid, worker_id)
                raise
            log.warning("abort job_id=%s worker=%s reason=lease_lost", jid, worker_id)
            self._lease_aborted.discard(jid)
            return
        except Exception as exc:  # noqa: BLE001  处理器异常必须兜底并按类型分流
            kind = classify_error(exc)
            await self._handle_error(worker_id, job, kind, f"{type(exc).__name__}: {exc}")
            return
        finally:
            if hb is not None:
                hb.cancel()
                try:
                    await hb
                except asyncio.CancelledError:
                    pass

        if await self._store.mark_succeeded(jid, worker_id, result, self._clock()):
            log.info("success job_id=%s worker=%s attempt=%d result=%r", jid, worker_id, job["attempts"], result)
        else:
            log.warning("success_write_lost job_id=%s worker=%s reason=lease_fenced", jid, worker_id)

    async def _heartbeat(self, worker_id: str, job_id: str, owner: asyncio.Task[Any]) -> None:
        interval = max(self._settings.lease_seconds / 3.0, 0.01)
        while not owner.done():
            await asyncio.sleep(interval)
            if owner.done():
                break
            ok = await self._store.heartbeat(
                job_id, worker_id, self._settings.lease_seconds, self._clock()
            )
            if not ok:
                get_logger().warning(
                    "lease_lost job_id=%s worker=%s -> cancelling execution", job_id, worker_id
                )
                self._lease_aborted.add(job_id)
                owner.cancel()
                return

    async def _handle_error(self, worker_id: str, job: dict, kind: str, message: str) -> None:
        log = get_logger()
        jid, now = job["id"], self._clock()
        if kind == "permanent" or job["attempts"] > job["max_retries"]:
            final_kind = "permanent" if kind == "permanent" else "retries_exhausted"
            if await self._store.mark_failed(
                jid, worker_id, error=message, error_kind=final_kind, now=now
            ):
                log.error(
                    "fail job_id=%s worker=%s kind=%s attempts=%d error=%s",
                    jid, worker_id, final_kind, job["attempts"], message,
                )
            return
        backoff = min(
            self._settings.base_backoff_seconds
            * self._settings.backoff_multiplier ** (job["attempts"] - 1),
            self._settings.max_backoff_seconds,
        )
        run_after = now + backoff
        if await self._store.mark_retry(
            jid, worker_id, error=message, error_kind="transient", run_after=run_after, now=now
        ):
            log.warning(
                "retry job_id=%s worker=%s attempt=%d backoff=%.3fs next_after=%.3f error=%s",
                jid, worker_id, job["attempts"], backoff, run_after, message,
            )
