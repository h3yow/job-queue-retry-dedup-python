"""后台轮询 Worker：认领 → 执行 → 回报成功/失败。

- 每次只认领一条任务，多个 Worker 并发时由队列层的原子 UPDATE 保证不重复执行；
- 回报带 lease_owner，租约过期后旧 Worker 的回报会被队列丢弃；
- 处理器抛 TaskExecutionError 按其 failure_type 处理，其他异常按临时故障重试。
"""
from __future__ import annotations

import asyncio
import logging
import uuid

from app.executors import ExecutorRegistry
from app.models import FailureType, TaskExecutionError
from app.queue import JobQueue

logger = logging.getLogger("jobqueue.worker")


class Worker:
    def __init__(
        self,
        queue: JobQueue,
        executors: ExecutorRegistry,
        worker_id: str | None = None,
        poll_interval: float = 0.05,
    ) -> None:
        self.queue = queue
        self.executors = executors
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.poll_interval = poll_interval
        self._task: asyncio.Task | None = None
        self._stopping = asyncio.Event()

    async def start(self) -> None:
        if self._task is None:
            self._stopping.clear()
            self._task = asyncio.create_task(self._loop(), name=f"worker-{self.worker_id}")
            logger.info("Worker 已启动 id=%s", self.worker_id)

    async def stop(self) -> None:
        logger.info("Worker 停止中 id=%s", self.worker_id)
        self._stopping.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._task = None

    async def _loop(self) -> None:
        while not self._stopping.is_set():
            try:
                processed = await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("Worker 轮询异常 id=%s", self.worker_id)
                processed = False
            if not processed:
                try:
                    await asyncio.wait_for(
                        self._stopping.wait(), timeout=self.poll_interval
                    )
                except TimeoutError:
                    pass

    async def run_once(self) -> bool:
        """认领并处理一条任务，返回是否处理了任务。"""
        job = await self.queue.claim_next(self.worker_id)
        if job is None:
            return False

        handler = self.executors.get(job.task_type)
        if handler is None:
            await self.queue.fail(
                job.id,
                self.worker_id,
                FailureType.PERMANENT.value,
                f"未注册的任务类型 {job.task_type!r}",
            )
            return True

        logger.info(
            "开始执行 id=%s type=%s attempt=%d owner=%s",
            job.id,
            job.task_type,
            job.attempts,
            self.worker_id,
        )
        try:
            result = await handler(job.payload)
        except TaskExecutionError as exc:
            await self.queue.fail(
                job.id, self.worker_id, exc.failure_type.value, exc.message
            )
        except Exception as exc:  # noqa: BLE001
            await self.queue.fail(
                job.id,
                self.worker_id,
                FailureType.TRANSIENT.value,
                f"未预期异常({type(exc).__name__}): {exc}",
            )
        else:
            await self.queue.complete(job.id, self.worker_id, result)
        return True
