"""FastAPI 入口：任务提交、状态查询、失败区查询与生命周期管理。

环境变量：
- JOBQUEUE_DB：SQLite 文件路径（默认 ./jobqueue.db）；
- JOBQUEUE_WORKERS：后台 Worker 数量（默认 1）。
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header
from pydantic import BaseModel, Field

from app.db import connect
from app.executors import (
    ExecutorRegistry,
    demo_echo,
    demo_flaky,
    demo_permanent_fail,
)
from app.models import Job, JobStatus, RejectReason, SubmitError
from app.queue import JobQueue
from app.worker import Worker

logger = logging.getLogger("jobqueue")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


class SubmitRequest(BaseModel):
    task_type: str = Field(..., description="任务类型，如 demo.echo")
    payload: str = Field(..., description="任务参数，UTF-8 字符串（建议 JSON）")
    idempotency_key: str | None = Field(
        None, description="幂等键；也可用 Idempotency-Key 请求头提供"
    )


class JobResponse(BaseModel):
    id: str
    idempotency_key: str
    task_type: str
    payload: str
    status: JobStatus
    attempts: int
    max_attempts: int
    available_at: float
    last_error: str | None
    last_failure_type: str | None
    result: str | None
    created_at: float
    updated_at: float

    @classmethod
    def from_job(cls, job: Job) -> "JobResponse":
        return cls(
            id=job.id,
            idempotency_key=job.idempotency_key,
            task_type=job.task_type,
            payload=job.payload,
            status=job.status,
            attempts=job.attempts,
            max_attempts=job.max_attempts,
            available_at=job.available_at,
            last_error=job.last_error,
            last_failure_type=(
                job.last_failure_type.value if job.last_failure_type else None
            ),
            result=job.result,
            created_at=job.created_at,
            updated_at=job.updated_at,
        )


class SubmitResponse(BaseModel):
    duplicated: bool
    message: str
    job: JobResponse


def create_app(
    db_path: str | None = None,
    worker_count: int | None = None,
    start_workers: bool = True,
    max_attempts: int = 3,
    base_delay: float = 0.05,
    lease_seconds: float = 30.0,
) -> FastAPI:
    """构造应用（工厂形式，便于测试指定临时数据库与参数）。"""
    db_path = db_path or os.environ.get("JOBQUEUE_DB", "jobqueue.db")
    worker_count = (
        worker_count
        if worker_count is not None
        else int(os.environ.get("JOBQUEUE_WORKERS", "1"))
    )

    executors = ExecutorRegistry()
    executors.register("demo.echo", demo_echo)
    executors.register("demo.flaky", demo_flaky)
    executors.register("demo.permanent_fail", demo_permanent_fail)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        conn = await connect(db_path)
        queue = JobQueue(
            conn,
            known_task_types=set(executors.known_types()),
            max_attempts=max_attempts,
            base_delay=base_delay,
            lease_seconds=lease_seconds,
        )
        recovered = await queue.recover_pending()
        logger.info("应用启动 db=%s 恢复任务数=%d", db_path, recovered)

        workers: list[Worker] = []
        if start_workers:
            for i in range(worker_count):
                w = Worker(queue, executors, worker_id=f"worker-{i + 1}")
                await w.start()
                workers.append(w)

        app.state.conn = conn
        app.state.queue = queue
        app.state.workers = workers
        try:
            yield
        finally:
            for w in workers:
                await w.stop()
            await conn.close()
            logger.info("应用已关闭 db=%s", db_path)

    app = FastAPI(title="异步任务队列服务", lifespan=lifespan)

    @app.exception_handler(SubmitError)
    async def submit_error_handler(request, exc: SubmitError):
        from fastapi.responses import JSONResponse

        status_code = 422 if exc.reason is RejectReason.PAYLOAD_TOO_LARGE else 400
        return JSONResponse(
            status_code=status_code,
            content={
                "error": "submit_rejected",
                "reason": exc.reason.value,
                "detail": exc.detail,
            },
        )

    @app.post("/jobs", response_model=SubmitResponse, status_code=201)
    async def submit_job(
        body: SubmitRequest,
        idempotency_key: str | None = Header(default=None),
    ) -> SubmitResponse:
        key = body.idempotency_key or idempotency_key
        if not key:
            raise SubmitError(
                RejectReason.INVALID_PAYLOAD,
                "必须提供 idempotency_key（请求体字段或 Idempotency-Key 请求头）",
            )
        job, created = await app.state.queue.submit(body.task_type, body.payload, key)
        resp = JobResponse.from_job(job)
        from fastapi.responses import JSONResponse

        return JSONResponse(
            status_code=201 if created else 200,
            content=SubmitResponse(
                duplicated=not created,
                message=(
                    "任务已创建"
                    if created
                    else "相同 idempotency_key 的任务已存在，返回既有任务，未重复执行"
                ),
                job=resp,
            ).model_dump(mode="json"),
        )

    @app.get("/jobs/{job_id}", response_model=JobResponse)
    async def get_job(job_id: str) -> JobResponse:
        from fastapi import HTTPException

        job = await app.state.queue.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="任务不存在")
        return JobResponse.from_job(job)

    @app.get("/jobs-by-key/{idempotency_key}", response_model=JobResponse)
    async def get_job_by_key(idempotency_key: str) -> JobResponse:
        from fastapi import HTTPException

        job = await app.state.queue.get_by_key(idempotency_key)
        if job is None:
            raise HTTPException(status_code=404, detail="任务不存在")
        return JobResponse.from_job(job)

    @app.get("/dead-letter", response_model=list[JobResponse])
    async def list_dead_letter() -> list[JobResponse]:
        jobs = await app.state.queue.list_failed()
        return [JobResponse.from_job(j) for j in jobs]

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
