"""HTTP 路由。

- POST /jobs                       提交任务（支持 Idempotency-Key 去重）
- GET  /jobs/{id}                  查询单个任务
- GET  /jobs                       列表（可按 status 过滤）
- GET  /dead-letters               失败区（重试耗尽 / 永久失败）
"""

import time
from typing import Annotated, Any

from fastapi import APIRouter, Header, HTTPException, Query, Request

from app.handlers import get_handler
from app.models import JobResponse, SubmitJobRequest, SubmitJobResponse
from app.store import IdempotencyConflict

router = APIRouter(prefix="/api/v1", tags=["jobs"])


def _to_response(job: dict[str, Any]) -> JobResponse:
    ts = lambda t: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t)) if t else None
    return JobResponse(
        id=job["id"],
        type=job["type"],
        status=job["status"],
        attempts=job["attempts"],
        max_retries=job["max_retries"],
        result=job["result"],
        last_error=job["last_error"],
        error_kind=job["error_kind"],
        created_at=ts(job["created_ts"]),
        updated_at=ts(job["updated_ts"]),
        run_after=ts(job.get("run_after_ts") or job["created_ts"]),
        leased_by=job["leased_by"],
    )


def _http(status: int, code: str, detail: str | None = None) -> HTTPException:
    return HTTPException(status_code=status, detail={"error_code": code, "detail": detail})


@router.post("/jobs", response_model=SubmitJobResponse, status_code=201)
async def submit_job(
    request: Request,
    idempotency_key: Annotated[str | None, Header(max_length=128)] = None,
) -> SubmitJobResponse:
    settings = request.app.state.settings
    store = request.app.state.store

    # 先看大小：避免无意义地解析超大 JSON
    raw = await request.body()
    if len(raw) > settings.max_payload_bytes:
        raise _http(
            413, "payload_too_large",
            f"请求体超过 {settings.max_payload_bytes} 字节上限",
        )
    try:
        body = SubmitJobRequest.model_validate_json(raw)
    except ValueError:
        raise _http(422, "invalid_submission", "请求体不是合法的任务 JSON（缺字段/类型错误/语法错误）")

    if get_handler(body.type) is None:
        # 语义层面的提交拒绝（类型未注册）与格式错误区分开
        raise _http(400, "unknown_job_type", f"不支持的任务类型: {body.type}")

    try:
        job, deduplicated = await store.create_job(
            type_=body.type,
            payload=body.payload,
            max_retries=settings.max_retries,
            idempotency_key=idempotency_key,
            now=time.time(),
            wall_now=time.time(),
        )
    except IdempotencyConflict:
        raise _http(
            409, "idempotency_key_conflict",
            "相同 Idempotency-Key 对应了不同的任务内容",
        )

    request.app.state.logger.info(
        "submit job_id=%s type=%s dedup=%s key=%s",
        job["id"], body.type, deduplicated, idempotency_key,
    )
    return SubmitJobResponse(job=_to_response(job), deduplicated=deduplicated)


@router.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job(job_id: str, request: Request) -> JobResponse:
    job = await request.app.state.store.get_job(job_id)
    if job is None:
        raise _http(404, "job_not_found", f"任务不存在: {job_id}")
    return _to_response(job)


@router.get("/jobs", response_model=list[JobResponse])
async def list_jobs(
    request: Request,
    status: Annotated[list[str] | None, Query()] = None,
) -> list[JobResponse]:
    allowed = {"pending", "running", "succeeded", "retrying", "failed"}
    if status and not set(status) <= allowed:
        raise _http(400, "invalid_status_filter", f"合法状态: {sorted(allowed)}")
    jobs = await request.app.state.store.list_jobs(status)
    return [_to_response(j) for j in jobs]


@router.get("/dead-letters", response_model=list[JobResponse])
async def dead_letters(request: Request) -> list[JobResponse]:
    jobs = await request.app.state.store.list_jobs(("failed",))
    return [_to_response(j) for j in jobs]
