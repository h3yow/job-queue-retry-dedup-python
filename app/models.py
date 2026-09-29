"""HTTP 请求/响应模型与状态枚举。"""

from typing import Any, Literal

from pydantic import BaseModel, Field

# 任务生命周期状态：
#   pending   已提交，等待认领
#   running   已被某个执行者认领并持有有效租约
#   succeeded 成功（终态）
#   retrying  本次执行遇到可重试故障，等待退避后再次认领
#   failed    永久失败或重试次数用尽（进入失败区，终态）
JobStatus = Literal["pending", "running", "succeeded", "retrying", "failed"]

TERMINAL_STATUSES = frozenset({"succeeded", "failed"})


class SubmitJobRequest(BaseModel):
    type: str = Field(min_length=1, max_length=64, description="任务类型，对应处理器名")
    payload: dict[str, Any] = Field(default_factory=dict)


class JobResponse(BaseModel):
    id: str
    type: str
    status: JobStatus
    attempts: int
    max_retries: int
    result: Any | None = None
    last_error: str | None = None
    error_kind: str | None = None
    created_at: str
    updated_at: str
    run_after: str | None = None
    leased_by: str | None = None


class SubmitJobResponse(BaseModel):
    job: JobResponse
    # created   首次创建；duplicate 幂等键命中既有任务
    deduplicated: bool
    accepted: Literal[True] = True


class ErrorResponse(BaseModel):
    error: str
    error_code: str
    detail: str | None = None
