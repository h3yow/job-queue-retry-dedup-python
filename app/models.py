"""领域模型：任务状态、失败类型、提交错误与异常。"""
from __future__ import annotations

import enum
from dataclasses import dataclass


class JobStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class FailureType(str, enum.Enum):
    """区分可重试的临时故障与不可重试的永久失败。"""

    TRANSIENT = "transient"
    PERMANENT = "permanent"


class RejectReason(str, enum.Enum):
    """提交被拒绝的可区分原因。"""

    INVALID_PAYLOAD = "invalid_payload"
    PAYLOAD_TOO_LARGE = "payload_too_large"
    UNKNOWN_TASK = "unknown_task"


class SubmitError(Exception):
    """提交被拒绝（客户端错误），携带可区分原因。"""

    def __init__(self, reason: RejectReason, detail: str = "") -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason.value}: {detail}")


class TaskExecutionError(Exception):
    """任务处理器抛出的业务失败。"""

    def __init__(self, failure_type: FailureType, message: str) -> None:
        self.failure_type = failure_type
        self.message = message
        super().__init__(f"{failure_type.value}: {message}")


@dataclass(frozen=True)
class Job:
    """一条任务记录的完整快照。"""

    id: str
    idempotency_key: str
    task_type: str
    payload: str
    status: JobStatus
    attempts: int
    max_attempts: int
    available_at: float
    leased_at: float | None
    lease_expires_at: float | None
    lease_owner: str | None
    last_error: str | None
    last_failure_type: FailureType | None
    result: str | None
    created_at: float
    updated_at: float
