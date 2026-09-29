"""任务处理器注册表与内置演示处理器。

处理器是 async 函数：入参为提交时的 payload 字符串，正常返回结果字符串；
抛 TaskExecutionError 表示业务失败（transient/permanent），
抛其他异常一律按临时故障处理。
"""
from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable

from app.models import FailureType, TaskExecutionError

logger = logging.getLogger("jobqueue.executors")

TaskHandler = Callable[[str], Awaitable[str]]


class ExecutorRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, TaskHandler] = {}

    def register(self, task_type: str, handler: TaskHandler) -> None:
        if not task_type:
            raise ValueError("task_type 不能为空")
        self._handlers[task_type] = handler
        logger.info("已注册处理器 type=%s", task_type)

    def get(self, task_type: str) -> TaskHandler | None:
        return self._handlers.get(task_type)

    def known_types(self) -> list[str]:
        return sorted(self._handlers)


async def demo_echo(payload: str) -> str:
    """总是成功，回显 payload。"""
    return json.dumps({"echoed": json.loads(payload)}, ensure_ascii=False)


async def demo_flaky(payload: str) -> str:
    """按 payload.fail_times 前 N 次抛临时故障，之后成功。

    调用次数由处理器内部计数模拟（演示重试链路）。
    """
    data = json.loads(payload)
    fail_times = int(data.get("fail_times", 0))
    current = getattr(demo_flaky, "_calls", 0) + 1
    demo_flaky._calls = current  # type: ignore[attr-defined]
    if current <= fail_times:
        raise TaskExecutionError(
            FailureType.TRANSIENT, f"模拟临时故障 第{current}次"
        )
    return json.dumps({"ok": True, "attempts": current}, ensure_ascii=False)


async def demo_permanent_fail(payload: str) -> str:
    """永远永久失败：用于验证不该重试的场景。"""
    raise TaskExecutionError(FailureType.PERMANENT, "模拟永久失败：参数无法处理")
