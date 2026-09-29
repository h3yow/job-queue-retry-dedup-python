"""任务处理器注册表与故障分类。

处理器约定：``async def handler(payload, ctx) -> result``
- 正常返回：结果会被持久化为任务成功结果；
- 抛 :class:`TransientFailure`：临时故障，按退避策略重试；
- 抛 :class:`PermanentFailure`：永久失败，不再重试，直接进失败区；
- ``KeyError/ValueError/TypeError`` 等输入类异常按永久失败处理；
- 其它未知异常保守地按可重试故障处理；
- ``asyncio.CancelledError`` 不吞掉（租约失效被中止时直接向上抛）。
"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")


class TransientFailure(Exception):
    """可重试的临时故障。"""


class PermanentFailure(Exception):
    """不该再重试的永久失败。"""


@dataclass
class JobContext:
    job_id: str
    attempt: int  # 1-based：第几次执行
    record_effect: Callable[[str], Awaitable[int]]


Handler = Callable[[dict[str, Any], JobContext], Awaitable[Any]]


@dataclass(frozen=True)
class HandlerEntry:
    func: Handler
    heartbeat: bool = True


_REGISTRY: dict[str, HandlerEntry] = {}


def register(name: str, *, heartbeat: bool = True) -> Callable[[Handler], Handler]:
    def deco(func: Handler) -> Handler:
        if name in _REGISTRY:
            raise RuntimeError(f"重复注册任务类型: {name}")
        _REGISTRY[name] = HandlerEntry(func=func, heartbeat=heartbeat)
        return func

    return deco


def get_handler(name: str) -> HandlerEntry | None:
    return _REGISTRY.get(name)


def classify_error(exc: BaseException) -> str:
    """返回 error_kind：permanent / transient。"""
    if isinstance(exc, PermanentFailure):
        return "permanent"
    if isinstance(exc, TransientFailure):
        return "transient"
    if isinstance(exc, (KeyError, ValueError, TypeError)):
        return "permanent"
    return "transient"


# ----------------------------------------------------------------内置任务类型


@register("echo")
async def echo(payload: dict[str, Any], ctx: JobContext) -> dict[str, Any]:
    """原样返回 payload，用于最基础的提交-成功链路。"""
    return {"echoed": payload, "attempt": ctx.attempt}


@register("flaky")
async def flaky(payload: dict[str, Any], ctx: JobContext) -> str:
    """前 ``fail_times`` 次执行抛临时故障，之后成功；用于验证重试链路。"""
    fail_times = int(payload.get("fail_times", 1))
    if ctx.attempt <= fail_times:
        raise TransientFailure(f"flaky 临时故障 attempt={ctx.attempt}/{fail_times}")
    return f"ok after {ctx.attempt} attempts"


@register("transient_fail")
async def transient_fail(payload: dict[str, Any], ctx: JobContext) -> None:
    """始终临时失败，直到重试用尽进入失败区。"""
    raise TransientFailure(f"下游不可用 attempt={ctx.attempt}")


@register("permanent_fail")
async def permanent_fail(payload: dict[str, Any], ctx: JobContext) -> None:
    """始终永久失败，首次执行即进失败区。"""
    raise PermanentFailure(f"参数/数据非法，不可重试 attempt={ctx.attempt}")


@register("bad_input")
async def bad_input(payload: dict[str, Any], ctx: JobContext) -> None:
    """缺字段，触发输入类异常 → 按永久失败处理。"""
    _ = payload["required_field"]


@register("record_effect")
async def record_effect(payload: dict[str, Any], ctx: JobContext) -> dict[str, int]:
    """记录一次外部副作用（以 job_id 去重），用于断言无重复执行。

    payload.delay_before：执行前先阻塞的秒数（测试用来制造"执行中崩溃"）。
    """
    delay = float(payload.get("delay_before", 0.0))
    if delay:
        await asyncio.sleep(delay)
    label = str(payload.get("label", "effect"))
    count = await ctx.record_effect(label)
    return {"effect_count": count, "attempt": ctx.attempt}


@register("stall", heartbeat=False)
async def stall(payload: dict[str, Any], ctx: JobContext) -> None:
    """执行到一半"卡死"且不续租：租约到期后应被其它执行者安全重领。"""
    await asyncio.Event().wait()  # 永远阻塞
