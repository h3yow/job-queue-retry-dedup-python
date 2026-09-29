"""骨架冒烟测试（实现填充后扩充）。"""
import pytest


@pytest.mark.asyncio
async def test_skeleton_imports():
    from app.models import JobStatus
    from app.queue import JobQueue

    assert JobStatus.PENDING.value == "pending"
    assert JobQueue is not None
