"""运行期可配置参数。

所有参数均可通过环境变量 ``JOBQ_<大写字段名>`` 覆盖，
测试中一般直接构造 ``Settings`` 传入。
"""

import os
from dataclasses import dataclass, fields


@dataclass(frozen=True)
class Settings:
    db_path: str = "data/jobs.db"
    worker_count: int = 2
    # 租约：任务被认领后若超过该时长没有心跳/结果，视为僵死可重新认领
    lease_seconds: float = 30.0
    max_retries: int = 3
    base_backoff_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    max_backoff_seconds: float = 60.0
    poll_interval_seconds: float = 0.1
    max_payload_bytes: int = 8192


def get_settings() -> Settings:
    kwargs: dict[str, object] = {}
    for f in fields(Settings):
        env = os.environ.get(f"JOBQ_{f.name.upper()}")
        if env is None:
            continue
        current = f.default
        kwargs[f.name] = type(current)(env) if not isinstance(current, bool) else env == "1"
    return Settings(**kwargs)
