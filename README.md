# 异步任务队列（FastAPI + SQLite）

一个真的能跑的异步任务处理服务：任务提交即落库，后台 worker 认领执行，支持
**幂等去重、指数退避重试、失败兜底（死信区）、进程重启恢复、多执行者抢占互斥**。
不依赖外部消息队列/Redis，仅用一个 SQLite 文件就保证上述一致性。

## 快速开始

```bash
uv sync
uv run python main.py            # 启动服务，默认 http://127.0.0.1:8000
# 或：uv run uvicorn main:app --reload
```

关键配置均可通过环境变量覆盖（`JOBQ_` 前缀）：

| 环境变量 | 默认值 | 含义 |
| --- | --- | --- |
| `JOBQ_DB_PATH` | `data/jobs.db` | SQLite 数据文件 |
| `JOBQ_WORKER_COUNT` | `2` | 单进程内 worker 协程数 |
| `JOBQ_LEASE_SECONDS` | `30` | 认领租约时长；超时无心跳即视为僵死，可被重领 |
| `JOBQ_MAX_RETRIES` | `3` | 最大重试次数（不含首次执行） |
| `JOBQ_BASE_BACKOFF_SECONDS` | `1` | 首次重试退避基数 |
| `JOBQ_BACKOFF_MULTIPLIER` | `2` | 退避增长倍数 |
| `JOBQ_MAX_BACKOFF_SECONDS` | `60` | 退避上限 |
| `JOBQ_MAX_PAYLOAD_BYTES` | `8192` | 请求体大小上限 |

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/jobs` | 提交任务。可用请求头 `Idempotency-Key` 去重 |
| GET | `/api/v1/jobs/{id}` | 查询单个任务 |
| GET | `/api/v1/jobs?status=pending&status=running` | 列表/按状态过滤 |
| GET | `/api/v1/dead-letters` | 失败区（终态 failed 的任务） |
| GET | `/health` | 健康检查 |

提交示例：

```bash
curl -X POST http://127.0.0.1:8000/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-1001' \
  -d '{"type":"flaky","payload":{"fail_times":2}}'
```

响应中 `deduplicated` 区分本次是否为重复请求：

- `false`：首次创建（HTTP 201）；
- `true`：幂等键命中已有任务，返回的是**同一个 job_id**，不会重复执行。

### 失败原因（可区分的错误码）

| HTTP | error_code | 触发条件 |
| --- | --- | --- |
| 422 | `invalid_submission` | JSON 非法、缺 `type`、字段类型错误 |
| 400 | `unknown_job_type` | `type` 没有对应处理器（格式正确但语义不支持）|
| 413 | `payload_too_large` | 请求体超过 `JOBQ_MAX_PAYLOAD_BYTES` |
| 409 | `idempotency_key_conflict` | 同一个 `Idempotency-Key` 绑定了不同的任务内容 |
| 404 | `job_not_found` | 查询不存在的任务 |
| 400 | `invalid_status_filter` | 列表过滤传入非法状态 |

## 任务状态流转

```
                 ┌──────────────────────────── 幂等键命中 ───────────────────────────┐
                 ▼                                                                   │
 POST /jobs ─► pending ──claim_next（原子 UPDATE）──► running ──执行成功──► succeeded(终态)
                 ▲                                      │
                 │                                      ├─ 临时故障 & 次数未用尽
                 │                                      ▼
                 └──────────────────────────────── retrying（run_after 到期后重新认领）
                                                        │
                                          临时故障且次数用尽 / 永久故障
                                                        ▼
                                                      failed(终态，进入 /dead-letters)
```

- `pending`：已落库等待执行；
- `running`：已被某个 worker 以租约（`leased_by` + `lease_expires_at`）认领；
- `retrying`：本次执行遇到可重试故障，退避到 `run_after` 之后才会再次被认领；
- `succeeded` / `failed`：终态。**只有处理器正常返回才会进入 succeeded**，未真正跑完的任务不会被标成功。

任务字段中的 `attempts` 是实际执行次数（首次为 1，每次认领 +1），`last_error`
保留最后一次失败的异常类型与信息，`error_kind` 标明失败性质。

## 重试与兜底策略

处理器抛错后按异常类型分流（见 `app/handlers.py`）：

- **临时故障（transient）**：抛 `TransientFailure`，或未知异常（保守按可重试处理）。
  任务置为 `retrying`，退避时间指数增长：

  ```
  backoff(n) = min(BASE * MULTIPLIER ** (attempts - 1), MAX_BACKOFF)
  ```

  在退避窗口内任务不会被任何 worker 认领，避免短时间反复冲击。
- **永久失败（permanent）**：抛 `PermanentFailure`，或 `KeyError/ValueError/TypeError`
  这类输入错误。**不再重试**，首次执行即进入失败区。
- **重试耗尽**：临时故障累计执行 `1 + MAX_RETRIES` 次仍失败，进入失败区，
  `error_kind=retries_exhausted`，并保留最后一次失败原因。
- 失败区可通过 `GET /api/v1/dead-letters` 查询，记录不会被自动删除。

内置任务类型：`echo`（原样返回）、`flaky`（前 N 次临时失败后成功）、
`transient_fail`（一直临时失败）、`permanent_fail`（永久失败）、
`bad_input`（缺字段，按永久失败处理）、`record_effect`（记录去重副作用，用于验证无重复执行）、
`stall`（执行卡死且不续租，用于验证租约重领）。

自定义处理器：

```python
from app.handlers import register, TransientFailure, PermanentFailure

@register("charge_order")
async def charge_order(payload, ctx):
    # ctx.job_id / ctx.attempt / await ctx.record_effect(label)
    ok = await downstream_charge(...)
    if not ok:
        raise TransientFailure("下游暂时不可用")   # 会重试
    return {"charged": True}
```

## 并发、抢占与重启正确性（核心机制）

所有正确性都建立在 SQLite 的单条原子更新与租约/fencing 之上（见 `app/store.py`）：

1. **原子认领**：worker 通过一条
   `UPDATE ... WHERE id=(SELECT ... ORDER BY ... LIMIT 1)` 抢占任务。
   多个协程、多个进程同时抢时，SQLite 的写锁保证只有一个 UPDATE 命中，
   同一个任务只可能被一个执行者推进。
2. **租约（lease）**：认领时写入 `leased_by` 与 `lease_expires_at`。
   - 处理器执行期间，看门狗按 `LEASE/3` 周期心跳续租；
   - 心跳发现租约已不属于自己（被他人抢走）时，立即 `cancel()` 当前执行，
     旧执行者的后续写回全部落空；
   - 任务"卡死"且不再续租（如进程被挂起、死循环）时，租约到期后
     其它 worker 可安全重新认领（认领日志中 `reclaimed=True`）。
3. **Fencing token**：成功/重试/失败的每一次状态回写都带
   `WHERE leased_by=<自己>` 条件。旧执行者即使晚到结果，也无法覆盖新执行者，
   日志表现为 `success_write_lost ... reason=lease_fenced`。
4. **重启恢复**：running 状态与租约都在磁盘上。新进程启动后，**只在租约过期后**
   原子重领这些任务——这是刻意的保守策略：无法区分持有未过期租约的对端是
   "死掉的旧进程"还是"正在执行的同伴"，提前回收会造成重复执行。任务既不会丢，
   也不会在旧进程里被重复执行完。
5. **副作用幂等**：`effects` 表以 `job_id` 为主键（`INSERT ... ON CONFLICT DO NOTHING`），
   测试用它断言一个任务无论被认领几次，外部副作用最多落一次。

## 本地验证

```bash
uv run pytest -v                 # 全量测试，实时打印关键状态流转日志
```

测试覆盖（见 `tests/`）：

- `test_dedup_submit.py`：重复幂等提交返回同一 job、不同内容 409、非法 JSON/缺字段、
  未知类型、超大 payload、无幂等键各自独立；
- `test_retry_failover.py`：临时故障按增长退避重试、用尽进死信区、永久失败不重试、
  输入错误归类、失败区保留最后原因、未跑完不得标成功；
- `test_restart_recovery.py`：执行中"进程崩溃"后新进程在租约到期时恢复并只执行一次、
  批量遗留 running 不丢失；
- `test_concurrency_claim.py`：多 worker 下每任务仅一次认领、僵死租约安全重领且
  旧结果被 fencing、心跳型任务被抢租约后看门狗主动中止；
- `test_multiprocess_claim.py`：**两个独立 OS 进程**的 worker 同时消费同一数据库，
  断言无重复执行、无重复副作用、无任务丢失。

日志中关键状态流转关键字：`submit` / `claim(reclaimed=...)` / `success` /
`retry(backoff=...)` / `fail(kind=...)` / `lease_lost` / `abort` /
`success_write_lost`。

手动验证重试与死信：

```bash
curl -X POST localhost:8000/api/v1/jobs -H 'Content-Type: application/json' \
  -d '{"type":"transient_fail"}'
watch -n0.5 'curl -s localhost:8000/api/v1/dead-letters'
```

## 目录结构

```
main.py               FastAPI 入口与 lifespan（装配 store/worker）
app/config.py         可调参数与环境变量
app/models.py         请求/响应模型、状态枚举
app/store.py          SQLite 持久化、原子认领、租约/fencing、副作用去重
app/handlers.py       处理器注册表、故障分类、内置任务类型
app/queue.py          WorkerPool：认领循环、心跳看门狗、重试/兜底
app/api.py            HTTP 路由与错误码
app/logging_setup.py  统一日志
tests/                去重 / 重试 / 重启 / 并发抢占 / 多进程 测试
```
