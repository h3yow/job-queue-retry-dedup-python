# 异步任务队列服务（FastAPI + SQLite）

一套可在本地真实运行的异步任务处理能力，覆盖**提交去重、后台执行、失败重试、死信兜底、
重启恢复、并发抢占**整条链路。任务状态持久化在 SQLite（WAL 模式），
进程重启与多执行者并发下结果一致、可解释。

## 1. 运行

```bash
uv sync
uv run python main.py
# 自定义：JOBQUEUE_DB=/tmp/jobs.db JOBQUEUE_WORKERS=2 uv run python main.py
```

- `JOBQUEUE_DB`：SQLite 文件路径，默认 `./jobqueue.db`
- `JOBQUEUE_WORKERS`：后台执行者数量，默认 `1`

## 2. API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/jobs` | 提交任务（必须带幂等键） |
| `GET` | `/jobs/{id}` | 按任务 ID 查询状态 |
| `GET` | `/jobs-by-key/{idempotency_key}` | 按幂等键查询任务 |
| `GET` | `/dead-letter` | 查询失败区（重试用尽 / 永久失败） |
| `GET` | `/health` | 健康检查 |

### 提交与去重规则

```bash
curl -s -X POST http://127.0.0.1:8000/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-1001-pay' \
  -d '{"task_type":"demo.echo","payload":"{\"order_id\":1001}"}'
```

- 幂等键放在请求体 `idempotency_key` 字段或 `Idempotency-Key` 请求头均可。
- **同一幂等键重复提交**：数据库 `UNIQUE(idempotency_key)` 约束兜底，绝不产生第二条任务，
  也绝不产生第二次执行副作用。返回 HTTP **200** 且 `duplicated=true`；首次提交返回 **201**
  且 `duplicated=false`。两种结果据此区分。
- **被拒绝的提交**（HTTP 4xx，响应体含 `error=submit_rejected` 与可区分 `reason`）：
  - `400 invalid_payload`：缺少幂等键、payload 为空或非字符串；
  - `400 unknown_task`：任务类型未注册；
  - `422 payload_too_large`：payload 超过 64KB 上限。

### 状态流转

```
PENDING ──认领(原子UPDATE, attempts+1, 发租约)──▶ RUNNING
RUNNING ──处理器正常返回 + 租约校验通过────────▶ SUCCEEDED   （终态）
RUNNING ──临时失败且还有重试额度──▶ PENDING（available_at=now+退避）
RUNNING ──永久失败 / 临时失败且重试已用尽──────▶ FAILED      （终态，进死信区）
```

- 只有处理器**正常返回**后才会写 `SUCCEEDED`，没真正跑完的任务不可能被标成功。
- `GET /dead-letter` 可查所有 FAILED 任务，记录里保留 `last_error`（最后一次失败原因）
  与 `last_failure_type`（`transient` / `permanent`）。

### 内置演示任务

| task_type | 行为 |
| --- | --- |
| `demo.echo` | 总是成功，回显 payload |
| `demo.flaky` | payload `{"fail_times":2}`：前 2 次抛临时故障，第 3 次成功，用于演示重试 |
| `demo.permanent_fail` | 永远抛永久失败，用于验证不重试、直入死信区 |

## 3. 重试与兜底策略

- 处理器抛 `TaskExecutionError(FailureType.TRANSIENT, ...)`（或任意未预期异常）视为
  **可重试的临时故障**；抛 `TaskExecutionError(FailureType.PERMANENT, ...)` 视为
  **不该再重试的永久失败**，立即 FAILED。
- **指数退避**：第 n 次临时失败后等待 `base_delay * 2^(n-1)`
  （生产默认 0.05s，测试中调到 0.005~0.02s 便于快速验证），退避期内任务不会被认领，
  避免短时间反复冲击同一任务。
- 默认 `max_attempts=3`（含首次执行）。重试用尽进入 FAILED 死信区，可查询、保留最后原因。

## 4. 重启恢复与并发正确性

- **原子认领**：认领是单条条件
  `UPDATE ... WHERE (status=PENDING AND available_at<=now) OR (status=RUNNING AND 租约已过期)`。
  SQLite 写锁保证多个执行者同时抢时**只有一个**能推进同一任务。
- **租约 + fencing token**：认领时写入 `lease_owner` 与租约到期时间。成功/失败回报都带
  `WHERE lease_owner=?` 条件更新；执行者卡死导致租约过期、任务被别人重认领后，
  旧执行者迟到的回报必然失效（结果不会覆盖新执行者，杜绝“重复执行完”）。
- **卡死重认领**：任务被认领后超过租约期（默认 30s）仍无结果，会被当作过期租约安全重认领。
- **重启恢复**：服务启动时执行 `recover_pending()`，把上次进程残留的 RUNNING 任务
  （旧进程已死，不可能再回报）租约置为过期并清空旧 owner，随后正常认领再执行一次：
  任务既不丢，也不会在旧进程还活着时被重复执行。

## 5. 本地验证

```bash
uv run pytest -q                      # 全量 20 个用例，日志实时打印关键状态流转
uv run pytest tests/test_queue.py -q  # 仅队列层
uv run pytest tests/test_api.py -q    # 仅 API + Worker 集成
uv run pytest tests/test_scenarios.py -q  # 重启恢复 / 并发抢占 / 去重副作用
```

测试覆盖：

1. **重复提交**：串行与 10 路并发提交同一幂等键，断言只产生一行记录、handler 副作用只发生一次，
   且 `duplicated` 标志与 201/200 状态码可区分；
2. **失败区分**：`invalid_payload` / `unknown_task` / `payload_too_large` 三种拒绝原因，
   transient 退避重试后成功、permanent 立即失败、重试用尽入死信区并保留最后原因；
3. **重启恢复**：任务 RUNNING 时直接关闭连接模拟进程被杀，新进程打开同一数据库恢复后
   任务恰好再执行一次并成功，状态不丢、不重复执行完；
4. **并发抢占**：8 路并发认领同一任务仅 1 个赢家；20 任务 × 5 个并发 worker 各执行恰好一次；
   租约过期后重认领，旧 worker 迟到的成功/失败回报均被 fencing 拒绝。

代码结构：

```
app/models.py    状态/失败类型/拒绝原因/异常/Job 快照
app/db.py        SQLite 连接(WAL)与建表
app/queue.py     幂等提交、原子认领、退避重试、死信、租约 fencing、启动恢复
app/executors.py 处理器注册表与演示处理器
app/worker.py    后台轮询执行者
main.py          FastAPI 路由与生命周期
tests/           queue / api / scenarios 三层测试
```
