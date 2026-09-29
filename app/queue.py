"""任务队列：幂等提交、原子认领、重试退避、死信与租约恢复。

关键一致性保证：
- 幂等键上有 UNIQUE 约束，重复提交在数据库层被挡下，绝不产生第二条任务；
- 认领使用单条条件 UPDATE ... WHERE 状态/到期/租约条件成立，
  SQLite 的写锁保证多个并发执行者中只有一个能真正抢到；
- 成功/失败回报都带租约持有者(lease_owner)校验，
  过期租约被别人重认领后，旧执行者的回报不会再生效（避免重复执行副作用）；
- 重试用尽后进入 FAILED（死信区），永久失败立即进入 FAILED，
  只有处理器正常返回后才会进入 SUCCEEDED。
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid

import aiosqlite

from app.models import FailureType, Job, JobStatus, RejectReason, SubmitError

logger = logging.getLogger("jobqueue.queue")

_JOB_COLUMNS = (
    "id, idempotency_key, task_type, payload, status, attempts, max_attempts, "
    "available_at, leased_at, lease_expires_at, lease_owner, last_error, "
    "last_failure_type, result, created_at, updated_at"
)


def _row_to_job(row: aiosqlite.Row) -> Job:
    return Job(
        id=row["id"],
        idempotency_key=row["idempotency_key"],
        task_type=row["task_type"],
        payload=row["payload"],
        status=JobStatus(row["status"]),
        attempts=row["attempts"],
        max_attempts=row["max_attempts"],
        available_at=row["available_at"],
        leased_at=row["leased_at"],
        lease_expires_at=row["lease_expires_at"],
        lease_owner=row["lease_owner"],
        last_error=row["last_error"],
        last_failure_type=(
            FailureType(row["last_failure_type"]) if row["last_failure_type"] else None
        ),
        result=row["result"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class JobQueue:
    def __init__(
        self,
        conn: aiosqlite.Connection,
        known_task_types: set[str] | None = None,
        max_attempts: int = 3,
        base_delay: float = 0.05,
        lease_seconds: float = 30.0,
        max_payload_bytes: int = 64 * 1024,
    ) -> None:
        self.conn = conn
        self.known_task_types = known_task_types or set()
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.lease_seconds = lease_seconds
        self.max_payload_bytes = max_payload_bytes
        # 认领/回报都是“读 + 条件写”，同一连接上用同一把锁串行化，
        # 避免协程交错导致事务重入或读写竞态。
        self._write_lock = asyncio.Lock()

    # ------------------------------------------------------------------ 提交

    def _validate_submit(self, task_type: str, payload: str) -> None:
        if not isinstance(task_type, str) or not task_type:
            raise SubmitError(
                RejectReason.UNKNOWN_TASK, f"task_type 为空: {task_type!r}"
            )
        if task_type not in self.known_task_types:
            raise SubmitError(
                RejectReason.UNKNOWN_TASK,
                f"未知任务类型 {task_type!r}，已注册: {sorted(self.known_task_types)}",
            )
        if not isinstance(payload, str):
            raise SubmitError(
                RejectReason.INVALID_PAYLOAD, "payload 必须是字符串(JSON)"
            )
        try:
            payload_bytes = len(payload.encode("utf-8"))
        except UnicodeEncodeError as exc:
            raise SubmitError(
                RejectReason.INVALID_PAYLOAD, f"payload 不是合法 UTF-8: {exc}"
            ) from exc
        if payload_bytes == 0:
            raise SubmitError(RejectReason.INVALID_PAYLOAD, "payload 不能为空")
        if payload_bytes > self.max_payload_bytes:
            raise SubmitError(
                RejectReason.PAYLOAD_TOO_LARGE,
                f"payload {payload_bytes} 字节超过上限 {self.max_payload_bytes}",
            )

    async def submit(
        self, task_type: str, payload: str, idempotency_key: str
    ) -> tuple[Job, bool]:
        """幂等提交。

        返回 (任务快照, created)：created=False 表示命中了同幂等键的既有任务，
        调用方据此向客户端返回可区分的“重复请求”结果。
        """
        self._validate_submit(task_type, payload)
        if not idempotency_key or not isinstance(idempotency_key, str):
            raise SubmitError(
                RejectReason.INVALID_PAYLOAD, "idempotency_key 不能为空"
            )

        existing = await self.get_by_key(idempotency_key)
        if existing is not None:
            logger.info(
                "提交去重命中 idempotency_key=%s -> 既有任务 id=%s status=%s",
                idempotency_key,
                existing.id,
                existing.status.value,
            )
            return existing, False

        now = time.time()
        job = Job(
            id=uuid.uuid4().hex,
            idempotency_key=idempotency_key,
            task_type=task_type,
            payload=payload,
            status=JobStatus.PENDING,
            attempts=0,
            max_attempts=self.max_attempts,
            available_at=now,
            leased_at=None,
            lease_expires_at=None,
            lease_owner=None,
            last_error=None,
            last_failure_type=None,
            result=None,
            created_at=now,
            updated_at=now,
        )
        try:
            await self.conn.execute(
                "INSERT INTO jobs (" + _JOB_COLUMNS + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    job.id,
                    job.idempotency_key,
                    job.task_type,
                    job.payload,
                    job.status.value,
                    job.attempts,
                    job.max_attempts,
                    job.available_at,
                    job.leased_at,
                    job.lease_expires_at,
                    job.lease_owner,
                    job.last_error,
                    None,
                    job.result,
                    job.created_at,
                    job.updated_at,
                ),
            )
            await self.conn.commit()
        except aiosqlite.IntegrityError:
            # 并发提交同一幂等键：另一个请求先插入了，回退为读取既有任务。
            await self.conn.rollback()
            existing = await self.get_by_key(idempotency_key)
            assert existing is not None
            logger.info(
                "并发去重竞争 idempotency_key=%s -> 既有任务 id=%s",
                idempotency_key,
                existing.id,
            )
            return existing, False

        logger.info(
            "任务已提交 id=%s type=%s key=%s status=%s",
            job.id,
            job.task_type,
            job.idempotency_key,
            job.status.value,
        )
        return job, True

    # ------------------------------------------------------------------ 认领

    async def claim_next(self, lease_owner: str) -> Job | None:
        """原子认领一条可执行任务。

        可执行条件（满足其一）：
        - PENDING 且 available_at <= now（含重试退避后到期的任务）；
        - RUNNING 但租约已过期（执行者崩溃/卡死，被安全重新认领）。

        先 BEGIN IMMEDIATE 取写锁，再在同一事务里“查候选 + 条件 UPDATE”，
        多个执行者并发时写锁串行化，只有一个能抢到同一任务。
        """
        now = time.time()
        async with self._write_lock:
            await self.conn.execute("BEGIN IMMEDIATE")
            try:
                rows = await self.conn.execute_fetchall(
                    "SELECT id, status FROM jobs WHERE "
                    " (status = ? AND available_at <= ?) "
                    " OR (status = ? AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?) "
                    " ORDER BY available_at LIMIT 1",
                    (
                        JobStatus.PENDING.value,
                        now,
                        JobStatus.RUNNING.value,
                        now,
                    ),
                )
                if not rows:
                    await self.conn.rollback()
                    return None
                candidate_id = rows[0]["id"]
                was_running = rows[0]["status"] == JobStatus.RUNNING.value

                cursor = await self.conn.execute(
                    "UPDATE jobs SET status=?, leased_at=?, lease_expires_at=?, "
                    "lease_owner=?, attempts=attempts+1, updated_at=? "
                    "WHERE id=? "
                    "RETURNING " + _JOB_COLUMNS,
                    (
                        JobStatus.RUNNING.value,
                        now,
                        now + self.lease_seconds,
                        lease_owner,
                        now,
                        candidate_id,
                    ),
                )
                row = await cursor.fetchone()
                await cursor.close()
            except BaseException:
                await self.conn.rollback()
                raise
            if row is None:  # 理论上不会发生（同一写事务内候选不会消失）
                await self.conn.rollback()
                return None
            await self.conn.commit()
        job = _row_to_job(row)
        logger.info(
            "任务已认领 id=%s owner=%s attempt=%d 来源=%s",
            job.id,
            lease_owner,
            job.attempts,
            "过期租约重认领" if was_running else "待执行队列",
        )
        return job

    # ------------------------------------------------------------------ 回报

    async def complete(self, job_id: str, lease_owner: str, result: str) -> bool:
        """标记成功。

        以 lease_owner 作为 fencing token 做条件 UPDATE：
        只有当前租约持有者能成功；旧执行者在租约被重认领后，其回报必然失效，
        因此重复执行的副作用不可能覆盖新执行者的结果。
        """
        now = time.time()
        async with self._write_lock:
            await self.conn.execute("BEGIN IMMEDIATE")
            try:
                cursor = await self.conn.execute(
                    "UPDATE jobs SET status=?, result=?, leased_at=NULL, "
                    "lease_expires_at=NULL, lease_owner=NULL, updated_at=? "
                    "WHERE id=? AND status=? AND lease_owner=?",
                    (
                        JobStatus.SUCCEEDED.value,
                        result,
                        now,
                        job_id,
                        JobStatus.RUNNING.value,
                        lease_owner,
                    ),
                )
                applied = cursor.rowcount == 1
                await cursor.close()
                await self.conn.commit()
            except BaseException:
                await self.conn.rollback()
                raise
        if applied:
            logger.info("任务成功 id=%s owner=%s", job_id, lease_owner)
        else:
            logger.warning(
                "成功回报被丢弃 id=%s stale_owner=%s（租约已失效或任务不在 running）",
                job_id,
                lease_owner,
            )
        return applied

    @staticmethod
    def backoff_delay(attempts: int, base_delay: float) -> float:
        """指数退避：第 1 次失败后等 base_delay*2^0，之后逐次翻倍。"""
        return base_delay * (2 ** max(0, attempts - 1))

    async def fail(
        self,
        job_id: str,
        lease_owner: str,
        failure_type: str,
        message: str,
    ) -> str:
        """记录一次失败，返回新状态值。

        与 complete 一样用 lease_owner fencing：过期执行者的失败回报直接丢弃。
        - PERMANENT：立即 FAILED（不再重试）；
        - TRANSIENT 且 attempts < max_attempts：回到 PENDING，按指数退避排期；
        - TRANSIENT 且重试已用尽：FAILED，保留最后一次失败原因。
        """
        ftype = FailureType(failure_type)
        now = time.time()

        async with self._write_lock:
            await self.conn.execute("BEGIN IMMEDIATE")
            try:
                # 先在 fencing 条件下读出当前 attempt 数（过期持有者读不到）。
                rows = await self.conn.execute_fetchall(
                    "SELECT attempts, max_attempts FROM jobs "
                    "WHERE id=? AND status=? AND lease_owner=?",
                    (job_id, JobStatus.RUNNING.value, lease_owner),
                )
                if not rows:
                    await self.conn.rollback()
                    logger.warning(
                        "失败回报被丢弃 id=%s stale_owner=%s type=%s（租约已失效或任务不在 running）",
                        job_id,
                        lease_owner,
                        ftype.value,
                    )
                    return JobStatus.RUNNING.value

                attempts = rows[0]["attempts"]
                max_attempts = rows[0]["max_attempts"]

                if ftype is FailureType.TRANSIENT and attempts < max_attempts:
                    delay = self.backoff_delay(attempts, self.base_delay)
                    await self.conn.execute(
                        "UPDATE jobs SET status=?, last_error=?, last_failure_type=?, "
                        "available_at=?, leased_at=NULL, lease_expires_at=NULL, "
                        "lease_owner=NULL, updated_at=? WHERE id=?",
                        (
                            JobStatus.PENDING.value,
                            message,
                            ftype.value,
                            now + delay,
                            now,
                            job_id,
                        ),
                    )
                    await self.conn.commit()
                    new_status = JobStatus.PENDING.value
                else:
                    # 永久失败，或临时失败但重试次数已用尽：进入失败区。
                    await self.conn.execute(
                        "UPDATE jobs SET status=?, last_error=?, last_failure_type=?, "
                        "leased_at=NULL, lease_expires_at=NULL, lease_owner=NULL, "
                        "updated_at=? WHERE id=?",
                        (JobStatus.FAILED.value, message, ftype.value, now, job_id),
                    )
                    await self.conn.commit()
                    new_status = JobStatus.FAILED.value
            except BaseException:
                await self.conn.rollback()
                raise

        if new_status is JobStatus.PENDING.value:
            logger.info(
                "任务临时失败 id=%s attempt=%d/%d -> pending，%.3fs 后重试: %s",
                job_id,
                attempts,
                max_attempts,
                delay,
                message,
            )
        else:
            logger.info(
                "任务进入失败区 id=%s attempts=%d/%d type=%s 最后原因: %s",
                job_id,
                attempts,
                max_attempts,
                ftype.value,
                message,
            )
        return new_status

    # ------------------------------------------------------------------ 查询

    async def get(self, job_id: str) -> Job | None:
        async with self.conn.execute(
            "SELECT " + _JOB_COLUMNS + " FROM jobs WHERE id=?", (job_id,)
        ) as cursor:
            row = await cursor.fetchone()
        return _row_to_job(row) if row is not None else None

    async def get_by_key(self, idempotency_key: str) -> Job | None:
        async with self.conn.execute(
            "SELECT " + _JOB_COLUMNS + " FROM jobs WHERE idempotency_key=?",
            (idempotency_key,),
        ) as cursor:
            row = await cursor.fetchone()
        return _row_to_job(row) if row is not None else None

    async def list_failed(self) -> list[Job]:
        async with self.conn.execute(
            "SELECT " + _JOB_COLUMNS + " FROM jobs WHERE status=? ORDER BY updated_at",
            (JobStatus.FAILED.value,),
        ) as cursor:
            rows = await cursor.fetchall()
        return [_row_to_job(r) for r in rows]

    # ------------------------------------------------------------------ 恢复

    async def recover_pending(self) -> int:
        """启动恢复：把上次进程残留的 RUNNING 任务标记为租约立即过期。

        这些任务来自进程被杀死/重启，不可能再有旧执行者回报；
        标记后 claim_next 会以“过期租约重认领”的方式安全重新执行一次，
        既不会丢，也不会在旧进程还活着时被重复执行。
        """
        now = time.time()
        cursor = await self.conn.execute(
            "UPDATE jobs SET lease_expires_at=?, lease_owner=NULL, updated_at=? "
            "WHERE status=? AND lease_expires_at IS NOT NULL",
            (now, now, JobStatus.RUNNING.value),
        )
        count = cursor.rowcount
        await cursor.close()
        await self.conn.commit()
        if count:
            logger.info("启动恢复：%d 个执行中任务的租约已置为过期，等待重认领", count)
        return count
