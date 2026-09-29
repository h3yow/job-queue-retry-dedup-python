"""多进程抢占测试用的独立 worker 进程入口。"""

import argparse
import asyncio

from app.config import Settings
from app.logging_setup import setup_logging
from app.queue import WorkerPool
from app.store import JobStore


async def main(db: str, seconds: float, workers: int, lease: float) -> None:
    setup_logging()
    settings = Settings(
        db_path=db, worker_count=workers, lease_seconds=lease, max_retries=2,
        base_backoff_seconds=0.02, backoff_multiplier=2.0,
        max_backoff_seconds=0.1, poll_interval_seconds=0.005,
    )
    store = JobStore(db)
    await store.connect()
    pool = WorkerPool(store, settings)
    await pool.start()
    await asyncio.sleep(seconds)
    await pool.stop()
    await store.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--seconds", type=float, default=2.0)
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--lease", type=float, default=0.5)
    args = ap.parse_args()
    asyncio.run(main(args.db, args.seconds, args.workers, args.lease))
