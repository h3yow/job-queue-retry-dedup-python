"""FastAPI 入口：装配存储、worker 池与 HTTP 路由。"""

import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app import api, handlers  # noqa: F401  导入以注册内置处理器
from app.config import get_settings
from app.logging_setup import get_logger, setup_logging
from app.queue import WorkerPool
from app.store import JobStore


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = app.state.settings
    logger = get_logger()
    db_dir = os.path.dirname(settings.db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    store = JobStore(settings.db_path)
    await store.connect()
    app.state.store = store
    pool = WorkerPool(store, settings)
    app.state.pool = pool
    await pool.start()
    try:
        yield
    finally:
        await pool.stop()
        await store.close()
        logger.info("shutdown complete")


def create_app(settings=None) -> FastAPI:
    settings = settings or get_settings()
    setup_logging()
    app = FastAPI(title="async-job-queue", lifespan=lifespan)
    app.state.settings = settings
    app.state.logger = get_logger()
    app.include_router(api.router)

    @app.exception_handler(RequestValidationError)
    async def _on_validation_error(request: Request, exc: RequestValidationError):
        return JSONResponse(
            status_code=422,
            content={
                "detail": {
                    "error_code": "invalid_submission",
                    "detail": "请求体不合法（缺字段/类型错误/JSON 格式错误）",
                    "errors": exc.errors(),
                }
            },
        )

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


app = create_app()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
