"""统一日志配置。

关键状态流转（提交 / 认领 / 成功 / 重试 / 永久失败 / 过期重领）都通过
``app.jobqueue`` 这个 logger 打出，便于测试与排障时还原任务的完整生命周期。
"""

import logging

LOGGER_NAME = "app.jobqueue"


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s [%(name)s] %(message)s",
                datefmt="%H:%M:%S",
            )
        )
        logger.addHandler(handler)
    logger.propagate = False
    return logger


def get_logger() -> logging.Logger:
    return logging.getLogger(LOGGER_NAME)
