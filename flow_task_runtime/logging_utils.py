"""flow_task_runtime 的日志工具。"""

from __future__ import annotations

import logging
import logging.handlers
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import SHANGHAI_TZ_NAME

SHANGHAI_TZ = timezone(timedelta(hours=8), name=SHANGHAI_TZ_NAME)


def now_local() -> datetime:
    """返回当前上海时区时间。

    返回:
        datetime: 带上海时区信息的当前时间。
    """
    return datetime.now(SHANGHAI_TZ)


def get_logger(name: str, log_file: Path) -> logging.Logger:
    """创建或获取一个滚动文件日志器。

    参数:
        name: 日志器名称。
        log_file: 日志文件落盘路径。

    返回:
        logging.Logger: 可复用的日志器对象。
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False
    return logger
