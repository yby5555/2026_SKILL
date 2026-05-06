"""flow_task_runtime 包的公开入口。

本目录是一套新的、自包含的视频任务消费实现。
它不依赖旧的 `flow/*.py` 或 `account_mgr/*.py` 业务脚本。
"""

from .config import RuntimeSettings, load_settings
from .consumer import consume_forever, main

__all__ = [
    "RuntimeSettings",
    "consume_forever",
    "load_settings",
    "main",
]
