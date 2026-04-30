"""任务执行策略数据模型。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..cookie_mode import CookieMode


@dataclass(slots=True)
class TaskPolicy:
    """表示单个任务解析后的标准执行策略。

    说明:
        调度层会先从松散的 `task_data` 中提取出代理、Cookie、重试和耗时等
        配置，再整理成这个结构化对象，供后续执行流程统一使用。

    属性:
        proxy:
            当前任务使用的代理配置。
        cookies:
            原始 Cookie 输入，可为字符串、字典或 Cookie 列表。
        cookie_mode:
            当前任务的 Cookie 处理模式。
        max_retries:
            当前任务允许的最大重试次数。
        retry_unknown_errors:
            是否重试未知错误。
        retry_task_errors:
            是否重试业务任务错误。
    """

    proxy: str | dict[str, Any] | None = None
    cookies: str | dict[str, str] | list[dict[str, Any]] | None = None
    cookie_mode: CookieMode = CookieMode.AUTO
    max_retries: int = 1
    retry_unknown_errors: bool = False
    retry_task_errors: bool = False
