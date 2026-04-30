"""Cookie 使用模式枚举。"""

from __future__ import annotations

from enum import Enum


class CookieMode(str, Enum):
    """定义任务执行时的 Cookie 使用模式。

    说明:
        该枚举用于描述任务应如何处理 Cookie。

    枚举值:
        AUTO:
            根据任务输入自动推断。
        NONE:
            不使用 Cookie。
        PROVIDED:
            使用任务显式提供的 Cookie。
    """

    AUTO = "auto"
    NONE = "none"
    PROVIDED = "provided"
