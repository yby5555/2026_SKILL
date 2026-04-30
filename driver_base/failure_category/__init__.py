"""任务失败分类枚举。"""

from __future__ import annotations

from enum import Enum


class FailureCategory(str, Enum):
    """定义任务失败后的统一分类。

    说明:
        调度层会把执行异常映射到这些类别，用于决定代理处置、worker 回收和
        是否继续重试。

    枚举值:
        PROXY_ERROR:
            明确由代理连接或代理可用性引发的错误。
        BAN_ERROR:
            目标站点封禁、验证码或访问受限类错误。
        TASK_ERROR:
            业务逻辑本身导致的任务错误。
        WORKER_ERROR:
            浏览器 worker 或底层执行器状态异常。
        UNKNOWN:
            暂时无法准确识别的其他异常。
    """

    PROXY_ERROR = "proxy_error"
    BAN_ERROR = "ban_error"
    TASK_ERROR = "task_error"
    WORKER_ERROR = "worker_error"
    UNKNOWN = "unknown"
