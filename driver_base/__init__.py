"""`driver_base` 包的公共导出。

这个目录提供多浏览器 worker、任务策略等基础模型与基类，
方便上层抓取器通过统一入口直接导入常用类型。

示例:
    from driver_base import MultiBrowserScraperBase, TaskPolicy

这样调用方无需分别从多个子模块导入对象。
"""

from .browser_worker import BrowserWorker
from .cookie_mode import CookieMode
from .failure_category import FailureCategory
from .multi_browser_scraper_base import MultiBrowserScraperBase
from .task_policy import TaskPolicy
from .worker_context import WorkerContext

__all__ = [
    "BrowserWorker",
    "CookieMode",
    "FailureCategory",
    "MultiBrowserScraperBase",
    "TaskPolicy",
    "WorkerContext",
]
