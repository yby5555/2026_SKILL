"""worker 运行时上下文快照模型。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class WorkerContext:
    """传给任务钩子的 worker 运行时快照。

    说明:
        这是一个轻量级只读数据对象，用来把当前 worker 的关键状态
        传给 `process_task()`、`initialize_context()`、`initialize_page()`
        等钩子，避免这些钩子直接依赖完整的 `BrowserWorker` 实例。

    与 `BrowserWorker` 分离的原因:
        1. 降低业务层对底层执行器实现的耦合。
        2. 只暴露任务执行真正需要的状态信息。
        3. 让钩子函数更容易测试和扩展。

    属性:
        worker_id:
            当前 worker 的唯一编号。
        browser_task_capacity:
            当前 worker 所在浏览器允许并发处理的上下文数量。
        active_tasks:
            当前 worker 正在处理的任务数。
        tasks_since_recycle:
            自上次浏览器重建以来，该 worker 已执行的任务数。
        consecutive_failures:
            当前 worker 连续失败的次数。
    """

    worker_id: int
    browser_task_capacity: int
    active_tasks: int
    tasks_since_recycle: int
    consecutive_failures: int
