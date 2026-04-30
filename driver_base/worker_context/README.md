# `worker_context` — Worker 运行时状态快照

## 目录定位

`WorkerContext` 是一个**轻量级只读数据对象**，在每次任务执行时由调度层构造，并传递给以下钩子函数：

- `process_task(page, task_data, worker)`
- `initialize_context(context, task_data, worker)`
- `initialize_page(page, task_data, worker)`

它让业务层可以感知当前 Worker 的运行状态，而无需直接持有 `BrowserWorker` 实例。

---

## 文件

| 文件 | 说明 |
|------|------|
| `__init__.py` | `WorkerContext` 数据类定义 |

---

## 数据结构

```python
@dataclass(slots=True)
class WorkerContext:
    worker_id: int              # Worker 唯一编号
    browser_task_capacity: int  # 当前浏览器允许的最大并发 Context 数
    active_tasks: int           # 当前 Worker 正在处理的任务数（含本任务）
    tasks_since_recycle: int    # 自上次浏览器重建以来已执行的任务总数
    consecutive_failures: int   # 当前 Worker 连续失败的次数
```

---

## 字段说明

| 字段 | 典型用途 |
|------|---------|
| `worker_id` | 日志标记、调试定位 |
| `browser_task_capacity` | 判断当前 Worker 负载上限 |
| `active_tasks` | 感知并发压力，决定是否做额外等待 |
| `tasks_since_recycle` | 判断浏览器"新鲜度"，规避指纹老化问题 |
| `consecutive_failures` | 感知 Worker 健康状态，决定策略降级 |

---

## 与 `BrowserWorker` 分离的设计意图

| 原因 | 说明 |
|------|------|
| **降低耦合** | 业务钩子不依赖 `BrowserWorker` 的完整实现，只读取状态 |
| **最小权限** | 钩子函数无法通过 `WorkerContext` 修改 Worker 内部状态 |
| **易于测试** | 单元测试中可直接构造 `WorkerContext` 实例，无需 mock 整个 Worker |
| **易于扩展** | 未来新增状态字段时，只需修改此数据类，不影响调度层 |

---

## 使用示例

```python
async def process_task(self, page, task_data, worker: WorkerContext):
    # 根据 Worker 健康状态调整策略
    if worker.consecutive_failures >= 3:
        # Worker 已连续失败多次，使用更保守的等待策略
        await page.wait_for_timeout(3000)

    # 在日志中标记是哪个 Worker 执行的
    print(f"[Worker-{worker.worker_id}] 开始处理任务: {task_data['url']}")
    await page.goto(task_data["url"])
    return await page.title()
```
