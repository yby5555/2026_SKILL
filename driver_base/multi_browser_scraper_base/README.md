# `multi_browser_scraper_base` — 多浏览器抓取调度基类

## 目录定位

`MultiBrowserScraperBase` 是整个框架的**调度层与门面**，也是**子类唯一需要继承的入口**。

它负责：
- 创建并管理由 N 个 `BrowserWorker` 组成的浏览器池
- 并发调度任务到空闲 Worker
- 统一处理 Cookie 装载、代理解析、浏览器回收触发
- 提供一系列可覆写的钩子方法供子类扩展

---

## 文件

| 文件 | 说明 |
|------|------|
| `__init__.py` | `MultiBrowserScraperBase` 类的完整实现 |

---

## 初始化参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `browser_pool_size` | `2` | 浏览器进程数（Worker 数量） |
| `max_contexts_per_browser` | `5` | 单浏览器最大并发 Context 数 |
| `headless` | `True` | 是否无头模式 |
| `locale` | `"en-US"` | 浏览器语言 |
| `timezone_id` | `"Asia/Shanghai"` | 时区 |
| `user_agent` | Chrome 124 UA | 默认 User-Agent |
| `navigation_timeout_ms` | `10_000` | 导航与操作超时（毫秒） |
| `extra_flags` | `None` | 额外浏览器启动参数 |
| `viewport` | `1366×900` | 视口尺寸 |
| `default_cookies` | `None` | 全局默认 Cookie |
| `default_cookie_domain` | `".tiktok.com"` | Cookie 注入的默认域 |
| `default_proxy` | `None` | 全局默认代理 |
| `solve_cloudflare` | `False` | 是否启用 CF 挑战解决 |
| `block_webrtc` | `True` | 是否屏蔽 WebRTC 泄露 |
| `hide_canvas` | `True` | 是否混淆 Canvas 指纹 |
| `recycle_browser_after_tasks` | `200` | 执行 N 个任务后回收浏览器 |
| `recycle_browser_after_failures` | `5` | 连续失败 N 次后回收浏览器 |

---

## 子类必须实现的方法

```python
async def process_task(
    self,
    page: Page,
    task_data: dict,
    worker: WorkerContext,
) -> Any:
    """执行实际的抓取业务逻辑。"""
    ...
```

---

## 可选覆写的钩子方法

| 方法 | 触发时机 | 默认行为 |
|------|----------|----------|
| `normalize_task(task_data)` | 任务进入调度前 | 浅拷贝 task_data |
| `resolve_task_proxy(task_data)` | 创建 Context 前 | 取 `task_data["proxy"]` 或 `default_proxy` |
| `build_launch_options(worker)` | 浏览器首次启动 | 返回标准反检测启动参数 |
| `build_context_options(worker, proxy)` | 创建 Context 时 | 返回含 locale/UA/视口的标准参数 |
| `initialize_context(context, task_data, worker)` | Context 创建后 | 空实现，返回 context |
| `initialize_page(page, task_data, worker)` | Page 创建后 | 空实现 |

---

## 关键内部方法

| 方法 | 说明 |
|------|------|
| `start()` | 初始化 Worker 池（幂等） |
| `close()` | 关闭所有 Worker |
| `run_tasks(tasks)` | 并发执行任务列表，返回结果（含异常对象） |
| `_run_single_task(task_data)` | 单任务完整生命周期（获取Worker → 执行 → 释放） |
| `_acquire_worker()` | 从池中挑选最优 Worker，满负载时阻塞等待 |
| `_release_worker(worker)` | 释放 Worker，触发按需回收检查 |
| `_build_cookie_payload(task_data)` | Cookie → Playwright storage_state 格式转换 |
| `_build_worker_context(worker)` | 构造 WorkerContext 快照传给钩子 |

---

## Worker 调度算法

`_acquire_worker()` 使用**多维最小值**策略：

```python
worker = min(
    available_workers,  # 过滤：active_tasks < max_contexts
    key=lambda w: (
        active_tasks_count,     # ① 优先选负载最低的
        consecutive_failures,   # ② 其次避免频繁失败的
        tasks_since_recycle,    # ③ 再次选最"新鲜"的
        worker_id,              # ④ 最后按 ID 打平
    ),
)
```

Worker 满负载时，协程在 `asyncio.Condition.wait()` 上阻塞，有 Worker 释放时广播唤醒。

---

## 使用示例

```python
from driver_base import MultiBrowserScraperBase, WorkerContext
from playwright.async_api import Page


class TikTokScraper(MultiBrowserScraperBase):
    async def process_task(self, page: Page, task_data: dict, worker: WorkerContext):
        await page.goto(task_data["url"])
        return await page.title()


async def main():
    tasks = [{"url": f"https://www.tiktok.com/@user{i}"} for i in range(50)]
    async with TikTokScraper(
            browser_pool_size=3,
            max_contexts_per_browser=5,
            default_proxy="http://user:pass@proxy:8080",
    ) as scraper:
        results = await scraper.run_tasks(tasks)

    # results 中 Exception 实例表示该任务失败
    for r in results:
        if isinstance(r, Exception):
            print(f"失败: {r}")
```

---

## 注意事项

> **`run_tasks` 使用 `return_exceptions=True`**: 任务失败不会中断其他任务，异常以对象形式出现在结果列表中，调用方需自行检查。

> **`close()` 不等待运行中的任务**: 直接退出 `async with` 块时，正在执行的任务可能被强制中断。
