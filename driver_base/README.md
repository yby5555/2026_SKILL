# `new_zdh` — 多浏览器抓取框架基类

## 目录定位

`new_zdh` 是一个通用的 **多浏览器 Worker 调度与抓取基础框架**，封装了浏览器池管理、Context/Page 生命周期、Cookie 注入、反检测与失败回收等通用能力。

上层业务抓取器只需继承 `MultiBrowserScraperBase`，并实现 `process_task()` 方法，即可获得完整的并发浏览器调度能力。

---

## 包结构

```
new_zdh/
├── README.md                     ← 本文件
├── __init__.py                   # 公共导出，所有类型统一在此暴露
│
├── multi_browser_scraper_base/   # 🏗  调度层基类（子类入口）
├── browser_worker/               # ⚙️  单个浏览器实例的执行器
├── worker_context/               # 📦  传给钩子的 Worker 运行时只读快照
├── task_policy/                  # 📋  任务执行策略（代理/Cookie/重试）
├── cookie_mode/                  # 🍪  Cookie 使用模式枚举
└── failure_category/             # ❌  任务失败类别枚举
```

---

## 公共导出

所有对外类型均从 `new_zdh` 直接导入，无需深入子模块：

```python
from driver_base import (
    MultiBrowserScraperBase,  # 基类，子类必须继承并实现 process_task
    BrowserWorker,  # Worker 实例（一般不直接使用）
    WorkerContext,  # 钩子函数接收的只读状态快照
    TaskPolicy,  # 结构化任务策略
    CookieMode,  # Cookie 注入模式枚举
    FailureCategory,  # 失败类别枚举
)
```

---

## 快速开始

```python
from driver_base import MultiBrowserScraperBase, WorkerContext
from playwright.async_api import Page


class MyScraper(MultiBrowserScraperBase):
    async def process_task(
            self,
            page: Page,
            task_data: dict,
            worker: WorkerContext,
    ):
        await page.goto(task_data["url"])
        return await page.title()


async def main():
    tasks = [{"url": "https://example.com"} for _ in range(20)]
    async with MyScraper(browser_pool_size=2, max_contexts_per_browser=5) as scraper:
        results = await scraper.run_tasks(tasks)
```

---

## 模块说明速查

| 子目录 | 核心类/枚举 | 一句话说明 |
|--------|------------|-----------|
| `multi_browser_scraper_base` | `MultiBrowserScraperBase` | 调度器基类，管理 Worker 池与任务并发 |
| `browser_worker` | `BrowserWorker` | 单个浏览器实例，含完整生命周期管理 |
| `worker_context` | `WorkerContext` | Worker 运行时快照，传给各钩子函数 |
| `task_policy` | `TaskPolicy` | 任务级代理/Cookie/重试配置结构体 |
| `cookie_mode` | `CookieMode` | AUTO / NONE / PROVIDED 三种注入模式 |
| `failure_category` | `FailureCategory` | 代理/封禁/任务/Worker/未知 五类失败 |

---

## 依赖项

| 依赖 | 用途 |
|------|------|
| `playwright` | 浏览器自动化核心 |
| `playwright-stealth` | 反检测脚本注入 |
| `scrapling` | `AsyncStealthySession` 封装与代理工具 |
| `session_context_base` | Cookie 格式转换工具 (`load_cookies`) |
