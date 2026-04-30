# `browser_worker` — 单个浏览器 Worker 执行器

## 目录定位

`BrowserWorker` 是框架的**执行层**，负责管理**单个浏览器进程**的完整生命周期：

- 懒初始化浏览器（首次任务触发启动，后续复用）
- 为每个任务创建独立的 `BrowserContext` 和 `Page`
- 注入 Cookie、代理、Stealth 反检测补丁
- 统计任务数与连续失败次数，按策略触发浏览器回收

`MultiBrowserScraperBase` 会按 `browser_pool_size` 创建 N 个 `BrowserWorker` 实例组成 Worker 池，调度层只负责挑选 Worker，具体浏览器操作全部由本类封装。

---

## 文件

| 文件 | 说明 |
|------|------|
| `__init__.py` | `BrowserWorker` 类的完整实现 |

---

## 核心状态

| 字段 | 类型 | 说明 |
|------|------|------|
| `worker_id` | `int` | Worker 唯一编号 |
| `max_contexts` | `int` | 当前浏览器允许的最大并发 Context 数 |
| `tasks_since_recycle` | `int` | 自上次回收以来累计执行的任务数 |
| `consecutive_failures` | `int` | 连续失败次数（成功后清零） |
| `request_recycle` | `bool` | 回收标记（不立即关闭，等待空闲） |
| `_session` | `AsyncStealthySession` | Scrapling 浏览器会话 |
| `_browser` | `Browser` | Playwright 浏览器实例 |
| `_stealth` | `Stealth` | 反检测实例（playwright-stealth） |

---

## 关键方法

| 方法 | 说明 |
|------|------|
| `ensure_started()` | 懒初始化浏览器，双重检查锁保证幂等 |
| `close()` | 关闭 session，重置所有计数和标记 |
| `recycle_if_needed(active_tasks)` | 仅当 `active_tasks==0` 且有回收标记时才真正关闭 |
| `record_success()` | 递增任务计数，清零失败计数，检查任务数阈值 |
| `record_failure(category)` | 递增失败计数，`WORKER_ERROR` 立即触发回收标记 |
| `run_task(task_data, proxy, worker_context)` | 创建 Context/Page → 注入钩子链 → 执行任务 → 清理资源 |
| `_resolve_context_options(proxy)` | 解析 Context 参数，优先使用 session 补丁方法 |

---

## 单次任务执行流程

```
run_task()
    │
    ├─ ensure_started()              # 懒初始化浏览器（幂等）
    ├─ _build_cookie_payload()       # Cookie → Playwright storage_state 格式
    ├─ _resolve_context_options()    # 合并代理配置
    ├─ browser.new_context()         # 创建隔离 Context（注入 Cookie）
    ├─ session._initialize_context() # Scrapling 内置 Context 初始化
    ├─ initialize_context()          # 🪝 用户自定义钩子
    ├─ context.new_page()            # 创建 Page
    ├─ stealth.apply_stealth_async() # 反检测补丁
    ├─ initialize_page()             # 🪝 用户自定义钩子
    ├─ process_task()                # 🪝 用户实现（核心业务逻辑）
    └─ finally: page.close() / context.close()  # 无论成功失败都清理
```

---

## 设计要点：`_build_context_with_proxy` 补丁

`ensure_started()` 在浏览器启动后，会向 `AsyncStealthySession` 实例动态注入 `_build_context_with_proxy` 方法：

```python
session._build_context_with_proxy = types.MethodType(patch_fn, session)
```

**作用**: 每次创建 Context 时复用 session 已有的默认配置，同时按任务动态覆盖代理，实现**任务级代理切换**且不破坏默认参数。

---

## 回收策略

回收采用 **"标记-延迟"** 两阶段设计：

```
触发条件（任意满足其一）：
  ① tasks_since_recycle >= recycle_after_tasks
  ② consecutive_failures >= recycle_after_failures
  ③ 发生 FailureCategory.WORKER_ERROR

→ 设置 request_recycle = True（仅打标记）

释放 Worker 时：
  recycle_if_needed(active_tasks=N)
  → 当 active_tasks == 0 时，才真正调用 close()
  → Worker 重置后下次任务到来时重新启动浏览器
```

---

## 注意事项

> **依赖 Scrapling 私有 API**: `_initialize_context`、`_config`、`browser` 等均为 Scrapling 内部属性，Scrapling 版本升级时需要验证兼容性。
