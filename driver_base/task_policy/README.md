# `task_policy` — 任务执行策略

## 目录定位

`TaskPolicy` 是一个**结构化任务策略数据类**，将松散的 `task_data` 字典中的代理、Cookie、重试等配置整理成统一的类型安全对象，供调度层和执行层使用。

---

## 文件

| 文件 | 说明 |
|------|------|
| `__init__.py` | `TaskPolicy` 数据类定义 |

---

## 数据结构

```python
@dataclass(slots=True)
class TaskPolicy:
    proxy: str | dict[str, Any] | None = None
    cookies: str | dict[str, str] | list[dict[str, Any]] | None = None
    cookie_mode: CookieMode = CookieMode.AUTO
    max_retries: int = 1
    retry_unknown_errors: bool = False
    retry_task_errors: bool = False
```

---

## 字段说明

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `proxy` | `str \| dict \| None` | `None` | 任务使用的代理，支持 URL 字符串或字典格式 |
| `cookies` | `str \| dict \| list \| None` | `None` | 原始 Cookie 输入（字符串、字典或 Cookie 列表） |
| `cookie_mode` | `CookieMode` | `AUTO` | Cookie 处理模式，详见 `cookie_mode` 模块 |
| `max_retries` | `int` | `1` | 任务最大重试次数（0 = 不重试） |
| `retry_unknown_errors` | `bool` | `False` | 是否对未知错误（`UNKNOWN`）执行重试 |
| `retry_task_errors` | `bool` | `False` | 是否对业务错误（`TASK_ERROR`）执行重试 |

---

## Cookie 输入格式

`cookies` 字段支持多种格式，会由 `session_context_base.load_cookies()` 统一转换为 Playwright `storage_state` 所需的 list 格式：

```python
# 格式 1：浏览器复制的 Cookie 字符串
cookies = "sid_guard=abc123; sessionid=xyz456"

# 格式 2：字典（name → value）
cookies = {"sid_guard": "abc123", "sessionid": "xyz456"}

# 格式 3：Playwright 格式的完整 Cookie 列表
cookies = [{"name": "sid_guard", "value": "abc123", "domain": ".tiktok.com"}]
```

---

## 使用示例

```python
from driver_base import TaskPolicy, CookieMode


# 从 task_data 中提取策略
def extract_policy(task_data: dict) -> TaskPolicy:
    return TaskPolicy(
        proxy=task_data.get("proxy"),
        cookies=task_data.get("cookies"),
        cookie_mode=CookieMode.PROVIDED if task_data.get("cookies") else CookieMode.NONE,
        max_retries=task_data.get("max_retries", 3),
        retry_unknown_errors=True,
    )
```

---

## 注意事项

> **当前框架中 `TaskPolicy` 尚未被 `_run_single_task` 自动使用。**  
> `max_retries`、`retry_unknown_errors`、`retry_task_errors` 等重试相关字段需要子类在 `_run_single_task` 或 `process_task` 中手动读取并实现重试逻辑。
