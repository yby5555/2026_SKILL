# `failure_category` — 任务失败类别枚举

## 目录定位

`FailureCategory` 定义了任务失败后的**统一分类标准**。调度层会将执行异常映射到这些类别，用于决定 Worker 是否需要回收、代理是否需要切换、任务是否应该重试。

---

## 文件

| 文件 | 说明 |
|------|------|
| `__init__.py` | `FailureCategory` 枚举定义 |

---

## 枚举值

| 枚举值 | 字符串值 | 触发场景 | Worker 回收行为 |
|--------|----------|----------|----------------|
| `PROXY_ERROR` | `"proxy_error"` | 代理连接失败、代理不可用 | 累计计数 |
| `BAN_ERROR` | `"ban_error"` | 站点封禁、验证码、访问受限 | 累计计数 |
| `TASK_ERROR` | `"task_error"` | 业务逻辑本身的错误 | 累计计数 |
| `WORKER_ERROR` | `"worker_error"` | 浏览器或执行器状态异常 | **立即标记回收** |
| `UNKNOWN` | `"unknown"` | 无法识别的其他异常 | 累计计数 |

---

## 与 Worker 回收的关系

```
record_failure(category) 内部逻辑：

  tasks_since_recycle += 1
  consecutive_failures += 1

  if category == WORKER_ERROR:
      → request_recycle = True  （立即标记，不等阈值）

  if tasks_since_recycle >= recycle_after_tasks:
      → request_recycle = True

  if consecutive_failures >= recycle_after_failures:
      → request_recycle = True
```

---

## 使用示例

```python
from driver_base import FailureCategory


# 在子类中按异常类型正确分类
async def _run_single_task(self, task_data):
    try:
        return await super()._run_single_task(task_data)
    except ProxyConnectionError as e:
        worker.record_failure(FailureCategory.PROXY_ERROR)
        raise
    except CaptchaError as e:
        worker.record_failure(FailureCategory.BAN_ERROR)
        raise
    except BusinessLogicError as e:
        worker.record_failure(FailureCategory.TASK_ERROR)
        raise
    except Exception as e:
        worker.record_failure(FailureCategory.UNKNOWN)
        raise
```

---

## 注意事项

> **当前基类 `_run_single_task` 对所有异常都使用 `WORKER_ERROR`**，这会导致业务逻辑错误也触发浏览器回收。  
> 建议在子类中覆写 `_run_single_task`，按实际异常类型传入正确的 `FailureCategory`。
