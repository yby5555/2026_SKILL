# `cookie_mode` — Cookie 使用模式枚举

## 目录定位

`CookieMode` 定义了任务执行时 Cookie 的**三种注入模式**，配合 `TaskPolicy` 使用，声明当前任务如何处理 Cookie。

---

## 文件

| 文件 | 说明 |
|------|------|
| `__init__.py` | `CookieMode` 枚举定义 |

---

## 枚举值

| 枚举值 | 字符串值 | 说明 |
|--------|----------|------|
| `CookieMode.AUTO` | `"auto"` | 自动推断：有 Cookie 就注入，否则跳过 |
| `CookieMode.NONE` | `"none"` | 强制不注入，以完全匿名身份访问 |
| `CookieMode.PROVIDED` | `"provided"` | 明确使用 task_data 中显式传入的 Cookie |

继承自 `str`，可直接与字符串比较或序列化。

---

## 使用示例

```python
from driver_base import CookieMode, TaskPolicy

# 匿名采集
policy = TaskPolicy(cookie_mode=CookieMode.NONE)

# 使用任务指定的 Cookie
policy = TaskPolicy(
    cookies="sid_guard=abc123; sessionid=xyz",
    cookie_mode=CookieMode.PROVIDED,
)
```

---

## 注意事项

> **当前框架的 `_build_cookie_payload` 尚未对 `CookieMode` 做分支判断。**  
> 需强制匿名时，请在子类中覆写 `_build_cookie_payload` 并手动读取 `cookie_mode`。
