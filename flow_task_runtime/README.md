# flow_task_runtime

## 目录作用

这是参考现有逻辑后，重新在新目录中实现的一套**自包含视频任务消费者**。

特点：

1. **不修改旧代码**
2. **不依赖旧业务脚本 import**
3. **浏览器数量可配置**
4. **支持一个 browser 内多个 content/context 并发**
5. **执行前先检查账号登录态和 AI 点数**
6. **保留 Redis 任务获取 / processing 恢复 / 失败重试 / Mongo 存储**

---

## 文件说明

- `config.py`
  - 集中管理 Redis / Mongo / 并发 / 超时 / 额度阈值等配置

- `logging_utils.py`
  - 统一日志与本地时间工具

- `queue_manager.py`
  - Redis 队列恢复、抢占、确认、回队重试

- `storage.py`
  - Redis / Mongo 客户端创建

- `account_pool.py`
  - Cookie 池获取、归还、移除

- `account_guard.py`
  - 登录态检查与 AI 点数检查

- `browser_pool.py`
  - 本目录自己的 Playwright 浏览器池实现

- `task_repository.py`
  - Mongo 任务记录与账号状态更新

- `flow_api.py`
  - Flow 页面接口交互、项目创建、轮询、下载

- `scraper.py`
  - 新的 Flow 抓取器，负责额度检查、参考图处理、生成和下载

- `consumer.py`
  - 总入口，负责把队列与抓取器串起来

---

## 运行方式

在仓库根目录执行：

```powershell
.\.venv310\Scripts\python.exe -m flow_task_runtime.consumer
```

如果你想给这个新目录单独补依赖环境，可以执行：

```powershell
flow_task_runtime\setup_env.bat
```

或手动执行：

```powershell
.\.venv310\Scripts\python.exe -m pip install -r flow_task_runtime\requirements.txt
```

---

## 可配置环境变量

- `FLOW_TASK_BROWSER_POOL_SIZE`
- `FLOW_TASK_CONTEXTS_PER_BROWSER`
- `FLOW_TASK_CONSUMER_WORKERS`
- `FLOW_TASK_HEADLESS`
- `FLOW_TASK_NAVIGATION_TIMEOUT_MS`
- `FLOW_TASK_TIMEOUT_MS`
- `FLOW_TASK_POLL_TIMEOUT_MS`
- `FLOW_TASK_MAX_RETRIES`
- `FLOW_TASK_MIN_CREDITS_THRESHOLD`
- `FLOW_TASK_OUTPUT_DIR`

---

## 默认并发模型

默认配置偏向你当前更需要的模式：

- `browser_pool_size=1`
- `contexts_per_browser=3`

也就是：

- **1 个浏览器**
- **3 个 content/context 并发**

如果你后面想改成多个 browser，只需要通过环境变量调整即可。
