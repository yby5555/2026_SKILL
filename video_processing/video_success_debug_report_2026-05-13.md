# 视频生成成功率调试记录（2026-05-13）

## 目标

把 `D:\2026_SKILL\video_processing\consumers\redis_task_consumer.py` 这一版消费者的视频生成成功率提升到 80% 以上。

> 当前状态：**还不能证明已经达到 80%**。用户已明确要求“不要跑任务”，所以本轮没有再启动消费者、没有投递 Redis 任务、没有跑视频生成任务。当前只能完成静态修复和单元级验证。

## 当前运行状态

- 已停止遗留的 `run_40_task_audit.py` 和 `redis_task_consumer` 进程树。
- 已停止它们拉起的 Playwright / Chromium 子进程。
- Redis 队列当前为空：
  - `task:create:queue = 0`
  - `task:create:processing = 0`
- 本次被清理的 audit40 遗留任务已备份：
  - `D:\2026_SKILL\.omx\logs\stopped-audit40-queue-backup-20260513-204913.json`
- 已清理残留账号占用槽位：
  - 清理前：`flow:cookie:inuse:s5524h24h723@mubanima26.sbs = 1`
  - 清理后：无 `flow:cookie:inuse:*` 残留。

## 已确认的问题

### 1. 上一轮成功率只有约 70%，没有达到 80%

上一轮 audit40 已下载视频数量为 28 个，按 40 个任务计算：

```text
28 / 40 = 70%
```

所以不能把当前状态声明为 80% 以上。

### 2. Cookie 槽位等待会阻塞整个 asyncio 事件循环

问题位置：

- `D:\2026_SKILL\video_processing\scrapers\automation_video_v2_click_consumer.py`
  - `normalize_task()` 里等待 Cookie 槽位时使用了同步 `time.sleep(3)`。
- `D:\2026_SKILL\driver_base\multi_browser_scraper_base\__init__.py`
  - `_run_single_task()` 原来直接同步调用 `self.normalize_task(task_data)`。

影响：

当 4 个 consumer 并发跑、但只有 2 个活跃账号槽位时，等待槽位的任务会在 `normalize_task()` 里同步 sleep。因为这段代码运行在 asyncio 事件循环线程里，所以它会阻塞其他已经拿到槽位、正在执行的任务继续推进/释放槽位，造成“越等越释放不出来”的情况。

这会放大以下失败：

- `Cookie 槽位等待超时（120s）`
- 任务被重试，进一步加大并发压力
- 已占用账号释放不及时

修复：

- `driver_base/multi_browser_scraper_base/__init__.py`
  - 把同步 `normalize_task()` 放到线程里执行：

```python
task = await asyncio.to_thread(self.normalize_task, task_data)
```

作用：

- Cookie 等待仍然存在，但不会卡住整个 asyncio 事件循环。
- 已拿到槽位的任务可以继续执行并正常释放 Cookie 槽位。
- 降低并发场景下的槽位假死/超时概率。

### 3. 消费者构建 scraper task 时丢失直接图片字段

问题位置：

- `D:\2026_SKILL\video_processing\utils\task_common.py`
  - `build_scraper_task()` 原来只读取：
    - `image_value`
    - 或 `image`
  - 但不保留这些直接字段：
    - `image_url`
    - `image_url_list`
    - `image_base64`
    - `image_base64_list`

影响：

你的 Redis/API 任务里如果直接传了 `image_url` / `image_url_list`，或者消费者提前把 COS 图片转成了 `image_base64` / `image_base64_list`，进入 `build_scraper_task()` 后可能被丢掉。

这会导致：

- reference / frame 类型任务实际没有带图片进入爬虫流程；
- 图片模式任务按无图或异常参数跑；
- 成功率被明显拉低，尤其是 `reference_single`、`reference_double`、`frame_single`、`frame_double`。

修复：

- `video_processing/utils/task_common.py`
  - `build_scraper_task()` 现在优先保留直接图片字段：

```python
direct_image_keys = ("image_base64_list", "image_base64", "image_url_list", "image_url")
```

作用：

- API/Redis 直接传图不会被丢。
- COS 转 base64 后的字段也能继续传到 scraper。
- 图片类任务的输入数据更稳定。

### 4. 浏览器指纹/UA 已改为任务级 Chromium 145 系列

相关文件：

- `D:\2026_SKILL\driver_base\multi_browser_scraper_base\__init__.py`
- `D:\2026_SKILL\driver_base\browser_worker\__init__.py`
- `D:\2026_SKILL\video_processing\scrapers\automation_video_v2_click_consumer.py`

当前逻辑：

- 不再固定使用旧的 Chrome/148 默认 UA。
- 未显式传 `FLOW_BROWSER_USER_AGENT` 时，每个任务生成一个不同的 Chromium 145 系列 UA，例如：
  - `Chrome/145.0.7632.6`
  - `Chrome/145.0.7632.7`
  - `Chrome/145.0.7632.8`
- `Sec-Ch-Ua` / `Sec-Ch-Ua-Full-Version-List` 跟随任务 UA 生成。
- `browser_identity` 日志现在记录任务实际使用的 `_browser_user_agent`。

注意：

- 这只是把请求侧/上下文侧的 UA 统一到真实 Playwright Chromium 145 系列。
- 没有承诺能解决 Google Flow 服务端 403。
- 没有实现验证码或风控绕过。

### 5. locale / timezone / viewport 已同步

相关文件：

- `D:\2026_SKILL\video_processing\consumers\redis_task_consumer.py`
- `D:\2026_SKILL\video_processing\scrapers\automation_video_v2_click_consumer.py`

当前逻辑：

- consumer 显式传入：
  - `locale=resolve_flow_locale()`
  - `timezone_id=resolve_flow_timezone_id()`
- 不再传 `viewport={"width": 0, "height": 0}`。
- 让 consumer 版本和 `D:\2026_SKILL\flow\test_anti_detection.py` 的浏览器环境更一致。

## 本轮改动文件

### 代码文件

1. `D:\2026_SKILL\driver_base\multi_browser_scraper_base\__init__.py`
   - 任务级 Chromium 145 UA。
   - `build_context_options()` 支持任务数据。
   - `initialize_context()` 使用任务级 UA。
   - `normalize_task()` 改为在线程中执行，避免同步等待阻塞事件循环。
   - 增加 `finalize_task()` 兜底清理钩子。

2. `D:\2026_SKILL\driver_base\browser_worker\__init__.py`
   - context 创建时把 `task_data` 传给 context options factory。
   - 保留旧签名兼容 fallback。

3. `D:\2026_SKILL\video_processing\scrapers\automation_video_v2_click_consumer.py`
   - 增加/修正 `browser_identity` 日志。
   - Cookie 槽位释放做了兜底。
   - `finalize_task()` 可在 process_task 前失败时归还槽位。

4. `D:\2026_SKILL\video_processing\consumers\redis_task_consumer.py`
   - 同步 locale/timezone/viewport。
   - 增强消费者异常处理和重试恢复。

5. `D:\2026_SKILL\video_processing\utils\task_common.py`
   - Redis timeout 可通过环境变量配置。
   - `build_scraper_task()` 保留直接图片字段，避免图片任务丢图。

### 测试文件

1. `D:\2026_SKILL\driver_base\tests\test_multi_browser_scraper_base.py`
   - 增加任务级 UA 测试。
   - 增加 `normalize_task()` 不阻塞事件循环的回归测试。

2. `D:\2026_SKILL\test_anti_detection_cases.py`
   - 增加直接 `image_url` 保留测试。
   - 增加 `image_base64_list` 保留测试。

## 已完成验证（没有跑视频生成任务）

执行环境：

```text
C:/Users/Administrator/AppData/Local/Programs/Python/Python311/python.exe
```

验证命令：

```powershell
python -m py_compile video_processing\utils\task_common.py test_anti_detection_cases.py driver_base\multi_browser_scraper_base\__init__.py
python -m unittest test_anti_detection_cases driver_base.tests.test_multi_browser_scraper_base
```

结果：

```text
Ran 17 tests in 0.244s
OK
```

之前相关验证：

```text
Ran 19 tests in 0.285s
OK
```

## 仍未验证 / 仍有风险

1. **没有重新跑实际生成任务**
   - 因为用户明确要求“不要跑任务”。
   - 所以不能证明成功率已经达到 80%。

2. **403 / reCAPTCHA evaluation failed 仍可能出现**
   - 这是 Google Flow 服务端返回：
     - `PUBLIC_ERROR_UNUSUAL_ACTIVITY`
   - 这类错误不应通过绕过验证码/风控处理。
   - 当前只能通过降低资源错误、修正输入参数、避免异常重试风暴来减少触发概率。

3. **账号池只有 2 个活跃账号时，4 并发仍然偏紧**
   - 现在不会因为 `normalize_task()` 阻塞事件循环而假死。
   - 但如果账号长期满载，仍可能排队等待。

4. **80% 目标需要真实运行验证**
   - 后续如果允许跑任务，建议先跑小批量，例如 10 个任务，成功率达到 8/10 后再跑 40 个任务。
   - 当前用户不允许跑，所以暂停在静态修复状态。

## 当前结论

本轮已修复两个会实质影响成功率的工程问题：

1. Cookie 槽位等待阻塞事件循环。
2. 图片任务在 `build_scraper_task()` 阶段丢失直接图片字段。

这两个修复都属于稳定性/参数正确性修复，不涉及验证码或风控绕过。

但由于没有重新跑生成任务，当前只能说：

```text
已完成影响成功率的静态修复；尚未证明成功率达到 80%+。
```
