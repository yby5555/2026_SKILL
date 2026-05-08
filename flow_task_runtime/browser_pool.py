"""自包含的 Playwright 浏览器池。

功能说明：
1. 提供一个浏览器池，支持每个 browser 并发多个 context
2. 每个 task 使用自己的 cookie 和可选 proxy
3. 不依赖 `driver_base` 目录下的旧实现
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from playwright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright


class FailureCategory(str):
    """任务失败分类的轻量枚举。"""

    PROXY_ERROR = "proxy_error"
    BAN_ERROR = "ban_error"
    TASK_ERROR = "task_error"
    WORKER_ERROR = "worker_error"
    UNKNOWN = "unknown"


class WorkerContext:
    """传给任务钩子的 worker 状态快照。"""

    __slots__ = ("worker_id", "browser_task_capacity", "active_tasks", "tasks_since_recycle", "consecutive_failures")

    def __init__(
        self,
        *,
        worker_id: int,
        browser_task_capacity: int,
        active_tasks: int,
        tasks_since_recycle: int,
        consecutive_failures: int,
    ) -> None:
        self.worker_id = worker_id
        self.browser_task_capacity = browser_task_capacity
        self.active_tasks = active_tasks
        self.tasks_since_recycle = tasks_since_recycle
        self.consecutive_failures = consecutive_failures


def normalize_cookies(
    cookies: str | dict[str, str] | list[dict[str, Any]] | None,
    *,
    default_domain: str = ".google.com",
) -> list[dict[str, Any]]:
    """把多种 cookie 输入格式统一成 Playwright 可识别结构。

    参数:
        cookies: 原始 cookie 输入。
        default_domain: 缺省 domain。

    返回:
        list[dict[str, Any]]: Playwright 可直接使用的 cookies 列表。
    """
    if not cookies:
        return []
    if isinstance(cookies, list):
        return cookies
    if isinstance(cookies, dict):
        return [
            {
                "name": str(name),
                "value": str(value),
                "domain": default_domain,
                "path": "/",
                "httpOnly": False,
                "secure": True,
            }
            for name, value in cookies.items()
        ]

    raw_text = cookies.strip()
    if not raw_text:
        return []
    if raw_text.startswith("["):
        data = json.loads(raw_text)
        if not isinstance(data, list):
            raise ValueError("Cookies JSON must be a list")
        return data

    result: list[dict[str, Any]] = []
    for part in raw_text.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        name, value = part.split("=", 1)
        result.append(
            {
                "name": name.strip(),
                "value": value.strip(),
                "domain": default_domain,
                "path": "/",
                "httpOnly": False,
                "secure": True,
            }
        )
    return result


def load_cookies(
    cookies_or_path: str | Path | dict[str, str] | list[dict[str, Any]] | None,
    *,
    default_domain: str = ".google.com",
) -> list[dict[str, Any]]:
    """从原始文本、对象或文件路径加载 cookie。

    参数:
        cookies_or_path: cookie 内容本身，或指向 cookie 文件的路径。
        default_domain: 缺省 domain。

    返回:
        list[dict[str, Any]]: 标准化后的 cookies 列表。
    """
    if isinstance(cookies_or_path, Path):
        return normalize_cookies(cookies_or_path.read_text(encoding="utf-8"), default_domain=default_domain)
    if isinstance(cookies_or_path, str):
        path = Path(cookies_or_path)
        if path.exists():
            return normalize_cookies(path.read_text(encoding="utf-8"), default_domain=default_domain)
    return normalize_cookies(cookies_or_path, default_domain=default_domain)


def normalize_proxy_config(proxy: str | dict[str, Any] | None) -> dict[str, str] | None:
    """把字符串或字典 proxy 统一成 Playwright context proxy 结构。

    参数:
        proxy: 原始代理配置。

    返回:
        dict[str, str] | None: Playwright 可识别的 proxy 配置。
    """
    if not proxy:
        return None
    if isinstance(proxy, str):
        return {"server": proxy}
    if "server" in proxy:
        normalized = {"server": str(proxy["server"])}
        if proxy.get("username") is not None:
            normalized["username"] = str(proxy["username"])
        if proxy.get("password") is not None:
            normalized["password"] = str(proxy["password"])
        return normalized
    if proxy.get("scheme") and proxy.get("host") and proxy.get("port"):
        auth = ""
        if proxy.get("username") is not None:
            auth = str(proxy["username"])
            if proxy.get("password") is not None:
                auth += f":{proxy['password']}"
            auth += "@"
        return {"server": f"{proxy['scheme']}://{auth}{proxy['host']}:{proxy['port']}"}
    raise ValueError(f"Unsupported proxy format: {proxy!r}")


def to_httpx_proxy(proxy: str | dict[str, Any] | None) -> str | None:
    """把 task proxy 转换成 httpx 可直接使用的字符串。

    参数:
        proxy: 原始代理配置。

    返回:
        str | None: httpx 可直接使用的代理字符串。
    """
    normalized = normalize_proxy_config(proxy)
    if normalized is None:
        return None
    server = normalized["server"]
    username = normalized.get("username")
    if username is None or "://" not in server:
        return server
    scheme, remainder = server.split("://", 1)
    password = normalized.get("password", "")
    return f"{scheme}://{username}:{password}@{remainder}"


class PlainPlaywrightBrowserWorker:
    """单个浏览器 worker。"""

    def __init__(
        self,
        *,
        worker_id: int,
        max_contexts: int,
        navigation_timeout_ms: int,
        launch_options_factory: Any,
        context_options_factory: Any,
        create_cookies_payload: Any,
        initialize_context: Any,
        initialize_page: Any,
        process_task: Any,
        recycle_after_tasks: int | None,
        recycle_after_failures: int | None,
        logger: Any | None = None,
    ) -> None:
        self.worker_id = worker_id
        self.max_contexts = max(1, int(max_contexts))
        self.navigation_timeout_ms = navigation_timeout_ms
        self._launch_options_factory = launch_options_factory
        self._context_options_factory = context_options_factory
        self._create_cookies_payload = create_cookies_payload
        self._initialize_context_hook = initialize_context
        self._initialize_page_hook = initialize_page
        self._process_task = process_task
        self._recycle_after_tasks = recycle_after_tasks
        self._recycle_after_failures = recycle_after_failures
        self.logger = logger
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._start_lock = asyncio.Lock()
        self.tasks_since_recycle = 0
        self.consecutive_failures = 0
        self.request_recycle = False
        self.recycle_reason: str | None = None
        self._closing = False

    def _log_info(self, message: str) -> None:
        if self.logger is not None:
            self.logger.info(message)

    def _log_warning(self, message: str) -> None:
        if self.logger is not None:
            self.logger.warning(message)

    def is_browser_connected(self) -> bool:
        """返回当前 worker 的 browser 是否仍处于连接状态。"""
        return self._browser is not None and self._browser.is_connected()

    def request_browser_recycle(self, reason: str) -> None:
        """标记该 worker 需要在空闲后回收。"""
        if not self.request_recycle:
            self._log_warning(f"[browser-worker {self.worker_id}] 标记浏览器回收: {reason}")
        self.request_recycle = True
        self.recycle_reason = reason

    def _on_browser_disconnected(self, *args: Any) -> None:
        """Playwright browser 断开事件回调。"""
        del args
        if self._closing:
            self._log_info(f"[browser-worker {self.worker_id}] 浏览器连接已关闭")
            return
        self.request_browser_recycle("browser disconnected")

    async def ensure_started(self) -> None:
        """确保 worker 底层 browser 已启动。"""
        if self.is_browser_connected():
            return
        async with self._start_lock:
            if self.is_browser_connected():
                return
            if self._browser is not None:
                self._log_warning(f"[browser-worker {self.worker_id}] 检测到浏览器已断开，准备重新启动")
            await self.close(reason="ensure_started cleanup")
            self._playwright = await async_playwright().start()
            try:
                launch_options = self._launch_options_factory(self)
                self._log_info(f"[browser-worker {self.worker_id}] 启动浏览器: {launch_options}")
                self._browser = await self._playwright.chromium.launch(**launch_options)
                self._browser.on("disconnected", self._on_browser_disconnected)
                self._log_info(f"[browser-worker {self.worker_id}] 浏览器启动完成，capacity={self.max_contexts}")
            except Exception:
                await self.close(reason="launch failed")
                raise

    async def close(self, *, reason: str | None = None) -> None:
        """关闭底层 browser 和 playwright 会话。"""
        if self._browser is not None or self._playwright is not None:
            suffix = f": {reason}" if reason else ""
            self._log_info(f"[browser-worker {self.worker_id}] 关闭浏览器/Playwright{suffix}")
        if self._browser is not None:
            try:
                self._closing = True
                await self._browser.close()
            except Exception:
                pass
        if self._playwright is not None:
            try:
                await self._playwright.stop()
            except Exception:
                pass
        self._browser = None
        self._playwright = None
        self.tasks_since_recycle = 0
        self.consecutive_failures = 0
        self.request_recycle = False
        self.recycle_reason = None
        self._closing = False

    async def recycle_if_needed(self, *, active_tasks: int) -> None:
        """在 worker 空闲且已请求回收时关闭 browser。"""
        if active_tasks > 0 or not self.request_recycle:
            return
        await self.close(reason=self.recycle_reason or "recycle requested")

    def record_success(self) -> None:
        """记录一次成功执行。"""
        self.tasks_since_recycle += 1
        self.consecutive_failures = 0
        if self._recycle_after_tasks and self.tasks_since_recycle >= self._recycle_after_tasks:
            self.request_browser_recycle(f"tasks_since_recycle={self.tasks_since_recycle}")

    def record_failure(self, category: FailureCategory) -> None:
        """记录一次失败执行。"""
        self.tasks_since_recycle += 1
        self.consecutive_failures += 1
        if category == FailureCategory.WORKER_ERROR:
            self.request_browser_recycle(f"worker error: {category}")
        if self._recycle_after_tasks and self.tasks_since_recycle >= self._recycle_after_tasks:
            self.request_browser_recycle(f"tasks_since_recycle={self.tasks_since_recycle}")
        if self._recycle_after_failures and self.consecutive_failures >= self._recycle_after_failures:
            self.request_browser_recycle(f"consecutive_failures={self.consecutive_failures}")

    async def run_task(self, *, task_data: dict[str, Any], worker_context: WorkerContext) -> Any:
        """创建 context/page 并执行单个任务。"""
        await self.ensure_started()
        if self._browser is None:
            raise RuntimeError(f"Worker {self.worker_id} is not ready")

        cookies_payload = await self._create_cookies_payload(task_data)
        context_options = self._context_options_factory(self, task_data)

        context: BrowserContext | None = None
        page: Page | None = None
        task_id = str(task_data.get("_id") or "<unknown>")
        try:
            self._log_info(
                f"[browser-worker {self.worker_id}][任务:{task_id}] 创建 context，"
                f"active={worker_context.active_tasks}/{worker_context.browser_task_capacity}"
            )
            if cookies_payload:
                context = await self._browser.new_context(storage_state={"cookies": cookies_payload}, **context_options)
            else:
                context = await self._browser.new_context(**context_options)
            context = await self._initialize_context_hook(context, task_data, worker_context)
            page = await context.new_page()
            self._log_info(f"[browser-worker {self.worker_id}][任务:{task_id}] context/page 已创建")
            page.set_default_navigation_timeout(self.navigation_timeout_ms)
            page.set_default_timeout(self.navigation_timeout_ms)
            await self._initialize_page_hook(page, task_data, worker_context)
            return await self._process_task(page, task_data, worker_context)
        finally:
            if page is not None:
                try:
                    await page.close()
                except Exception:
                    pass
            if context is not None:
                try:
                    await context.close()
                except Exception:
                    pass
            if page is not None or context is not None:
                self._log_info(f"[browser-worker {self.worker_id}][任务:{task_id}] context/page 已关闭")


class PlainPlaywrightBrowserPoolBase:
    """Playwright-only 浏览器池基类。"""

    def __init__(
        self,
        *,
        browser_pool_size: int = 1,
        max_contexts_per_browser: int = 3,
        headless: bool = True,
        locale: str | None = None,
        timezone_id: str | None = None,
        user_agent: str | None = None,
        navigation_timeout_ms: int = 60_000,
        task_timeout_ms: int | None = None,
        extra_flags: list[str] | None = None,
        viewport: dict[str, int] | None = None,
        default_cookies: str | dict[str, str] | list[dict[str, Any]] | None = None,
        default_cookie_domain: str = ".google.com",
        default_proxy: str | dict[str, Any] | None = None,
        ignore_https_errors: bool = False,
        add_default_launch_flags: bool = True,
        recycle_browser_after_tasks: int | None = 50,
        recycle_browser_after_failures: int | None = 3,
        logger: Any | None = None,
    ) -> None:
        self.browser_pool_size = max(1, int(browser_pool_size))
        self.max_contexts_per_browser = max(1, int(max_contexts_per_browser))
        self.headless = bool(headless)
        self.locale = locale
        self.timezone_id = timezone_id
        self.user_agent = user_agent
        self.navigation_timeout_ms = int(navigation_timeout_ms)
        self.task_timeout_ms = self._resolve_task_timeout_ms(task_timeout_ms)
        self.extra_flags = list(extra_flags or [])
        self.viewport = viewport
        self.default_cookies = default_cookies
        self.default_cookie_domain = default_cookie_domain
        self.default_proxy = default_proxy
        self.ignore_https_errors = bool(ignore_https_errors)
        self.add_default_launch_flags = bool(add_default_launch_flags)
        self._recycle_browser_after_tasks = recycle_browser_after_tasks
        self._recycle_browser_after_failures = recycle_browser_after_failures
        self.logger = logger
        self._workers: list[PlainPlaywrightBrowserWorker] = []
        self._worker_active_counts: dict[int, int] = {}
        self._workers_recycling: set[int] = set()
        self._pool_condition = asyncio.Condition()
        self._started = False
        self._closed = False
        self._start_lock = asyncio.Lock()
        self._next_worker_id = 0

    def _log_info(self, message: str) -> None:
        if self.logger is not None:
            self.logger.info(message)

    def _log_warning(self, message: str) -> None:
        if self.logger is not None:
            self.logger.warning(message)

    async def start(self) -> None:
        """启动 browser pool。"""
        if self._started and not self._closed:
            return
        async with self._start_lock:
            if self._started and not self._closed:
                return
            self._started = True
            self._closed = False
            self._log_info(
                "[browser-pool] 已启动: "
                f"browser_pool_size={self.browser_pool_size}, "
                f"contexts_per_browser={self.max_contexts_per_browser}, "
                f"headless={self.headless}, "
                f"recycle_after_tasks={self._recycle_browser_after_tasks}, "
                f"recycle_after_failures={self._recycle_browser_after_failures}"
            )

    async def close(self) -> None:
        """关闭所有 browser worker。"""
        if not self._started or self._closed:
            return
        for worker in self._workers:
            await worker.close(reason="pool closing")
        async with self._pool_condition:
            self._workers.clear()
            self._worker_active_counts.clear()
            self._workers_recycling.clear()
            self._pool_condition.notify_all()
        self._closed = True
        self._log_info("[browser-pool] 已关闭")

    async def __aenter__(self) -> "PlainPlaywrightBrowserPoolBase":
        """进入异步上下文时自动启动 browser pool。"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """退出异步上下文时自动关闭 browser pool。"""
        await self.close()

    def build_launch_options(self, worker: PlainPlaywrightBrowserWorker) -> dict[str, Any]:
        """构建 browser 启动参数。"""
        del worker
        args: list[str] = []
        if self.add_default_launch_flags:
            args.extend(
                [
                    "--disable-blink-features=AutomationControlled",
                    "--disable-infobars",
                ]
            )
        args.extend(self.extra_flags)
        if self.viewport and self.viewport.get("width", 0) > 0 and self.viewport.get("height", 0) > 0:
            args.append(f"--window-size={self.viewport['width']},{self.viewport['height']}")
        return {"headless": self.headless, "args": args}

    def build_context_options(self, worker: PlainPlaywrightBrowserWorker, task_data: dict[str, Any]) -> dict[str, Any]:
        """构建 context 创建参数。"""
        del worker
        options: dict[str, Any] = {}
        if self.locale:
            options["locale"] = self.locale
        if self.timezone_id:
            options["timezone_id"] = self.timezone_id
        if self.user_agent:
            options["user_agent"] = self.user_agent
        if self.ignore_https_errors:
            options["ignore_https_errors"] = self.ignore_https_errors
        proxy = normalize_proxy_config(self.resolve_task_proxy(task_data))
        if proxy is not None:
            options["proxy"] = proxy
        if self.viewport and self.viewport.get("width", 0) > 0 and self.viewport.get("height", 0) > 0:
            options["viewport"] = self.viewport
        else:
            options["viewport"] = None
        return options

    def normalize_task(self, task_data: dict[str, Any]) -> dict[str, Any]:
        """为子类提供任务标准化扩展点。"""
        return dict(task_data)

    def resolve_task_proxy(self, task_data: dict[str, Any]) -> str | dict[str, Any] | None:
        """解析当前任务要使用的 proxy。"""
        return task_data.get("proxy", self.default_proxy)

    async def initialize_context(self, context: BrowserContext, task_data: dict[str, Any], worker: WorkerContext) -> BrowserContext:
        """为子类提供 context 初始化扩展点。"""
        del task_data, worker
        return context

    async def initialize_page(self, page: Page, task_data: dict[str, Any], worker: WorkerContext) -> None:
        """为子类提供 page 初始化扩展点。"""
        del page, task_data, worker
        return None

    async def finalize_task(self, task_data: dict[str, Any], *, result: Any | None = None, exc: Exception | None = None) -> None:
        """为子类提供任务结束时的清理扩展点。"""
        del task_data, result, exc
        return None

    async def process_task(self, page: Page, task_data: dict[str, Any], worker: WorkerContext) -> Any:
        """子类必须实现的核心任务逻辑。"""
        del page, task_data, worker
        raise NotImplementedError

    async def classify_failure(self, exc: Exception, task_data: dict[str, Any], *, worker: WorkerContext | None) -> FailureCategory:
        """根据异常内容判断失败类型。"""
        del task_data, worker
        text = f"{type(exc).__name__}: {exc}".lower()
        if isinstance(exc, asyncio.TimeoutError):
            return FailureCategory.WORKER_ERROR
        if any(flag in text for flag in ("proxy", "407", "connection refused", "tunnel")):
            return FailureCategory.PROXY_ERROR
        if any(flag in text for flag in ("403", "captcha", "blocked", "429")):
            return FailureCategory.BAN_ERROR
        if any(flag in text for flag in ("browser has been closed", "context has been closed", "page has been closed")):
            return FailureCategory.WORKER_ERROR
        if isinstance(exc, (ValueError, TypeError, KeyError, AssertionError, NotImplementedError)):
            return FailureCategory.TASK_ERROR
        return FailureCategory.UNKNOWN

    async def run_tasks(self, tasks: list[dict[str, Any]]) -> list[Any]:
        """并发执行一批任务。"""
        await self.start()
        return await asyncio.gather(*(self._run_single_task(task) for task in tasks), return_exceptions=True)

    async def _run_single_task(self, task_data: dict[str, Any]) -> Any:
        """执行单个任务并更新 worker 统计。"""
        task = self.normalize_task(task_data)
        worker_record: PlainPlaywrightBrowserWorker | None = None
        result: Any | None = None
        caught_exc: Exception | None = None
        try:
            worker_record = await self._acquire_worker()
            worker_context = self._build_worker_context(worker_record)
            result = await self._run_worker_task(worker_record, task, worker_context)
            worker_record.record_success()
            return result
        except Exception as exc:
            caught_exc = exc
            if worker_record is not None:
                category = await self.classify_failure(exc, task, worker=self._build_worker_context(worker_record))
                worker_record.record_failure(category)
            raise
        finally:
            try:
                await self.finalize_task(task, result=result, exc=caught_exc)
            finally:
                if worker_record is not None:
                    await self._release_worker(worker_record)

    async def _build_cookie_payload(self, task_data: dict[str, Any]) -> list[dict[str, Any]]:
        """为当前任务构建 cookie payload。"""
        cookies = task_data.get("cookies", self.default_cookies)
        return load_cookies(cookies, default_domain=self.default_cookie_domain)

    async def _acquire_worker(self) -> PlainPlaywrightBrowserWorker:
        """从 worker 池中获取一个当前可用的 worker。"""
        await self.start()
        while True:
            async with self._pool_condition:
                available = [
                    worker
                    for worker in self._workers
                    if self._worker_active_counts.get(worker.worker_id, 0) < worker.max_contexts
                    and worker.worker_id not in self._workers_recycling
                    and not worker.request_recycle
                ]
                if available:
                    worker = min(
                        available,
                        key=lambda item: (
                            self._worker_active_counts.get(item.worker_id, 0),
                            item.consecutive_failures,
                            item.tasks_since_recycle,
                            item.worker_id,
                        ),
                    )
                    self._worker_active_counts[worker.worker_id] += 1
                    self._log_info(
                        f"[browser-pool] 分配 worker={worker.worker_id}, "
                        f"active={self._worker_active_counts[worker.worker_id]}/{worker.max_contexts}"
                    )
                    return worker
                idle_recycle_worker = next(
                    (
                        worker
                        for worker in self._workers
                        if self._worker_active_counts.get(worker.worker_id, 0) == 0
                        and worker.request_recycle
                        and worker.worker_id not in self._workers_recycling
                    ),
                    None,
                )
                if idle_recycle_worker is not None:
                    self._workers_recycling.add(idle_recycle_worker.worker_id)
                    should_retire_worker = idle_recycle_worker
                elif len(self._workers) < self.browser_pool_size:
                    worker = self._create_worker()
                    self._workers.append(worker)
                    self._worker_active_counts[worker.worker_id] = 1
                    self._log_info(
                        f"[browser-pool] 创建并分配 worker={worker.worker_id}, "
                        f"active=1/{worker.max_contexts}, workers={len(self._workers)}/{self.browser_pool_size}"
                    )
                    return worker
                else:
                    await self._pool_condition.wait()
                    continue

            if should_retire_worker is not None:
                await self._retire_worker(should_retire_worker)
                continue

    async def _release_worker(self, worker: PlainPlaywrightBrowserWorker) -> None:
        """释放 worker 占用并在需要时执行回收。"""
        should_recycle = False
        async with self._pool_condition:
            active_count = self._worker_active_counts.get(worker.worker_id, 0)
            if active_count > 0:
                self._worker_active_counts[worker.worker_id] = active_count - 1
            current_active = self._worker_active_counts.get(worker.worker_id, 0)
            if current_active == 0 and worker.request_recycle:
                self._workers_recycling.add(worker.worker_id)
                should_recycle = True
            self._log_info(
                f"[browser-pool] 释放 worker={worker.worker_id}, "
                f"active={current_active}/{worker.max_contexts}, recycle={worker.request_recycle}"
            )
            self._pool_condition.notify_all()

        if not should_recycle:
            await worker.recycle_if_needed(active_tasks=current_active)
            return

        await self._retire_worker(worker)

    async def _retire_worker(self, worker: PlainPlaywrightBrowserWorker) -> None:
        """关闭并从池中移除一个空闲 worker，后续任务会创建全新的 worker 对象。"""
        reason = worker.recycle_reason or "worker retired"
        self._log_warning(f"[browser-pool] 回收 worker={worker.worker_id}: {reason}")
        try:
            await worker.close(reason=reason)
        finally:
            async with self._pool_condition:
                if self._worker_active_counts.get(worker.worker_id, 0) == 0:
                    self._workers = [item for item in self._workers if item.worker_id != worker.worker_id]
                    self._worker_active_counts.pop(worker.worker_id, None)
                self._workers_recycling.discard(worker.worker_id)
                self._pool_condition.notify_all()

    def _build_worker_context(self, worker: PlainPlaywrightBrowserWorker) -> WorkerContext:
        """根据当前 worker 状态生成快照。"""
        return WorkerContext(
            worker_id=worker.worker_id,
            browser_task_capacity=worker.max_contexts,
            active_tasks=self._worker_active_counts.get(worker.worker_id, 0),
            tasks_since_recycle=worker.tasks_since_recycle,
            consecutive_failures=worker.consecutive_failures,
        )

    def _create_worker(self) -> PlainPlaywrightBrowserWorker:
        """创建一个新的 worker。"""
        worker = PlainPlaywrightBrowserWorker(
            worker_id=self._next_worker_id,
            max_contexts=self.max_contexts_per_browser,
            navigation_timeout_ms=self.navigation_timeout_ms,
            launch_options_factory=self.build_launch_options,
            context_options_factory=self.build_context_options,
            create_cookies_payload=self._build_cookie_payload,
            initialize_context=self.initialize_context,
            initialize_page=self.initialize_page,
            process_task=self.process_task,
            recycle_after_tasks=self._recycle_browser_after_tasks,
            recycle_after_failures=self._recycle_browser_after_failures,
            logger=self.logger,
        )
        self._next_worker_id += 1
        return worker

    def _resolve_task_timeout_ms(self, task_timeout_ms: int | None) -> int | None:
        """解析任务总超时设置。"""
        if task_timeout_ms is None:
            return max(self.navigation_timeout_ms * 3, 30_000)
        timeout_ms = int(task_timeout_ms)
        if timeout_ms <= 0:
            return None
        return timeout_ms

    async def _run_worker_task(self, worker: PlainPlaywrightBrowserWorker, task_data: dict[str, Any], worker_context: WorkerContext) -> Any:
        """通过指定 worker 执行任务，并应用总超时。"""
        task_coro = worker.run_task(task_data=task_data, worker_context=worker_context)
        if self.task_timeout_ms is None:
            return await task_coro
        return await asyncio.wait_for(task_coro, timeout=self.task_timeout_ms / 1000)
