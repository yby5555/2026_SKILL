"""多浏览器抓取基类与 Cookie 辅助函数。

该模块提供 `MultiBrowserScraperBase`，用于管理多个浏览器 worker，
统一处理任务调度、Cookie 规范化、代理注入、失败分类和浏览器回收策略。
上层业务通常通过继承该基类并实现 `process_task()` 来完成具体抓取逻辑。
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

from playwright.async_api import BrowserContext, Page
from scrapling.engines.toolbelt.navigation import construct_proxy_dict

from ..browser_worker import BrowserWorker
from ..failure_category import FailureCategory
from ..worker_context import WorkerContext

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def normalize_cookies(
    cookies: str | dict[str, str] | list[dict[str, Any]] | None,
    *,
    default_domain: str = ".tiktok.com",
) -> list[dict[str, Any]]:
    """将多种格式的 Cookie 输入转换为 Playwright 可识别的列表结构。

    功能:
        接收字符串、字典、Cookie 列表或空值，并统一输出标准 Cookie 列表。

    参数:
        cookies:
            原始 Cookie 输入。支持 `name=value; ...` 字符串、字典、Cookie
            对象列表或 `None`。
        default_domain:
            当输入中未显式提供域名时使用的默认域名。

    返回:
        list[dict[str, Any]]:
            标准化后的 Cookie 列表，可直接传给 Playwright。
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
        name = name.strip()
        value = value.strip()
        if not name:
            continue
        result.append(
            {
                "name": name,
                "value": value,
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
    default_domain: str = ".tiktok.com",
) -> list[dict[str, Any]]:
    """加载并标准化 Cookie 输入。

    功能:
        如果传入的是文件路径则先读取文件内容，否则直接把原始输入交给
        `normalize_cookies()` 处理。

    参数:
        cookies_or_path:
            Cookie 内容本身，或指向 Cookie 文件的路径。
        default_domain:
            未显式指定域名时使用的默认域名。

    返回:
        list[dict[str, Any]]:
            规范化后的 Cookie 列表。
    """
    if isinstance(cookies_or_path, Path):
        return normalize_cookies(cookies_or_path.read_text(encoding="utf-8"), default_domain=default_domain)
    if isinstance(cookies_or_path, str):
        path = Path(cookies_or_path)
        if path.exists():
            return normalize_cookies(path.read_text(encoding="utf-8"), default_domain=default_domain)
    return normalize_cookies(cookies_or_path, default_domain=default_domain)


_WORKER_ERROR_HINTS = (
    "worker",
    "browser has been closed",
    "browser closed",
    "target page, context or browser has been closed",
    "context has been closed",
    "page has been closed",
    "session has not been started",
    "not ready",
    "crash",
    "closed unexpectedly",
    "connection closed",
    "disconnected",
)
_PROXY_ERROR_HINTS = (
    "proxy",
    "tunnel",
    "407",
    "econnreset",
    "connection reset",
    "connection refused",
    "tlsv1 alert",
)
_BAN_ERROR_HINTS = (
    "403",
    "forbidden",
    "captcha",
    "challenge",
    "verify you are human",
    "access denied",
    "blocked",
    "too many requests",
    "429",
)
_TASK_ERROR_TYPES = (ValueError, TypeError, KeyError, AssertionError, NotImplementedError)


class MultiBrowserScraperBase:
    """管理多浏览器 worker 池的抓取基类。

    功能:
        负责初始化 worker 池、分配任务、构建浏览器上下文配置、分类失败原因，
        并在达到阈值时回收浏览器实例。

    使用方式:
        子类通常需要重写 `process_task()`，必要时也可以扩展
        `initialize_context()`、`initialize_page()`、`normalize_task()` 等钩子。
    """

    def __init__(
        self,
        *,
        browser_pool_size: int = 2,
        max_contexts_per_browser: int = 5,
        headless: bool = True,
        locale: str = "en-US",
        timezone_id: str = "Asia/Shanghai",
        user_agent: str = DEFAULT_USER_AGENT,
        navigation_timeout_ms: int = 10_000,
        task_timeout_ms: int | None = None,
        extra_flags: list[str] | None = None,
        viewport: dict[str, int] | None = None,
        default_cookies: str | dict[str, str] | list[dict[str, Any]] | None = None,
        default_cookie_domain: str = ".tiktok.com",
        default_proxy: str | dict[str, Any] | None = None,
        solve_cloudflare: bool = False,
        block_webrtc: bool = True,
        hide_canvas: bool = True,
        recycle_browser_after_tasks: int | None = 200,
        recycle_browser_after_failures: int | None = 5,
    ) -> None:
        """初始化多浏览器抓取基类。

        功能:
            保存浏览器池规模、上下文并发、超时、代理、Cookie 和回收策略等配置。

        参数:
            browser_pool_size:
                浏览器 worker 数量。
            max_contexts_per_browser:
                单个浏览器允许并发创建的最大 context 数量。
            headless:
                是否以无头模式启动浏览器。
            locale:
                默认语言区域。
            timezone_id:
                默认时区标识。
            user_agent:
                页面默认 User-Agent。
            navigation_timeout_ms:
                页面导航和通用操作默认超时，单位毫秒。
            task_timeout_ms:
                单个任务总超时，单位毫秒；`None` 表示自动计算。
            extra_flags:
                额外浏览器启动参数。
            viewport:
                默认视口尺寸。
            default_cookies:
                默认 Cookie 输入。
            default_cookie_domain:
                默认 Cookie 域名。
            default_proxy:
                默认代理配置。
            solve_cloudflare:
                是否启用 Cloudflare 绕过能力。
            block_webrtc:
                是否屏蔽 WebRTC。
            hide_canvas:
                是否隐藏 Canvas 指纹。
            recycle_browser_after_tasks:
                单个 worker 执行多少任务后请求回收浏览器。
            recycle_browser_after_failures:
                单个 worker 连续失败多少次后请求回收浏览器。

        返回:
            None:
                构造函数只初始化对象状态，不返回额外结果。
        """
        self.browser_pool_size = max(1, int(browser_pool_size))
        self.max_contexts_per_browser = max(1, int(max_contexts_per_browser))
        self.headless = headless
        self.locale = locale
        self.timezone_id = timezone_id
        self.user_agent = user_agent
        self.navigation_timeout_ms = int(navigation_timeout_ms)
        self.task_timeout_ms = self._resolve_task_timeout_ms(task_timeout_ms)
        self.extra_flags = list(extra_flags or [])
        self.viewport = viewport or {"width": 1366, "height": 900}
        self.default_cookies = default_cookies
        self.default_cookie_domain = default_cookie_domain
        self.default_proxy = default_proxy
        self.solve_cloudflare = solve_cloudflare
        self.block_webrtc = block_webrtc
        self.hide_canvas = hide_canvas

        self._workers: list[BrowserWorker] = []
        self._pool_condition = asyncio.Condition()
        self._started = False
        self._closed = False
        self._start_lock = asyncio.Lock()
        self._worker_active_counts: dict[int, int] = {}
        self._workers_recycling: set[int] = set()
        self._recycle_browser_after_tasks = recycle_browser_after_tasks
        self._recycle_browser_after_failures = recycle_browser_after_failures

    async def start(self) -> None:
        """启动浏览器 worker 池。

        功能:
            首次调用时创建并注册所有 `BrowserWorker`，重复调用会直接复用。

        参数:
            无。

        返回:
            None:
                只更新内部状态，不返回业务结果。
        """
        if self._started and not self._closed:
            return

        async with self._start_lock:
            if self._started and not self._closed:
                return
            if not self._started:
                self._workers = [
                    BrowserWorker(
                        worker_id=index,
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
                    )
                    for index in range(self.browser_pool_size)
                ]
                self._worker_active_counts = {worker.worker_id: 0 for worker in self._workers}
                self._started = True
            self._workers_recycling.clear()
            self._closed = False

    async def close(self) -> None:
        """关闭所有 worker 并释放浏览器资源。

        功能:
            逐个关闭浏览器 worker，清理回收标记，并唤醒等待中的任务调度协程。

        参数:
            无。

        返回:
            None:
                只负责资源回收，不返回业务结果。
        """
        if not self._started or self._closed:
            return

        for worker in self._workers:
            await worker.close()
        async with self._pool_condition:
            self._workers_recycling.clear()
            self._pool_condition.notify_all()
        self._closed = True

    async def __aenter__(self) -> "MultiBrowserScraperBase":
        """进入异步上下文并自动启动 worker 池。

        功能:
            让调用方可以通过 `async with` 使用该基类。

        参数:
            无。

        返回:
            MultiBrowserScraperBase:
                当前抓取器实例自身。
        """
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """退出异步上下文并关闭 worker 池。

        功能:
            无论上下文块内是否抛出异常，都尝试释放底层浏览器资源。

        参数:
            exc_type:
                上下文中抛出的异常类型。
            exc:
                上下文中抛出的异常实例。
            tb:
                异常回溯对象。

        返回:
            None:
                仅执行清理逻辑。
        """
        await self.close()

    def build_launch_options(self, worker: BrowserWorker) -> dict[str, Any]:
        """构建单个 worker 的浏览器启动参数。

        功能:
            根据全局配置和操作系统差异，生成启动浏览器所需的参数字典。

        参数:
            worker:
                当前要启动的浏览器 worker。

        返回:
            dict[str, Any]:
                传给底层浏览器会话构造器的启动配置。
        """
        args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars",
            *self.extra_flags,
        ]
        
        # 只有在 viewport 宽度和高度都大于 0 时才设置窗口大小
        if self.viewport and self.viewport.get('width', 0) > 0 and self.viewport.get('height', 0) > 0:
            args.append(f"--window-size={self.viewport['width']},{self.viewport['height']}")
            
        if sys.platform != "win32":
            args.extend(
                [
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--no-first-run",
                    "--no-zygote",
                ]
            )
        return {
            "max_pages": worker.max_contexts,
            "headless": self.headless,
            "solve_cloudflare": self.solve_cloudflare,
            "block_webrtc": self.block_webrtc,
            "hide_canvas": self.hide_canvas,
            "timeout": self.navigation_timeout_ms,
            "extra_flags": args,
        }

    def resolve_task_proxy(self, task_data: dict[str, Any]) -> str | dict[str, Any] | None:
        """解析单个任务实际使用的代理配置。

        功能:
            优先读取任务级代理，若未提供则回退到实例级默认代理。

        参数:
            task_data:
                当前任务数据。

        返回:
            str | dict[str, Any] | None:
                当前任务要使用的代理配置。
        """
        return task_data.get("proxy", self.default_proxy)

    def build_context_options(
        self,
        worker: BrowserWorker,
        proxy: str | dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """构建 Playwright browser context 配置。

        功能:
            生成语言、时区、UA、视口和请求头等上下文参数，并按需注入代理。

        参数:
            worker:
                当前 worker 实例；本实现中仅用于保持接口一致。
            proxy:
                当前任务使用的代理配置。

        返回:
            dict[str, Any]:
                传给 `browser.new_context(...)` 的配置字典。
        """
        del worker
        proxy_settings = construct_proxy_dict(proxy)
        options: dict[str, Any] = {
            "locale": self.locale,
            "timezone_id": self.timezone_id,
            "user_agent": self.user_agent,
            "extra_http_headers": {
                "Accept-Language": f"{self.locale},en;q=0.9",
                "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"',
                "Upgrade-Insecure-Requests": "1",
            },
        }

        # 只有在 viewport 宽度和高度都大于 0 时才设置视口
        if self.viewport and self.viewport.get('width', 0) > 0 and self.viewport.get('height', 0) > 0:
            options["viewport"] = self.viewport
            options["screen"] = self.viewport
        else:
            # 如果设为 0，则传 None 让 Playwright 自动处理（通常是随窗口大小）
            options["viewport"] = None
            
        if proxy_settings:
            options["proxy"] = proxy_settings
        return options

    async def initialize_context(
        self,
        context: BrowserContext,
        task_data: dict[str, Any],
        worker: WorkerContext,
    ) -> BrowserContext:
        """执行 context 级初始化钩子。

        功能:
            预留给子类扩展，用于在创建页面前对 `BrowserContext` 做初始化处理。

        参数:
            context:
                当前刚创建好的浏览器上下文。
            task_data:
                当前任务数据。
            worker:
                当前 worker 的运行时快照。

        返回:
            BrowserContext:
                处理后的浏览器上下文。
        """
        del task_data, worker
        return context

    async def initialize_page(
        self,
        page: Page,
        task_data: dict[str, Any],
        worker: WorkerContext,
    ) -> None:
        """执行 page 级初始化钩子。

        功能:
            预留给子类扩展，用于在业务逻辑执行前对页面做额外设置。

        参数:
            page:
                当前任务对应的新页面对象。
            task_data:
                当前任务数据。
            worker:
                当前 worker 的运行时快照。

        返回:
            None:
                默认实现不做额外处理。
        """
        del page, task_data, worker
        return None

    def normalize_task(self, task_data: dict[str, Any]) -> dict[str, Any]:
        """标准化任务数据。

        功能:
            为子类提供统一的任务预处理入口，默认仅返回原任务的浅拷贝。

        参数:
            task_data:
                原始任务数据。

        返回:
            dict[str, Any]:
                标准化后的任务数据。
        """
        return dict(task_data)

    async def classify_failure(
        self,
        exc: Exception,
        task_data: dict[str, Any],
        *,
        worker: WorkerContext | None,
    ) -> FailureCategory:
        """根据异常信息归类任务失败原因。

        功能:
            结合异常类型和异常文本，把失败映射为代理错误、封禁错误、
            worker 错误、任务错误或未知错误。

        参数:
            exc:
                当前任务抛出的异常对象。
            task_data:
                当前任务数据；默认实现中未直接使用，保留给子类扩展。
            worker:
                当前 worker 运行时快照；默认实现中未直接使用。

        返回:
            FailureCategory:
                归类后的失败类型，用于后续回收或重试策略判断。
        """
        del task_data, worker
        if isinstance(exc, asyncio.TimeoutError):
            return FailureCategory.WORKER_ERROR

        message = self._build_exception_text(exc)
        if any(hint in message for hint in _PROXY_ERROR_HINTS):
            return FailureCategory.PROXY_ERROR
        if any(hint in message for hint in _BAN_ERROR_HINTS):
            return FailureCategory.BAN_ERROR
        if any(hint in message for hint in _WORKER_ERROR_HINTS):
            return FailureCategory.WORKER_ERROR
        if isinstance(exc, _TASK_ERROR_TYPES):
            return FailureCategory.TASK_ERROR
        return FailureCategory.UNKNOWN

    async def process_task(
        self,
        page: Page,
        task_data: dict[str, Any],
        worker: WorkerContext,
    ) -> Any:
        """执行业务任务逻辑。

        功能:
            这是子类必须实现的核心方法，负责在页面对象上完成实际抓取或操作。

        参数:
            page:
                当前任务使用的页面对象。
            task_data:
                当前任务数据。
            worker:
                当前 worker 的运行时快照。

        返回:
            Any:
                业务任务执行结果，由子类自行定义。
        """
        del page, task_data, worker
        raise NotImplementedError

    async def run_tasks(self, tasks: list[dict[str, Any]]) -> list[Any]:
        """并发执行一批任务。

        功能:
            启动 worker 池后，为每个任务创建协程并收集执行结果或异常对象。

        参数:
            tasks:
                要执行的任务列表。

        返回:
            list[Any]:
                与输入任务顺序一致的结果列表；失败项会保留异常对象。
        """
        await self.start()
        return await asyncio.gather(*(self._run_single_task(task) for task in tasks), return_exceptions=True)

    async def _run_single_task(self, task_data: dict[str, Any]) -> Any:
        """执行单个任务并维护 worker 状态。

        功能:
            负责获取 worker、创建运行时快照、执行任务、记录成功或失败，
            并在结束后释放 worker。

        参数:
            task_data:
                原始任务数据。

        返回:
            Any:
                单个任务的执行结果。
        """
        task = self.normalize_task(task_data)
        worker_record: BrowserWorker | None = None
        context_id = 0
        try:
            worker_record = await self._acquire_worker()
            context_id = self._worker_active_counts.get(worker_record.worker_id, 1) - 1
            worker_context = self._build_worker_context(worker_record, context_id)
            result = await self._run_worker_task(worker_record, task, worker_context)
            worker_record.record_success()
            return result
        except Exception as exc:
            if worker_record is not None:
                failure_category = await self.classify_failure(
                    exc,
                    task,
                    worker=self._build_worker_context(worker_record, context_id),
                )
                worker_record.record_failure(failure_category)
            raise
        finally:
            if worker_record is not None:
                await self._release_worker(worker_record)

    async def _build_cookie_payload(self, task_data: dict[str, Any]) -> list[dict[str, Any]]:
        """为单个任务构建 Cookie 载荷。

        功能:
            从任务数据或默认配置中提取 Cookie，并转换为标准列表格式。

        参数:
            task_data:
                当前任务数据。

        返回:
            list[dict[str, Any]]:
                传给浏览器上下文的 Cookie 列表。
        """
        cookies = task_data.get("cookies", self.default_cookies)
        return load_cookies(cookies, default_domain=self.default_cookie_domain)

    async def _acquire_worker(self) -> BrowserWorker:
        """获取一个当前可用的 worker。

        功能:
            在 worker 池中选择活动任务数最少、失败次数较少且未处于回收中的
            worker；如果没有可用 worker，则等待。

        参数:
            无。

        返回:
            BrowserWorker:
                被分配给当前任务的 worker 实例。
        """
        await self.start()
        while True:
            async with self._pool_condition:
                available = [
                    worker
                    for worker in self._workers
                    if self._worker_active_counts.get(worker.worker_id, 0) < worker.max_contexts
                    and worker.worker_id not in self._workers_recycling
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
                    return worker
                await self._pool_condition.wait()

    async def _release_worker(self, worker: BrowserWorker) -> None:
        """释放 worker 并在必要时触发回收。

        功能:
            递减 worker 活动计数；若该 worker 已请求回收且当前无活跃任务，
            则执行浏览器回收流程。

        参数:
            worker:
                需要释放的 worker 实例。

        返回:
            None:
                仅更新状态并在必要时回收资源。
        """
        should_recycle = False
        async with self._pool_condition:
            active_count = self._worker_active_counts.get(worker.worker_id, 0)
            if active_count > 0:
                self._worker_active_counts[worker.worker_id] = active_count - 1
            current_active = self._worker_active_counts.get(worker.worker_id, 0)
            if current_active == 0 and worker.request_recycle:
                self._workers_recycling.add(worker.worker_id)
                should_recycle = True
            self._pool_condition.notify_all()

        if not should_recycle:
            await worker.recycle_if_needed(active_tasks=current_active)
            return

        try:
            await worker.recycle_if_needed(active_tasks=0)
        finally:
            async with self._pool_condition:
                self._workers_recycling.discard(worker.worker_id)
                self._pool_condition.notify_all()

    def _build_worker_context(self, worker: BrowserWorker, context_id: int = 0) -> WorkerContext:
        """基于当前 worker 状态构建运行时快照。

        功能:
            把 `BrowserWorker` 的关键指标封装成 `WorkerContext`，供钩子和
            业务处理函数使用。

        参数:
            worker:
                当前 worker 实例。
            context_id:
                当前任务在所属浏览器内分配的上下文序号。

        返回:
            WorkerContext:
                当前 worker 的只读状态快照。
        """
        return WorkerContext(
            worker_id=worker.worker_id,
            browser_task_capacity=worker.max_contexts,
            active_tasks=self._worker_active_counts.get(worker.worker_id, 0),
            tasks_since_recycle=worker.tasks_since_recycle,
            consecutive_failures=worker.consecutive_failures,
            context_id=context_id,
        )

    def _resolve_task_timeout_ms(self, task_timeout_ms: int | None) -> int | None:
        """解析单任务超时时间。

        功能:
            将传入的任务超时配置转换为规范值；未提供时自动给出默认值，
            小于等于 0 时视为不限制超时。

        参数:
            task_timeout_ms:
                原始任务超时配置，单位毫秒。

        返回:
            int | None:
                规范化后的超时时间；`None` 表示不设置总超时。
        """
        if task_timeout_ms is None:
            return max(self.navigation_timeout_ms * 3, 30_000)
        timeout_ms = int(task_timeout_ms)
        if timeout_ms <= 0:
            return None
        return timeout_ms

    async def _run_worker_task(
        self,
        worker: BrowserWorker,
        task_data: dict[str, Any],
        worker_context: WorkerContext,
    ) -> Any:
        """通过指定 worker 执行任务，并应用总超时控制。

        功能:
            调用 `BrowserWorker.run_task()` 执行任务；如果配置了总超时，则用
            `asyncio.wait_for()` 包装执行过程。

        参数:
            worker:
                负责执行任务的 worker。
            task_data:
                当前任务数据。
            worker_context:
                当前 worker 的运行时快照。

        返回:
            Any:
                worker 执行任务后返回的结果。
        """
        task_coro = worker.run_task(
            task_data=task_data,
            proxy=self.resolve_task_proxy(task_data),
            worker_context=worker_context,
        )
        if self.task_timeout_ms is None:
            return await task_coro
        return await asyncio.wait_for(task_coro, timeout=self.task_timeout_ms / 1000)

    @staticmethod
    def _build_exception_text(exc: Exception) -> str:
        """把异常对象转换为统一的小写文本。

        功能:
            生成包含异常类型和异常消息的字符串，便于后续做关键字匹配分类。

        参数:
            exc:
                要处理的异常对象。

        返回:
            str:
                小写后的异常文本。
        """
        return f"{type(exc).__name__}: {exc}".lower()
