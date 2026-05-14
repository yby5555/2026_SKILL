"""浏览器 worker 执行器。

该模块封装单个浏览器 worker 的生命周期，包括浏览器启动、context/page
创建、任务执行、失败统计以及按条件回收浏览器。
"""

from __future__ import annotations

import asyncio
import os
import types
from typing import Any

from playwright.async_api import BrowserContext, Page
from playwright_stealth.stealth import Stealth
from scrapling.engines._browsers._stealth import AsyncStealthySession
from scrapling.engines.toolbelt.navigation import construct_proxy_dict




from ..failure_category import FailureCategory
from ..worker_context import WorkerContext


class BrowserWorker:
    """单个浏览器 worker 的执行器。

    说明:
        一个 `BrowserWorker` 负责维护一个底层浏览器会话，并在每次任务执行时:
        - 按需启动或复用浏览器
        - 创建新的 `BrowserContext`
        - 创建新的 `Page`
        - 注入 Cookie、Stealth 和上下文初始化逻辑
        - 调用上层提供的任务处理函数
        - 根据任务数和失败次数决定是否回收浏览器

    为什么按 worker 粒度管理:
        调度层只需要挑选空闲 worker 并控制并发，而具体浏览器生命周期、
        上下文创建与失败后的回收策略，都由这个类自己封装处理。
    """

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
    ) -> None:
        """初始化一个浏览器 worker。

        参数:
            worker_id:
                worker 的唯一编号。
            max_contexts:
                单个浏览器可并发承载的最大 context 数量。
            navigation_timeout_ms:
                页面导航与常规操作的默认超时。
            launch_options_factory:
                构造浏览器启动参数的工厂函数。
            context_options_factory:
                构造 `browser.new_context(...)` 参数的工厂函数。
            create_cookies_payload:
                将任务 Cookie 转换成 Playwright `storage_state` 的回调。
            initialize_context:
                context 级初始化钩子。
            initialize_page:
                page 级初始化钩子。
            process_task:
                真正执行业务逻辑的任务处理函数。
            recycle_after_tasks:
                达到指定任务数后，请求回收浏览器。
            recycle_after_failures:
                连续失败达到阈值后，请求回收浏览器。

        设计说明:
            `BrowserWorker` 不关心任务调度策略，它只负责把浏览器、context、
            page 和执行钩子串起来，并维护自身的运行状态。
        """
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

        self._session: Any | None = None
        self._browser: Any | None = None
        self._start_lock = asyncio.Lock()
        self._stealth = Stealth()
        self.tasks_since_recycle = 0
        self.consecutive_failures = 0
        self.request_recycle = False
        self._context_counter = 0

    async def ensure_started(self) -> None:
        """确保 worker 底层浏览器已经启动。

        说明:
            首次调用时会创建 `AsyncStealthySession` 并提取浏览器实例；
            后续调用会直接复用已有浏览器，不会重复启动。

        为什么需要 `_start_lock`:
            多个协程可能同时抢占同一个 worker，锁可以保证浏览器只会被
            初始化一次，避免并发启动导致重复资源创建。

        额外处理:
            启动成功后会给 session 打一个 `_build_context_with_proxy` 补丁，
            让后续创建 context 时可以按任务动态注入代理配置。
        """
        if self._browser is not None and await self._is_browser_healthy():
            return

        async with self._start_lock:
            if self._browser is not None and await self._is_browser_healthy():
                return
            if self._session is not None or self._browser is not None:
                await self.close()

            session = AsyncStealthySession(**self._launch_options_factory(self))
            await session.start()

            browser = getattr(session, "browser", None)
            if browser is None and getattr(session, "context", None) is not None:
                browser = session.context.browser
            if browser is None:
                await session.close()
                raise RuntimeError(f"Worker {self.worker_id} started without browser instance")

            def _build_context_with_proxy_patch(session_self: Any, proxy: str | dict[str, Any] | None = None) -> dict[str, Any]:
                """根据代理参数生成 context 配置。

                参数:
                    session_self:
                        当前 `AsyncStealthySession` 实例。
                    proxy:
                        本次任务要使用的代理配置。

                返回:
                    dict[str, Any]:
                        创建 context 时要传入的参数。

                说明:
                    这里复制 session 中已有的 context 配置，再按需覆盖代理，
                    这样既能复用默认配置，也能做到任务级代理切换。
                """
                context_options = session_self._context_options.copy()
                requested_options = self._context_options_factory(self, proxy)
                if os.getenv("FLOW_FORCE_CONTEXT_FINGERPRINT", "").lower() in {"1", "true", "yes", "on"}:
                    context_options.update(requested_options)
                else:
                    for key in ("locale", "timezone_id", "viewport", "screen", "ignore_https_errors"):
                        if key in requested_options:
                            context_options[key] = requested_options[key]
                if proxy:
                    context_options["proxy"] = construct_proxy_dict(proxy)
                return context_options

            session._build_context_with_proxy = types.MethodType(_build_context_with_proxy_patch, session)
            self._session = session
            self._browser = browser

    async def _is_browser_healthy(self) -> bool:
        """检查当前 browser/session 引用是否仍然可用。"""
        if self._browser is None or self._session is None:
            return False

        is_connected = getattr(self._browser, "is_connected", None)
        try:
            if callable(is_connected) and not bool(is_connected()):
                return False
        except Exception:
            return False

        try:
            version = getattr(self._browser, "version", None)
            if callable(version):
                version = version()
            return bool(version)
        except Exception:
            return False

    async def close(self) -> None:
        """关闭 worker 持有的浏览器会话并重置状态。

        调用后会清空浏览器与 session 引用，同时把任务计数、连续失败次数
        和回收标记恢复到初始状态。
        """
        if self._session is not None:
            try:
                await self._session.close()
            except Exception:
                pass
        self._session = None
        self._browser = None
        self.tasks_since_recycle = 0
        self.consecutive_failures = 0
        self.request_recycle = False
        self._context_counter = 0

    async def recycle_if_needed(self, *, active_tasks: int) -> None:
        """在满足条件时回收浏览器。

        参数:
            active_tasks:
                当前 worker 仍在执行的任务数。

        只有当 `active_tasks == 0` 且已设置回收标记时才会真正关闭浏览器，
        这样可以避免中途打断仍在运行的任务。
        """
        if active_tasks > 0 or not self.request_recycle:
            return
        await self.close()

    def record_success(self) -> None:
        """记录一次成功执行，并判断是否需要回收浏览器。

        成功后会递增任务计数、清零连续失败次数；如果累计任务数达到阈值，
        则仅设置回收标记，真正关闭动作会在 worker 空闲后执行。
        """
        self.tasks_since_recycle += 1
        self.consecutive_failures = 0
        if self._recycle_after_tasks and self.tasks_since_recycle >= self._recycle_after_tasks:
            self.request_recycle = True

    def record_failure(self, category: FailureCategory) -> None:
        """记录一次失败执行，并按失败类别更新回收策略。

        参数:
            category:
                本次失败的分类结果。

        说明:
            worker 级错误会立即请求回收浏览器；同时任务数和连续失败数也会
            持续累计，达到对应阈值后同样会触发回收。
        """
        self.tasks_since_recycle += 1
        self.consecutive_failures += 1
        if category is FailureCategory.WORKER_ERROR:
            self.request_recycle = True
        if self._recycle_after_tasks and self.tasks_since_recycle >= self._recycle_after_tasks:
            self.request_recycle = True
        if self._recycle_after_failures and self.consecutive_failures >= self._recycle_after_failures:
            self.request_recycle = True

    async def run_task(
        self,
        *,
        task_data: dict[str, Any],
        proxy: str | dict[str, Any] | None,
        worker_context: WorkerContext,
    ) -> Any:
        """创建 context/page 并执行单个任务。

        参数:
            task_data:
                当前任务数据。
            proxy:
                当前任务使用的代理信息。
            worker_context:
                当前 worker 的状态快照。

        返回:
            Any:
                业务层 `process_task()` 的返回值。
        """
        await self.ensure_started()
        if self._browser is None or self._session is None:
            raise RuntimeError(f"Worker {self.worker_id} is not ready")

        cookies_payload = await self._create_cookies_payload(task_data)
        context_options = self._resolve_context_options(proxy)
        context: BrowserContext | None = None
        page: Page | None = None
        try:
            if cookies_payload:
                context = await self._browser.new_context(
                    storage_state={"cookies": cookies_payload},
                    **context_options,
                )
            else:
                context = await self._browser.new_context(**context_options)

            context = await self._session._initialize_context(self._session._config, context)
            context = await self._initialize_context_hook(context, task_data, worker_context)
            page = await context.new_page()
            page.set_default_navigation_timeout(self.navigation_timeout_ms)
            page.set_default_timeout(self.navigation_timeout_ms)
            # 默认不再对页面注入 playwright_stealth，避免改写 webdriver /
            # plugins / UA-CH 等浏览器指纹；需要旧行为时显式设置环境变量。
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

    def _resolve_context_options(self, proxy: str | dict[str, Any] | None) -> dict[str, Any]:
        """解析当前任务对应的 Playwright context 配置。

        参数:
            proxy:
                当前任务要使用的代理配置。

        返回:
            dict[str, Any]:
                传给 `browser.new_context(...)` 的参数。

        优先使用 session 上补丁后的 `_build_context_with_proxy`，这样可以复用
        既有默认配置；如果不存在，再回退到外部提供的工厂函数。
        """
        if self._session is None:
            raise RuntimeError(f"Worker {self.worker_id} session has not been started")
        if hasattr(self._session, "_build_context_with_proxy"):
            return self._session._build_context_with_proxy(proxy)
        return self._context_options_factory(self, proxy)
