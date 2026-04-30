from __future__ import annotations
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from playwright.async_api import BrowserContext, Page
from scrapling.engines._browsers._stealth import AsyncStealthySession
from playwright_stealth.stealth import Stealth

import asyncio
import json
import sys
import types


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def build_playwright_proxy(proxy: str | dict[str, Any] | None) -> dict[str, str] | None:
    """
    将代理字符串或字典转换为 Playwright 支持的代理格式。
    
    参数:
        proxy: 代理地址，可以是带 scheme 的 URL 字符串，也可以是字典格式。
        
    返回:
        Playwright 接受的 proxy 字典配置（包含 server, username, password）。
    """
    if not proxy:
        return None
    if isinstance(proxy, dict):
        if "server" in proxy:
            return {str(k): str(v) for k, v in proxy.items() if v is not None}
        scheme = proxy.get("scheme") or proxy.get("protocol") or "http"
        host = proxy.get("host") or proxy.get("ip")
        port = proxy.get("port")
        username = proxy.get("username") or proxy.get("user")
        password = proxy.get("password") or proxy.get("pass")
        if not host or not port:
            raise ValueError(f"Invalid proxy dict: {proxy}")
        result = {"server": f"{scheme}://{host}:{port}"}
        if username:
            result["username"] = str(username)
        if password:
            result["password"] = str(password)
        return result

    parsed = urlparse(proxy)
    if not parsed.scheme or not parsed.hostname or not parsed.port:
        raise ValueError(f"Invalid proxy string: {proxy}")

    result = {"server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"}
    if parsed.username:
        result["username"] = parsed.username
    if parsed.password:
        result["password"] = parsed.password
    return result


def normalize_cookies(
    cookies: str | dict[str, str] | list[dict[str, Any]] | None,
    *,
    default_domain: str = ".tiktok.com",
) -> list[dict[str, Any]]:
    """
    将不同格式的 Cookie (字符串、字典、列表) 统一转换为 Playwright 需要的字典列表格式。
    
    参数:
        cookies: Cookie 数据（支持原始分号分隔的文本、JSON文本、字典或列表）。
        default_domain: 未指定域名的 Cookie 所默认挂载的域名。
        
    返回:
        格式化后的 Cookie 列表，适用于 context.add_cookies()。
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
    """
    从文件路径或字符串/字典中加载并解析 Cookie。
    
    参数:
        cookies_or_path: Cookie 内容或存储 Cookie 文件的路径。
        default_domain: 默认的 Cookie 域名。
        
    返回:
        解析并格式化后的 Cookie 列表。
    """
    if isinstance(cookies_or_path, Path):
        return normalize_cookies(cookies_or_path.read_text(encoding="utf-8"), default_domain=default_domain)
    if isinstance(cookies_or_path, str):
        path = Path(cookies_or_path)
        if path.exists():
            return normalize_cookies(path.read_text(encoding="utf-8"), default_domain=default_domain)
    return normalize_cookies(cookies_or_path, default_domain=default_domain)


class BaseSessionContextScraper:
    """
    基于 AsyncStealthySession 的“单 browser + 多 context”可复用抓取基类。

    适合：
    - 一个浏览器进程承载多个异步任务
    - 每个任务使用独立上下文进行隔离
    - 每个任务单独指定代理 / Cookie
    """

    def __init__(
        self,
        *,
        max_tabs: int = 5,  # 最大并发标签页/任务数量，控制最大并发数，默认 5
        headless: bool = True,  # 是否使用无头模式启动浏览器，默认 True（后台运行）
        locale: str = "en-US",  # 浏览器的默认语言环境，默认 "en-US"
        timezone_id: str = "Asia/Shanghai",  # 浏览器的默认时区，默认 "Asia/Shanghai"
        user_agent: str = DEFAULT_USER_AGENT,  # 浏览器默认使用的 User-Agent 字符串
        navigation_timeout_ms: int = 10_000,  # 页面加载和所有导航动作的默认超时时间（毫秒），默认 10,000 (10秒)
        extra_flags: list[str] | None = None,  # 启动浏览器时附加的额外命令行参数列表
        viewport: dict[str, int] | None = None,  # 浏览器窗口大小配置，未指定时默认 {"width": 1366, "height": 900}
        default_proxy: str | dict[str, Any] | None = None,  # 默认代理配置（支持 URL 字符串或字典格式），默认不使用代理
        default_cookies: str | Path | dict[str, str] | list[dict[str, Any]] | None = None,  # 默认注入的 Cookie 数据或文件路径，默认 None
        default_cookie_domain: str = ".tiktok.com",  # 当 Cookie 未指定 domain 属性时，默认挂载的域名，默认 ".tiktok.com"
        solve_cloudflare: bool = False,  # 是否开启内置的 Cloudflare 盾牌绕过机制，默认 True
        block_webrtc: bool = True,  # 是否屏蔽 WebRTC，防止真实 IP 被泄漏，默认 True
        hide_canvas: bool = True,  # 是否隐藏 Canvas 指纹特征，降低被识别为机器人的概率，默认 True
    ) -> None:
        """
        初始化 Session Context Scraper，配置浏览器启动参数、并发数及默认设置。
        """
        self.max_tabs = max(1, int(max_tabs))
        self.headless = headless
        self.locale = locale
        self.timezone_id = timezone_id
        self.user_agent = user_agent
        self.navigation_timeout_ms = navigation_timeout_ms
        self.extra_flags = list(extra_flags or [])
        self.viewport = viewport or {"width": 1366, "height": 900}
        self.default_proxy = default_proxy
        self.default_cookies = default_cookies
        self.default_cookie_domain = default_cookie_domain
        self.solve_cloudflare = solve_cloudflare
        self.block_webrtc = block_webrtc
        self.hide_canvas = hide_canvas

        self._session: Any | None = None
        self._browser: Any | None = None
        self._semaphore = asyncio.Semaphore(self.max_tabs)
        self._start_lock = asyncio.Lock()
        self._stealth = Stealth()

    async def start(self) -> None:
        """
        启动异步隐身浏览器会话，并获取浏览器实例对象。
        如果浏览器已经启动，则直接返回。
        同时对 Scrapling 的代理生成方法进行补丁修复，以支持每次创建 context 动态传入的 proxy。
        """
        if self._browser:
            return
        async with self._start_lock:
            if self._browser:
                return

            session = AsyncStealthySession(**self.build_launch_options())
            await session.start()

            browser = getattr(session, "browser", None)
            if browser is None and getattr(session, "context", None) is not None:
                browser = session.context.browser
            if browser is None:
                await session.close()
                raise RuntimeError("AsyncStealthySession started but no browser/context is available")

            self._session = session
            self._browser = browser

            # 动态修复 _build_context_with_proxy 以支持传参
            if self._session:
                def _build_context_with_proxy_patch(session_self, proxy=None):
                    context_options = session_self._context_options.copy()
                    if proxy:
                        context_options["proxy"] = build_playwright_proxy(proxy)
                    return context_options

                self._session._build_context_with_proxy = types.MethodType(_build_context_with_proxy_patch, self._session)

    async def close(self) -> None:
        """
        关闭浏览器会话并释放所有关联的资源。
        """
        if self._session:
            await self._session.close()
        self._session = None
        self._browser = None

    async def __aenter__(self) -> "BaseSessionContextScraper":
        """
        异步上下文管理器入口，自动启动浏览器实例。
        """
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """
        异步上下文管理器出口，自动关闭浏览器实例。
        """
        await self.close()

    async def get_session(self) -> "BaseSessionContextScraper":
        """
        获取当前会话，如果浏览器尚未启动则触发启动流程。
        """
        await self.start()
        return self

    async def close_session(self) -> None:
        """
        显式调用关闭当前浏览器会话（同 close 方法）。
        """
        await self.close()

    def build_launch_options(self) -> dict[str, Any]:
        """
        构建用于启动 AsyncStealthySession 的配置字典，包含抗指纹特征和无头模式设置等。
        """
        args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars",
            f"--window-size={self.viewport['width']},{self.viewport['height']}",
            *self.extra_flags,
        ]
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
            "max_pages": self.max_tabs,
            "headless": self.headless,
            "solve_cloudflare": self.solve_cloudflare,
            "block_webrtc": self.block_webrtc,
            "hide_canvas": self.hide_canvas,
            "timeout": self.navigation_timeout_ms,
            "extra_flags": args,
        }

    def build_context_options(self, proxy: str | dict[str, Any] | None = None) -> dict[str, Any]:
        """
        构建单个浏览器上下文 (BrowserContext) 的配置参数，包含时区、语言环境、UA 以及代理信息。
        
        参数:
            proxy: 当前上下文需要使用的代理配置。
        """
        proxy_settings = build_playwright_proxy(proxy or self.default_proxy)
        options: dict[str, Any] = {
            "locale": self.locale,
            "timezone_id": self.timezone_id,
            "user_agent": self.user_agent,
            "viewport": self.viewport,
            "screen": self.viewport,
            "extra_http_headers": {
                "Accept-Language": f"{self.locale},en;q=0.9",
                "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"',
                "Upgrade-Insecure-Requests": "1",
            },
        }
        if proxy_settings:
            options["proxy"] = proxy_settings
        return options

    async def create_context(
        self,
        *,
        proxy: str | dict[str, Any] | None = None,
        cookies: str | Path | dict[str, str] | list[dict[str, Any]] | None = None,
    ) -> BrowserContext:
        """
        创建一个隔离的浏览器上下文 (Context)，并注入相关的代理和 Cookie。
        
        参数:
            proxy: 覆盖默认的代理设置。
            cookies: 覆盖默认的 Cookie 设置。
        """
        if not self._browser or not self._session:
            raise RuntimeError("Session has not been started")

        if hasattr(self._session, "_build_context_with_proxy"):
            context_options = self._session._build_context_with_proxy(proxy or self.default_proxy)
        else:
            context_options = self.build_context_options(proxy or self.default_proxy)

        cookie_payload = load_cookies(cookies or self.default_cookies, default_domain=self.default_cookie_domain)
        if cookie_payload:
            context = await self._browser.new_context(
                storage_state={"cookies": cookie_payload},
                **context_options,
            )
        else:
            context = await self._browser.new_context(**context_options)

        context = await self._session._initialize_context(self._session._config, context)
        return context

    async def initialize_context(self, context: BrowserContext, task_data: dict[str, Any]) -> BrowserContext:
        """
        上下文初始化钩子方法，供子类重写以在任务执行前对 Context 进行额外定制（如路由拦截等）。
        """
        return context

    async def create_page(self, context: BrowserContext, task_data: dict[str, Any]) -> Page:
        """
        在给定的上下文中创建一个新页面 (Page)，并设置全局和导航的超时时间。
        注意：此处通过 context.new_page() 创建的页面，属于无痕隔离上下文（Incognito Context），
        每个 context 之间的缓存、Cookie 都是相互隔离的。
        """
        page = await context.new_page()
        # 设置导航超时：限制 page.goto(), page.go_back(), page.reload() 等涉及页面跳转的方法的最大等待时间
        page.set_default_navigation_timeout(self.navigation_timeout_ms)
        # 设置操作超时：限制 page.click(), page.fill(), page.wait_for_selector() 等具体元素交互操作的最大等待时间
        page.set_default_timeout(self.navigation_timeout_ms)
        await self._stealth.apply_stealth_async(page)
        return page

    def normalize_task(self, task_data: dict[str, Any]) -> dict[str, Any]:
        """
        规范化抓取任务数据，默认仅做浅拷贝，子类可重写以实现参数校验或补全。
        """
        return dict(task_data)

    async def process_task(self, page: Page, task_data: dict[str, Any]) -> Any:
        """
        处理单个抓取任务的抽象方法。子类必须实现该方法，并在其中编写具体的页面抓取或交互逻辑。
        """
        raise NotImplementedError

    async def _run_task(self, task_data: dict[str, Any]) -> Any:
        """
        内部方法，用于执行单个任务的完整生命周期：
        规范化数据 -> 获取浏览器 -> 创建上下文 -> 建立页面 -> 处理任务 -> 清理并关闭上下文。
        受最大并发数 (Semaphore) 限制。
        """
        async with self._semaphore:
            task_data = self.normalize_task(task_data)
            await self.get_session()

            proxy = task_data.get("proxy")
            cookies = task_data.get("cookies")
            context = await self.create_context(proxy=proxy, cookies=cookies)
            context = await self.initialize_context(context, task_data)
            page = await self.create_page(context, task_data)
            try:
                return await self.process_task(page, task_data)
            finally:
                await page.close()
                await context.close()

    async def run_tasks(self, tasks: list[dict[str, Any]]) -> list[Any]:
        """
        并发执行多个抓取任务，并收集所有任务的返回结果。
        
        参数:
            tasks: 包含抓取输入、代理、Cookie 等配置的任务字典列表。
            
        返回:
            每个任务对应的结果列表，如遇异常则返回异常对象。
        """
        await self.start()
        return await asyncio.gather(*(self._run_task(task) for task in tasks), return_exceptions=True)
