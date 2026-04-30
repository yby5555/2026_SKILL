from __future__ import annotations

import asyncio
import base64
import json
import logging
import logging.handlers
import os
import sys
from pathlib import Path
from time import monotonic
from typing import Any
from uuid import uuid4

import httpx
from playwright.async_api import async_playwright
from scrapling.engines.toolbelt.navigation import construct_proxy_dict

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.append(str(_ROOT))

_ACCOUNT_MGR = _ROOT / "account_mgr"
if str(_ACCOUNT_MGR) not in sys.path:
    sys.path.insert(0, str(_ACCOUNT_MGR))

from driver_base import MultiBrowserScraperBase
from driver_base.browser_worker import BrowserWorker

from account_checker import LOGIN_EXPIRED_PATTERN, MIN_CREDITS_THRESHOLD
from account_mgr.redis_utils import get_next_cookie, release_cookie, remove_from_pool

_LOG_DIR = _ROOT / "flow" / "log"
_LOG_DIR.mkdir(parents=True, exist_ok=True)
_LOG_FILE = _LOG_DIR / "automation_video_fetch_consumer.log"

logger = logging.getLogger("VideoFetchScraperConsumer")
logger.setLevel(logging.INFO)
if not logger.handlers:
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.handlers.RotatingFileHandler(
        _LOG_FILE,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False


class _NoOpStealth:
    async def apply_stealth_async(self, page) -> None:
        del page


class PlainPlaywrightWorker(BrowserWorker):
    def __init__(self, *args, launch_headless: bool, launch_args: list[str], **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._playwright = None
        self._browser = None
        self._session = True
        self._launch_headless = launch_headless
        self._launch_args = list(launch_args)
        self._stealth = _NoOpStealth()

    async def ensure_started(self) -> None:
        if self._browser is not None and await self._is_browser_healthy():
            return

        async with self._start_lock:
            if self._browser is not None and await self._is_browser_healthy():
                return
            await self.close()
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(
                headless=self._launch_headless,
                args=self._launch_args,
            )
            self._session = True

    async def _is_browser_healthy(self) -> bool:
        if self._browser is None:
            return False
        try:
            return bool(self._browser.is_connected())
        except Exception:
            return False

    async def close(self) -> None:
        if self._browser is not None:
            try:
                await self._browser.close()
            except Exception:
                pass
        self._browser = None
        if self._playwright is not None:
            try:
                await self._playwright.stop()
            except Exception:
                pass
        self._playwright = None
        self._session = None
        self.tasks_since_recycle = 0
        self.consecutive_failures = 0
        self.request_recycle = False

    async def run_task(
        self,
        *,
        task_data: dict[str, Any],
        proxy: str | dict[str, Any] | None,
        worker_context,
    ) -> Any:
        await self.ensure_started()
        if self._browser is None:
            raise RuntimeError(f"Worker {self.worker_id} is not ready")

        cookies_payload = await self._create_cookies_payload(task_data)
        context_options = self._context_options_factory(self, proxy)
        context = None
        page = None
        try:
            if cookies_payload:
                context = await self._browser.new_context(
                    storage_state={"cookies": cookies_payload},
                    **context_options,
                )
            else:
                context = await self._browser.new_context(**context_options)
            context = await self._initialize_context_hook(context, task_data, worker_context)
            page = await context.new_page()
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


FLOW_HOME_URL = "https://labs.google/fx/zh/tools/flow"
CREATE_PROJECT_ENDPOINT = "https://labs.google/fx/api/trpc/project.createProject"
CREDITS_ENDPOINT = "https://aisandbox-pa.googleapis.com/v1/credits?key=AIzaSyBtrm0o5ab1c-Ec8ZuLcGt3oJAA5VWt3pY"
GENERATE_ENDPOINT = "https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoText"
GENERATE_REFERENCE_ENDPOINT = "https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoReferenceImages"
STATUS_ENDPOINT = "https://aisandbox-pa.googleapis.com/v1/video:batchCheckAsyncVideoGenerationStatus"
UPLOAD_IMAGE_ENDPOINT = "https://aisandbox-pa.googleapis.com/v1/flow/uploadImage"
TEXT_VIDEO_MODEL_KEY = "veo_3_1_t2v_lite"
REFERENCE_VIDEO_MODEL_KEY = "veo_3_1_r2v_lite"
SITE_KEY = "6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV"
RECAPTCHA_SCRIPT_URL = f"https://www.google.com/recaptcha/enterprise.js?render={SITE_KEY}"


async def browser_fetch_json(
    page,
    url: str,
    method: str = "POST",
    payload: Any | None = None,
    headers: dict[str, str] | None = None,
    credentials: str = "omit",
) -> dict[str, Any]:
    return await page.evaluate(
        """async ({url, method, payload, headers, credentials}) => {
            const init = { method, headers: headers || {}, credentials };
            if (payload !== undefined && payload !== null) {
                init.body = typeof payload === "string" ? payload : JSON.stringify(payload);
            }
            const response = await fetch(url, init);
            const text = await response.text();
            let jsonPayload = null;
            try {
                jsonPayload = text ? JSON.parse(text) : null;
            } catch (error) {
                jsonPayload = null;
            }
            return {
                ok: response.ok,
                status: response.status,
                text,
                json: jsonPayload,
                headers: Object.fromEntries(response.headers.entries()),
            };
        }""",
        {
            "url": url,
            "method": method,
            "payload": payload,
            "headers": headers or {},
            "credentials": credentials,
        },
    )


async def ensure_recaptcha_ready(page) -> dict[str, Any]:
    last_state: dict[str, Any] = {}
    for attempt in range(3):
        try:
            await page.add_script_tag(url=RECAPTCHA_SCRIPT_URL)
        except Exception as exc:
            logger.warning(f"第 {attempt + 1} 次注入 enterprise.js 失败: {exc}")
        try:
            await page.wait_for_function(
                "() => !!window.grecaptcha && !!window.grecaptcha.enterprise",
                timeout=10_000,
            )
        except Exception:
            pass
        last_state = await page.evaluate(
            """() => ({
                href: location.href,
                readyState: document.readyState,
                hasGrecaptcha: !!window.grecaptcha,
                hasEnterprise: !!window.grecaptcha?.enterprise,
            })"""
        )
        if last_state.get("hasEnterprise"):
            return last_state
        await page.wait_for_timeout(2000)
    raise RuntimeError(f"grecaptcha.enterprise 未就绪: {last_state}")


async def prepare_runtime(page) -> tuple[str, str, dict[str, Any]]:
    await page.goto(FLOW_HOME_URL, wait_until="domcontentloaded")
    state = await ensure_recaptcha_ready(page)
    logger.info(f"recaptcha-state: {state}")
    logger.info("prepare_runtime: 开始获取 session")
    session_payload = await browser_fetch_json(
        page,
        url="/fx/api/auth/session",
        method="GET",
        credentials="include",
    )
    session_json = session_payload.get("json") or {}
    access_token = session_json.get("access_token")
    if not access_token:
        raise RuntimeError(f"session 中未拿到 access_token: {session_json}")
    logger.info("prepare_runtime: session 获取成功，开始执行 recaptcha")
    recaptcha_token = await asyncio.wait_for(
        page.evaluate(
            """async (siteKey) => {
                await new Promise((resolve) => window.grecaptcha.enterprise.ready(resolve));
                return await window.grecaptcha.enterprise.execute(siteKey, { action: "VIDEO_GENERATION" });
            }""",
            SITE_KEY,
        ),
        timeout=30,
    )
    if not recaptcha_token:
        raise RuntimeError("未拿到 recaptcha token")
    logger.info("prepare_runtime: recaptcha 执行成功，开始获取 credits")
    credits_payload = await browser_fetch_json(
        page,
        url=CREDITS_ENDPOINT,
        method="GET",
        headers={"authorization": f"Bearer {access_token}"},
    )
    credits_json = credits_payload.get("json") or {}
    logger.info("prepare_runtime: credits 获取成功")
    return access_token, recaptcha_token, credits_json


async def create_project(page, task_title: str) -> str:
    response = await browser_fetch_json(
        page,
        url=CREATE_PROJECT_ENDPOINT,
        method="POST",
        payload={"json": {"projectTitle": task_title, "toolName": "PINHOLE"}},
        headers={"content-type": "application/json"},
        credentials="include",
    )
    payload = response.get("json") or {}
    project_id = (
        payload.get("result", {})
        .get("data", {})
        .get("json", {})
        .get("result", {})
        .get("projectId")
    )
    if not project_id:
        raise RuntimeError(f"createProject 失败: {response}")
    return project_id


def build_generate_payload(
    prompt: str,
    project_id: str,
    recaptcha_token: str,
    user_paygate_tier: str,
) -> dict[str, Any]:
    return {
        "mediaGenerationContext": {
            "batchId": str(uuid4()),
            "audioFailurePreference": "BLOCK_SILENCED_VIDEOS",
        },
        "clientContext": {
            "projectId": project_id,
            "tool": "PINHOLE",
            "userPaygateTier": user_paygate_tier,
            "sessionId": f";{int(asyncio.get_running_loop().time() * 1000)}",
            "recaptchaContext": {
                "token": recaptcha_token,
                "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB",
            },
        },
        "requests": [
            {
                "aspectRatio": "VIDEO_ASPECT_RATIO_PORTRAIT",
                "seed": 12345,
                "textInput": {
                    "structuredPrompt": {
                        "parts": [{"text": prompt}],
                    }
                },
                "videoModelKey": TEXT_VIDEO_MODEL_KEY,
                "metadata": {},
            }
        ],
        "useV2ModelConfig": True,
    }


def build_reference_generate_payload(
    prompt: str,
    project_id: str,
    recaptcha_token: str,
    user_paygate_tier: str,
    reference_media_ids: list[str],
) -> dict[str, Any]:
    if not reference_media_ids:
        raise ValueError("reference_media_ids 不能为空")
    return {
        "mediaGenerationContext": {
            "batchId": str(uuid4()),
            "audioFailurePreference": "BLOCK_SILENCED_VIDEOS",
        },
        "clientContext": {
            "projectId": project_id,
            "tool": "PINHOLE",
            "userPaygateTier": user_paygate_tier,
            "sessionId": f";{int(asyncio.get_running_loop().time() * 1000)}",
            "recaptchaContext": {
                "token": recaptcha_token,
                "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB",
            },
        },
        "requests": [
            {
                "aspectRatio": "VIDEO_ASPECT_RATIO_PORTRAIT",
                "seed": 12345,
                "textInput": {
                    "structuredPrompt": {
                        "parts": [{"text": prompt}],
                    }
                },
                "referenceImages": [
                    {
                        "mediaId": media_id,
                        "isUserUploadedImage": True,
                    }
                    for media_id in reference_media_ids
                ],
                "videoModelKey": REFERENCE_VIDEO_MODEL_KEY,
                "metadata": {},
            }
        ],
        "useV2ModelConfig": True,
    }


def _task_expects_reference_images(task_data: dict[str, Any]) -> bool:
    return any(
        task_data.get(key)
        for key in ("image_url", "image_base64", "image_url_list", "image_base64_list")
    )


def _strip_data_uri(image_base64: str) -> str:
    payload = str(image_base64).strip()
    if payload.startswith("data:") and "," in payload:
        payload = payload.split(",", 1)[1].strip()
    return payload


async def _download_url_image(image_url: str) -> bytes:
    async with httpx.AsyncClient(verify=False, timeout=30.0, follow_redirects=True) as client:
        response = await client.get(image_url)
        response.raise_for_status()
        return response.content


def _decode_base64_image(image_base64: str) -> bytes:
    return base64.b64decode(_strip_data_uri(image_base64))


async def collect_reference_image_bytes(task_data: dict[str, Any]) -> tuple[list[bytes], list[str]]:
    image_buffers: list[bytes] = []
    errors: list[str] = []

    image_urls = task_data.get("image_url_list") or []
    image_base64_list = task_data.get("image_base64_list") or []

    if image_urls:
        for idx, current_image_url in enumerate(image_urls, start=1):
            try:
                image_buffers.append(await _download_url_image(str(current_image_url)))
            except Exception as exc:
                errors.append(f"第 {idx} 张 URL 图片下载失败: {exc}")
        return image_buffers, errors

    if image_base64_list:
        for idx, current_image_base64 in enumerate(image_base64_list, start=1):
            try:
                image_buffers.append(_decode_base64_image(str(current_image_base64)))
            except Exception as exc:
                errors.append(f"第 {idx} 张 Base64 图片解码失败: {exc}")
        return image_buffers, errors

    if task_data.get("image_url"):
        try:
            image_buffers.append(await _download_url_image(str(task_data["image_url"])))
        except Exception as exc:
            errors.append(f"参考图 URL 下载失败: {exc}")
        return image_buffers, errors

    if task_data.get("image_base64"):
        try:
            image_buffers.append(_decode_base64_image(str(task_data["image_base64"])))
        except Exception as exc:
            errors.append(f"参考图 Base64 解码失败: {exc}")
        return image_buffers, errors

    return image_buffers, errors


async def upload_reference_image(
    page,
    access_token: str,
    project_id: str,
    image_bytes: bytes,
) -> dict[str, Any]:
    payload = {
        "clientContext": {
            "projectId": project_id,
            "tool": "PINHOLE",
        },
        "imageBytes": base64.b64encode(image_bytes).decode("utf-8"),
    }
    response = await browser_fetch_json(
        page,
        url=UPLOAD_IMAGE_ENDPOINT,
        method="POST",
        payload=payload,
        headers={"authorization": f"Bearer {access_token}"},
    )
    if not response.get("ok"):
        raise RuntimeError(f"uploadImage 失败: {response.get('status')} {response.get('text')}")
    payload_json = response.get("json") or {}
    media = payload_json.get("media") or {}
    if not media.get("name"):
        raise RuntimeError(f"uploadImage 未返回 media.name: {payload_json}")
    return payload_json


async def wait_for_video_status(
    page,
    access_token: str,
    media_items: list[dict[str, Any]],
    worker_id: Any,
    timeout_ms: int,
    email: str | None,
) -> dict[str, Any]:
    deadline = monotonic() + timeout_ms / 1000
    last_statuses: dict[str, str] = {}

    while monotonic() < deadline:
        response = await browser_fetch_json(
            page,
            url=STATUS_ENDPOINT,
            method="POST",
            payload={"media": media_items},
            headers={"authorization": f"Bearer {access_token}"},
        )
        payload = response.get("json") or {}
        tracked_items = payload.get("media") or payload.get("result", {}).get("data", {}).get("media", [])
        if tracked_items:
            last_statuses = {
                item["name"]: item.get("mediaMetadata", {}).get("mediaStatus", {}).get("mediaGenerationStatus", "UNKNOWN")
                for item in tracked_items
                if item.get("name")
            }
            logger.info(f"[Worker {email}-{worker_id}] 当前视频状态: {last_statuses}")

            if any(status == "MEDIA_GENERATION_STATUS_FAILED" for status in last_statuses.values()):
                raise RuntimeError(f"视频生成失败: {json.dumps(payload, ensure_ascii=False)}")

            if last_statuses and all(status == "MEDIA_GENERATION_STATUS_SUCCESSFUL" for status in last_statuses.values()):
                return {"media": tracked_items, "api_full_response": json.dumps(payload, ensure_ascii=False)}
        await asyncio.sleep(10)

    raise TimeoutError(f"等待视频生成超时，最后一次状态: {last_statuses}")


async def resolve_video_url(page, media_name: str) -> str:
    redirect_url = f"https://labs.google/fx/api/trpc/media.getMediaUrlRedirect?name={media_name}"
    cookies = await page.context.cookies()
    cookie_dict = {cookie["name"]: cookie["value"] for cookie in cookies}
    user_agent = await page.evaluate("() => navigator.userAgent")
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=False) as client:
        response = await client.get(
            redirect_url,
            cookies=cookie_dict,
            headers={
                "User-Agent": user_agent,
                "Referer": "https://labs.google/",
                "Accept": "*/*",
            },
        )
    if response.status_code != 307:
        raise RuntimeError(f"未拿到视频重定向地址: {response.status_code}")
    location = response.headers.get("location")
    if not location:
        raise RuntimeError("视频重定向响应缺少 location")
    return location


async def download_video_to_local(page, download_url: str, worker_id: Any, save_path: str) -> bool:
    try:
        user_agent = await page.evaluate("() => navigator.userAgent")
        cookies = await page.context.cookies()
        cookie_dict = {cookie["name"]: cookie["value"] for cookie in cookies}
        headers = {
            "User-Agent": user_agent,
            "Referer": "https://labs.google/",
            "Accept": "video/webm,video/ogg,video/*;q=0.9,application/ogg;q=0.7,audio/*;q=0.6,*/*;q=0.5",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Sec-Fetch-Dest": "video",
            "Sec-Fetch-Mode": "no-cors",
            "Sec-Fetch-Site": "cross-site",
        }
        async with httpx.AsyncClient(timeout=180.0, verify=False, follow_redirects=True) as client:
            async with client.stream("GET", download_url, cookies=cookie_dict, headers=headers) as response:
                response.raise_for_status()
                os.makedirs(os.path.dirname(save_path), exist_ok=True)
                with open(save_path, "wb") as handle:
                    async for chunk in response.aiter_bytes(chunk_size=8192):
                        if chunk:
                            handle.write(chunk)

        if os.path.exists(save_path) and os.path.getsize(save_path) > 1024:
            logger.info(f"[Worker {worker_id}] 视频下载成功: {save_path}")
            return True
        logger.error(f"[Worker {worker_id}] 下载文件过小或不存在: {save_path}")
        return False
    except Exception as exc:
        logger.error(f"[Worker {worker_id}] 下载视频失败: {exc}")
        return False


class GoogleFlowVideoFetchScraperV2(MultiBrowserScraperBase):
    """基于 MultiBrowserScraperBase + 页面内 fetch 的 Google Flow 视频生成器。"""

    def __init__(self, *args, **kwargs) -> None:
        kwargs.setdefault("locale", "zh-CN")
        kwargs.setdefault("timezone_id", "Asia/Shanghai")
        kwargs.setdefault("default_cookie_domain", ".google.com")
        super().__init__(*args, **kwargs)

    async def start(self) -> None:
        if self._started and not self._closed:
            return

        async with self._start_lock:
            if self._started and not self._closed:
                return
            if not self._started:
                launch_args = [
                    "--disable-blink-features=AutomationControlled",
                    "--disable-infobars",
                    *self.extra_flags,
                ]
                if self.viewport and self.viewport.get("width", 0) > 0 and self.viewport.get("height", 0) > 0:
                    launch_args.append(f"--window-size={self.viewport['width']},{self.viewport['height']}")
                if sys.platform != "win32":
                    launch_args.extend(
                        [
                            "--no-sandbox",
                            "--disable-setuid-sandbox",
                            "--disable-dev-shm-usage",
                            "--disable-gpu",
                            "--no-first-run",
                            "--no-zygote",
                        ]
                    )
                self._workers = [
                    PlainPlaywrightWorker(
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
                        launch_headless=self.headless,
                        launch_args=launch_args,
                    )
                    for index in range(self.browser_pool_size)
                ]
                self._worker_active_counts = {worker.worker_id: 0 for worker in self._workers}
                self._started = True
            self._workers_recycling.clear()
            self._closed = False

    def normalize_task(self, task_data: dict[str, Any]) -> dict[str, Any]:
        task_copy = dict(task_data)
        if not task_copy.get("email") or not task_copy.get("cookies"):
            next_result = get_next_cookie()
            if not next_result:
                raise RuntimeError("Redis Cookie Pool 为空，无法启动任务，请先运行 login_scheduler.py")
            task_copy["email"], task_copy["cookies"] = next_result
        return task_copy

    def resolve_task_proxy(self, task_data: dict[str, Any]) -> str | dict[str, Any] | None:
        del task_data
        return None

    def build_context_options(
        self,
        worker,
        proxy: str | dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        del worker
        options: dict[str, Any] = {
            "locale": self.locale,
            "timezone_id": self.timezone_id,
            "extra_http_headers": {
                "Accept-Language": f"{self.locale},en;q=0.9",
            },
        }
        if self.viewport and self.viewport.get("width", 0) > 0 and self.viewport.get("height", 0) > 0:
            options["viewport"] = self.viewport
            options["screen"] = self.viewport
        else:
            options["viewport"] = None
        if proxy:
            options["proxy"] = construct_proxy_dict(proxy)
        return options

    async def _switch_account(self, page, task_data: dict[str, Any], worker, current_email: str | None, *, remove_bad_account: bool) -> tuple[str, list]:
        if current_email:
            try:
                release_cookie(current_email)
                logger.info(f"[Worker {worker.worker_id}] 已释放旧账号 {current_email} 的并发槽位")
            except Exception as rel_err:
                logger.warning(f"[Worker {worker.worker_id}] 释放旧账号槽位失败（忽略）: {rel_err}")
            if remove_bad_account:
                remove_from_pool(current_email)

        next_result = get_next_cookie()
        if not next_result:
            raise RuntimeError("Redis Cookie Pool 已空，无法切换账号")

        new_email, new_cookies = next_result
        await page.context.clear_cookies()
        await page.context.add_cookies(new_cookies)
        task_data["email"] = new_email
        task_data["cookies"] = new_cookies
        logger.info(f"[Worker {worker.worker_id}] 切换到新账号: {new_email}")
        return new_email, new_cookies

    async def _prepare_runtime_with_account_rotation(self, page, task_data: dict[str, Any], worker) -> tuple[str, str, dict[str, Any]]:
        max_switches = 3
        current_email = task_data.get("email")

        for attempt in range(max_switches + 1):
            try:
                access_token, recaptcha_token, credits = await prepare_runtime(page)
                credits_left = credits.get("remainingCredits")
                if isinstance(credits_left, (int, float)) and credits_left < MIN_CREDITS_THRESHOLD:
                    raise RuntimeError(f"账号 {current_email} 额度不足({credits_left})")
                return access_token, recaptcha_token, credits
            except Exception as exc:
                url = page.url
                login_expired = bool(LOGIN_EXPIRED_PATTERN.search(url) or "/signin" in url)
                low_credits = "额度不足" in str(exc)
                logger.warning(f"[Worker {worker.worker_id}] 当前账号 {current_email} 初始化失败: {exc}")
                if attempt >= max_switches:
                    raise RuntimeError(f"[Worker {worker.worker_id}] 已切换 {max_switches} 次 Cookie 仍无可用账号，任务终止") from exc
                current_email, _ = await self._switch_account(
                    page,
                    task_data,
                    worker,
                    current_email,
                    remove_bad_account=login_expired or low_credits,
                )
                await page.goto(FLOW_HOME_URL, wait_until="domcontentloaded")
                await asyncio.sleep(2)

        raise RuntimeError("未能准备运行时环境")

    async def process_task(self, page, task_data: dict[str, Any], worker) -> dict[str, Any]:
        prompt = str(task_data.get("prompt", "")).strip()
        if not prompt:
            raise RuntimeError("prompt 不能为空")
        poll_timeout_ms = int(task_data.get("poll_timeout_ms", 4 * 60 * 1000))
        task_id = task_data.get("_id") or str(uuid4())
        task_email = task_data.get("email")

        try:
            access_token, recaptcha_token, credits = await self._prepare_runtime_with_account_rotation(page, task_data, worker)
            project_id = await create_project(page, f"fetch {task_id}")
            logger.info(f"[Worker {worker.worker_id}] createProject 成功: {project_id}")

            reference_image_bytes, preparation_errors = await collect_reference_image_bytes(task_data)
            if preparation_errors:
                raise RuntimeError("; ".join(preparation_errors))

            expects_reference_images = _task_expects_reference_images(task_data)
            if expects_reference_images and not reference_image_bytes:
                raise RuntimeError("任务携带了参考图字段，但没有准备出可用图片")

            user_paygate_tier = credits.get("userPaygateTier", "PAYGATE_TIER_NOT_PAID")
            if reference_image_bytes:
                reference_media_ids: list[str] = []
                for image_bytes in reference_image_bytes:
                    upload_payload = await upload_reference_image(
                        page,
                        access_token=access_token,
                        project_id=project_id,
                        image_bytes=image_bytes,
                    )
                    reference_media = upload_payload["media"]
                    reference_media_ids.append(reference_media["name"])
                    logger.info(f"[Worker {worker.worker_id}] uploadImage 成功: {reference_media['name']}")
                payload = build_reference_generate_payload(
                    prompt=prompt,
                    project_id=project_id,
                    recaptcha_token=recaptcha_token,
                    user_paygate_tier=user_paygate_tier,
                    reference_media_ids=reference_media_ids,
                )
                generate_url = GENERATE_REFERENCE_ENDPOINT
            else:
                payload = build_generate_payload(
                    prompt=prompt,
                    project_id=project_id,
                    recaptcha_token=recaptcha_token,
                    user_paygate_tier=user_paygate_tier,
                )
                generate_url = GENERATE_ENDPOINT

            generate_response = await browser_fetch_json(
                page,
                url=generate_url,
                method="POST",
                payload=payload,
                headers={"authorization": f"Bearer {access_token}"},
            )
            logger.info(f"[Worker {worker.worker_id}] generate-status: {generate_response.get('status')} {generate_response.get('ok')}")
            if not generate_response.get("ok"):
                raise RuntimeError(generate_response.get("text") or "generate 请求失败")

            media_items = (generate_response.get("json") or {}).get("media") or []
            if not media_items:
                raise RuntimeError(f"generate 未返回 media: {generate_response}")

            primary_media = media_items[0]
            final_status_payload = await wait_for_video_status(
                page=page,
                access_token=access_token,
                media_items=[{"name": primary_media["name"], "projectId": primary_media.get("projectId")}],
                worker_id=worker.worker_id,
                timeout_ms=poll_timeout_ms,
                email=task_data.get("email"),
            )
            final_media_items = final_status_payload.get("media", [])
            if not final_media_items:
                raise RuntimeError(f"最终状态未返回 media: {final_status_payload}")

            download_url = await resolve_video_url(page, final_media_items[0]["name"])
            demo_dir = os.path.join(os.getcwd(), "demo_videos")
            local_path = os.path.join(demo_dir, f"{task_id}.mp4")
            is_downloaded = await download_video_to_local(page, download_url, worker.worker_id, local_path)
            if not is_downloaded:
                raise RuntimeError(f"流式下载视频失败, URL是: {download_url}")

            return {
                "local_video_path": str(local_path),
                "api_full_response": final_status_payload.get("api_full_response"),
            }
        finally:
            final_email = task_data.get("email") or task_email
            if final_email:
                try:
                    release_cookie(final_email)
                    logger.info(f"[Worker {worker.worker_id}] 账号 {final_email} 并发槽位已归还")
                except Exception as rel_err:
                    logger.warning(f"[Worker {worker.worker_id}] 归还槽位失败（忽略）: {rel_err}")
