"""flow_task_runtime 的自包含抓取器实现。"""

from __future__ import annotations

import base64
from pathlib import Path
from typing import Any

import httpx

from .account_guard import ensure_account_ready
from .account_pool import get_next_cookie, release_cookie, remove_from_pool
from .browser_pool import PlainPlaywrightBrowserPoolBase, WorkerContext
from .config import RuntimeSettings
from .flow_api import (
    CREDITS_ENDPOINT,
    GENERATE_ENDPOINT,
    GENERATE_REFERENCE_ENDPOINT,
    RECAPTCHA_SCRIPT_URL,
    REFERENCE_VIDEO_MODEL_KEY,
    SITE_KEY,
    TEXT_VIDEO_MODEL_KEY,
    build_flow_api_headers,
    browser_fetch_json,
    build_generate_payload,
    build_reference_generate_payload,
    create_project,
    download_video,
    resolve_video_url,
    upload_reference_image,
    wait_for_video,
)
from .queue_manager import DEFAULT_POLL_TIMEOUT_MS, build_scraper_task


def strip_data_uri(image_base64: str) -> str:
    """移除 Base64 图片可能携带的 data URI 头。

    参数:
        image_base64: 原始 Base64 字符串。

    返回:
        str: 去掉 data URI 头后的纯 Base64 内容。
    """
    image_text = str(image_base64).strip()
    if image_text.startswith("data:") and "," in image_text:
        image_text = image_text.split(",", 1)[1].strip()
    return image_text


def decode_base64_image(image_base64: str) -> bytes:
    """把 Base64 图片解码成字节。

    参数:
        image_base64: 原始 Base64 字符串。

    返回:
        bytes: 图片二进制字节。
    """
    return base64.b64decode(strip_data_uri(image_base64))


async def download_url_image(image_url: str) -> bytes:
    """从 URL 下载参考图字节。

    参数:
        image_url: 参考图 URL。

    返回:
        bytes: 下载到的图片字节。
    """
    async with httpx.AsyncClient(verify=False, timeout=30.0, follow_redirects=True) as client:
        response = await client.get(image_url)
        response.raise_for_status()
        return response.content


async def collect_reference_image_bytes(task_data: dict[str, Any]) -> tuple[list[bytes], list[str]]:
    """从任务中提取参考图片，并统一转换成字节数组。

    参数:
        task_data: 当前任务字典。

    返回:
        tuple[list[bytes], list[str]]:
            返回 (成功图片字节列表, 错误信息列表)。
    """
    image_buffers: list[bytes] = []
    errors: list[str] = []

    image_url_list = task_data.get("image_url_list") or []
    image_base64_list = task_data.get("image_base64_list") or []

    if image_url_list:
        for index, image_url in enumerate(image_url_list, start=1):
            try:
                image_buffers.append(await download_url_image(str(image_url)))
            except Exception as exc:
                errors.append(f"第 {index} 张 URL 图片下载失败: {exc}")
        return image_buffers, errors

    if image_base64_list:
        for index, image_base64 in enumerate(image_base64_list, start=1):
            try:
                image_buffers.append(decode_base64_image(str(image_base64)))
            except Exception as exc:
                errors.append(f"第 {index} 张 Base64 图片解码失败: {exc}")
        return image_buffers, errors

    if task_data.get("image_url"):
        try:
            image_buffers.append(await download_url_image(str(task_data["image_url"])))
        except Exception as exc:
            errors.append(f"URL 图片下载失败: {exc}")
        return image_buffers, errors

    if task_data.get("image_base64"):
        try:
            image_buffers.append(decode_base64_image(str(task_data["image_base64"])))
        except Exception as exc:
            errors.append(f"Base64 图片解码失败: {exc}")
        return image_buffers, errors

    return image_buffers, errors


class CreditCheckedFlowScraper(PlainPlaywrightBrowserPoolBase):
    """带账号登录态和 AI 点数检查的 Flow 抓取器。"""

    def __init__(
        self,
        *,
        settings: RuntimeSettings,
        redis_client: Any,
        account_collection: Any,
        logger: Any,
        **kwargs: Any,
    ) -> None:
        """初始化抓取器。

        参数:
            settings: 运行时配置对象。
            redis_client: Redis 客户端。
            account_collection: Mongo 账号集合。
            logger: 日志器对象。
            **kwargs: 传给浏览器池基类的参数。
        """
        kwargs.setdefault("logger", logger)
        super().__init__(**kwargs)
        self.settings = settings
        self.redis_client = redis_client
        self.account_collection = account_collection
        self.logger = logger

    def normalize_task(self, task_data: dict[str, Any]) -> dict[str, Any]:
        """把队列任务转成抓取器执行结构，并为其分配 cookie。

        参数:
            task_data: 原始任务字典。

        返回:
            dict[str, Any]: 标准化后的任务字典。
        """
        task = dict(task_data)
        task.update(build_scraper_task(task_data))
        if not task.get("email") or not task.get("cookies"):
            next_item = get_next_cookie(self.redis_client, self.settings)
            if not next_item:
                raise RuntimeError("Redis cookie 池为空")
            task["email"], task["cookies"] = next_item
            task["_cookie_acquired_from_pool"] = True
        else:
            task.setdefault("_cookie_acquired_from_pool", False)
        task.setdefault("output_dir", str(self.settings.output_dir))
        task.setdefault("poll_timeout_ms", self.settings.poll_timeout_ms or DEFAULT_POLL_TIMEOUT_MS)
        return task

    async def finalize_task(
        self,
        task_data: dict[str, Any],
        *,
        result: Any | None = None,
        exc: Exception | None = None,
    ) -> None:
        """任务结束后归还账号并发槽位。

        参数:
            task_data: 当前任务字典。
            result: 任务成功结果。
            exc: 任务异常对象。
        """
        del result, exc
        email = str(task_data.get("email") or "").strip()
        if not email:
            return
        try:
            new_value = release_cookie(self.redis_client, self.settings, email)
            source = "runtime-pool" if task_data.get("_cookie_acquired_from_pool") else "task-provided"
            self.logger.info(f"[cookie] 已归还账号 {email}，source={source}，当前占用数={new_value}")
        except Exception as release_exc:
            self.logger.exception(f"[cookie] 归还账号 {email} 失败: {release_exc}")

    async def prepare_runtime_after_credit_check(self, page: Any) -> tuple[str, str, dict[str, Any]]:
        """在额度检查通过后准备 access token、recaptcha token 和 credits payload。

        参数:
            page: 当前 Playwright 页面对象。

        返回:
            tuple[str, str, dict[str, Any]]:
                返回 (access_token, recaptcha_token, credits_payload)。
        """
        await page.add_script_tag(url=RECAPTCHA_SCRIPT_URL)
        await page.wait_for_timeout(5000)

        session_payload = await browser_fetch_json(
            page,
            url="/fx/api/auth/session",
            method="GET",
            headers=build_flow_api_headers(json_body=False),
            credentials="include",
        )
        session_json = session_payload.get("json") or {}
        access_token = session_json.get("access_token")
        if not access_token:
            raise RuntimeError(f"session 未拿到 access_token: {session_json}")

        recaptcha_token = await page.evaluate(
            """async (siteKey) => {
                await new Promise((resolve) => window.grecaptcha.enterprise.ready(resolve));
                return await window.grecaptcha.enterprise.execute(siteKey, { action: "VIDEO_GENERATION" });
            }""",
            SITE_KEY,
        )
        if not recaptcha_token:
            raise RuntimeError("未拿到 recaptcha token")

        credits_payload = await browser_fetch_json(
            page,
            url=CREDITS_ENDPOINT,
            method="GET",
            headers=build_flow_api_headers(access_token, json_body=False),
        )
        return access_token, recaptcha_token, credits_payload.get("json") or {}

    async def process_task(self, page: Any, task_data: dict[str, Any], worker: WorkerContext) -> dict[str, Any]:
        """执行单个 Flow 视频任务。

        参数:
            page: 当前 Playwright 页面对象。
            task_data: 当前任务字典。
            worker: 当前 worker 状态快照。

        返回:
            dict[str, Any]:
                返回最小结果结构，仅包含 local_video_path 和 api_full_response。
        """
        task_id = str(task_data.get("_id") or "").strip()
        prompt = str(task_data.get("prompt") or "").strip()
        if not task_id or not prompt:
            raise ValueError("任务缺少 _id 或 prompt")

        email = str(task_data.get("email") or "").strip()
        await ensure_account_ready(
            page,
            email=email,
            account_collection=self.account_collection,
            redis_client=self.redis_client,
            settings=self.settings,
            logger=self.logger,
            remove_from_pool=remove_from_pool,
        )
        access_token, recaptcha_token, credits_payload = await self.prepare_runtime_after_credit_check(page)
        project_id = await create_project(page, task_id)

        reference_image_bytes, reference_errors = await collect_reference_image_bytes(task_data)
        if reference_errors:
            raise RuntimeError("; ".join(reference_errors))

        user_paygate_tier = credits_payload.get("userPaygateTier", "PAYGATE_TIER_NOT_PAID")
        if reference_image_bytes:
            reference_media_ids: list[str] = []
            for image_bytes in reference_image_bytes:
                upload_payload = await upload_reference_image(page, access_token=access_token, project_id=project_id, image_bytes=image_bytes)
                reference_media_ids.append(upload_payload["media"]["name"])
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
            headers=build_flow_api_headers(access_token),
        )
        if not generate_response.get("ok"):
            raise RuntimeError(generate_response.get("text") or "generate 请求失败")

        media_items = (generate_response.get("json") or {}).get("media") or []
        if not media_items:
            raise RuntimeError(f"generate 未返回 media: {generate_response}")

        final_payload = await wait_for_video(
            page=page,
            access_token=access_token,
            media_items=[{"name": media_items[0]["name"], "projectId": media_items[0].get("projectId")}],
            timeout_ms=int(task_data.get("poll_timeout_ms", self.settings.poll_timeout_ms)),
        )
        final_media = final_payload.get("media") or []
        if not final_media:
            raise RuntimeError(f"最终状态未返回 media: {final_payload}")

        proxy = task_data.get("proxy")
        video_url = await resolve_video_url(page, final_media[0]["name"], proxy=proxy)
        save_path = Path(task_data.get("output_dir") or self.settings.output_dir) / f"{task_id}.mp4"
        await download_video(page, video_url, save_path, proxy=proxy)
        return {"local_video_path": str(save_path), "api_full_response": final_payload}
