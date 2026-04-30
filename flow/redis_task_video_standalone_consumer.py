from __future__ import annotations

import asyncio
import base64
import json
import sys
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
from playwright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright

ROOT = Path(__file__).resolve().parent.parent
FLOW_DIR = ROOT / "flow"
ACCOUNT_DIR = ROOT / "account_mgr"
for path in (str(ROOT), str(FLOW_DIR), str(ACCOUNT_DIR)):
    if path not in sys.path:
        sys.path.insert(0, path)

from account_mgr.redis_utils import get_next_cookie, release_cookie
from task_pipeline_common import (
    DEFAULT_MAX_RETRIES,
    DEFAULT_POLL_TIMEOUT_MS,
    TASK_CREATE_PROCESSING_QUEUE,
    TASK_CREATE_QUEUE,
    build_scraper_task,
    create_redis_client,
    create_task_collection,
    dumps_queue_payload,
    get_logger,
    make_local_video_path,
    parse_queue_payload,
    now_local,
)

logger = get_logger("RedisTaskVideoStandaloneConsumer")

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

MAX_RETRIES = DEFAULT_MAX_RETRIES
REDIS_BLOCK_TIMEOUT_SECONDS = 5
DEFAULT_TASK_PRIORITY = 10
RETRY_PRIORITY_STEP = 10
SCORE_TIME_FACTOR = 10**13
MAX_TIMESTAMP_MS = 9_999_999_999_999

# 直接起浏览器跑 fetch 方案，单 worker 成本比点击版更高，先保守并发。
CONSUMER_WORKERS = 2


def upsert_task_generation_result(
    collection: Any,
    task: dict[str, Any],
    local_video_path: str,
    api_full_response: Any,
    project_id: str,
) -> None:
    collection.update_one(
        {"_id": str(task["_id"])},
        {
            "$set": {
                "local_video_path": local_video_path,
                "api_full_response": api_full_response,
                "project_id": project_id,
                "updated_at": now_local(),
            },
        },
        upsert=True,
    )


def mark_task_failed(collection: Any, task: dict[str, Any], error_message: str) -> None:
    del error_message
    collection.update_one(
        {"_id": str(task["_id"])},
        {
            "$set": {
                "msg": "失败",
                "updated_at": now_local(),
            },
        },
        upsert=True,
    )


def _compute_retry_priority(retry_count: int) -> int:
    return DEFAULT_TASK_PRIORITY + max(0, retry_count) * RETRY_PRIORITY_STEP


def _encode_queue_score(priority: int, timestamp_ms: int | None = None) -> int:
    if timestamp_ms is None:
        timestamp_ms = int(time.time() * 1000)
    bounded_priority = max(0, int(priority))
    inverted_time = max(0, MAX_TIMESTAMP_MS - int(timestamp_ms))
    return bounded_priority * SCORE_TIME_FACTOR + inverted_time


def _decode_queue_priority(score: float | int) -> int:
    return int(float(score)) // SCORE_TIME_FACTOR


def _recover_processing_queue(redis_client: Any) -> int:
    recovered = 0
    while True:
        moved_items = redis_client.zpopmax(TASK_CREATE_PROCESSING_QUEUE, count=100)
        if not moved_items:
            break
        for raw_payload, score in moved_items:
            redis_client.zadd(TASK_CREATE_QUEUE, {raw_payload: score})
            recovered += 1
    if recovered:
        logger.info(f"[recover] 已将 {recovered} 条遗留 processing 任务恢复回主队列")
    return recovered


async def _pop_highest_priority_task(redis_client: Any) -> tuple[str, float] | tuple[None, None]:
    result = await asyncio.to_thread(
        redis_client.bzpopmax,
        TASK_CREATE_QUEUE,
        REDIS_BLOCK_TIMEOUT_SECONDS,
    )
    if not result:
        return None, None

    _, raw_payload, score = result
    raw_score = float(score)
    redis_client.zadd(TASK_CREATE_PROCESSING_QUEUE, {raw_payload: raw_score})
    return raw_payload, raw_score


def _remove_processing_payload(redis_client: Any, raw_payload: str) -> None:
    redis_client.zrem(TASK_CREATE_PROCESSING_QUEUE, raw_payload)


def validate_task(task: dict[str, Any]) -> tuple[str, str]:
    task_id = str(task.get("_id", "")).strip()
    prompt = str(task.get("prompt", "")).strip()
    if not task_id:
        raise ValueError("缺少 _id")
    if not prompt:
        raise ValueError("缺少 prompt")
    if int(task.get("type", -1)) != 1:
        raise ValueError(f"只支持处理 type=1 的视频生成任务, 当前 type={task.get('type')}")
    return task_id, prompt


async def browser_fetch_json(
    page: Page,
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


async def prepare_runtime(page: Page) -> tuple[str, str, dict[str, Any]]:
    await page.goto(FLOW_HOME_URL, wait_until="domcontentloaded")
    await page.add_script_tag(url=RECAPTCHA_SCRIPT_URL)
    await page.wait_for_timeout(5000)
    state = await page.evaluate(
        """() => ({
            href: location.href,
            hasGrecaptcha: !!window.grecaptcha,
            hasEnterprise: !!window.grecaptcha?.enterprise,
        })"""
    )
    logger.info(f"recaptcha-state: {state}")
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
        headers={"authorization": f"Bearer {access_token}"},
    )
    credits_json = credits_payload.get("json") or {}
    return access_token, recaptcha_token, credits_json


async def create_project(page: Page, task_id: str) -> str:
    response = await browser_fetch_json(
        page,
        url=CREATE_PROJECT_ENDPOINT,
        method="POST",
        payload={"json": {"projectTitle": f"standalone {task_id}", "toolName": "PINHOLE"}},
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
    image_text = str(image_base64).strip()
    if image_text.startswith("data:") and "," in image_text:
        image_text = image_text.split(",", 1)[1].strip()
    return image_text


async def _download_url_image(image_url: str) -> bytes:
    async with httpx.AsyncClient(verify=False, timeout=30.0, follow_redirects=True) as client:
        response = await client.get(image_url)
        response.raise_for_status()
        return response.content


def _decode_base64_image(image_base64: str) -> bytes:
    payload = _strip_data_uri(image_base64)
    return base64.b64decode(payload)


async def collect_reference_image_bytes(task_data: dict[str, Any]) -> tuple[list[bytes], list[str]]:
    image_buffers: list[bytes] = []
    errors: list[str] = []

    image_url_list = task_data.get("image_url_list") or []
    image_base64_list = task_data.get("image_base64_list") or []

    if image_url_list:
        for idx, image_url in enumerate(image_url_list, start=1):
            try:
                image_buffers.append(await _download_url_image(str(image_url)))
            except Exception as exc:
                errors.append(f"第 {idx} 张 URL 图片下载失败: {exc}")
        return image_buffers, errors

    if image_base64_list:
        for idx, image_base64 in enumerate(image_base64_list, start=1):
            try:
                image_buffers.append(_decode_base64_image(str(image_base64)))
            except Exception as exc:
                errors.append(f"第 {idx} 张 Base64 图片解码失败: {exc}")
        return image_buffers, errors

    if task_data.get("image_url"):
        try:
            image_buffers.append(await _download_url_image(str(task_data["image_url"])))
        except Exception as exc:
            errors.append(f"URL 图片下载失败: {exc}")
        return image_buffers, errors

    if task_data.get("image_base64"):
        try:
            image_buffers.append(_decode_base64_image(str(task_data["image_base64"])))
        except Exception as exc:
            errors.append(f"Base64 图片解码失败: {exc}")
        return image_buffers, errors

    return image_buffers, errors


async def upload_reference_image(
    page: Page,
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


async def wait_for_video(
    page: Page,
    access_token: str,
    media_items: list[dict[str, Any]],
    timeout_ms: int,
) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + max(30, timeout_ms / 1000)
    while asyncio.get_running_loop().time() < deadline:
        response = await browser_fetch_json(
            page,
            url=STATUS_ENDPOINT,
            method="POST",
            payload={"media": media_items},
            headers={"authorization": f"Bearer {access_token}"},
        )
        payload = response.get("json") or {}
        media = payload.get("media") or payload.get("result", {}).get("data", {}).get("media", [])
        if media:
            status = media[0].get("mediaMetadata", {}).get("mediaStatus", {}).get("mediaGenerationStatus")
            logger.info(f"poll-status: {status}")
            if status == "MEDIA_GENERATION_STATUS_SUCCESSFUL":
                return payload
            if status == "MEDIA_GENERATION_STATUS_FAILED":
                logger.error(f"poll-failure-payload: {json.dumps(payload, ensure_ascii=False)}")
                raise RuntimeError(f"生成失败: {json.dumps(payload, ensure_ascii=False)}")
        await asyncio.sleep(10)
    raise TimeoutError("轮询视频状态超时")


async def resolve_video_url(page: Page, media_name: str) -> str:
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


async def download_video(page: Page, video_url: str, save_path: Path) -> None:
    cookies = await page.context.cookies()
    cookie_dict = {cookie["name"]: cookie["value"] for cookie in cookies}
    user_agent = await page.evaluate("() => navigator.userAgent")
    async with httpx.AsyncClient(timeout=180.0, verify=False) as client:
        async with client.stream(
            "GET",
            video_url,
            cookies=cookie_dict,
            headers={
                "User-Agent": user_agent,
                "Referer": "https://labs.google/",
                "Accept": "video/webm,video/ogg,video/*;q=0.9,*/*;q=0.5",
            },
        ) as response:
            response.raise_for_status()
            save_path.parent.mkdir(parents=True, exist_ok=True)
            with save_path.open("wb") as handle:
                async for chunk in response.aiter_bytes(chunk_size=8192):
                    if chunk:
                        handle.write(chunk)


async def create_browser_context(playwright: Playwright, cookies: list[dict[str, Any]]) -> tuple[Browser, BrowserContext, Page]:
    browser = await playwright.chromium.launch(headless=True, args=["--start-maximized"])
    context = await browser.new_context(viewport=None)
    if cookies:
        await context.add_cookies(cookies)
    page = await context.new_page()
    return browser, context, page


async def generate_video_for_task(playwright: Playwright, task: dict[str, Any]) -> dict[str, Any]:
    task_id, prompt = validate_task(task)
    task_data = build_scraper_task(task)
    poll_timeout_ms = int(task_data.get("poll_timeout_ms", DEFAULT_POLL_TIMEOUT_MS))
    expects_reference_images = _task_expects_reference_images(task_data)
    reference_image_bytes, reference_errors = await collect_reference_image_bytes(task_data)
    if reference_errors:
        raise RuntimeError("; ".join(reference_errors))
    if expects_reference_images and not reference_image_bytes:
        raise RuntimeError("任务携带了参考图字段，但没有准备出可用图片")

    next_item = get_next_cookie()
    if not next_item:
        raise RuntimeError("Redis cookie 池为空")
    email, cookies = next_item

    browser: Browser | None = None
    context: BrowserContext | None = None
    try:
        browser, context, page = await create_browser_context(playwright, cookies)
        access_token, recaptcha_token, credits = await prepare_runtime(page)
        project_id = await create_project(page, task_id)

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
        if not generate_response.get("ok"):
            raise RuntimeError(generate_response.get("text") or "generate 请求失败")

        media_items = (generate_response.get("json") or {}).get("media") or []
        if not media_items:
            raise RuntimeError(f"generate 未返回 media: {generate_response}")

        final_payload = await wait_for_video(
            page=page,
            access_token=access_token,
            media_items=[{"name": media_items[0]["name"], "projectId": media_items[0].get("projectId")}],
            timeout_ms=poll_timeout_ms,
        )
        final_media = final_payload.get("media") or []
        if not final_media:
            raise RuntimeError(f"最终状态未返回 media: {final_payload}")

        video_url = await resolve_video_url(page, final_media[0]["name"])
        save_path = make_local_video_path(task_id)
        await download_video(page, video_url, save_path)

        return {
            "local_video_path": str(save_path),
            "api_full_response": final_payload,
            "project_id": project_id,
        }
    finally:
        if context is not None:
            await context.close()
        if browser is not None:
            await browser.close()
        try:
            release_cookie(email)
        except Exception:
            pass


async def handle_single_task(playwright: Playwright, collection: Any, task: dict[str, Any]) -> Path:
    task_id, _ = validate_task(task)
    result = await generate_video_for_task(playwright, task)
    downloaded_path = result.get("local_video_path")
    api_full_response = result.get("api_full_response")
    project_id = result.get("project_id", "")

    if not downloaded_path:
        raise RuntimeError("未能从结果中获取到 downloaded_path，视频生成可能失败")

    upsert_task_generation_result(
        collection=collection,
        task=task,
        local_video_path=str(downloaded_path),
        api_full_response=api_full_response,
        project_id=str(project_id),
    )
    logger.info(f"[任务:{task_id}] 任务记录已更新, 视频路径: {downloaded_path}")
    return Path(downloaded_path)


async def consumer_worker(
    worker_name: str,
    playwright: Playwright,
    redis_client: Any,
    task_collection: Any,
) -> None:
    while True:
        raw_payload, current_score = await _pop_highest_priority_task(redis_client)
        if not raw_payload:
            continue

        task: dict[str, Any] | None = None
        local_path: Path | None = None
        try:
            task = parse_queue_payload(raw_payload)
            task_id, prompt = validate_task(task)
            current_priority = _decode_queue_priority(current_score)
            logger.info(
                f"[{worker_name}][任务:{task_id}] 开始处理，priority={current_priority}, score={current_score}, prompt={prompt[:60]!r}"
            )

            local_path = await handle_single_task(playwright, task_collection, task)
            _remove_processing_payload(redis_client, raw_payload)
            logger.info(f"[{worker_name}][任务:{task_id}] 处理成功，已移出 processing 队列")
        except Exception as exc:
            if task is None:
                logger.exception(f"[{worker_name}] 解析任务失败或结构错误: {exc}")
                _remove_processing_payload(redis_client, raw_payload)
                continue

            task_id = str(task.get("_id", "")).strip() or "unknown"
            if local_path and local_path.exists():
                local_path.unlink(missing_ok=True)

            retry_count = int(task.get("retry_count", 0))
            logger.exception(f"[{worker_name}][任务:{task_id}] 第 {retry_count + 1} 次处理失败: {exc}")

            if retry_count + 1 < MAX_RETRIES:
                _remove_processing_payload(redis_client, raw_payload)
                task["retry_count"] = retry_count + 1
                retry_priority = _compute_retry_priority(task["retry_count"])
                retry_score = _encode_queue_score(retry_priority)
                redis_client.zadd(TASK_CREATE_QUEUE, {dumps_queue_payload(task): retry_score})
                logger.info(
                    f"[{worker_name}][任务:{task_id}] 任务已放回主队列重试，"
                    f"retry_count={task['retry_count']}, priority={retry_priority}, score={retry_score}"
                )
            else:
                _remove_processing_payload(redis_client, raw_payload)
                mark_task_failed(task_collection, task, str(exc))
                logger.info(f"[{worker_name}][任务:{task_id}] 达到最大重试次数，已标记为失败")


async def consume_forever() -> None:
    redis_client = create_redis_client()
    task_collection = create_task_collection()
    _recover_processing_queue(redis_client)

    logger.info(
        "[启动] Standalone Browser Fetch 视频消费者启动，"
        f"concurrent_workers={CONSUMER_WORKERS}"
    )
    async with async_playwright() as playwright:
        workers = [
            asyncio.create_task(
                consumer_worker(
                    worker_name=f"standalone-consumer-{index + 1}",
                    playwright=playwright,
                    redis_client=redis_client,
                    task_collection=task_collection,
                )
            )
            for index in range(CONSUMER_WORKERS)
        ]
        await asyncio.gather(*workers)


def main() -> None:
    asyncio.run(consume_forever())


if __name__ == "__main__":
    main()
