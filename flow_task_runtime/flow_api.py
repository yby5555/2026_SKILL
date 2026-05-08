"""Flow 页面与接口交互逻辑。"""

from __future__ import annotations

import asyncio
import base64
import json
from typing import Any
from uuid import uuid4

import httpx

from .browser_pool import to_httpx_proxy

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
FLOW_API_ACCEPT_HEADER = "application/json, text/plain, */*"


def build_flow_api_headers(
    access_token: str | None = None,
    *,
    json_body: bool = True,
    extra_headers: dict[str, str] | None = None,
) -> dict[str, str]:
    """构建 Flow/AI Sandbox fetch 请求头。

    说明：
    - `User-Agent`、`Origin`、`Referer`、`Sec-Fetch-*` 等浏览器受控头不能通过 fetch
      headers 手动设置；这些会由页面上下文和 fetch 选项自动补齐。
    - 这里仅设置 CORS 允许且业务必要的头，避免添加任意自定义头导致预检失败。
    """
    headers = {
        "accept": FLOW_API_ACCEPT_HEADER,
    }
    if json_body:
        headers["content-type"] = "application/json"
    if access_token:
        headers["authorization"] = f"Bearer {access_token}"
    if extra_headers:
        headers.update({str(key).lower(): str(value) for key, value in extra_headers.items()})
    return headers


async def browser_fetch_json(
    page: Any,
    url: str,
    method: str = "POST",
    payload: Any | None = None,
    headers: dict[str, str] | None = None,
    credentials: str = "omit",
    referrer: str = FLOW_HOME_URL,
    referrer_policy: str = "strict-origin-when-cross-origin",
) -> dict[str, Any]:
    """在浏览器页面上下文里发起 fetch 并返回 JSON 结果。

    参数:
        page: 当前 Playwright 页面对象。
        url: 请求地址。
        method: HTTP 方法。
        payload: 请求体。
        headers: 请求头。
        credentials: fetch 的 credentials 选项。
        referrer: 浏览器 fetch referrer 选项，用于让请求更接近页面内真实请求。
        referrer_policy: 浏览器 fetch referrerPolicy 选项。

    返回:
        dict[str, Any]: 包含 ok/status/text/json/headers 的响应字典。
    """
    return await page.evaluate(
        """async ({url, method, payload, headers, credentials, referrer, referrerPolicy}) => {
            const init = {
                method,
                headers: headers || {},
                credentials,
                mode: "cors",
                cache: "no-store",
                referrer,
                referrerPolicy,
            };
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
            "referrer": referrer,
            "referrerPolicy": referrer_policy,
        },
    )


async def create_project(page: Any, task_id: str) -> str:
    """创建 Flow 项目并返回 project_id。

    参数:
        page: 当前 Playwright 页面对象。
        task_id: 当前任务 ID，用于生成项目标题。

    返回:
        str: Flow 返回的 project_id。
    """
    response = await browser_fetch_json(
        page,
        url=CREATE_PROJECT_ENDPOINT,
        method="POST",
        payload={"json": {"projectTitle": f"runtime {task_id}", "toolName": "PINHOLE"}},
        headers=build_flow_api_headers(),
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
        raise RuntimeError(f"createProject failed: {response}")
    return project_id


def build_generate_payload(prompt: str, project_id: str, recaptcha_token: str, user_paygate_tier: str) -> dict[str, Any]:
    """构建纯文本视频生成请求体。

    参数:
        prompt: 视频生成提示词。
        project_id: Flow 项目 ID。
        recaptcha_token: 当前页面拿到的 recaptcha token。
        user_paygate_tier: 当前账号付费层级。

    返回:
        dict[str, Any]: 文本视频生成请求体。
    """
    return {
        "mediaGenerationContext": {"batchId": str(uuid4()), "audioFailurePreference": "BLOCK_SILENCED_VIDEOS"},
        "clientContext": {
            "projectId": project_id,
            "tool": "PINHOLE",
            "userPaygateTier": user_paygate_tier,
            "sessionId": f";{int(asyncio.get_running_loop().time() * 1000)}",
            "recaptchaContext": {"token": recaptcha_token, "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB"},
        },
        "requests": [
            {
                "aspectRatio": "VIDEO_ASPECT_RATIO_PORTRAIT",
                "seed": 12345,
                "textInput": {"structuredPrompt": {"parts": [{"text": prompt}]}},
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
    """构建参考图视频生成请求体。

    参数:
        prompt: 视频生成提示词。
        project_id: Flow 项目 ID。
        recaptcha_token: 当前页面拿到的 recaptcha token。
        user_paygate_tier: 当前账号付费层级。
        reference_media_ids: 已上传参考图的 media id 列表。

    返回:
        dict[str, Any]: 参考图视频生成请求体。
    """
    if not reference_media_ids:
        raise ValueError("reference_media_ids cannot be empty")
    return {
        "mediaGenerationContext": {"batchId": str(uuid4()), "audioFailurePreference": "BLOCK_SILENCED_VIDEOS"},
        "clientContext": {
            "projectId": project_id,
            "tool": "PINHOLE",
            "userPaygateTier": user_paygate_tier,
            "sessionId": f";{int(asyncio.get_running_loop().time() * 1000)}",
            "recaptchaContext": {"token": recaptcha_token, "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB"},
        },
        "requests": [
            {
                "aspectRatio": "VIDEO_ASPECT_RATIO_PORTRAIT",
                "seed": 12345,
                "textInput": {"structuredPrompt": {"parts": [{"text": prompt}]}},
                "referenceImages": [{"mediaId": media_id, "isUserUploadedImage": True} for media_id in reference_media_ids],
                "videoModelKey": REFERENCE_VIDEO_MODEL_KEY,
                "metadata": {},
            }
        ],
        "useV2ModelConfig": True,
    }


async def upload_reference_image(page: Any, access_token: str, project_id: str, image_bytes: bytes) -> dict[str, Any]:
    """上传单张参考图，返回 Flow 返回的 media 结构。

    参数:
        page: 当前 Playwright 页面对象。
        access_token: 当前账号的 access token。
        project_id: Flow 项目 ID。
        image_bytes: 参考图字节内容。

    返回:
        dict[str, Any]: Flow 上传接口返回的响应字典。
    """
    payload = {
        "clientContext": {"projectId": project_id, "tool": "PINHOLE"},
        "imageBytes": base64.b64encode(image_bytes).decode("utf-8"),
    }
    response = await browser_fetch_json(
        page,
        url=UPLOAD_IMAGE_ENDPOINT,
        method="POST",
        payload=payload,
        headers=build_flow_api_headers(access_token),
    )
    if not response.get("ok"):
        raise RuntimeError(f"uploadImage failed: {response.get('status')} {response.get('text')}")
    payload_json = response.get("json") or {}
    media = payload_json.get("media") or {}
    if not media.get("name"):
        raise RuntimeError(f"uploadImage did not return media.name: {payload_json}")
    return payload_json


async def wait_for_video(page: Any, access_token: str, media_items: list[dict[str, Any]], timeout_ms: int) -> dict[str, Any]:
    """轮询等待视频生成完成。

    参数:
        page: 当前 Playwright 页面对象。
        access_token: 当前账号的 access token。
        media_items: 要轮询的视频 media 列表。
        timeout_ms: 最大轮询等待时长。

    返回:
        dict[str, Any]: 最终成功状态对应的响应字典。
    """
    deadline = asyncio.get_running_loop().time() + max(30, timeout_ms / 1000)
    while asyncio.get_running_loop().time() < deadline:
        response = await browser_fetch_json(
            page,
            url=STATUS_ENDPOINT,
            method="POST",
            payload={"media": media_items},
            headers=build_flow_api_headers(access_token),
        )
        payload = response.get("json") or {}
        media = payload.get("media") or payload.get("result", {}).get("data", {}).get("media", [])
        if media:
            status = media[0].get("mediaMetadata", {}).get("mediaStatus", {}).get("mediaGenerationStatus")
            if status == "MEDIA_GENERATION_STATUS_SUCCESSFUL":
                return payload
            if status == "MEDIA_GENERATION_STATUS_FAILED":
                raise RuntimeError(f"generation failed: {json.dumps(payload, ensure_ascii=False)}")
        await asyncio.sleep(10)
    raise TimeoutError("video generation polling timed out")


async def resolve_video_url(page: Any, media_name: str, *, proxy: str | dict[str, Any] | None = None) -> str:
    """解析视频下载重定向地址。

    参数:
        page: 当前 Playwright 页面对象。
        media_name: 视频 media 名称。
        proxy: 当前任务的可选代理配置。

    返回:
        str: 最终视频下载地址。
    """
    redirect_url = f"https://labs.google/fx/api/trpc/media.getMediaUrlRedirect?name={media_name}"
    cookies = await page.context.cookies()
    cookie_dict = {cookie["name"]: cookie["value"] for cookie in cookies}
    user_agent = await page.evaluate("() => navigator.userAgent")
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=False, proxy=to_httpx_proxy(proxy)) as client:
        response = await client.get(
            redirect_url,
            cookies=cookie_dict,
            headers={"User-Agent": user_agent, "Referer": "https://labs.google/", "Accept": "*/*"},
        )
    if response.status_code != 307:
        raise RuntimeError(f"did not receive video redirect: {response.status_code}")
    location = response.headers.get("location")
    if not location:
        raise RuntimeError("video redirect response is missing location")
    return location


async def download_video(page: Any, video_url: str, save_path: Any, *, proxy: str | dict[str, Any] | None = None) -> None:
    """下载视频到本地文件。

    参数:
        page: 当前 Playwright 页面对象。
        video_url: 最终视频下载地址。
        save_path: 本地保存路径。
        proxy: 当前任务的可选代理配置。
    """
    cookies = await page.context.cookies()
    cookie_dict = {cookie["name"]: cookie["value"] for cookie in cookies}
    user_agent = await page.evaluate("() => navigator.userAgent")
    async with httpx.AsyncClient(timeout=180.0, verify=False, proxy=to_httpx_proxy(proxy)) as client:
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
