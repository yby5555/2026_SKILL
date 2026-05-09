from __future__ import annotations

import asyncio
import base64
import json
import sys
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
from playwright.async_api import async_playwright

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
ACCOUNT_DIR = ROOT / "account_mgr"
if str(ACCOUNT_DIR) not in sys.path:
    sys.path.insert(0, str(ACCOUNT_DIR))

from account_mgr.redis_utils import get_next_cookie, release_cookie  # noqa: E402


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


async def prepare_runtime(page) -> tuple[str, str, dict[str, Any]]:
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
    print("recaptcha-state:", state)
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


async def create_project(page) -> str:
    response = await browser_fetch_json(
        page,
        url=CREATE_PROJECT_ENDPOINT,
        method="POST",
        payload={"json": {"projectTitle": "standalone browser fetch", "toolName": "PINHOLE"}},
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


def normalize_reference_image_paths(reference_images: Any) -> list[Path]:
    if reference_images is None:
        return []
    if isinstance(reference_images, (str, Path)):
        items = [reference_images]
    else:
        items = list(reference_images)
    return [Path(item) for item in items]


async def upload_reference_image(
    page,
    access_token: str,
    project_id: str,
    image_path: Path,
) -> dict[str, Any]:
    image_bytes = image_path.read_bytes()
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


async def wait_for_video(page, access_token: str, media_items: list[dict[str, Any]]) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + 8 * 60
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
            print("poll-status:", status)
            if status == "MEDIA_GENERATION_STATUS_SUCCESSFUL":
                return payload
            if status == "MEDIA_GENERATION_STATUS_FAILED":
                print("poll-failure-payload:", json.dumps(payload, ensure_ascii=False))
                raise RuntimeError(f"生成失败: {json.dumps(payload, ensure_ascii=False)}")
        await asyncio.sleep(10)
    raise TimeoutError("轮询视频状态超时")


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


async def download_video(page, video_url: str, save_path: Path) -> None:
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


async def main() -> None:
    next_item = get_next_cookie()
    if not next_item:
        raise RuntimeError("Redis cookie 池为空")
    email, cookies = next_item
    print("email:", email)

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False, args=["--start-maximized"])
            context = await browser.new_context(storage_state={"cookies": cookies}, viewport=None)
            page = await context.new_page()

            access_token, recaptcha_token, credits = await prepare_runtime(page)
            project_id = await create_project(page)
            print("project_id:", project_id)

            prompt = "第一张是我传入的人物图片，第二张是代码，帮我生成这个人物边喝酒边打代码"
            reference_images = [
                Path(r"D:\2026_SKILL\flow\1ad0fd6709f24b3ebc7ca0da7a44e698.png"),
                Path(r"D:\2026_SKILL\flow\videos\pending\image.png"),

            ]
            reference_image_paths = normalize_reference_image_paths(reference_images)
            existing_reference_image_paths = [path for path in reference_image_paths if path.exists()]
            if existing_reference_image_paths:
                print("generate_mode: reference")
                reference_media_ids: list[str] = []
                for reference_image_path in existing_reference_image_paths:
                    upload_payload = await upload_reference_image(
                        page,
                        access_token=access_token,
                        project_id=project_id,
                        image_path=reference_image_path,
                    )
                    reference_media = upload_payload["media"]
                    reference_media_ids.append(reference_media["name"])
                    print("uploaded_reference_media:", reference_media["name"], "from", reference_image_path)
                payload = build_reference_generate_payload(
                    prompt=prompt,
                    project_id=project_id,
                    recaptcha_token=recaptcha_token,
                    user_paygate_tier=credits.get("userPaygateTier", "PAYGATE_TIER_NOT_PAID"),
                    reference_media_ids=reference_media_ids,
                )
                print("reference_video_model_key:", REFERENCE_VIDEO_MODEL_KEY)
                generate_url = GENERATE_REFERENCE_ENDPOINT
            else:
                print("generate_mode: text")
                payload = build_generate_payload(
                    prompt=prompt,
                    project_id=project_id,
                    recaptcha_token=recaptcha_token,
                    user_paygate_tier=credits.get("userPaygateTier", "PAYGATE_TIER_NOT_PAID"),
                )
                print("text_video_model_key:", TEXT_VIDEO_MODEL_KEY)
                generate_url = GENERATE_ENDPOINT

            generate_response = await browser_fetch_json(
                page,
                url=generate_url,
                method="POST",
                payload=payload,
                headers={"authorization": f"Bearer {access_token}"},
            )
            print("generate-status:", generate_response.get("status"), generate_response.get("ok"))
            if not generate_response.get("ok"):
                raise RuntimeError(generate_response.get("text") or "generate 请求失败")

            media_items = (generate_response.get("json") or {}).get("media") or []
            if not media_items:
                raise RuntimeError(f"generate 未返回 media: {generate_response}")

            final_payload = await wait_for_video(
                page,
                access_token=access_token,
                media_items=[{"name": media_items[0]["name"], "projectId": media_items[0].get("projectId")}],
            )
            final_media = final_payload.get("media") or []
            if not final_media:
                raise RuntimeError(f"最终状态未返回 media: {final_payload}")

            video_url = await resolve_video_url(page, final_media[0]["name"])
            print("video_url:", video_url)
            save_path = ROOT / "flow" / "demo_videos" / f"{project_id}.mp4"
            await download_video(page, video_url, save_path)
            print("saved:", save_path)

            await browser.close()
    finally:
        try:
            release_cookie(email)
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
