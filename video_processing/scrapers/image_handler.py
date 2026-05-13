"""
视频处理的图片处理工具
=======================
此模块包含用于处理图片上传、处理和管理的工具，在视频生成工作流的背景下。

它包括图片格式转换、元数据提取和与文件上传对话框交互的函数。
"""

import asyncio
import base64
import hashlib
import logging
import mimetypes
import random
import re
import sys
from pathlib import Path
from time import monotonic
from typing import Any, Optional
from urllib.parse import parse_qs, unquote, urlparse

import httpx

logger = logging.getLogger(__name__)


def _mime_to_extension(mime_type: str) -> str:
    """
    将 MIME 类型转换为文件扩展名。

    参数:
        mime_type: MIME 类型字符串（例如 "image/png"）

    返回:
        str: 包括点的文件扩展名（例如 ".png"）

    示例:
        ext = _mime_to_extension("image/jpeg")
        print(ext)  # ".jpg"
    """
    guessed_extension = mimetypes.guess_extension(mime_type or "")
    return guessed_extension or ".png"


def _derive_image_upload_meta(
    image_url: Optional[str],
    image_base64: Optional[str]
) -> tuple[str, str, Optional[str]]:
    """
    从 URL 或 base64 数据派生图片元数据。

    此函数从图片 URL 或 base64 编码的图片数据中提取文件名、MIME 类型和规范化的 base64 数据。

    参数:
        image_url: 图片的 URL（可选）
        image_base64: Base64 编码的图片数据（可选）

    返回:
        tuple[str, str, Optional[str]]: (file_name, mime_type, normalized_base64)

    示例:
        filename, mime_type, b64_data = _derive_image_upload_meta(
            "https://example.com/image.jpg",
            None
        )
    """
    file_name = "image.png"
    mime_type = "image/png"
    normalized_base64 = image_base64

    if image_url:
        parsed = urlparse(image_url)
        extracted_name = Path(unquote(parsed.path)).name
        if extracted_name:
            file_name = extracted_name
        guessed_mime, _ = mimetypes.guess_type(file_name)
        if guessed_mime:
            mime_type = guessed_mime

    if image_base64 and image_base64.startswith("data:"):
        header, _, payload = image_base64.partition(",")
        mime_match = re.match(r"data:(image/[\w.+-]+);base64$", header, re.IGNORECASE)
        if mime_match:
            mime_type = mime_match.group(1).lower()
            file_name = f"image{_mime_to_extension(mime_type)}"
        normalized_base64 = payload

    if "." not in Path(file_name).name:
        file_name = f"{file_name}{_mime_to_extension(mime_type)}"

    return file_name, mime_type, normalized_base64


def _resolve_reference_image_repeat_count(task_data: dict[str, Any]) -> int:
    """
    解析参考图片应该重复的次数。

    此函数根据任务配置参数确定参考图片应该重复的次数。

    参数:
        task_data: 任务配置字典

    返回:
        int: 参考图片重复的次数（最小为 1）

    示例:
        count = _resolve_reference_image_repeat_count({"reference_image_count": 3})
        print(count)  # 3
    """
    for key in ("reference_image_count", "image_count", "reference_count"):
        raw_value = task_data.get(key)
        if raw_value is None:
            continue
        try:
            return max(1, int(raw_value))
        except Exception:
            continue
    return 1


def _get_requested_reference_image_count(task_data: dict[str, Any]) -> int:
    """
    获取任务请求的参考图片总数。

    此函数计算来自所有来源的参考图片总数（URL 列表、base64 列表或单个图片）。

    参数:
        task_data: 任务配置字典

    返回:
        int: 请求的参考图片总数

    示例:
        count = _get_requested_reference_image_count({
            "image_url_list": ["url1.jpg", "url2.jpg"]
        })
        print(count)  # 2
    """
    image_urls = task_data.get("image_url_list") or []
    if image_urls:
        return len(image_urls)

    image_base64_list = task_data.get("image_base64_list") or []
    if image_base64_list:
        return len(image_base64_list)

    if task_data.get("image_url") or task_data.get("image_base64"):
        return _resolve_reference_image_repeat_count(task_data)

    return 0


def _extract_media_name_from_src(src: Optional[str]) -> Optional[str]:
    """
    从 URL 源字符串中提取媒体名称。

    此函数解析 URL 以提取媒体名称参数，该参数常用于媒体托管服务。

    参数:
        src: 可能包含媒体名称参数的 URL 字符串

    返回:
        Optional[str]: 提取的媒体名称，如果未找到则为 None

    示例:
        name = _extract_media_name_from_src(
            "https://example.com/media?name=video123.mp4"
        )
        print(name)  # "video123.mp4"
    """
    if not src:
        return None

    parsed = urlparse(src)
    media_name = parse_qs(parsed.query).get("name", [None])[0]
    if media_name:
        return media_name

    match = re.search(r"[?&]name=([^&]+)", src)
    if match:
        return unquote(match.group(1))
    return None


async def upload_reference_image_via_picker(
    page,
    worker_id: Any,
    file_buffer: bytes,
    file_name: str,
    mime_type: str,
    log_prefix: str = ""
) -> str:
    """
    使用文件选择器对话框上传参考图片。

    此函数通过页面的文件选择器界面上传参考图片，模拟人类文件选择行为。

    参数:
        page: Playwright 页面对象
        worker_id: 用于日志记录的工作器标识符
        file_buffer: 文件内容（字节）
        file_name: 要上传的文件名
        mime_type: 文件的 MIME 类型
        log_prefix: 日志消息的可选前缀

    返回:
        str: 上传图片的媒体名称

    异常:
        RuntimeError: 如果无法打开上传对话框或文件上传失败

    示例:
        media_name = await upload_reference_image_via_picker(
            page,
            worker_id=1,
            file_buffer=image_bytes,
            file_name="reference.jpg",
            mime_type="image/jpeg"
        )
    """
    # 这是一个占位符 - 实际实现将取决于特定的页面结构和上传机制
    # 目前，我们将返回一个模拟的媒体名称
    return f"uploaded_{file_name}"


async def upload_frame_image_via_picker(
    page,
    worker_id: Any,
    frame_type: str,
    file_buffer: bytes,
    file_name: str,
    mime_type: str,
    log_prefix: str = ""
) -> str:
    """
    使用文件选择器对话框上传帧图片。

    此函数通过页面的文件选择器界面上传帧图片（开始帧或结束帧），用于视频生成工作流。

    参数:
        page: Playwright 页面对象
        worker_id: 用于日志记录的工作器标识符
        frame_type: 帧类型（"起始"为开始帧，"结束"为结束帧）
        file_buffer: 文件内容（字节）
        file_name: 要上传的文件名
        mime_type: 文件的 MIME 类型
        log_prefix: 日志消息的可选前缀

    返回:
        str: 上传的帧图片的媒体名称

    异常:
        RuntimeError: 如果无法打开上传对话框或文件上传失败

    示例:
        media_name = await upload_frame_image_via_picker(
            page,
            worker_id=1,
            frame_type="起始",
            file_buffer=frame_bytes,
            file_name="start_frame.jpg",
            mime_type="image/jpeg"
        )
    """
    # 这是一个占位符 - 实际实现将取决于特定的页面结构和上传机制
    # 目前，我们将返回一个模拟的媒体名称
    return f"frame_{frame_type}_{file_name}"


async def download_image_from_url(image_url: str, timeout: float = 30.0) -> tuple[bytes, str, str]:
    """
    从 URL 下载图片。

    此函数从给定的 URL 下载图片，并返回图片数据以及检测到的 MIME 类型和文件扩展名。

    参数:
        image_url: 要下载的图片 URL
        timeout: 请求超时时间（秒）（默认: 30.0）

    返回:
        tuple[bytes, str, str]: (file_buffer, file_name, mime_type)

    异常:
        httpx.HTTPError: 如果下载失败
        RuntimeError: 如果下载的内容不是图片

    示例:
        buffer, filename, mime = await download_image_from_url(
            "https://example.com/image.jpg"
        )
    """
    async with httpx.AsyncClient(verify=False, timeout=timeout) as client:
        response = await client.get(image_url)
        response.raise_for_status()
        file_buffer = response.content

        content_type = response.headers.get("content-type", "").split(";")[0].strip()
        if content_type.startswith("image/"):
            mime_type = content_type
        else:
            mime_type = "image/png"  # 默认回退

        # 尝试从 URL 或 MIME 类型确定文件扩展名
        parsed_url = urlparse(image_url)
        url_path = parsed_url.path
        if url_path and "." in url_path:
            file_name = Path(url_path).name
        else:
            file_name = f"image{_mime_to_extension(mime_type)}"

        return file_buffer, file_name, mime_type


def calculate_file_hash(file_path: str) -> str:
    """
    计算文件的 MD5 哈希值。

    参数:
        file_path: 文件路径

    返回:
        str: MD5 哈希值（十六进制字符串）

    示例:
        file_hash = calculate_file_hash("video.mp4")
        print(file_hash)  # "5d41402abc4b2a76b9719d911017c592"
    """
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def get_file_size_mb(file_path: str) -> float:
    """
    获取文件大小（MB）。

    参数:
        file_path: 文件路径

    返回:
        float: 文件大小（MB）

    示例:
        size_mb = get_file_size_mb("video.mp4")
        print(f"文件大小: {size_mb:.2f} MB")
    """
    path = Path(file_path)
    if path.exists():
        return path.stat().st_size / (1024 * 1024)
    return 0.0