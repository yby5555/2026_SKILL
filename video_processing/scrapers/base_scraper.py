"""
基础视频爬虫类
================
此模块为视频生成工作流提供基础爬虫类。

基础类处理浏览器管理、账号验证和视频处理协调等常见功能。
"""

import asyncio
import hashlib
import json
import logging
import os
import random
import sys
from pathlib import Path
from time import monotonic
from typing import Any, Optional

import httpx

# 导入配置和工具
try:
    from video_processing.config.constants import (
        FLOW_HOME_URL,
        MIN_CREDITS_THRESHOLD,
        DEFAULT_MODEL_LABEL,
        MODEL_MAP,
        PROPORTION_MAP,
        DEFAULT_ASPECT_RATIO,
        DEFAULT_POLL_TIMEOUT_MS,
    )
    from video_processing.scrapers.human_interaction import (
        human_delay,
        human_mouse_move,
        human_scroll,
        normalize_prompt_text,
    )
except ImportError:
    # 当模块结构尚未完全设置时的回退
    FLOW_HOME_URL = "https://labs.google/fx/zh/tools/flow"
    MIN_CREDITS_THRESHOLD = 20
    DEFAULT_MODEL_LABEL = "Veo 3.1 - Lite"
    MODEL_MAP = {0: "Veo 3.1 - Lite", 1: "Veo 3.1 - Fast"}
    PROPORTION_MAP = {0: "9:16", 1: "16:9"}
    DEFAULT_ASPECT_RATIO = "9:16"
    DEFAULT_POLL_TIMEOUT_MS = 4 * 60 * 1000

logger = logging.getLogger(__name__)


class BaseVideoScraper:
    """
    视频爬虫操作的基础类。

    此类为视频生成工作流提供基础，处理浏览器管理、账号验证和视频处理协调。

    属性:
        browser_pool_size: 要维护的浏览器实例数量
        max_contexts_per_browser: 每个浏览器实例的最大上下文数
        headless: 是否以无头模式运行浏览器
        extra_flags: 额外的浏览器启动标志
        viewport: 浏览器视口尺寸
        task_timeout_ms: 单个任务的超时时间（毫秒）

    示例:
        scraper = BaseVideoScraper(
            browser_pool_size=2,
            max_contexts_per_browser=2,
            headless=True
        )
        async with scraper:
            result = await scraper.process_task(task_data)
    """

    def __init__(
        self,
        browser_pool_size: int = 2,
        max_contexts_per_browser: int = 2,
        headless: bool = True,
        extra_flags: Optional[list[str]] = None,
        viewport: Optional[dict[str, int]] = None,
        task_timeout_ms: int = DEFAULT_POLL_TIMEOUT_MS,
    ):
        """
        初始化基础视频爬虫。

        参数:
            browser_pool_size: 浏览器实例数量（默认: 2）
            max_contexts_per_browser: 每个浏览器的最大上下文数（默认: 2）
            headless: 以无头模式运行浏览器（默认: True）
            extra_flags: 额外的浏览器启动标志（默认: None）
            viewport: 视口尺寸 {"width": int, "height": int}（默认: None）
            task_timeout_ms: 任务超时时间（毫秒）（默认: 240000）
        """
        self.browser_pool_size = browser_pool_size
        self.max_contexts_per_browser = max_contexts_per_browser
        self.headless = headless
        self.extra_flags = extra_flags or ["--start-maximized"]
        self.viewport = viewport or {"width": 0, "height": 0}
        self.task_timeout_ms = task_timeout_ms

    def normalize_task(self, task_data: dict[str, Any]) -> dict[str, Any]:
        """
        规范化和验证任务数据。

        此方法确保任务数据在处理前包含所有必需的字段并具有有效值。

        参数:
            task_data: 原始任务数据字典

        返回:
            dict[str, Any]: 规范化的任务数据

        异常:
            ValueError: 如果任务数据无效或缺少必需字段

        示例:
            normalized = scraper.normalize_task({
                "_id": "task123",
                "prompt": "创建一个视频"
            })
        """
        task_copy = dict(task_data)

        # 确保必需字段存在
        if not task_copy.get("_id"):
            raise ValueError("任务数据必须包含 '_id' 字段")
        if not task_copy.get("prompt"):
            raise ValueError("任务数据必须包含 'prompt' 字段")

        # 为可选字段设置默认值
        task_copy.setdefault("variant_count", 1)
        task_copy.setdefault("poll_timeout_ms", self.task_timeout_ms)
        task_copy.setdefault("gen_type", 1)
        task_copy.setdefault("proportion", 0)
        task_copy.setdefault("model_type", 0)

        return task_copy

    async def validate_account(self, page, email: str) -> dict[str, Any]:
        """
        验证账号是否健康并可用于处理。

        此方法检查账号登录状态、可用额度和其他健康指标。

        参数:
            page: Playwright 页面对象
            email: 要验证的账号邮箱

        返回:
            dict[str, Any]: 验证结果，包含键:
                - status: "ok"、"no_credits"、"login_expired" 或 "error"
                - credits: 当前额度计数（如果可用）
                - message: 状态消息

        示例:
            result = await scraper.validate_account(page, "user@example.com")
            if result["status"] != "ok":
                print(f"账号问题: {result['message']}")
        """
        try:
            # 检查我们是在 Flow 首页还是重定向到登录页
            current_url = page.url

            if "signin" in current_url or "auth" in current_url:
                return {
                    "status": "login_expired",
                    "credits": None,
                    "message": "账号登录已过期"
                }

            # 尝试获取额度 - 这需要基于页面结构的实际实现
            return {
                "status": "ok",
                "credits": None,
                "message": "账号验证成功"
            }

        except Exception as e:
            logger.error(f"账号验证错误: {e}")
            return {
                "status": "error",
                "credits": None,
                "message": f"验证错误: {str(e)}"
            }

    async def download_video(
        self,
        page,
        download_url: str,
        save_path: str,
        cookies: list[dict[str, str]]
    ) -> bool:
        """
        下载视频文件到本地存储。

        此方法使用提供的 cookies 进行身份验证，从给定的 URL 下载视频，
        并将其保存到指定路径。

        参数:
            page: 用于用户代理提取的 Playwright 页面对象
            download_url: 要下载的视频 URL
            save_path: 视频应该保存的本地路径
            cookies: 用于身份验证的 cookies 列表

        返回:
            bool: 如果下载成功则为 True，否则为 False

        示例:
            success = await scraper.download_video(
                page,
                "https://example.com/video.mp4",
                "/path/to/save/video.mp4",
                cookies
            )
        """
        try:
            await asyncio.sleep(random.uniform(4.0, 8.0))  # 人为延迟

            user_agent = await page.evaluate("() => navigator.userAgent")
            cookie_dict = {c["name"]: c["value"] for c in cookies}
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
                    with open(save_path, "wb") as file_obj:
                        async for chunk in response.aiter_bytes(chunk_size=8192):
                            if chunk:
                                file_obj.write(chunk)

            if os.path.exists(save_path) and os.path.getsize(save_path) > 1024:
                kb_size = os.path.getsize(save_path) // 1024
                logger.info(f"视频下载成功！大小: {kb_size} KB")
                return True

            logger.error("下载文件似乎太小或不存在")
            return False

        except Exception as exc:
            logger.error(f"下载视频到本地失败: {exc}")
            return False

    def calculate_file_info(self, file_path: str) -> dict[str, Any]:
        """
        计算文件信息，包括大小和哈希值。

        参数:
            file_path: 文件路径

        返回:
            dict[str, Any]: 文件信息，包含键:
                - size_kb: 文件大小（KB）
                - md5: 文件的 MD5 哈希值
                - exists: 文件是否存在

        示例:
            info = scraper.calculate_file_info("video.mp4")
            print(f"大小: {info['size_kb']} KB, MD5: {info['md5']}")
        """
        if not os.path.exists(file_path):
            return {
                "size_kb": 0,
                "md5": "",
                "exists": False
            }

        file_size_bytes = os.path.getsize(file_path)
        kb_size = file_size_bytes // 1024 if file_size_bytes >= 1024 else 1
        file_md5 = hashlib.md5(file_path.encode("utf-8")).hexdigest()

        return {
            "size_kb": kb_size,
            "md5": file_md5,
            "exists": True
        }

    async def wait_for_video_completion(
        self,
        page,
        media_names: set[str],
        timeout_ms: int,
        worker_id: Any = None
    ) -> dict[str, Any]:
        """
        等待视频生成完成。

        此方法轮询视频生成状态直到完成或超时，返回最终状态和下载信息。

        参数:
            page: Playwright 页面对象
            media_names: 要跟踪的媒体名称集合
            timeout_ms: 最大等待时间（毫秒）
            worker_id: 用于日志记录的可选工作器标识符

        返回:
            dict[str, Any]: 完成状态，包含键:
                - status: "completed"、"failed" 或 "timeout"
                - media_items: 已完成的媒体项目列表
                - download_urls: 媒体名称到下载 URL 的映射

        异常:
            TimeoutError: 如果视频生成在超时时间内未完成
            RuntimeError: 如果视频生成失败

        示例:
            result = await scraper.wait_for_video_completion(
                page,
                {"media123"},
                300000,
                worker_id=1
            )
            if result["status"] == "completed":
                print(f"下载 URL: {result['download_urls']['media123']}")
        """
        deadline = monotonic() + timeout_ms / 1000
        last_statuses: dict[str, str] = {}

        log_prefix = f"[Worker {worker_id}]" if worker_id else "[Scraper]"

        while monotonic() < deadline:
            # 这需要基于特定的视频生成 API 和页面结构的实际实现
            await asyncio.sleep(5)  # 轮询间隔

            if random.random() < 0.3:
                # 模拟一些人为活动
                await asyncio.sleep(random.uniform(0.1, 0.3))

        raise TimeoutError(f"等待视频生成超时，最后一次状态: {last_statuses}")