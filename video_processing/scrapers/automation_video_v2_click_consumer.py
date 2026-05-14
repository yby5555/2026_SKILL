"""
Google Flow 视频生成自动化消费者（V2 点击版）
================================================
此模块实现面向 Redis 任务队列的 Google Flow 视频生成爬虫。

核心流程：
    1. 从 Redis 有序集合消费任务
    2. 通过浏览器自动化访问 Google Flow 平台
    3. 模拟人类操作完成视频配置、图片上传、提交生成
    4. 轮询视频生成状态，下载生成结果

该模块包含完整的人机交互模拟、页面元素定位、图片上传、
视频状态轮询等功能，是视频处理流水线的核心采集组件。
"""

from __future__ import annotations

import asyncio
import base64
import contextvars
import hashlib
import json
import logging
import logging.handlers
import mimetypes
import os
import random
import re
import sys
import time
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import httpx

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.append(str(_ROOT))

_ACCOUNT_MGR = _ROOT / "account_mgr"
if str(_ACCOUNT_MGR) not in sys.path:
    sys.path.insert(0, str(_ACCOUNT_MGR))

from driver_base import MultiBrowserScraperBase

from video_processing.utils.account_utils import (
    LOGIN_EXPIRED_PATTERN,
    MIN_CREDITS_THRESHOLD,
    _click_avatar_and_get_credits,
)
from account_mgr.redis_utils import get_next_cookie, get_pool_status, release_cookie, remove_from_pool, report_cookie_invalid
from account_mgr.cos_utils import download_cos_image, is_cos_url

# 创建视频处理模块的日志目录
_LOG_DIR = _ROOT / "video_processing" / "log"
_LOG_DIR.mkdir(parents=True, exist_ok=True)
_LOG_FILE = _LOG_DIR / "automation_video_consumer.log"

logger = logging.getLogger("VideoScraperConsumer")
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


FLOW_HOME_URL = "https://labs.google/fx/zh/tools/flow"
VIDEO_SOURCE_LABEL = "素材"
FRAME_SOURCE_LABEL = "帧"
DEFAULT_ASPECT_RATIO = "9:16"

_current_log_prefix: contextvars.ContextVar[str] = contextvars.ContextVar("_current_log_prefix", default="")
DEFAULT_MODEL_LABEL = "Veo 3.1 - Lite"
MODEL_MAP = {
    0: "Veo 3.1 - Lite",
    1: "Veo 3.1 - Fast",
}
PROPORTION_MAP = {
    0: "9:16",
    1: "16:9",
}


def resolve_flow_locale() -> str:
    """Resolve the Flow browser locale shared by demo and consumer entrypoints."""
    return (
        os.getenv("FLOW_VIDEO_LOCALE")
        or os.getenv("FLOW_DEMO_LOCALE")
        or os.getenv("FLOW_BROWSER_LOCALE")
        or "en-US"
    )


def resolve_flow_timezone_id() -> str:
    """Resolve the Flow browser timezone shared by demo and consumer entrypoints."""
    return (
        os.getenv("FLOW_VIDEO_TIMEZONE_ID")
        or os.getenv("FLOW_DEMO_TIMEZONE_ID")
        or os.getenv("FLOW_BROWSER_TIMEZONE_ID")
        or "UTC"
    )


def _lp(worker_id: Any = "", context_id: Any = "") -> str:
    """获取当前日志前缀，优先使用上下文变量中已设置的值，否则返回 [B{worker_id}:C{context_id}]。"""
    p = _current_log_prefix.get("")
    return p or f"[B{worker_id}:C{context_id}]"


async def human_delay(min_sec: float = 0.5, max_sec: float = 2.0) -> None:
    """模拟人类操作间的随机延迟，等待 min_sec 到 max_sec 秒。"""
    await asyncio.sleep(random.uniform(min_sec, max_sec))


def normalize_prompt_text(text: str) -> str:
    """将换行符替换为空格，合并连续空格，返回清理后的单行文本。"""
    text = re.sub(r"\r\n?|\n", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


async def get_textbox_content(textbox_loc) -> str:
    """
    从文本框元素中提取当前文本内容。

    依次尝试 value、innerText、textContent 三种属性，
    返回第一个非空的规范化文本，都为空则返回空字符串。

    参数:
        textbox_loc: Playwright 文本框定位器
    """
    content = await textbox_loc.evaluate(
        """(el) => {
        const read = (value) => typeof value === "string" ? value : "";
        return {
            value: read(el.value),
            innerText: read(el.innerText),
            textContent: read(el.textContent),
        };
    }"""
    )

    for candidate in (content.get("value", ""), content.get("innerText", ""), content.get("textContent", "")):
        normalized = normalize_prompt_text(candidate)
        if normalized:
            return normalized
    return ""


async def fill_textbox_with_validation(page, textbox_loc, text: str, worker_id: Any, max_attempts: int = 2) -> None:
    """
    向文本框写入文本并校验，最多重试 max_attempts 次。

    模拟人类点击、全选、输入，每次写入后读取内容进行比对，
    不匹配则重试，全部失败后抛出 RuntimeError。

    参数:
        page: Playwright 页面对象
        textbox_loc: 目标文本框定位器
        text: 要输入的目标文本
        worker_id: Worker 编号，用于日志
        max_attempts: 最大尝试次数

    异常:
        RuntimeError: 所有尝试均校验失败
    """
    for attempt in range(1, max_attempts + 1):
        box = await textbox_loc.bounding_box()
        if box:
            await page.mouse.move(box["x"] + box["width"] / 3, box["y"] + box["height"] / 2, steps=15)

        await human_click(page, textbox_loc)
        await human_delay(0.2, 0.4)
        await human_click(page, textbox_loc)
        await human_delay(0.5, 1.0)

        await page.keyboard.press("Control+A")
        await human_delay(0.2, 0.6)
        await page.keyboard.insert_text(text)
        await human_delay(0.6, 1.2)

        current_text = await get_textbox_content(textbox_loc)
        if current_text == text:
            return

        logger.info(
            f"{_lp(worker_id)} 第 {attempt} 次写入校验失败，"
            f"当前文本: {current_text!r}，目标文本: {text!r}"
        )
        await human_delay(0.5, 0.9)

    raise RuntimeError("输入框写入后校验失败，未继续点击创建")


async def human_mouse_move(page) -> None:
    """在页面视口内随机移动鼠标 2-5 次，模拟人类浏览行为。"""
    viewport = page.viewport_size
    if not viewport:
        return
    width, height = viewport["width"], viewport["height"]
    for _ in range(random.randint(2, 5)):
        x = random.randint(100, max(120, width - 100))
        y = random.randint(100, max(120, height - 100))
        await page.mouse.move(x, y, steps=random.randint(5, 15))
        await asyncio.sleep(random.uniform(0.1, 0.3))


async def human_scroll(page) -> None:
    """模拟人类滚动页面，主要向下滚动，有 30% 概率额外向上滚一段。"""
    await page.mouse.wheel(0, random.randint(100, 400))
    await human_delay(0.2, 0.6)
    if random.random() < 0.3:
        await page.mouse.wheel(0, -random.randint(50, 200))
        await human_delay(0.1, 0.3)


async def human_click(page, locator_or_element, timeout: int = 5000) -> None:
    """
    模拟人类点击：先获取元素位置，移动鼠标到中心点，按下再松开。

    如果无法获取 bounding_box，则回退到 Playwright 原生 click。

    参数:
        page: Playwright 页面对象
        locator_or_element: 要点击的定位器或元素
        timeout: 回退点击的超时毫秒数
    """
    try:
        box = await locator_or_element.bounding_box()
        if not box:
            await locator_or_element.click(timeout=timeout)
            return

        target_x = box["x"] + box["width"] / 2
        target_y = box["y"] + box["height"] / 2
        await page.mouse.move(target_x, target_y, steps=random.randint(5, 10))
        await human_delay(0.1, 0.2)
        await page.mouse.down()
        await human_delay(0.05, 0.15)
        await page.mouse.up()
    except Exception:
        await locator_or_element.click(timeout=timeout)


async def _log_page_diagnostics(page, worker_id: Any, reason: str) -> None:
    """记录当前页面关键状态，便于定位偶发无按钮/挑战页问题。"""
    try:
        title = await page.title()
    except Exception:
        title = "<title unavailable>"
    try:
        url = page.url
    except Exception:
        url = "<url unavailable>"
    try:
        button_texts = await page.locator("button").evaluate_all(
            """(buttons) => buttons
              .map((button) => (button.innerText || button.textContent || "").trim())
              .filter(Boolean)
              .slice(0, 20)"""
        )
    except Exception:
        button_texts = []
    logger.info(
        f"{_lp(worker_id)} 页面诊断({reason}): url={url!r}, "
        f"title={title!r}, buttons={button_texts}"
    )


async def _log_browser_identity(page, log_prefix: str, configured_user_agent: str) -> None:
    """Log configured and actual browser UA/Client-Hints for diagnosis."""
    try:
        identity = await page.evaluate(
            """async (configuredUserAgent) => {
                const data = navigator.userAgentData;
                let highEntropy = null;
                if (data && data.getHighEntropyValues) {
                    try {
                        highEntropy = await data.getHighEntropyValues([
                            "architecture",
                            "bitness",
                            "brands",
                            "fullVersionList",
                            "mobile",
                            "model",
                            "platform",
                            "platformVersion",
                            "uaFullVersion",
                        ]);
                    } catch (error) {
                        highEntropy = { error: String(error) };
                    }
                }
                return {
                    configuredUserAgent,
                    userAgent: navigator.userAgent,
                    platform: navigator.platform,
                    language: navigator.language,
                    languages: navigator.languages,
                    userAgentData: data ? {
                        brands: data.brands,
                        mobile: data.mobile,
                        platform: data.platform,
                        highEntropy,
                    } : null,
                };
            }""",
            configured_user_agent,
        )
        logger.info(
            f"{log_prefix} browser_identity="
            f"{json.dumps(identity, ensure_ascii=False, default=str)}"
        )
    except Exception as exc:
        logger.info(f"{log_prefix} browser_identity_failed={exc}")


async def wait_for_flow_home_ready(page, worker_id: Any, timeout_ms: int = 30000) -> None:
    """等待 Flow 首页 SPA 渲染出可操作入口或明确登录/风控页面。"""
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=min(timeout_ms, 10000))
    except Exception:
        pass
    try:
        await page.wait_for_load_state("networkidle", timeout=timeout_ms)
    except Exception:
        pass

    ready_locator = page.locator(
        'button:has-text("新建项目"), button:has-text("New project"), '
        'a:has-text("新建项目"), a:has-text("New project"), '
        '[role="textbox"], button:has-text("登录"), button:has-text("Sign in")'
    ).first
    try:
        await ready_locator.wait_for(state="visible", timeout=timeout_ms)
    except Exception:
        await _log_page_diagnostics(page, worker_id, "flow_home_not_ready")


async def find_new_project_button(page):
    """兼容中英文 UI 查找 Flow 新建项目入口。"""
    selectors = [
        'button:has-text("新建项目")',
        'button:has-text("New project")',
        'a:has-text("新建项目")',
        'a:has-text("New project")',
        'button:has-text("新建")',
        'button:has-text("Create")',
    ]
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            if await locator.is_visible(timeout=1500):
                return locator
        except Exception:
            continue
    return None


async def get_creation_mode_button(page):
    """
    查找页面底部的视频配置按钮。

    通过评分算法从所有 menu 按钮中选出最可能是配置按钮的那个，
    综合考虑与创建按钮的位置关系、文本关键词等因素。

    参数:
        page: Playwright 页面对象

    返回:
        最佳匹配按钮的 Locator，未找到则返回 None
    """

    submit_btn = page.locator("button").filter(
        has_text=re.compile(r"arrow_forward", re.IGNORECASE)
    ).filter(has_text="创建").first

    if await submit_btn.count() == 0:
        submit_btn = page.locator('button:has-text("创建")').last

    submit_box = None
    if await submit_btn.count() > 0:
        try:
            submit_box = await submit_btn.bounding_box()
        except Exception:
            submit_box = None

    all_menu_btns = page.locator('button[aria-haspopup="menu"]')
    count = 0
    for _ in range(3):
        count = await all_menu_btns.count()
        if count > 0:
            break
        await asyncio.sleep(1)

    best_btn = None
    best_score = None

    for idx in range(count):
        btn = all_menu_btns.nth(idx)
        try:
            if not await btn.is_visible(timeout=200):
                continue

            current_box = await btn.bounding_box()
            if not current_box:
                continue

            text = (await btn.inner_text()).strip()
            lowered = text.lower()

            if any(
                flag in lowered
                for flag in [
                    "more_vert",
                    "settings",
                    "帮助",
                    "help",
                    "filter",
                    "search",
                    "add",
                    "添加",
                    "play_movies",
                    "scenebuilder",
                    "nav_rail",
                    "dashboard",
                    "archive",
                ]
            ):
                continue

            score = 0.0
            if submit_box:
                horizontal_gap = submit_box["x"] - (current_box["x"] + current_box["width"])
                vertical_gap = abs(current_box["y"] - submit_box["y"])
                if horizontal_gap < -20 or horizontal_gap > 350:
                    continue
                score += max(0.0, 350 - horizontal_gap)
                score += max(0.0, 120 - vertical_gap)
            else:
                score += current_box["y"] * 3 + current_box["x"]

            if re.search(r"x[1-4]", lowered):
                score += 80
            if any(flag in lowered for flag in ["video", "视频", "image", "图片", "nano banana", "veo"]):
                score += 40

            if best_score is None or score > best_score:
                best_btn = btn
                best_score = score
        except Exception:
            continue

    return best_btn


async def get_creation_settings_menu(page):
    """
    在已展开的菜单中查找包含 tablist 的配置菜单。

    参数:
        page: Playwright 页面对象

    返回:
        包含 [role="tablist"] 的菜单 Locator，未找到返回 None
    """
    menus = page.locator('[role="menu"]')
    count = await menus.count()
    for idx in range(count):
        menu = menus.nth(idx)
        try:
            if not await menu.is_visible(timeout=200):
                continue
            if await menu.locator('[role="tablist"]').count() > 0:
                return menu
        except Exception:
            continue
    return None


async def open_creation_settings_menu(page, worker_id: Any):
    """
    点击底部配置按钮并等待配置菜单展开。

    最多重试 5 次查找按钮，点击后等待菜单出现，
    如果第一次未展开则再次点击尝试。

    参数:
        page: Playwright 页面对象
        worker_id: Worker 编号，用于日志

    返回:
        tuple: (触发按钮 Locator, 菜单 Locator)

    异常:
        RuntimeError: 按钮不存在或菜单无法展开
    """
    target_btn = None
    for attempt in range(5):
        target_btn = await get_creation_mode_button(page)
        if target_btn:
            break
        logger.info(f"{_lp(worker_id)} 未找到底部配置按钮，重试中... ({attempt + 1}/5)")
        await asyncio.sleep(2)

    if not target_btn:
        raise RuntimeError("经过重试后仍找不到底部配置按钮，页面可能假死或网络中断")

    if await target_btn.get_attribute("aria-expanded") != "true":
        await human_click(page, target_btn)
        await human_delay(0.8, 1.5)

    menu = await get_creation_settings_menu(page)
    if menu:
        return target_btn, menu

    logger.info(f"{_lp(worker_id)} 菜单未出现，重试打开")
    await human_click(page, target_btn)
    await human_delay(1.5, 2.0)

    menu = await get_creation_settings_menu(page)
    if not menu:
        raise RuntimeError("底部配置菜单未成功展开")
    return target_btn, menu


async def select_tab_in_group(page, menu, group_index: int, label_pattern, worker_id: Any, description: str) -> None:
    """
    在配置菜单的第 group_index 个 tablist 中选择匹配 label_pattern 的标签页。

    如果目标标签已经是选中状态则跳过。

    参数:
        page: Playwright 页面对象
        menu: 配置菜单 Locator
        group_index: tablist 分组索引（从 0 开始）
        label_pattern: 文本匹配正则表达式
        worker_id: Worker 编号，用于日志
        description: 选项描述文字，用于日志和错误提示

    异常:
        RuntimeError: 未找到目标标签页
    """
    tablist = menu.locator('[role="tablist"]').nth(group_index)
    target_tab = tablist.locator('[role="tab"]').filter(has_text=label_pattern).first
    if await target_tab.count() == 0:
        raise RuntimeError(f"第 {group_index + 1} 组未找到 {description}")

    if await target_tab.get_attribute("aria-selected") == "true":
        logger.info(f"{_lp(worker_id)} {description} 已经选中")
        return

    await human_click(page, target_tab)
    await human_delay(0.6, 1.1)


async def ensure_video_model(page, menu, worker_id: Any, target_model: str) -> None:
    """
    确保视频模型切换到目标模型（如 Veo 3.1 - Lite）。

    如果当前模型已是目标模型则跳过，否则点击下拉菜单切换。

    参数:
        page: Playwright 页面对象
        menu: 配置菜单 Locator
        worker_id: Worker 编号，用于日志
        target_model: 目标模型名称

    异常:
        RuntimeError: 模型按钮或目标选项不存在
    """
    model_btn = menu.locator('button[aria-haspopup="menu"]').filter(
        has_text=re.compile(r"Veo 3\.1", re.IGNORECASE)
    ).first
    if await model_btn.count() == 0:
        raise RuntimeError("未找到视频模型下拉按钮")

    current_model = normalize_prompt_text(await model_btn.inner_text())
    if target_model.lower() in current_model.lower():
        logger.info(f"{_lp(worker_id)} 视频模型已经是 {target_model}")
        return

    logger.info(f"{_lp(worker_id)} 正在切换视频模型到 {target_model}")
    await human_click(page, model_btn)
    await human_delay(0.5, 0.9)

    target_option = page.locator('[role="menuitem"]').filter(has_text=target_model).first
    if await target_option.count() == 0:
        raise RuntimeError(f"模型菜单中未找到 {target_model}")

    await human_click(page, target_option)
    await human_delay(0.6, 1.0)


async def ensure_video_mode(page, worker_id: Any, variant_count: int, source_label: str = VIDEO_SOURCE_LABEL, aspect_ratio: str = DEFAULT_ASPECT_RATIO, model_label: str = DEFAULT_MODEL_LABEL) -> None:
    """
    完整设置视频生成配置：模式、来源、宽高比、数量、模型，并做最终校验。

    依次操作底部配置菜单中的 4 个 tablist 分组和模型下拉菜单，
    设置完成后重新打开菜单读取实际选中值进行二次确认。

    参数:
        page: Playwright 页面对象
        worker_id: Worker 编号，用于日志
        variant_count: 视频生成数量（1-4）
        source_label: 来源标签（"素材" 或 "帧"）
        aspect_ratio: 宽高比（"9:16" 或 "16:9"）
        model_label: 模型名称

    异常:
        RuntimeError: 任何配置步骤失败或最终校验不通过
    """
    logger.info(f"{_lp(worker_id)} 正在设置视频配置 ({source_label}, {aspect_ratio}, x{variant_count}, {model_label})...")

    try:
        await asyncio.sleep(2)

        _, menu = await open_creation_settings_menu(page, worker_id)

        await select_tab_in_group(
            page,
            menu,
            0,
            re.compile(r"video|videocam|视频", re.IGNORECASE),
            worker_id,
            "视频模式",
        )

        _, menu = await open_creation_settings_menu(page, worker_id)

        await select_tab_in_group(
            page,
            menu,
            1,
            re.compile(rf"{re.escape(source_label)}", re.IGNORECASE),
            worker_id,
            f"来源 {source_label}",
        )

        aspect_ratio_crop_map = {"9:16": "crop_9_16", "16:9": "crop_16_9"}
        aspect_ratio_crop = aspect_ratio_crop_map.get(aspect_ratio, "crop_9_16")
        await select_tab_in_group(
            page,
            menu,
            2,
            re.compile(rf"{re.escape(aspect_ratio)}|{re.escape(aspect_ratio_crop)}", re.IGNORECASE),
            worker_id,
            f"比例 {aspect_ratio}",
        )

        target_variant = f"x{variant_count}"
        variant_pattern = re.compile(
            rf"^(?:{re.escape(target_variant)}|{re.escape(str(variant_count) + 'x')})$",
            re.IGNORECASE,
        )
        await select_tab_in_group(
            page,
            menu,
            3,
            variant_pattern,
            worker_id,
            f"生成数量 {target_variant}",
        )

        await ensure_video_model(page, menu, worker_id, model_label)

        if await menu.is_visible(timeout=500):
            await page.keyboard.press("Escape")
            await human_delay(0.3, 0.6)

        _, menu = await open_creation_settings_menu(page, worker_id)
        selected_video_tab = menu.locator('[role="tablist"]').nth(0).locator('[role="tab"][aria-selected="true"]').first
        selected_source_tab = menu.locator('[role="tablist"]').nth(1).locator('[role="tab"][aria-selected="true"]').first
        selected_ratio_tab = menu.locator('[role="tablist"]').nth(2).locator('[role="tab"][aria-selected="true"]').first
        selected_variant_tab = menu.locator('[role="tablist"]').nth(3).locator('[role="tab"][aria-selected="true"]').first
        model_btn = menu.locator('button[aria-haspopup="menu"]').filter(
            has_text=re.compile(r"Veo 3\.1", re.IGNORECASE)
        ).first

        selected_video_text = normalize_prompt_text(await selected_video_tab.inner_text())
        selected_source_text = normalize_prompt_text(await selected_source_tab.inner_text())
        selected_ratio_text = normalize_prompt_text(await selected_ratio_tab.inner_text())
        selected_variant_text = normalize_prompt_text(await selected_variant_tab.inner_text())
        selected_model_text = normalize_prompt_text(await model_btn.inner_text())

        if "视频" not in selected_video_text and "videocam" not in selected_video_text.lower():
            raise RuntimeError(f"最终校验失败，模式不是视频: {selected_video_text}")
        if source_label not in selected_source_text and "chrome_extension" not in selected_source_text.lower() and "crop_free" not in selected_source_text.lower():
            raise RuntimeError(f"最终校验失败，来源不是 {source_label}: {selected_source_text}")
        if aspect_ratio not in selected_ratio_text and aspect_ratio_crop not in selected_ratio_text.lower():
            raise RuntimeError(f"最终校验失败，比例不是 {aspect_ratio}: {selected_ratio_text}")
        selected_variant_normalized = selected_variant_text.lower()
        expected_variant_values = {target_variant.lower(), f"{variant_count}x"}
        if selected_variant_normalized not in expected_variant_values:
            raise RuntimeError(f"最终校验失败，生成数量不是 {target_variant}: {selected_variant_text}")
        if model_label.lower() not in selected_model_text.lower():
            raise RuntimeError(f"最终校验失败，模型不是 {model_label}: {selected_model_text}")

        await page.keyboard.press("Escape")
        await human_delay(0.3, 0.6)
        await human_delay(0.8, 1.5)
    except Exception as exc:
        logger.info(f"{_lp(worker_id)} 设置模式错误: {exc}")
        try:
            await page.keyboard.press("Escape")
        except Exception:
            pass
        raise RuntimeError(f"设置视频模式失败: {exc}")


def _mime_to_extension(mime_type: str) -> str:
    """根据 MIME 类型猜测文件扩展名，无法识别时默认返回 ".png"。"""
    guessed_extension = mimetypes.guess_extension(mime_type or "")
    return guessed_extension or ".png"


def _derive_image_upload_meta(image_url: str | None, image_base64: str | None) -> tuple[str, str, str | None]:
    """
    从图片 URL 或 Base64 数据中提取上传所需的元信息。

    参数:
        image_url: 图片 URL（可选）
        image_base64: Base64 编码的图片数据，支持 data URI 前缀（可选）

    返回:
        tuple: (文件名, MIME 类型, 去除前缀后的纯 Base64 数据)
    """
    file_name = "image.png"
    mime_type = "image/png"
    normalized_base64 = image_base64

    if image_url:
        parsed = urlparse(image_url)
        extracted_name = Path(unquote(parsed.path)).name
        if extracted_name:
            file_name = extracted_name if not is_cos_url(image_url) else "image.png"
        guessed_mime, _ = mimetypes.guess_type(file_name)
        if guessed_mime:
            mime_type = guessed_mime
        if is_cos_url(image_url):
            if guessed_mime:
                guessed_ext = mimetypes.guess_extension(guessed_mime) or ".png"
            else:
                guessed_ext = os.path.splitext(extracted_name)[1] or ".png"
            file_name = f"image{guessed_ext}"

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
    从任务数据中解析参考图片重复使用次数。

    依次尝试读取 reference_image_count、image_count、reference_count 字段，
    取第一个有效值（最小为 1），全部无效时返回 1。

    参数:
        task_data: 任务字典
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
    计算任务请求的参考图片总数。

    优先统计 image_url_list / image_base64_list 的长度，
    其次根据单图 + repeat_count 推算，都为空返回 0。

    参数:
        task_data: 任务字典
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


def _extract_media_name_from_src(src: str | None) -> str | None:
    """
    从图片 src URL 的查询参数中提取 media name 标识符。

    优先用 parse_qs 解析 name 参数，失败则用正则匹配。

    参数:
        src: 图片 src URL

    返回:
        media name 字符串，或 None
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


async def _get_visible_create_image_dialog(page):
    dialogs = page.locator('[role="dialog"]')
    dialog_count = await dialogs.count()
    for idx in range(dialog_count - 1, -1, -1):
        dialog = dialogs.nth(idx)
        try:
            if not await dialog.is_visible(timeout=200):
                continue
            dialog_text = normalize_prompt_text(await dialog.inner_text())
            if any(flag in dialog_text for flag in ["上传图片", "搜索资源", "图片", "语音"]):
                return dialog
        except Exception:
            continue
    return None


async def _find_bottom_create_dialog_button(page):
    submit_btn = page.locator("button").filter(
        has_text=re.compile(r"arrow_forward", re.IGNORECASE)
    ).filter(has_text="创建").first
    if await submit_btn.count() == 0:
        submit_btn = page.locator('button:has-text("创建")').last

    submit_box = None
    if await submit_btn.count() > 0:
        try:
            submit_box = await submit_btn.bounding_box()
        except Exception:
            submit_box = None

    textbox = page.locator('[role="textbox"]').last
    textbox_box = None
    if await textbox.count() > 0:
        try:
            textbox_box = await textbox.bounding_box()
        except Exception:
            textbox_box = None

    buttons = page.locator("button")
    button_count = await buttons.count()
    best_btn = None
    best_score = None

    for idx in range(button_count):
        btn = buttons.nth(idx)
        try:
            if not await btn.is_visible(timeout=200):
                continue

            box = await btn.bounding_box()
            if not box:
                continue

            text = normalize_prompt_text(await btn.inner_text())
            lowered = text.lower()

            if "创建" not in text and "add_2" not in lowered:
                continue
            if "新建项目" in text:
                continue
            if "arrow_forward" in lowered:
                continue

            score = 0.0
            if await btn.get_attribute("aria-haspopup") == "dialog":
                score += 180
            if "add_2" in lowered:
                score += 120
            if "图片" in text or "image" in lowered:
                score += 40

            if submit_box:
                horizontal_gap = submit_box["x"] - (box["x"] + box["width"])
                vertical_gap = abs((box["y"] + box["height"] / 2) - (submit_box["y"] + submit_box["height"] / 2))
                if -30 <= horizontal_gap <= 260:
                    score += max(0.0, 260 - abs(horizontal_gap))
                    score += max(0.0, 120 - vertical_gap)
                else:
                    score -= 100

            if textbox_box:
                vertical_to_textbox = abs((box["y"] + box["height"] / 2) - (textbox_box["y"] + textbox_box["height"] / 2))
                score += max(0.0, 120 - vertical_to_textbox)
                if box["x"] <= textbox_box["x"] + textbox_box["width"] + 180:
                    score += 40

            score += box["y"] * 0.2

            if best_score is None or score > best_score:
                best_btn = btn
                best_score = score
        except Exception:
            continue

    return best_btn


async def _open_create_image_dialog(page, worker_id: Any):
    for attempt in range(4):
        create_btn = await _find_bottom_create_dialog_button(page)
        await human_delay(1, 2)
        await human_click(page, create_btn)
        await human_delay(1, 2)
        dialog = await _get_visible_create_image_dialog(page)
        if dialog:
            logger.info(f"{_lp(worker_id)} 已打开图片选择面板")
            return dialog

    raise RuntimeError("未找到底部创建按钮，无法打开图片选择面板")


async def _list_prompt_attachment_media_names(page) -> list[str]:
    attachment_imgs = page.locator('button[data-card-open] img[src*="media.getMediaUrlRedirect?name="]')
    srcs = await attachment_imgs.evaluate_all(
        """(nodes) => nodes
        .map((node) => node.getAttribute("src") || "")
        .filter(Boolean)"""
    )

    media_names: list[str] = []
    for src in srcs:
        media_name = _extract_media_name_from_src(src)
        if media_name:
            media_names.append(media_name)
    return media_names


async def _wait_for_prompt_attachment(
    page,
    worker_id: Any,
    response_media_name: str | None,
    timeout_ms: int = 30000,
    previous_media_names: set[str] | None = None,
) -> str:
    previous_media_names = previous_media_names or set()
    deadline = monotonic() + timeout_ms / 1000

    while monotonic() < deadline:
        current_media_names = await _list_prompt_attachment_media_names(page)
        current_name_set = set(current_media_names)

        if response_media_name and response_media_name in current_name_set:
            logger.info(f"{_lp(worker_id)} 已检测到底部附件挂载: {response_media_name}")
            return response_media_name

        new_media_names = [name for name in current_media_names if name not in previous_media_names]
        if new_media_names:
            actual_media_name = new_media_names[-1]
            logger.info(f"{_lp(worker_id)} 已检测到底部新增附件: {actual_media_name}")
            return actual_media_name

        await human_delay(0.4, 0.8)

    raise RuntimeError(
        f"等待图片出现在输入框附件区超时: response={response_media_name}, "
        f"before={sorted(previous_media_names)}, after={await _list_prompt_attachment_media_names(page)}"
    )



async def pause_before_file_selection(worker_id: Any, min_sec: float = 4.0, max_sec: float = 5.0) -> None:
    """Pause after the upload click/file chooser opens to mimic a human finding an image."""
    logger.info(f"{_lp(worker_id)} file chooser opened; pausing {min_sec:.0f}-{max_sec:.0f}s to mimic human image selection")
    await human_delay(min_sec, max_sec)


async def upload_reference_image_via_picker(
    page,
    worker_id: Any,
    file_buffer: bytes,
    file_name: str,
    mime_type: str,
) -> str:
    dialog = await _open_create_image_dialog(page, worker_id)
    previous_attachment_media_names = set(await _list_prompt_attachment_media_names(page))
    upload_option = dialog.locator('div:has-text("上传图片"), li:has-text("上传图片"), button:has-text("上传图片")').last
    if await upload_option.count() == 0:
        raise RuntimeError("图片选择面板打开后未找到“上传图片”入口")

    logger.info(f"{_lp(worker_id)} 正在上传参考图片: {file_name}")
    async with page.expect_file_chooser(timeout=15000) as fc_info:
        await human_click(page, upload_option)

    try:
        agree_btn = page.locator('button:has-text("我同意"), button:has-text("I Agree"), button:has-text("同意")').first
        if await agree_btn.is_visible(timeout=2000):
            logger.info(f"{_lp(worker_id)} 检测到法律声明，点击“我同意”...")
            await human_click(page, agree_btn)
            await human_delay(1.5, 2.5)
            dialog = await _open_create_image_dialog(page, worker_id)
            upload_option = dialog.locator(
                'div:has-text("上传图片"), li:has-text("上传图片"), button:has-text("上传图片")'
            ).last
            await human_click(page, upload_option)
    except Exception as exc:
        logger.info(f"{_lp(worker_id)} 上传图片时未触发或未完成法律声明处理: {exc}")

    file_chooser = await fc_info.value
    await pause_before_file_selection(worker_id)
    async with page.expect_response(
        lambda r: "uploadImage" in r.url and r.request.method == "POST",
        timeout=90000,
    ) as upload_info:
        await file_chooser.set_files({
            "name": file_name,
            "mimeType": mime_type,
            "buffer": file_buffer,
        })

    response = await upload_info.value
    payload = await response.json()
    response_media_name = (payload.get("media") or {}).get("name")
    logger.info(f"{_lp(worker_id)} 图片已成功上传 (文件选择器)")

    await human_delay(3, 5)

    attached_media_name = await _wait_for_prompt_attachment(
        page,
        worker_id,
        response_media_name,
        previous_media_names=previous_attachment_media_names,
    )

    visible_dialog = await _get_visible_create_image_dialog(page)
    if visible_dialog:
        try:
            await page.keyboard.press("Escape")
            await human_delay(0.3, 0.6)
        except Exception:
            pass

    logger.info(f"{_lp(worker_id)} 已将图片挂载到输入框: {attached_media_name}")
    return attached_media_name


async def _find_frame_area(page, frame_type: str):
    frame_divs = page.locator('div[aria-haspopup="dialog"]')
    count = await frame_divs.count()
    for idx in range(count):
        div = frame_divs.nth(idx)
        try:
            text = normalize_prompt_text(await div.inner_text())
            if text == frame_type:
                return div
        except Exception:
            continue
    return None


async def _open_frame_image_dialog(page, worker_id: Any, frame_type: str):
    for attempt in range(4):
        frame_area = await _find_frame_area(page, frame_type)
        if not frame_area:
            raise RuntimeError(f"未找到{frame_type}帧区域 (尝试 {attempt + 1}/4)")

        await human_click(page, frame_area)
        await human_delay(1, 2)
        dialog = await _get_visible_create_image_dialog(page)
        if dialog:
            logger.info(f"{_lp(worker_id)} 已打开{frame_type}帧图片选择面板")
            return dialog

    raise RuntimeError(f"未成功打开{frame_type}帧图片选择面板")


async def upload_frame_image_via_picker(
    page,
    worker_id: Any,
    frame_type: str,
    file_buffer: bytes,
    file_name: str,
    mime_type: str,
) -> str:
    dialog = await _open_frame_image_dialog(page, worker_id, frame_type)
    upload_option = dialog.locator('div:has-text("上传图片"), li:has-text("上传图片"), button:has-text("上传图片")').last
    if await upload_option.count() == 0:
        raise RuntimeError(f"{frame_type}帧图片面板中未找到'上传图片'入口")

    logger.info(f"{_lp(worker_id)} 正在上传{frame_type}帧图片: {file_name}")
    async with page.expect_file_chooser(timeout=15000) as fc_info:
        await human_click(page, upload_option)

    file_chooser = await fc_info.value
    await pause_before_file_selection(worker_id)
    async with page.expect_response(
        lambda r: "uploadImage" in r.url and r.request.method == "POST",
        timeout=90000,
    ) as upload_info:
        await file_chooser.set_files({
            "name": file_name,
            "mimeType": mime_type,
            "buffer": file_buffer,
        })

    response = await upload_info.value
    payload = await response.json()
    response_media_name = (payload.get("media") or {}).get("name")
    logger.info(f"{_lp(worker_id)} {frame_type}帧图片已上传: {response_media_name}")

    await human_delay(2, 4)

    visible_dialog = await _get_visible_create_image_dialog(page)
    if visible_dialog:
        try:
            await page.keyboard.press("Escape")
            await human_delay(0.3, 0.6)
        except Exception:
            pass

    frame_mounted = await _wait_for_frame_image_mounted(page, worker_id, frame_type, timeout_ms=30000)
    if frame_mounted:
        logger.info(f"{_lp(worker_id)} {frame_type}帧图片已挂载确认")
    else:
        logger.warning(f"{_lp(worker_id)} {frame_type}帧图片挂载未确认，继续执行")

    logger.info(f"{_lp(worker_id)} {frame_type}帧图片上传完成: {response_media_name}")
    return response_media_name


async def _wait_for_frame_image_mounted(
    page,
    worker_id: Any,
    frame_type: str,
    timeout_ms: int = 30000,
) -> bool:
    deadline = monotonic() + timeout_ms / 1000
    frame_index = 0 if frame_type == "起始" else 1
    while monotonic() < deadline:
        try:
            swap_btn = page.locator('button').filter(has_text="交换第一帧和最后一帧").first
            if await swap_btn.count() > 0:
                container = swap_btn.locator("xpath=..")
                if await container.count() > 0:
                    frame_divs = container.locator(":scope > div").filter(has=page.locator("img"))
                    if await frame_divs.count() > frame_index:
                        img = frame_divs.nth(frame_index).locator("img")
                        if await img.count() > 0:
                            return True
        except Exception:
            pass
        try:
            frame_area = await _find_frame_area(page, frame_type)
            if frame_area:
                img = frame_area.locator("img")
                if await img.count() > 0:
                    return True
        except Exception:
            pass
        await human_delay(0.5, 1.0)
    return False


class GoogleFlowVideoScraperV2(MultiBrowserScraperBase):
    """面向 Redis 消费者的精简版 Google Flow 视频生成抓取器。"""

    def __init__(self, **kwargs: Any) -> None:
        """设置 Flow 场景默认浏览器环境。

        默认值优先从环境变量读取，避免 VPN/代理场景下继续强制使用中文+
        上海时区，导致浏览器语言、时区与出口 IP 不一致。
        """
        kwargs.setdefault("locale", resolve_flow_locale())
        kwargs.setdefault("timezone_id", resolve_flow_timezone_id())
        kwargs.setdefault("default_cookie_domain", ".google.com")
        kwargs.setdefault("recycle_browser_after_failures", 1)
        super().__init__(**kwargs)

    def normalize_task(self, task_data: dict[str, Any]) -> dict[str, Any]:
        """
        预处理任务数据，自动分配 Cookie。

        COS 图片链接会先下载转为 base64，确保走 base64 成熟路径。
        Cookie 槽位暂时被占满时会等待重试，池真正为空时才抛异常。

        参数:
            task_data: 原始任务字典

        返回:
            补全了 email 和 cookies 的任务字典副本

        异常:
            RuntimeError: Cookie 池为空
        """
        task_copy = dict(task_data)

        image_urls = task_copy.get("image_url_list") or []
        image_url = task_copy.get("image_url")

        if image_urls and all(is_cos_url(u) for u in image_urls if u):
            base64_list = []
            for u in image_urls:
                if u:
                    base64_list.append(base64.b64encode(download_cos_image(u)).decode("utf-8"))
            task_copy["image_base64_list"] = base64_list
            task_copy.pop("image_url_list", None)
        elif image_url and is_cos_url(image_url):
            task_copy["image_base64"] = base64.b64encode(download_cos_image(image_url)).decode("utf-8")
            task_copy.pop("image_url", None)

        if not task_copy.get("email") or not task_copy.get("cookies"):
            deadline = time.time() + 120
            while True:
                next_result = get_next_cookie()
                if next_result:
                    task_copy["email"], task_copy["cookies"] = next_result
                    break

                status = get_pool_status()
                if not status["active"]:
                    raise RuntimeError("Redis Cookie Pool 为空，无法启动任务，请先运行 login_scheduler.py")

                if time.time() >= deadline:
                    raise RuntimeError(
                        f"Cookie 槽位等待超时（120s），"
                        f"池中 {len(status['active'])} 个活跃账号均被占满"
                    )

                time.sleep(3)
        return task_copy

    async def _ensure_account_healthy(self, page, task_data: dict[str, Any], worker) -> None:
        """
        检查当前账号健康状态，不健康时自动切换 Cookie。

        检查逻辑：访问 Flow 首页 → 读取 AI 点数 → 判断额度是否足够。
        最多切换 3 次 Cookie，全部不满足则抛出异常。

        参数:
            page: Playwright 页面对象
            task_data: 任务字典（会被原地更新 email/cookies）
            worker: Worker 上下文

        异常:
            RuntimeError: 连续切换 3 次 Cookie 后仍无可用账号
        """
        max_switches = 3
        current_email = task_data.get("email")

        async def _check_current_page() -> str:
            url = page.url
            if LOGIN_EXPIRED_PATTERN.search(url) or "/signin" in url:
                logger.info(f"{_lp(worker.worker_id)} 账号 {current_email} 登录态失效")
                if current_email:
                    report_cookie_invalid(current_email)
                return "login_expired"

            credits = await _click_avatar_and_get_credits(page)
            if credits is None:
                logger.info(f"{_lp(worker.worker_id)} 账号 {current_email} AI 点数读取失败，跳过额度检测继续执行")
                return "ok"

            logger.info(f"{_lp(worker.worker_id)} 账号 {current_email} AI 点数: {credits}")
            if credits < MIN_CREDITS_THRESHOLD:
                logger.info(f"{_lp(worker.worker_id)} 账号 {current_email} 额度不足({credits})")
                if current_email:
                    report_cookie_invalid(current_email)
                return "no_credits"
            return "ok"

        for attempt in range(max_switches + 1):
            status = await _check_current_page()
            if status == "ok":
                logger.info(f"{_lp(worker.worker_id)} 账号 {current_email} 检测通过，继续执行")
                return

            if attempt >= max_switches:
                raise RuntimeError(f"{_lp(worker.worker_id, worker.context_id)} 已切换 {max_switches} 次 Cookie 仍无可用账号，任务终止")

            next_result = get_next_cookie()
            if not next_result:
                raise RuntimeError("Redis Cookie Pool 已空，无法切换账号")

            new_email, new_cookies = next_result
            logger.info(f"{_lp(worker.worker_id)} 切换到新账号: {new_email}")

            await page.context.clear_cookies()
            await page.context.add_cookies(new_cookies)

            task_data["email"] = new_email
            task_data["cookies"] = new_cookies
            current_email = new_email

            logger.info(f"{_lp(worker.worker_id)} 重新加载 Flow 首页...")
            await page.goto(FLOW_HOME_URL, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2)

    async def _wait_for_video_status(
        self,
        page,
        media_names: set[str],
        worker_id: Any,
        timeout_ms: int,
        email: str | None,
    ) -> dict[str, Any]:
        """
        轮询视频生成状态，等待所有媒体变为终态或超时。

        通过拦截 batchCheckAsyncVideoGenerationStatus 响应获取状态，
        SUCCESSFUL 时提取下载 URL 并返回。

        参数:
            page: Playwright 页面对象
            media_names: 待查询的 media name 集合
            worker_id: Worker 编号，用于日志
            timeout_ms: 轮询超时毫秒数
            email: 当前账号邮箱，用于日志

        返回:
            dict: 包含成功媒体下载信息的字典

        异常:
            TimeoutError: 超时未完成
            RuntimeError: 生成失败（FAILED 状态）
        """
        deadline = monotonic() + timeout_ms / 1000
        last_statuses: dict[str, str] = {}

        while monotonic() < deadline:
            remaining_ms = max(1000, int((deadline - monotonic()) * 1000))
            async with page.expect_response(
                lambda r: (
                    "batchCheckAsyncVideoGenerationStatus" in r.url and r.request.method == "POST"
                ),
                timeout=remaining_ms,
            ) as response_info:
                pass

            response = await response_info.value
            payload = await response.json()
            if "result" in payload and "data" in payload["result"]:
                media_items = payload["result"]["data"].get("media", [])
            else:
                media_items = payload.get("media", [])

            tracked_items = [item for item in media_items if item.get("name") in media_names]
            if not tracked_items:
                if random.random() < 0.2:
                    await human_mouse_move(page)
                continue

            last_statuses = {
                item["name"]: item.get("mediaMetadata", {}).get("mediaStatus", {}).get("mediaGenerationStatus", "UNKNOWN")
                for item in tracked_items
            }
            logger.info(f"{_lp()} 当前视频状态: {last_statuses}")

            full_api_response = json.dumps(payload, ensure_ascii=False)

            if any(status == "MEDIA_GENERATION_STATUS_FAILED" for status in last_statuses.values()):
                raise RuntimeError(f"视频生成失败: {last_statuses}")

            if media_names.issubset(last_statuses.keys()) and all(
                status == "MEDIA_GENERATION_STATUS_SUCCESSFUL" for status in last_statuses.values()
            ):
                return {"media": tracked_items, "api_full_response": full_api_response}

            if random.random() < 0.3:
                await human_mouse_move(page)
                await human_scroll(page)

        raise TimeoutError(f"等待视频生成超时，最后一次状态: {last_statuses}")

    async def _download_video_to_local(self, page, download_url: str, worker_id: Any, cookies: list, save_path: str) -> tuple[bool, str]:
        """
        下载视频到本地文件。

        模拟人类延迟后通过 HTTP GET 下载视频内容，
        验证文件大小并计算 MD5，写入本地路径。

        参数:
            page: Playwright 页面对象（用于模拟人类行为）
            download_url: 视频下载 URL
            worker_id: Worker 编号，用于日志
            cookies: 浏览器 Cookie 列表，用于鉴权
            save_path: 本地保存路径

        返回:
            tuple[bool, str]: (下载成功返回 True，失败返回 False, 视频 MIME 类型)
        """
        await human_delay(4.0, 8.0)
        await human_mouse_move(page)
        await human_scroll(page)

        try:
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
            video_mime_type = ""
            async with httpx.AsyncClient(timeout=180.0, verify=False, follow_redirects=True) as client:
                async with client.stream("GET", download_url, cookies=cookie_dict, headers=headers) as response:
                    response.raise_for_status()
                    video_mime_type = (response.headers.get("content-type") or "").split(";")[0].strip()
                    with open(save_path, "wb") as file_obj:
                        async for chunk in response.aiter_bytes(chunk_size=8192):
                            if chunk:
                                file_obj.write(chunk)

            if os.path.exists(save_path) and os.path.getsize(save_path) > 1024:
                kb_size = os.path.getsize(save_path) // 1024
                logger.info(f"{_lp(worker_id)} 视频下载成功！大小: {kb_size} KB, MIME: {video_mime_type}, URL: {download_url[:50]}...")
                return True, video_mime_type

            logger.error(f"{_lp(worker_id)} 下载文件似乎太小或不存在")
            return False, video_mime_type
        except Exception as exc:
            logger.error(f"{_lp(worker_id)} 下载视频到本地失败: {exc}")
            return False, ""

    async def _prepare_video_project(self, page, worker, prompt: str, variant_count: int, task_data: dict[str, Any]) -> None:
        """
        完整的视频项目准备流程：配置参数 → 上传图片 → 填写提示词。

        根据 gen_type 区分帧模式和素材模式，上传参考图/帧图片，
        处理 Base64 和 URL 两种图片来源，填写提示词到输入框。

        参数:
            page: Playwright 页面对象
            worker: Worker 上下文
            prompt: 用户提示词文本
            variant_count: 视频变体数量
            task_data: 完整任务字典

        异常:
            RuntimeError: 图片上传失败或页面元素未找到
        """
        gen_type = int(task_data.get("gen_type", 1))
        is_frame_mode = gen_type == 0
        source_label = FRAME_SOURCE_LABEL if is_frame_mode else VIDEO_SOURCE_LABEL
        proportion = int(task_data.get("proportion", 0))
        aspect_ratio = PROPORTION_MAP.get(proportion, DEFAULT_ASPECT_RATIO)
        model_type = int(task_data.get("model_type", 0))
        model_label = MODEL_MAP.get(model_type, DEFAULT_MODEL_LABEL)

        try:
            cookie_btn = page.locator(
                '.glue-cookie-notification-bar button:has-text("Got it"), '
                '.glue-cookie-notification-bar button:has-text("同意"), '
                '.glue-cookie-notification-bar button:has-text("Accept"), '
                ".glue-cookie-notification-bar button"
            ).first
            if await cookie_btn.is_visible(timeout=2000):
                logger.info(f"{_lp(worker.worker_id)} 发现 Cookie 弹窗，正在关闭...")
                await human_click(page, cookie_btn)
                await human_delay(1.0, 2.0)
        except Exception:
            pass

        await self._ensure_account_healthy(page, task_data, worker)

        logger.info(f"{_lp(worker.worker_id)} 等待并点击新建项目...")
        try:
            new_btn = page.locator('button:has-text("新建项目")')
            if await new_btn.is_visible(timeout=5000):
                await human_delay(1.0, 2.0)
                await human_click(page, new_btn)
                await human_delay(2.0, 4.0)
            else:
                raise RuntimeError("超时未找到'新建项目'按钮，页面可能未加载完成")
        except Exception as exc:
            raise RuntimeError(f"点击'新建项目'失败: {exc}")

        await ensure_video_mode(page, worker.worker_id, variant_count, source_label=source_label, aspect_ratio=aspect_ratio, model_label=model_label)

        upload_payloads: list[tuple[bytes, str, str]] = []
        requested_reference_count = _get_requested_reference_image_count(task_data)
        preparation_errors: list[str] = []
        image_urls = task_data.get("image_url_list") or []
        image_base64_list = task_data.get("image_base64_list") or []
        image_url = task_data.get("image_url")
        image_base64 = task_data.get("image_base64")

        if image_urls:
            for idx, current_image_url in enumerate(image_urls, start=1):
                if not current_image_url:
                    preparation_errors.append(f"第 {idx} 张图片 URL 为空")
                    continue
                file_name, mime_type, _ = _derive_image_upload_meta(current_image_url, None)
                logger.info(f"{_lp(worker.worker_id)} 正在从 URL 下载第 {idx}/{len(image_urls)} 张参考图片: {current_image_url}")
                try:
                    async with httpx.AsyncClient(verify=False, timeout=30.0) as client:
                        resp = await client.get(current_image_url)
                        resp.raise_for_status()
                        file_buffer = resp.content
                        content_type = (resp.headers.get("content-type") or "").split(";")[0].strip()
                        if content_type.startswith("image/"):
                            mime_type = content_type
                    upload_payloads.append((file_buffer, file_name, mime_type))
                except Exception as exc:
                    logger.error(f"{_lp(worker.worker_id)} 从 URL 下载第 {idx} 张图片失败: {exc}")
                    preparation_errors.append(f"第 {idx} 张 URL 图片下载失败: {exc}")
        elif image_base64_list:
            total_base64_count = len(image_base64_list)
            for idx, current_image_base64 in enumerate(image_base64_list, start=1):
                if not current_image_base64:
                    preparation_errors.append(f"第 {idx} 张 Base64 图片为空")
                    continue
                file_name, mime_type, normalized_image_base64 = _derive_image_upload_meta(None, current_image_base64)
                logger.info(f"{_lp(worker.worker_id)} 正在处理第 {idx}/{total_base64_count} 张 Base64 参考图片")
                try:
                    file_buffer = base64.b64decode(normalized_image_base64)
                    if not file_buffer:
                        raise ValueError("解码结果为空")
                    upload_payloads.append((file_buffer, file_name, mime_type))
                except Exception as exc:
                    logger.error(f"{_lp(worker.worker_id)} 第 {idx} 张 Base64 解码图片失败: {exc}")
                    preparation_errors.append(f"第 {idx} 张 Base64 解码失败: {exc}")
        else:
            file_name, mime_type, normalized_image_base64 = _derive_image_upload_meta(image_url, image_base64)
            file_buffer = None

            if image_url:
                logger.info(f"{_lp(worker.worker_id)} 正在从 URL 下载参考图片: {image_url}")
                try:
                    async with httpx.AsyncClient(verify=False, timeout=30.0) as client:
                        resp = await client.get(image_url)
                        resp.raise_for_status()
                        file_buffer = resp.content
                        content_type = (resp.headers.get("content-type") or "").split(";")[0].strip()
                        if content_type.startswith("image/"):
                            mime_type = content_type
                except Exception as exc:
                    logger.error(f"{_lp(worker.worker_id)} 从 URL 下载图片失败: {exc}")
                    preparation_errors.append(f"参考图 URL 下载失败: {exc}")
            elif image_base64:
                logger.info(f"{_lp(worker.worker_id)} 正在处理 Base64 格式的参考图片")
                try:
                    file_buffer = base64.b64decode(normalized_image_base64)
                    if not file_buffer:
                        raise ValueError("解码结果为空")
                except Exception as exc:
                    logger.error(f"{_lp(worker.worker_id)} Base64 解码图片失败: {exc}")
                    preparation_errors.append(f"参考图 Base64 解码失败: {exc}")

            if file_buffer:
                reference_image_repeat_count = _resolve_reference_image_repeat_count(task_data)
                upload_payloads.extend([(file_buffer, file_name, mime_type)] * reference_image_repeat_count)

        if requested_reference_count:
            prepared_reference_count = len(upload_payloads)
            if prepared_reference_count != requested_reference_count:
                error_suffix = f"，失败详情: {'; '.join(preparation_errors)}" if preparation_errors else ""
                raise RuntimeError(
                    f"参考图准备失败: 期望 {requested_reference_count} 张，成功 {prepared_reference_count} 张{error_suffix}"
                )

        if upload_payloads:
            try:
                if is_frame_mode:
                    total_upload_count = len(upload_payloads)
                    frame_types = ["起始", "结束"]
                    for upload_index, (file_buffer, current_file_name, current_mime_type) in enumerate(upload_payloads):
                        frame_type = frame_types[upload_index] if upload_index < len(frame_types) else "起始"
                        logger.info(
                            f"{_lp(worker.worker_id, worker.context_id)} 正在上传第 {upload_index + 1}/{total_upload_count} 张帧图片 ({frame_type}帧)"
                        )
                        await upload_frame_image_via_picker(
                            page=page,
                            worker_id=worker.worker_id,
                            frame_type=frame_type,
                            file_buffer=file_buffer,
                            file_name=current_file_name,
                            mime_type=current_mime_type,
                        )
                else:
                    total_upload_count = len(upload_payloads)
                    for upload_index, (file_buffer, current_file_name, current_mime_type) in enumerate(upload_payloads, start=1):
                        logger.info(
                            f"{_lp(worker.worker_id, worker.context_id)} 正在上传第 {upload_index}/{total_upload_count} 张参考图"
                        )
                        await upload_reference_image_via_picker(
                            page=page,
                            worker_id=worker.worker_id,
                            file_buffer=file_buffer,
                            file_name=current_file_name,
                            mime_type=current_mime_type,
                        )
            except Exception as exc:
                logger.error(f"{_lp(worker.worker_id)} 上传图片失败: {exc}")
                raise

        input_prompt = normalize_prompt_text(prompt)
        logger.info(f"{_lp(worker.worker_id)} 等待输入框并输入提示词: {input_prompt[:10]}")
        await page.wait_for_selector('[role="textbox"]', state="visible", timeout=15000)
        await human_delay(1.5, 3.0)

        textbox_loc = page.locator('[role="textbox"]')
        await fill_textbox_with_validation(page, textbox_loc, input_prompt, worker.worker_id)
        await human_delay(1.0, 2.5)

    async def _submit_video_generation_humanized(self, page, worker_id: Any, is_frame_mode: bool = False, log_prefix: str = "") -> tuple[str, dict[str, Any]]:
        """
        模拟人类操作提交视频生成请求并拦截响应。

        点击创建按钮 → 拦截 submit 请求响应 → 提取 media name。
        帧模式下会在创建前额外等待并点击确认。

        参数:
            page: Playwright 页面对象
            worker_id: Worker 编号，用于日志
            is_frame_mode: 是否为帧模式
            log_prefix: 日志前缀

        返回:
            tuple[str, dict]: (media name, 完整响应数据)

        异常:
            RuntimeError: 提交按钮未找到或响应异常
        """
        lp = log_prefix or _lp(worker_id)
        logger.info(f"{lp} 等待提交按钮出现...")
        await human_delay(1.5, 3.0)

        submit_btn = page.locator("button").filter(
            has_text=re.compile(r"arrow_forward", re.IGNORECASE)
        ).filter(has_text="创建").first
        if not await submit_btn.is_visible(timeout=2500):
            submit_btn = page.locator('button:has-text("创建")').last

        if not await submit_btn.is_visible(timeout=5000):
            raise RuntimeError("超时未找到真实可点击的创建提交按钮")

        if is_frame_mode:
            api_matcher = lambda r: (
                r.request.method == "POST"
                and (
                    "batchAsyncGenerateVideoStartImage" in r.url
                    or "batchAsyncGenerateVideoStartAndEndImage" in r.url
                    or "batchAsyncGenerateVideoText" in r.url
                )
            )
        else:
            api_matcher = lambda r: (
                r.request.method == "POST"
                and (
                    "batchAsyncGenerateVideoText" in r.url
                    or "batchAsyncGenerateVideoReferenceImages" in r.url
                )
            )

        submit_response_future: asyncio.Future = asyncio.get_running_loop().create_future()
        captured_urls: list[str] = []

        def _on_response(resp) -> None:
            if submit_response_future.done():
                return
            try:
                url = resp.url
            except Exception:
                return
            if any(kw in url for kw in ("batchAsyncGenerateVideo", "batchCheckAsyncVideo")):
                captured_urls.append(f"{resp.request.method} {url} [{resp.status}]")
            if api_matcher(resp):
                submit_response_future.set_result(resp)

        page.on("response", _on_response)
        try:
            await human_click(page, submit_btn)
            await human_delay(0.5, 1.0)
            await human_mouse_move(page)

            try:
                response = await asyncio.wait_for(submit_response_future, timeout=60)
                payload = await response.json()
                if "error" in payload:
                    compact_payload = json.dumps(payload, ensure_ascii=False)[:2000]
                    logger.info(f"{lp} 提交 API 返回 error，status={response.status}, payload={compact_payload}")
                return "submit", payload
            except asyncio.TimeoutError:
                if captured_urls:
                    logger.info(
                        f"{lp} expect_response 超时，"
                        f"期间收到的相关请求: {captured_urls}"
                    )
                else:
                    logger.info(
                        f"{lp} expect_response 超时，"
                        f"期间未收到任何 batchAsyncGenerate/batchCheck 请求"
                    )
        finally:
            try:
                page.remove_listener("response", _on_response)
            except Exception:
                pass

        raise RuntimeError(f"{lp} 未捕获到视频生成API响应，放弃等待")

    async def process_task(self, page, task_data: dict[str, Any], worker) -> dict[str, Any]:
        """
        单个视频生成任务的完整处理流程。

        流程：注入 Cookie → 检查账号健康 → 配置参数 → 上传图片 →
             填写提示词 → 提交生成 → 轮询状态 → 下载视频 → 归还 Cookie。

        参数:
            page: Playwright 页面对象（每个任务独立）
            task_data: 任务字典，包含 prompt、gen_type、图片数据等
            worker: WorkerContext 上下文

        返回:
            dict: 处理结果，包含 status、video_path、file_md5 等字段
        """
        prompt = task_data.get("prompt")
        variant_count = int(task_data.get("variant_count", 1))
        poll_timeout_ms = int(task_data.get("poll_timeout_ms", 4 * 60 * 1000))
        gen_type = int(task_data.get("gen_type", 1))
        is_frame_mode = gen_type == 0
        task_id = str(task_data.get("_id", "unknown"))
        log_prefix = f"[B{worker.worker_id}:C{worker.context_id}][任务:{task_id}]"
        _current_log_prefix.set(log_prefix)

        try:
            logger.info(f"{log_prefix} 正在访问 Flow 首页...")
            await page.goto(FLOW_HOME_URL, wait_until="domcontentloaded")
            # await wait_for_flow_home_ready(page, worker.worker_id)
            # await _log_browser_identity(page, log_prefix, self.user_agent)
            await human_delay(5, 7)
            await human_mouse_move(page)

            await self._prepare_video_project(page, worker, prompt, variant_count, task_data)

            media_name = None
            response_kind, response_payload = await self._submit_video_generation_humanized(page, worker.worker_id, is_frame_mode=is_frame_mode, log_prefix=log_prefix)

            try:
                logger.info(f"{log_prefix} 收到{response_kind}响应")
                if "result" in response_payload and "data" in response_payload["result"]:
                    media_items = response_payload["result"]["data"].get("media", [])
                else:
                    media_items = response_payload.get("media", [])

                if media_items:
                    primary_media_item = media_items[0]
                    media_name = primary_media_item.get("name")
                    logger.info(f"{log_prefix} Media name: {media_name}")
                else:
                    logger.info(f"{log_prefix} 响应结构: {list(response_payload.keys())}")
            except Exception as exc:
                logger.info(f"{log_prefix} 捕获media name失败: {exc}")

            if not media_name:
                compact_payload = json.dumps(response_payload, ensure_ascii=False)[:2000]
                logger.info(f"{log_prefix} 生成提交响应未返回 media，payload={compact_payload}")
                raise RuntimeError(f"无法从视频生成响应中获取media name: {compact_payload}")

            await human_mouse_move(page)
            await asyncio.sleep(2)

            logger.info(f"{log_prefix} 等待视频生成 (可能需要4-8分钟)...")
            final_status_payload = await self._wait_for_video_status(
                page=page,
                media_names={media_name},
                worker_id=worker.worker_id,
                timeout_ms=poll_timeout_ms,
                email=task_data.get("email"),
            )
            final_media_items = final_status_payload.get("media", [])
            final_media_by_name = {item["name"]: item for item in final_media_items if item.get("name")}
            primary_media = final_media_by_name.get(media_name, {"name": media_name})
            download_url = (
                f"https://labs.google/fx/api/trpc/media.getMediaUrlRedirect?name={primary_media['name']}"
            )

            cookies = await page.context.cookies()
            task_identifier = task_data.get("_id")

            video_filename = f"{task_identifier}.mp4"
            # 创建下载目录（放在video_processing目录下）
            script_dir = Path(__file__).resolve().parent.parent.parent
            demo_dir = script_dir / "video_processing" / "downloaded_videos"
            demo_dir.mkdir(parents=True, exist_ok=True)
            local_path = str(demo_dir / video_filename)

            is_downloaded, video_mime_type = await self._download_video_to_local(
                page,
                download_url,
                worker.worker_id,
                cookies,
                local_path,
            )
            if not is_downloaded:
                raise RuntimeError(f"流式下载视频失败, URL是: {download_url}")

            file_size_bytes = os.path.getsize(local_path)
            kb_size = file_size_bytes // 1024 if file_size_bytes >= 1024 else 1
            with open(local_path, "rb") as video_file:
                file_md5 = hashlib.md5(video_file.read()).hexdigest()

            logger.info(f"{log_prefix} 视频下载成功！大小: {kb_size} KB, MIME: {video_mime_type}, file_md5: {file_md5}")

            return {
                "local_video_path": str(local_path),
                "api_full_response": final_status_payload.get("api_full_response"),
                "file_md5": file_md5,
                "filesize": kb_size,
                "video_mime_type": video_mime_type,
            }
        except Exception:
            raise
        finally:
            task_email = task_data.get("email")
            if task_email:
                try:
                    release_cookie(task_email)
                    logger.info(f"{log_prefix} 账号 {task_email} 并发槽位已归还")
                except Exception as rel_err:
                    logger.warning(f"{log_prefix} 归还槽位失败（忽略）: {rel_err}")
