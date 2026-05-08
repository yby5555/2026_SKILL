from __future__ import annotations

import asyncio
import base64
import json
import logging
import logging.handlers
import mimetypes
import os
import random
import re
import sys
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import aiofiles
import httpx
import requests

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.append(str(_ROOT))

_ACCOUNT_MGR = _ROOT / "account_mgr"
if str(_ACCOUNT_MGR) not in sys.path:
    sys.path.insert(0, str(_ACCOUNT_MGR))

from driver_base import MultiBrowserScraperBase

from account_checker import (
    LOGIN_EXPIRED_PATTERN,
    MIN_CREDITS_THRESHOLD,
    _click_avatar_and_get_credits,
)
from account_mgr.redis_utils import get_next_cookie, release_cookie, remove_from_pool

_LOG_DIR = _ROOT / "flow" / "log"
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
VIDEO_ASPECT_RATIO_LABEL = "9:16"
VIDEO_MODEL_LABEL = "Veo 3.1 - Lite"

# ── 青果海外代理配置 ──────────────────────────────────────────────
QG_OVERSEAS_PROXY_API_URL = "https://overseas.proxy.qg.net/get"
QG_OVERSEAS_PROXY_KEY = "9BLWYKGO"
QG_OVERSEAS_PROXY_PASSWORD = "7854763CD921"
QG_OVERSEAS_PROXY_AREA = "990400"


def get_overseas_proxy(*, timeout: int = 10) -> str | None:
    """从青果网络获取海外代理，并返回带账号密码的完整代理 URL。

    逻辑/功能:
        向青果代理 API 发送请求获取一个海外代理 IP，
        拼接为 http://<key>:<password>@<server> 格式返回。
        任何异常均返回 None 并记录警告日志。

    入参:
        timeout: HTTP 请求超时秒数，默认 10。

    出参:
        str | None: 完整代理 URL；获取失败返回 None。
    """
    try:
        response = requests.get(
            QG_OVERSEAS_PROXY_API_URL,
            params={"key": QG_OVERSEAS_PROXY_KEY, "area": QG_OVERSEAS_PROXY_AREA, "num": "1"},
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
        proxy_list = payload.get("data") or []
        if not proxy_list:
            logger.warning("[代理] 青果代理 API 返回空列表")
            return None
        proxy_addr = str(proxy_list[0].get("server") or "").strip()
        if not proxy_addr:
            logger.warning("[代理] 青果代理 API 返回的 server 地址为空")
            return None
        proxy_url = f"http://{QG_OVERSEAS_PROXY_KEY}:{QG_OVERSEAS_PROXY_PASSWORD}@{proxy_addr}"
        logger.info(f"[代理] 获取海外代理成功: {proxy_addr}")
        return proxy_url
    except Exception as exc:
        logger.warning(f"[代理] 获取海外代理失败: {exc}")
        return None


async def human_delay(min_sec: float = 0.5, max_sec: float = 2.0) -> None:
    """模拟人类操作的随机等待。

    逻辑/功能:
        在指定的最小和最大秒数之间随机休眠，用于在自动化操作中插入自然延迟，
        降低被反自动化系统检测的风险。

    入参:
        min_sec: 最小等待秒数，默认 0.5。
        max_sec: 最大等待秒数，默认 2.0。

    出参:
        None
    """
    await asyncio.sleep(random.uniform(min_sec, max_sec))


def normalize_prompt_text(text: str) -> str:
    """将多行文本规范化为单行纯文本。

    逻辑/功能:
        将所有换行符替换为空格，再将连续空白字符合并为单个空格，最后去除首尾空白。
        用于统一处理用户输入的提示词和页面抓取的文本。

    入参:
        text: 待规范化的原始文本。

    出参:
        str: 规范化后的单行纯文本。
    """
    text = re.sub(r"\r\n?|\n", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


async def get_textbox_content(textbox_loc) -> str:
    """读取文本输入框的当前内容。

    逻辑/功能:
        通过 JavaScript 评估同时读取元素的 value、innerText、textContent 三个属性，
        按优先级返回第一个非空的规范化文本。适配不同类型的可编辑元素。

    入参:
        textbox_loc: Playwright Locator，指向目标输入框元素。

    出参:
        str: 输入框当前文本内容；若所有属性均为空则返回空字符串。
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
    """向输入框写入文本并校验写入是否成功。

    逻辑/功能:
        模拟人类操作流程：移动鼠标到输入框 → 点击聚焦 → 全选 → 插入文本。
        写入后读取实际内容进行校验，若不一致则重试，超过最大次数后抛出异常。

    入参:
        page: Playwright Page 对象。
        textbox_loc: 目标输入框的 Locator。
        text: 要写入的目标文本。
        worker_id: 当前 worker 标识，用于日志。
        max_attempts: 最大重试次数，默认 2。

    出参:
        None

    异常:
        RuntimeError: 多次写入后校验仍不通过。
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
            f"[Worker {worker_id}] 第 {attempt} 次写入校验失败，"
            f"当前文本: {current_text!r}，目标文本: {text!r}"
        )
        await human_delay(0.5, 0.9)

    raise RuntimeError("输入框写入后校验失败，未继续点击创建")


async def human_mouse_move(page) -> None:
    """模拟人类随机鼠标移动。

    逻辑/功能:
        在页面视口范围内随机移动鼠标 2-5 次，每次移动带有随机步数和间隔，
        用于模拟真实用户的鼠标活动轨迹。

    入参:
        page: Playwright Page 对象。

    出参:
        None
    """
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
    """模拟人类随机滚动页面。

    逻辑/功能:
        向下随机滚动 100-400 像素，30% 概率再向上回滚一小段距离，
        模拟真实用户的浏览行为。

    入参:
        page: Playwright Page 对象。

    出参:
        None
    """
    await page.mouse.wheel(0, random.randint(100, 400))
    await human_delay(0.2, 0.6)
    if random.random() < 0.3:
        await page.mouse.wheel(0, -random.randint(50, 200))
        await human_delay(0.1, 0.3)


async def human_click(page, locator_or_element, timeout: int = 5000) -> None:
    """模拟人类点击操作。

    逻辑/功能:
        先获取元素的边界框，将鼠标移动到元素中心，然后执行 mousedown/mouseup。
        如果获取边界框失败，则回退到 Playwright 原生 click 方法。

    入参:
        page: Playwright Page 对象。
        locator_or_element: 目标元素的 Locator 或 ElementHandle。
        timeout: 回退点击的超时毫秒数，默认 5000。

    出参:
        None
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


async def get_creation_mode_button(page):
    """定位底部创建按钮旁的模式配置下拉按钮。

    逻辑/功能:
        遍历页面中所有 aria-haspopup="menu" 的按钮，根据与「创建」按钮的位置关系、
        按钮文本内容等多维度评分，选出最可能的配置菜单触发按钮。
        会过滤掉导航、帮助、搜索等无关按钮。

    入参:
        page: Playwright Page 对象。

    出参:
        Locator | None: 找到的配置按钮 Locator；未找到则返回 None。
    """
    await asyncio.sleep(1)

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
    """查找当前可见的创建配置菜单。

    逻辑/功能:
        遍历页面上所有 role="menu" 的元素，找出包含 tablist（即视频/图片模式切换组）
        且当前可见的菜单元素。

    入参:
        page: Playwright Page 对象。

    出参:
        Locator | None: 找到的配置菜单 Locator；未找到则返回 None。
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
    """打开底部创建配置菜单并返回按钮和菜单。

    逻辑/功能:
        先定位配置按钮（最多重试 5 次），如果按钮未展开则点击展开，
        然后查找并返回配置菜单。若菜单未出现会重试一次点击。

    入参:
        page: Playwright Page 对象。
        worker_id: 当前 worker 标识，用于日志。

    出参:
        tuple[Locator, Locator]: (配置按钮, 配置菜单) 的元组。

    异常:
        RuntimeError: 配置按钮找不到或菜单无法展开。
    """
    target_btn = None
    for attempt in range(5):
        target_btn = await get_creation_mode_button(page)
        if target_btn:
            break
        logger.info(f"[Worker {worker_id}] 未找到底部配置按钮，重试中... ({attempt + 1}/5)")
        await asyncio.sleep(2)

    if not target_btn:
        raise RuntimeError("经过重试后仍找不到底部配置按钮，页面可能假死或网络中断")

    if await target_btn.get_attribute("aria-expanded") != "true":
        await human_click(page, target_btn)
        await human_delay(0.8, 1.5)

    menu = await get_creation_settings_menu(page)
    if menu:
        return target_btn, menu

    logger.info(f"[Worker {worker_id}] 菜单未出现，重试打开")
    await human_click(page, target_btn)
    await human_delay(1.5, 2.0)

    menu = await get_creation_settings_menu(page)
    if not menu:
        raise RuntimeError("底部配置菜单未成功展开")
    return target_btn, menu


async def select_tab_in_group(page, menu, group_index: int, label_pattern, worker_id: Any, description: str) -> None:
    """在配置菜单中选择指定 tab 组的指定选项。

    逻辑/功能:
        定位菜单中第 group_index 个 tablist，找到匹配 label_pattern 的 tab，
        若未选中则点击选中。

    入参:
        page: Playwright Page 对象。
        menu: 配置菜单 Locator。
        group_index: tab 组索引（0 起始）。
        label_pattern: tab 文本匹配模式（字符串或正则）。
        worker_id: 当前 worker 标识，用于日志。
        description: 操作描述，用于日志和异常信息。

    出参:
        None

    异常:
        RuntimeError: 指定 tab 未找到。
    """
    tablist = menu.locator('[role="tablist"]').nth(group_index)
    target_tab = tablist.locator('[role="tab"]').filter(has_text=label_pattern).first
    if await target_tab.count() == 0:
        raise RuntimeError(f"第 {group_index + 1} 组未找到 {description}")

    if await target_tab.get_attribute("aria-selected") == "true":
        logger.info(f"[Worker {worker_id}] {description} 已经选中")
        return

    await human_click(page, target_tab)
    await human_delay(0.6, 1.1)


async def ensure_video_model(page, menu, worker_id: Any, target_model: str) -> None:
    """确保视频模型已切换到目标值。

    逻辑/功能:
        在配置菜单中找到视频模型下拉按钮，读取当前已选模型，
        若不是目标模型则点击切换。

    入参:
        page: Playwright Page 对象。
        menu: 配置菜单 Locator。
        worker_id: 当前 worker 标识，用于日志。
        target_model: 目标模型名称，如 "Veo 3.1 - Lite"。

    出参:
        None

    异常:
        RuntimeError: 未找到模型按钮或目标模型选项。
    """
    model_btn = menu.locator('button[aria-haspopup="menu"]').filter(
        has_text=re.compile(r"Veo 3\.1", re.IGNORECASE)
    ).first
    if await model_btn.count() == 0:
        raise RuntimeError("未找到视频模型下拉按钮")

    current_model = normalize_prompt_text(await model_btn.inner_text())
    if target_model.lower() in current_model.lower():
        logger.info(f"[Worker {worker_id}] 视频模型已经是 {target_model}")
        return

    logger.info(f"[Worker {worker_id}] 正在切换视频模型到 {target_model}")
    await human_click(page, model_btn)
    await human_delay(0.5, 0.9)

    target_option = page.locator('[role="menuitem"]').filter(has_text=target_model).first
    if await target_option.count() == 0:
        raise RuntimeError(f"模型菜单中未找到 {target_model}")

    await human_click(page, target_option)
    await human_delay(0.6, 1.0)


async def ensure_video_mode(page, worker_id: Any, variant_count: int) -> None:
    """配置视频生成的全部参数并校验。

    逻辑/功能:
        依次设置视频模式、来源类型、宽高比、生成数量和视频模型，
        最后重新打开菜单逐项校验所有配置是否正确。
        校验失败时抛出异常。

    入参:
        page: Playwright Page 对象。
        worker_id: 当前 worker 标识，用于日志。
        variant_count: 视频生成数量 (1-4)。

    出参:
        None

    异常:
        RuntimeError: 任何配置项设置失败或最终校验不通过。
    """
    logger.info(
        f"[Worker {worker_id}] 正在设置视频配置 "
        f"(素材, {VIDEO_ASPECT_RATIO_LABEL}, x{variant_count}, {VIDEO_MODEL_LABEL})..."
    )

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
            re.compile(rf"{re.escape(VIDEO_SOURCE_LABEL)}|chrome_extension", re.IGNORECASE),
            worker_id,
            f"来源 {VIDEO_SOURCE_LABEL}",
        )

        await select_tab_in_group(
            page,
            menu,
            2,
            re.compile(rf"{re.escape(VIDEO_ASPECT_RATIO_LABEL)}|crop_9_16", re.IGNORECASE),
            worker_id,
            f"比例 {VIDEO_ASPECT_RATIO_LABEL}",
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

        await ensure_video_model(page, menu, worker_id, VIDEO_MODEL_LABEL)

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
        if VIDEO_SOURCE_LABEL not in selected_source_text and "chrome_extension" not in selected_source_text.lower():
            raise RuntimeError(f"最终校验失败，来源不是 {VIDEO_SOURCE_LABEL}: {selected_source_text}")
        if VIDEO_ASPECT_RATIO_LABEL not in selected_ratio_text and "crop_9_16" not in selected_ratio_text.lower():
            raise RuntimeError(f"最终校验失败，比例不是 {VIDEO_ASPECT_RATIO_LABEL}: {selected_ratio_text}")
        selected_variant_normalized = selected_variant_text.lower()
        expected_variant_values = {target_variant.lower(), f"{variant_count}x"}
        if selected_variant_normalized not in expected_variant_values:
            raise RuntimeError(f"最终校验失败，生成数量不是 {target_variant}: {selected_variant_text}")
        if VIDEO_MODEL_LABEL.lower() not in selected_model_text.lower():
            raise RuntimeError(f"最终校验失败，模型不是 {VIDEO_MODEL_LABEL}: {selected_model_text}")

        await page.keyboard.press("Escape")
        await human_delay(0.3, 0.6)
        await human_delay(0.8, 1.5)
    except Exception as exc:
        logger.info(f"[Worker {worker_id}] 设置模式错误: {exc}")
        try:
            await page.keyboard.press("Escape")
        except Exception:
            pass
        raise RuntimeError(f"设置视频模式失败: {exc}")


def _mime_to_extension(mime_type: str) -> str:
    """将 MIME 类型转换为文件扩展名。

    逻辑/功能:
        调用 mimetypes.guess_extension 推测扩展名，无法推测时默认返回 ".png"。

    入参:
        mime_type: MIME 类型字符串，如 "image/jpeg"。

    出参:
        str: 文件扩展名，如 ".jpg"。
    """
    guessed_extension = mimetypes.guess_extension(mime_type or "")
    return guessed_extension or ".png"


def _derive_image_upload_meta(image_url: str | None, image_base64: str | None) -> tuple[str, str, str | None]:
    """从 URL 或 Base64 输入推导图片上传所需的元数据。

    逻辑/功能:
        根据 image_url 提取文件名和 MIME 类型；如果有 Base64 前缀 (data:...;base64)
        则解析出 MIME 并剥离前缀。确保返回的文件名带有扩展名。

    入参:
        image_url: 图片 URL，可为 None。
        image_base64: Base64 编码的图片数据，可带 data: 前缀，可为 None。

    出参:
        tuple[str, str, str | None]: (文件名, MIME类型, 规范化后的Base64数据或None)。
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
    """解析参考图片重复上传次数。

    逻辑/功能:
        依次检查 reference_image_count、image_count、reference_count 字段，
        返回第一个有效的正整数，最小为 1。用于单张图片重复上传 N 次的场景。

    入参:
        task_data: 任务数据字典。

    出参:
        int: 重复次数，默认 1。
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
    """计算任务要求的参考图片总数。

    逻辑/功能:
        优先检查 image_url_list 和 image_base64_list 的长度，
        其次检查单张 image_url/image_base64 + 重复次数，
        均无则返回 0 表示无参考图。

    入参:
        task_data: 任务数据字典。

    出参:
        int: 期望的参考图片总数。
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
    """从图片 src URL 中提取 Google media name。

    逻辑/功能:
        解析 URL 查询参数中的 name 字段，用于关联上传响应与输入框附件。

    入参:
        src: 图片元素的 src 属性值，可为 None。

    出参:
        str | None: media name 字符串；解析失败返回 None。
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
    """查找当前可见的图片创建/上传对话框。

    逻辑/功能:
        从后往前遍历所有 role="dialog" 元素，找到包含「上传图片」等关键词
        且当前可见的对话框。

    入参:
        page: Playwright Page 对象。

    出参:
        Locator | None: 找到的对话框 Locator；未找到返回 None。
    """
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
    """定位底部「创建」区域的图片添加按钮。

    逻辑/功能:
        遍历所有按钮，根据文本包含「创建」或 "add_2"、aria-haspopup="dialog"、
        与提交按钮/输入框的位置关系等多维度评分，返回最佳匹配的按钮。

    入参:
        page: Playwright Page 对象。

    出参:
        Locator | None: 找到的图片添加按钮 Locator；未找到返回 None。
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
    """点击图片添加按钮打开图片选择对话框。

    逻辑/功能:
        定位并点击底部图片创建按钮，等待对话框出现，最多重试 4 次。

    入参:
        page: Playwright Page 对象。
        worker_id: 当前 worker 标识，用于日志。

    出参:
        Locator: 已打开的图片选择对话框。

    异常:
        RuntimeError: 无法打开图片选择面板。
    """
    for attempt in range(4):
        create_btn = await _find_bottom_create_dialog_button(page)
        await human_delay(1, 2)
        await human_click(page, create_btn)
        await human_delay(1, 2)
        dialog = await _get_visible_create_image_dialog(page)
        if dialog:
            logger.info(f"[Worker {worker_id}] 已打开图片选择面板")
            return dialog

    raise RuntimeError("未找到底部创建按钮，无法打开图片选择面板")


async def _list_prompt_attachment_media_names(page) -> list[str]:
    """获取输入框附件区当前挂载的所有图片 media name。

    逻辑/功能:
        查找输入框附件区中所有带 media.getMediaUrlRedirect 的 img 元素，
        从 src URL 中解析出各个 media name。

    入参:
        page: Playwright Page 对象。

    出参:
        list[str]: 当前已挂载的 media name 列表。
    """
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
    """等待图片上传后出现在输入框附件区。

    逻辑/功能:
        轮询检查输入框附件区是否出现了上传响应中的 media name，
        或者是否出现了新的附件（相对于上传前的快照）。超时则抛出异常。

    入参:
        page: Playwright Page 对象。
        worker_id: 当前 worker 标识，用于日志。
        response_media_name: 上传接口返回的 media name，可为 None。
        timeout_ms: 超时毫秒数，默认 30000。
        previous_media_names: 上传前已存在的 media name 集合。

    出参:
        str: 实际挂载到输入框的 media name。

    异常:
        RuntimeError: 等待超时。
    """
    previous_media_names = previous_media_names or set()
    deadline = monotonic() + timeout_ms / 1000

    while monotonic() < deadline:
        current_media_names = await _list_prompt_attachment_media_names(page)
        current_name_set = set(current_media_names)

        if response_media_name and response_media_name in current_name_set:
            logger.info(f"[Worker {worker_id}] 已检测到底部附件挂载: {response_media_name}")
            return response_media_name

        new_media_names = [name for name in current_media_names if name not in previous_media_names]
        if new_media_names:
            actual_media_name = new_media_names[-1]
            logger.info(f"[Worker {worker_id}] 已检测到底部新增附件: {actual_media_name}")
            return actual_media_name

        await human_delay(0.4, 0.8)

    raise RuntimeError(
        f"等待图片出现在输入框附件区超时: response={response_media_name}, "
        f"before={sorted(previous_media_names)}, after={await _list_prompt_attachment_media_names(page)}"
    )


async def upload_reference_image_via_picker(
    page,
    worker_id: Any,
    file_buffer: bytes,
    file_name: str,
    mime_type: str,
) -> str:
    """通过文件选择器上传参考图片并等待挂载到输入框。

    逻辑/功能:
        1. 打开图片选择对话框，点击「上传图片」。
        2. 先处理可能弹出的法律声明（「我同意」），避免弹窗与 file_chooser 竞态。
        3. 注册 file_chooser 监听，点击上传触发文件选择器。
        4. 通过 file_chooser 设置文件，等待 uploadImage API 响应。
        5. 等待图片出现在输入框附件区，关闭对话框。

    入参:
        page: Playwright Page 对象。
        worker_id: 当前 worker 标识，用于日志。
        file_buffer: 图片文件的二进制内容。
        file_name: 图片文件名。
        mime_type: 图片 MIME 类型。

    出参:
        str: 已挂载到输入框的 media name。

    异常:
        RuntimeError: 任何上传步骤失败。
    """
    dialog = await _open_create_image_dialog(page, worker_id)
    previous_attachment_media_names = set(await _list_prompt_attachment_media_names(page))
    upload_option = dialog.locator('div:has-text("上传图片"), li:has-text("上传图片"), button:has-text("上传图片")').last
    if await upload_option.count() == 0:
        raise RuntimeError("图片选择面板打开后未找到“上传图片”入口")

    logger.info(f"[Worker {worker_id}] 正在上传参考图片: {file_name}")

    # ── 先处理可能的法律声明弹窗，避免与 expect_file_chooser 竞态 ──
    try:
        await human_click(page, upload_option)
        await human_delay(0.5, 1.0)

        agree_btn = page.locator('button:has-text("我同意"), button:has-text("I Agree"), button:has-text("同意")').first
        if await agree_btn.is_visible(timeout=2000):
            logger.info(f"[Worker {worker_id}] 检测到法律声明，正在处理...")
            await human_click(page, agree_btn)
            await human_delay(1.5, 2.5)
            # 法律声明处理后需要重新打开面板
            dialog = await _open_create_image_dialog(page, worker_id)
            upload_option = dialog.locator(
                'div:has-text("上传图片"), li:has-text("上传图片"), button:has-text("上传图片")'
            ).last
    except Exception as exc:
        logger.info(f"[Worker {worker_id}] 法律声明预检查: {exc}")

    # ── 法律声明已处理（或不存在），现在安全地注册 file_chooser 监听 ──
    async with page.expect_file_chooser(timeout=15000) as fc_info:
        await human_click(page, upload_option)

    file_chooser = await fc_info.value
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
    logger.info(f"[Worker {worker_id}] 图片已成功上传 (文件选择器)")

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

    logger.info(f"[Worker {worker_id}] 已将图片挂载到输入框: {attached_media_name}")
    return attached_media_name


class GoogleFlowVideoScraperV2(MultiBrowserScraperBase):
    """面向 Redis 消费者的精简版 Google Flow 视频生成抓取器。"""

    def normalize_task(self, task_data: dict[str, Any]) -> dict[str, Any]:
        """任务数据预处理：确保每个任务都携带有效的账号信息和代理。

        逻辑/功能:
            拷贝原始 task_data，检查 email 和 cookies 字段，
            若缺失则从 Redis Cookie Pool 中获取一组可用的账号并填充。
            同时为任务获取一个独立的海外代理 IP 注入 proxy 字段，
            代理获取失败时降级为无代理运行。

        入参:
            task_data: 原始任务数据字典。

        出参:
            dict[str, Any]: 处理后的任务数据副本，包含 email、cookies 和 proxy。

        异常:
            RuntimeError: Redis Cookie Pool 为空。
        """
        task_copy = dict(task_data)
        if not task_copy.get("email") or not task_copy.get("cookies"):
            next_result = get_next_cookie()
            if not next_result:
                raise RuntimeError("Redis Cookie Pool 为空，无法启动任务，请先运行 login_scheduler.py")
            task_copy["email"], task_copy["cookies"] = next_result

        # 为每个任务获取独立的海外代理
        if not task_copy.get("proxy"):
            proxy_url = get_overseas_proxy()
            if proxy_url:
                task_copy["proxy"] = proxy_url
                logger.info(f"[normalize_task] 任务 {task_copy.get('_id', '?')} 已注入代理")
            else:
                logger.warning(f"[normalize_task] 任务 {task_copy.get('_id', '?')} 代理获取失败，将无代理运行")

        return task_copy

    async def _ensure_account_healthy(self, page, task_data: dict[str, Any], worker) -> None:
        """检查并保障当前账号可用，必要时自动切换。

        逻辑/功能:
            检查登录态和额度，若登录失效或额度不足，自动从 Redis 获取新账号并释放旧槽位，
            更新 context cookie 并重新导航。最多尝试切换 3 次。

        入参:
            page: Playwright Page 对象。
            task_data: 任务数据字典，会被就地修改 email/cookies 字段。
            worker: BrowserWorker 实例，用于日志。

        出参:
            None

        异常:
            RuntimeError: 账号切换失败或无可用账号。
        """
        max_switches = 3
        current_email = task_data.get("email")

        async def _check_current_page() -> str:
            """检测当前页面的账号状态。

            逻辑/功能:
                检查当前 URL 是否为登录页，若是则将账号从池中移除；
                否则给头像组件获取 AI 点数，检查是否低于最低阈值。

            出参:
                str: "ok" | "login_expired" | "no_credits"。
            """
            url = page.url
            if LOGIN_EXPIRED_PATTERN.search(url) or "/signin" in url:
                logger.info(f"[Worker {worker.worker_id}] 账号 {current_email} 登录态失效")
                if current_email:
                    remove_from_pool(current_email)
                return "login_expired"

            credits = await _click_avatar_and_get_credits(page)
            if credits is None:
                logger.info(f"[Worker {worker.worker_id}] 账号 {current_email} AI 点数读取失败，跳过额度检测继续执行")
                return "ok"

            logger.info(f"[Worker {worker.worker_id}] 账号 {current_email} AI 点数: {credits}")
            if credits < MIN_CREDITS_THRESHOLD:
                logger.info(f"[Worker {worker.worker_id}] 账号 {current_email} 额度不足({credits})")
                if current_email:
                    remove_from_pool(current_email)
                return "no_credits"
            return "ok"

        for attempt in range(max_switches + 1):
            status = await _check_current_page()
            if status == "ok":
                logger.info(f"[Worker {worker.worker_id}] 账号 {current_email} 检测通过，继续执行")
                return

            if attempt >= max_switches:
                raise RuntimeError(f"[Worker {worker.worker_id}] 已切换 {max_switches} 次 Cookie 仍无可用账号，任务终止")

            # 释放旧账号的并发槽位，防止槽位泄漏
            if current_email:
                try:
                    release_cookie(current_email)
                    logger.info(f"[Worker {worker.worker_id}] 已释放旧账号 {current_email} 的并发槽位")
                except Exception as rel_err:
                    logger.warning(f"[Worker {worker.worker_id}] 释放旧账号槽位失败（忽略）: {rel_err}")

            next_result = get_next_cookie()
            if not next_result:
                raise RuntimeError("Redis Cookie Pool 已空，无法切换账号")

            new_email, new_cookies = next_result
            logger.info(f"[Worker {worker.worker_id}] 切换到新账号: {new_email}")

            await page.context.clear_cookies()
            await page.context.add_cookies(new_cookies)

            task_data["email"] = new_email
            task_data["cookies"] = new_cookies
            current_email = new_email

            logger.info(f"[Worker {worker.worker_id}] 重新加载 Flow 首页...")
            await page.goto(FLOW_HOME_URL, wait_until="domcontentloaded")
            await asyncio.sleep(2)

    async def _wait_for_video_status(
        self,
        page,
        media_names: set[str],
        worker_id: Any,
        timeout_ms: int,
        email: str | None,
    ) -> dict[str, Any]:
        """轮询等待视频生成完成。

        逻辑/功能:
            在超时时间内循环监听 batchCheckAsyncVideoGenerationStatus API 响应，
            解析响应中的 media 状态，直到所有 media 都成功或失败。
            轮询间隙随机模拟鼠标和滚动行为。

        入参:
            page: Playwright Page 对象。
            media_names: 待监控的 media name 集合。
            worker_id: 当前 worker 标识，用于日志。
            timeout_ms: 总超时毫秒数。
            email: 当前账号邮箱，用于日志。

        出参:
            dict[str, Any]: 包含 "media" (已完成的媒体列表) 和 "api_full_response" (原始响应JSON)。

        异常:
            RuntimeError: 视频生成失败。
            TimeoutError: 等待超时。
        """
        deadline = monotonic() + timeout_ms / 1000
        last_statuses: dict[str, str] = {}

        while monotonic() < deadline:
            remaining_ms = max(15000, int((deadline - monotonic()) * 1000))
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
            logger.info(f"[Worker {email}-{worker_id}] 当前视频状态: {last_statuses}")

            if any(status == "MEDIA_GENERATION_STATUS_FAILED" for status in last_statuses.values()):
                raise RuntimeError(f"视频生成失败: {last_statuses}")

            if media_names.issubset(last_statuses.keys()) and all(
                status == "MEDIA_GENERATION_STATUS_SUCCESSFUL" for status in last_statuses.values()
            ):
                # 仅在最终成功时才序列化完整响应，避免每轮循环都生成大字符串
                full_api_response = json.dumps(payload, ensure_ascii=False)
                return {"media": tracked_items, "api_full_response": full_api_response}

            if random.random() < 0.3:
                await human_mouse_move(page)
                await human_scroll(page)

        raise TimeoutError(f"等待视频生成超时，最后一次状态: {last_statuses}")

    async def _download_video_to_local(self, page, download_url: str, worker_id: Any, cookies: list, save_path: str) -> bool:
        """将生成的视频流式下载到本地文件。

        逻辑/功能:
            使用 httpx 异步流式下载，带上页面 cookie 和浏览器 UA，
            通过 aiofiles 异步写入文件。下载后检查文件大小是否合理。

        入参:
            page: Playwright Page 对象，用于获取 UA。
            download_url: 视频下载 URL。
            worker_id: 当前 worker 标识，用于日志。
            cookies: 页面 cookie 列表，用于认证。
            save_path: 本地保存路径。

        出参:
            bool: 下载成功返回 True，失败返回 False。
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
            async with httpx.AsyncClient(timeout=180.0, verify=False, follow_redirects=True) as client:
                async with client.stream("GET", download_url, cookies=cookie_dict, headers=headers) as response:
                    response.raise_for_status()
                    async with aiofiles.open(save_path, "wb") as file_obj:
                        async for chunk in response.aiter_bytes(chunk_size=8192):
                            if chunk:
                                await file_obj.write(chunk)

            if os.path.exists(save_path) and os.path.getsize(save_path) > 1024:
                kb_size = os.path.getsize(save_path) // 1024
                logger.info(f"[Worker {worker_id}] 视频下载成功！大小: {kb_size} KB, URL: {download_url[:50]}...")
                return True

            logger.error(f"[Worker {worker_id}] 下载文件似乎太小或不存在")
            return False
        except Exception as exc:
            logger.error(f"[Worker {worker_id}] 下载视频到本地失败: {exc}")
            return False

    async def _prepare_video_project(self, page, worker, prompt: str, variant_count: int, task_data: dict[str, Any]) -> None:
        """准备视频生成项目：配置参数、上传参考图、填写提示词。

        逻辑/功能:
            1. 关闭 Cookie 弹窗。
            2. 检查账号健康状态。
            3. 点击「新建项目」创建工作区。
            4. 配置视频模式、模型、宽高比、生成数量等。
            5. 准备并上传参考图片（支持 URL/Base64 单张和多张）。
            6. 上传完成后释放图片缓冲区和 task_data 中的大字段。
            7. 填写提示词。

        入参:
            page: Playwright Page 对象。
            worker: BrowserWorker 实例。
            prompt: 视频生成提示词。
            variant_count: 生成数量 (1-4)。
            task_data: 任务数据字典（含参考图、账号等）。

        出参:
            None

        异常:
            RuntimeError: 任何准备步骤失败。
        """
        try:
            cookie_btn = page.locator(
                '.glue-cookie-notification-bar button:has-text("Got it"), '
                '.glue-cookie-notification-bar button:has-text("同意"), '
                '.glue-cookie-notification-bar button:has-text("Accept"), '
                ".glue-cookie-notification-bar button"
            ).first
            if await cookie_btn.is_visible(timeout=2000):
                logger.info(f"[Worker {worker.worker_id}] 发现 Cookie 弹窗，正在关闭...")
                await human_click(page, cookie_btn)
                await human_delay(1.0, 2.0)
        except Exception:
            pass

        await self._ensure_account_healthy(page, task_data, worker)

        logger.info(f"[Worker {worker.worker_id}] 等待并点击新建项目...")
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

        await ensure_video_mode(page, worker.worker_id, variant_count)

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
                logger.info(f"[Worker {worker.worker_id}] 正在从 URL 下载第 {idx}/{len(image_urls)} 张参考图片: {current_image_url}")
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
                    logger.error(f"[Worker {worker.worker_id}] 从 URL 下载第 {idx} 张图片失败: {exc}")
                    preparation_errors.append(f"第 {idx} 张 URL 图片下载失败: {exc}")
        elif image_base64_list:
            total_base64_count = len(image_base64_list)
            for idx, current_image_base64 in enumerate(image_base64_list, start=1):
                if not current_image_base64:
                    preparation_errors.append(f"第 {idx} 张 Base64 图片为空")
                    continue
                file_name, mime_type, normalized_image_base64 = _derive_image_upload_meta(None, current_image_base64)
                logger.info(f"[Worker {worker.worker_id}] 正在处理第 {idx}/{total_base64_count} 张 Base64 参考图片")
                try:
                    file_buffer = base64.b64decode(normalized_image_base64)
                    if not file_buffer:
                        raise ValueError("解码结果为空")
                    upload_payloads.append((file_buffer, file_name, mime_type))
                except Exception as exc:
                    logger.error(f"[Worker {worker.worker_id}] 第 {idx} 张 Base64 解码图片失败: {exc}")
                    preparation_errors.append(f"第 {idx} 张 Base64 解码失败: {exc}")
        else:
            file_name, mime_type, normalized_image_base64 = _derive_image_upload_meta(image_url, image_base64)
            file_buffer = None

            if image_url:
                logger.info(f"[Worker {worker.worker_id}] 正在从 URL 下载参考图片: {image_url}")
                try:
                    async with httpx.AsyncClient(verify=False, timeout=30.0) as client:
                        resp = await client.get(image_url)
                        resp.raise_for_status()
                        file_buffer = resp.content
                        content_type = (resp.headers.get("content-type") or "").split(";")[0].strip()
                        if content_type.startswith("image/"):
                            mime_type = content_type
                except Exception as exc:
                    logger.error(f"[Worker {worker.worker_id}] 从 URL 下载图片失败: {exc}")
                    preparation_errors.append(f"参考图 URL 下载失败: {exc}")
            elif image_base64:
                logger.info(f"[Worker {worker.worker_id}] 正在处理 Base64 格式的参考图片")
                try:
                    file_buffer = base64.b64decode(normalized_image_base64)
                    if not file_buffer:
                        raise ValueError("解码结果为空")
                except Exception as exc:
                    logger.error(f"[Worker {worker.worker_id}] Base64 解码图片失败: {exc}")
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
                total_upload_count = len(upload_payloads)
                for upload_index, (file_buffer, current_file_name, current_mime_type) in enumerate(upload_payloads, start=1):
                    logger.info(
                        f"[Worker {worker.worker_id}] 正在上传第 {upload_index}/{total_upload_count} 张参考图"
                    )
                    await upload_reference_image_via_picker(
                        page=page,
                        worker_id=worker.worker_id,
                        file_buffer=file_buffer,
                        file_name=current_file_name,
                        mime_type=current_mime_type,
                    )
            except Exception as exc:
                logger.error(f"[Worker {worker.worker_id}] 按图片面板路径上传参考图失败: {exc}")
                raise
            finally:
                # 上传完成后立即释放图片缓冲区，避免 MB 级数据在后续 4-8 分钟轮询期间驻留内存
                upload_payloads.clear()

        # 清理 task_data 中的大体积图片字段，后续轮询阶段不再需要
        for _img_key in ("image_base64", "image_base64_list", "image_url_list"):
            task_data.pop(_img_key, None)

        input_prompt = normalize_prompt_text(prompt)
        logger.info(f"[Worker {worker.worker_id}] 等待输入框并输入提示词: {input_prompt[:10]}")
        await page.wait_for_selector('[role="textbox"]', state="visible", timeout=15000)
        await human_delay(1.5, 3.0)

        textbox_loc = page.locator('[role="textbox"]')
        await fill_textbox_with_validation(page, textbox_loc, input_prompt, worker.worker_id)
        await human_delay(1.0, 2.5)

    async def _submit_video_generation_humanized(self, page, worker_id: Any) -> tuple[str, dict[str, Any]]:
        """点击提交按钮并捕获视频生成响应。

        逻辑/功能:
            定位并点击「创建」提交按钮，监听 batchAsyncGenerateVideo 系列 API 响应。
            如果首次监听失败，会回退到监听状态查询响应作为备用。

        入参:
            page: Playwright Page 对象。
            worker_id: 当前 worker 标识，用于日志。

        出参:
            tuple[str, dict[str, Any]]: (响应类型, 响应JSON)。
                响应类型为 "submit" 或 "poll"。

        异常:
            RuntimeError: 提交按钮未找到或两次响应监听均失败。
        """
        logger.info(f"[Worker {worker_id}] 等待提交按钮出现...")
        await human_delay(1.5, 3.0)

        submit_btn = page.locator("button").filter(
            has_text=re.compile(r"arrow_forward", re.IGNORECASE)
        ).filter(has_text="创建").first
        if not await submit_btn.is_visible(timeout=2500):
            submit_btn = page.locator('button:has-text("创建")').last

        if not await submit_btn.is_visible(timeout=5000):
            raise RuntimeError("超时未找到真实可点击的创建提交按钮")

        try:
            async with page.expect_response(
                lambda r: (
                    r.request.method == "POST"
                    and (
                        "batchAsyncGenerateVideoText" in r.url
                        or "batchAsyncGenerateVideoReferenceImages" in r.url
                    )
                ),
                timeout=60000,
            ) as response_info:
                await human_click(page, submit_btn)
                await human_delay(0.5, 1.0)
                await human_mouse_move(page)

            response = await response_info.value
            return "submit", await response.json()
        except Exception as submit_exc:
            logger.info(f"[Worker {worker_id}] 未捕获到首个生成响应: {submit_exc}，改为等待页面状态轮询...")
            raise

        async with page.expect_response(
            lambda r: "batchCheckAsyncVideoGenerationStatus" in r.url and r.request.method == "POST",
            timeout=180000,
        ) as status_info:
            await human_delay(1.0, 2.0)
            await human_mouse_move(page)

        response = await status_info.value
        return "status", await response.json()

    async def process_task(self, page, task_data: dict[str, Any], worker) -> dict[str, Any]:
        """执行完整的视频生成任务流程。

        逻辑/功能:
            1. 导航到 Flow 首页。
            2. 调用 _prepare_video_project 配置项目、上传参考图、填写提示词。
            3. 调用 _submit_video_generation_humanized 提交并获取响应。
            4. 解析 media name。
            5. 调用 _wait_for_video_status 轮询等待生成完成。
            6. 调用 _download_video_to_local 流式下载视频到本地。
            任务完成后在 finally 中释放 Redis 并发槽位。

        入参:
            page: Playwright Page 对象（由基类 BrowserWorker 创建和回收）。
            task_data: 任务数据字典，包含 prompt、variant_count、图片、账号等。
            worker: BrowserWorker 实例。

        出参:
            dict[str, Any]: 包含:
                - local_video_path: 本地视频文件路径。
                - api_full_response: 生成状态查询的完整 API 响应。

        异常:
            RuntimeError: 任务执行中任何步骤失败。
            TimeoutError: 视频生成超时。
        """
        prompt = task_data.get("prompt")
        variant_count = int(task_data.get("variant_count", 1))
        poll_timeout_ms = int(task_data.get("poll_timeout_ms", 4 * 60 * 1000))

        try:
            logger.info(f"[Worker {worker.worker_id}] 正在访问 Flow 首页...")
            await page.goto(FLOW_HOME_URL, wait_until="domcontentloaded")
            await human_delay(2.0, 4.0)
            await human_mouse_move(page)

            await self._prepare_video_project(page, worker, prompt, variant_count, task_data)

            media_name = None
            response_kind, response_payload = await self._submit_video_generation_humanized(page, worker.worker_id)

            try:
                logger.info(f"[Worker {worker.worker_id}] 收到{response_kind}响应")
                if "result" in response_payload and "data" in response_payload["result"]:
                    media_items = response_payload["result"]["data"].get("media", [])
                else:
                    media_items = response_payload.get("media", [])

                if media_items:
                    primary_media_item = media_items[0]
                    media_name = primary_media_item.get("name")
                    logger.info(f"[Worker {worker.worker_id}] Media name: {media_name}")
                else:
                    logger.info(f"[Worker {worker.worker_id}] 响应结构: {list(response_payload.keys())}")
            except Exception as exc:
                logger.info(f"[Worker {worker.worker_id}] 捕获media name失败: {exc}")

            if not media_name:
                raise RuntimeError("无法从视频生成响应中获取media name")

            await human_mouse_move(page)
            await asyncio.sleep(2)

            logger.info(f"[Worker {worker.worker_id}] 等待视频生成 (可能需要4-8分钟)...")
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
            demo_dir = os.path.join(os.getcwd(), "demo_videos")
            os.makedirs(demo_dir, exist_ok=True)
            local_path = os.path.join(demo_dir, video_filename)

            is_downloaded = await self._download_video_to_local(
                page,
                download_url,
                worker.worker_id,
                cookies,
                local_path,
            )
            if not is_downloaded:
                raise RuntimeError(f"流式下载视频失败, URL是: {download_url}")

            return {
                "local_video_path": str(local_path),
                "api_full_response": final_status_payload.get("api_full_response"),
            }
        except Exception as exc:
            logger.error(f"[Worker {worker.worker_id}] 任务执行失败: {exc}")
            raise
        finally:
            task_email = task_data.get("email")
            if task_email:
                try:
                    release_cookie(task_email)
                    logger.info(f"[Worker {worker.worker_id}] 账号 {task_email} 并发槽位已归还")
                except Exception as rel_err:
                    logger.warning(f"[Worker {worker.worker_id}] 归还槽位失败（忽略）: {rel_err}")
