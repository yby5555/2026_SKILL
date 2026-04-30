import asyncio
import random
import re
import sys
import base64
import os
from pathlib import Path
from time import monotonic
from typing import Any
import httpx
import json

# Add parent directory to sys.path
_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(_ROOT))

# account_mgr 路径
_ACCOUNT_MGR = _ROOT / "account_mgr"
if str(_ACCOUNT_MGR) not in sys.path:
    sys.path.insert(0, str(_ACCOUNT_MGR))

from driver_base import MultiBrowserScraperBase

# 账号检测模块
from account_checker import (
    _click_avatar_and_get_credits,
    LOGIN_EXPIRED_PATTERN,
    MIN_CREDITS_THRESHOLD,
)
from account_mgr.redis_utils import get_next_cookie, remove_from_pool

# 任务存储（MongoDB）
_flow_dir = Path(__file__).resolve().parent
import sys as _sys

if str(_flow_dir) not in _sys.path:
    _sys.path.insert(0, str(_flow_dir))
from api_task_store import create_task as _db_create_task, update_task as _db_update_task
from account_mgr.mongo_utils import (
    create_mongo_client, get_collection,
    mark_pending, mark_abnormal,
)
from account_mgr.cos_utils import upload_file_to_cos, upload_bytes_to_cos, get_presigned_url

import logging
import logging.handlers

_log_dir = _ROOT / "flow" / "log"
_log_dir.mkdir(parents=True, exist_ok=True)
_log_file = _log_dir / "automation_video.log"

logger = logging.getLogger("VideoScraper")
logger.setLevel(logging.INFO)
# 防止重复添加 Handler 导致日志重复
if not logger.handlers:
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    # 只写入文件，不在终端打印
    fh = logging.handlers.RotatingFileHandler(_log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # 移除向上传播，避免被 root logger 捕获后再次打印
    logger.propagate = False

FLOW_HOME_URL = "https://labs.google/fx/zh/tools/flow"
VIDEO_SOURCE_LABEL = "素材"
VIDEO_ASPECT_RATIO_LABEL = "9:16"
VIDEO_MODEL_LABEL = "Veo 3.1 - Lite"


async def human_delay(min_sec: float = 0.5, max_sec: float = 2.0):
    """模拟人类随机暂停"""
    await asyncio.sleep(random.uniform(min_sec, max_sec))


async def human_type(page, locator_str: str, text: str):
    """模拟人类逐字符输入，带随机延迟"""
    for char in text:
        if char in "\r\n":
            char = " "
        await page.locator(locator_str).type(char, delay=random.randint(30, 150))
        if random.random() < 0.05:
            await human_delay(0.2, 0.6)


def normalize_prompt_text(text: str) -> str:
    """将多行 prompt 压成单行，避免换行被页面当成 Enter 提交。"""
    text = re.sub(r"\r\n?|\n", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


async def get_textbox_content(textbox_loc) -> str:
    """
    兼容标准 input/textarea 和富文本 contenteditable 元素，安全地读取输入框当前的文本内容。

    逻辑功能：
    1. 使用 page.evaluate 在浏览器上下文中执行 JavaScript 代码。
    2. JavaScript 函数会尝试从元素中提取三种常见的文本属性：
       - `value`: 针对标准的 <input> 或 <textarea>。
       - `innerText`: 针对普通的 DOM 元素（如 <div> 或 <p>），能保留部分换行格式。
       - `textContent`: 备用的文本获取方式，提取纯文本。
    3. 返回一个包含这三个属性的字典。
    4. Python 侧遍历这三个候选值，找到第一个非空的字符串，并调用 normalize_prompt_text 去除多余的空格。
    5. 返回最终清理后的文本；如果全为空则返回空字符串。
    """
    content = await textbox_loc.evaluate("""(el) => {
        const read = (value) => typeof value === "string" ? value : "";
        return {
            value: read(el.value),
            innerText: read(el.innerText),
            textContent: read(el.textContent),
        };
    }""")

    for candidate in (content.get("value", ""), content.get("innerText", ""), content.get("textContent", "")):
        normalized = normalize_prompt_text(candidate)
        if normalized:
            return normalized
    return ""


async def fill_textbox_with_validation(page, textbox_loc, text: str, worker_id: Any, max_attempts: int = 2) -> None:
    """按聚焦、全选、一次性写入、校验的流程填写 prompt。"""
    for attempt in range(1, max_attempts + 1):
        box = await textbox_loc.bounding_box()
        if box:
            await page.mouse.move(box["x"] + box["width"] / 3, box["y"] + box["height"] / 2, steps=15)

        # 重复点击能让编辑器类输入框更稳定地拿到焦点
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


async def human_mouse_move(page):
    """模拟随机鼠标移动"""
    viewport = page.viewport_size
    if not viewport:
        return
    width, height = viewport['width'], viewport['height']
    for _ in range(random.randint(2, 5)):
        x = random.randint(100, max(120, width - 100))
        y = random.randint(100, max(120, height - 100))
        await page.mouse.move(x, y, steps=random.randint(5, 15))
        await asyncio.sleep(random.uniform(0.1, 0.3))


async def human_scroll(page):
    """模拟随机鼠标滚动"""
    await page.mouse.wheel(0, random.randint(100, 400))
    await human_delay(0.2, 0.6)
    if random.random() < 0.3:
        await page.mouse.wheel(0, -random.randint(50, 200))
        await human_delay(0.1, 0.3)


async def human_click(page, locator_or_element, timeout=5000):
    """模拟人类鼠标移动并点击（精确点击中心，避免小按钮点偏）"""
    try:
        if hasattr(locator_or_element, 'bounding_box'):
            box = await locator_or_element.bounding_box()
        else:
            box = await locator_or_element.bounding_box()

        if not box:
            await locator_or_element.click(timeout=timeout)
            return

        # 小按钮精确点击中心点
        target_x = box['x'] + box['width'] / 2
        target_y = box['y'] + box['height'] / 2

        # 移动过去
        await page.mouse.move(target_x, target_y, steps=random.randint(5, 10))
        await human_delay(0.1, 0.2)
        await page.mouse.down()
        await human_delay(0.05, 0.15)
        await page.mouse.up()
    except Exception:
        # 兜底直接 click
        await locator_or_element.click(timeout=timeout)


async def get_creation_mode_button(page):
    """
    定位真正的创建模式按钮（底部输入框左侧的那个配置按钮），而不是顶部工具栏或模型选择器。

    逻辑功能：
    1. 首先尝试找到右下角的“创建(Submit)”按钮，并获取其屏幕坐标(bounding_box)。
    2. 获取页面上所有带有 `aria-haspopup="menu"` 属性的下拉菜单按钮。
    3. 遍历这些按钮，通过以下策略打分，找出最符合特征的“底部配置按钮”：
       - 排除包含顶部工具栏常见文案（如帮助、设置等）的按钮。
       - 计算该按钮与“创建”按钮的距离：由于配置按钮通常紧挨着提交按钮的左侧，
         因此两者在水平方向和垂直方向的间距越小，得分越高。
       - 如果按钮文本中包含 "x1"~"x4" (变体数量) 或者 "video" / "视频" 等关键字，给予额外加分。
    4. 最终返回得分最高的那个按钮。
    """
    # 稍微等一下页面加载（特别是并发时可能较慢）
    await asyncio.sleep(1)

    # 尝试查找任意形态的提交按钮（先找带图标的，再找普通文本的）
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

    # 重试几次获取所有的菜单按钮，因为刚加载时 DOM 可能还没刷出来
    all_menu_btns = page.locator('button[aria-haspopup="menu"]')
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

            # 排除顶部工具栏、更多菜单等明显无关按钮
            if any(flag in lowered for flag in
                   ["more_vert", "settings", "帮助", "help", "filter", "search", "add", "添加", "play_movies",
                    "scenebuilder", "nav_rail", "dashboard", "archive"]):
                continue

            # 模式按钮通常在底部输入区，且紧挨着右侧提交按钮左边
            score = 0.0
            if submit_box:
                horizontal_gap = submit_box["x"] - (current_box["x"] + current_box["width"])
                vertical_gap = abs(current_box["y"] - submit_box["y"])
                if horizontal_gap < -20 or horizontal_gap > 350:  # 放宽间距容忍度
                    continue
                score += max(0.0, 350 - horizontal_gap)
                score += max(0.0, 120 - vertical_gap)
            else:
                # 靠下（Y大）且靠右（X大），由于是按钮组最左侧，X可能在中间，所以 Y 的权重调高
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
    返回当前可见的底部配置菜单浮层，而不是模型下拉框。

    逻辑功能：
    1. 查找页面上所有 role 为 "menu" 的可见元素。
    2. 遍历这些菜单，检查其内部是否包含 `role="tablist"` 元素。
       （因为只有底部的“配置菜单”才包含多个 tab 分组（比如：图片/视频、素材/比例 等），
       而普通的模型选择下拉框只包含 menuitem）
    3. 返回找到的第一个符合条件的配置菜单浮层。
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
    打开底部配置菜单并返回菜单 locator。

    逻辑功能：
    1. 调用 get_creation_mode_button 定位底部配置按钮。
    2. 检查按钮的 aria-expanded 属性，如果菜单未展开，则模拟人工点击该按钮将其展开。
    3. 调用 get_creation_settings_menu 获取展开后的菜单浮层。
    4. 如果菜单浮层未成功出现（可能是点击没生效或动画延迟），则进行一次重试点击。
    5. 返回 (触发按钮对象, 展开后的菜单浮层对象)。
    """
    target_btn = None
    # 放大重试次数以抵抗由于并发造成的局部卡顿和网络延迟
    for attempt in range(5):
        target_btn = await get_creation_mode_button(page)
        if target_btn:
            break
        logger.info(f"[Worker {worker_id}] 未找到底部配置按钮，重试中... ({attempt + 1}/5)")
        await asyncio.sleep(2)

    if not target_btn:
        raise RuntimeError("经过重试后仍找不到底部配置按钮，页面可能假死或网络中断")

    if await target_btn.get_attribute("aria-expanded") != "true":
        # logger.info(f"[Worker {worker_id}] 正在打开底部配置菜单")
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
    """在指定 tab 组里选择目标选项。"""
    tablist = menu.locator('[role="tablist"]').nth(group_index)
    target_tab = tablist.locator('[role="tab"]').filter(has_text=label_pattern).first
    if await target_tab.count() == 0:
        raise RuntimeError(f"第 {group_index + 1} 组未找到 {description}")

    if await target_tab.get_attribute("aria-selected") == "true":
        logger.info(f"[Worker {worker_id}] {description} 已经选中")
        return

    # logger.info(f"[Worker {worker_id}] 正在选择 {description}")
    await human_click(page, target_tab)
    await human_delay(0.6, 1.1)


async def ensure_video_model(page, menu, worker_id: Any, target_model: str) -> None:
    """确保视频模型为目标值。"""
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
    """确保底部配置为视频、素材、9:16、xN、Veo 3.1 - Lite。"""
    logger.info(
        f"[Worker {worker_id}] 正在设置视频配置 (素材, {VIDEO_ASPECT_RATIO_LABEL}, x{variant_count}, {VIDEO_MODEL_LABEL})...")

    try:
        await asyncio.sleep(2)

        target_btn, menu = await open_creation_settings_menu(page, worker_id)

        # 第 1 组：图片 / 视频
        await select_tab_in_group(
            page,
            menu,
            0,
            re.compile(r"video|videocam|视频", re.IGNORECASE),
            worker_id,
            "视频模式",
        )

        # 切换模式后菜单会重绘，重新获取一遍
        target_btn, menu = await open_creation_settings_menu(page, worker_id)

        # 第 2 组：帧 / 素材
        await select_tab_in_group(
            page,
            menu,
            1,
            re.compile(rf"{re.escape(VIDEO_SOURCE_LABEL)}|chrome_extension", re.IGNORECASE),
            worker_id,
            f"来源 {VIDEO_SOURCE_LABEL}",
        )

        # 第 3 组：比例
        await select_tab_in_group(
            page,
            menu,
            2,
            re.compile(rf"{re.escape(VIDEO_ASPECT_RATIO_LABEL)}|crop_9_16", re.IGNORECASE),
            worker_id,
            f"比例 {VIDEO_ASPECT_RATIO_LABEL}",
        )

        # 第 4 组：变体数量
        target_variant = f"x{variant_count}"
        await select_tab_in_group(
            page,
            menu,
            3,
            re.compile(rf"^{re.escape(target_variant)}$", re.IGNORECASE),
            worker_id,
            f"生成数量 {target_variant}",
        )

        # 模型选择
        await ensure_video_model(page, menu, worker_id, VIDEO_MODEL_LABEL)

        # 关闭菜单
        if await menu.is_visible(timeout=500):
            await page.keyboard.press("Escape")
            await human_delay(0.3, 0.6)

        # 最终校验
        target_btn, menu = await open_creation_settings_menu(page, worker_id)
        selected_video_tab = menu.locator('[role="tablist"]').nth(0).locator('[role="tab"][aria-selected="true"]').first
        selected_source_tab = menu.locator('[role="tablist"]').nth(1).locator(
            '[role="tab"][aria-selected="true"]').first
        selected_ratio_tab = menu.locator('[role="tablist"]').nth(2).locator('[role="tab"][aria-selected="true"]').first
        selected_variant_tab = menu.locator('[role="tablist"]').nth(3).locator(
            '[role="tab"][aria-selected="true"]').first
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
        if selected_variant_text.lower() != target_variant.lower():
            raise RuntimeError(f"最终校验失败，生成数量不是 {target_variant}: {selected_variant_text}")
        if VIDEO_MODEL_LABEL.lower() not in selected_model_text.lower():
            raise RuntimeError(f"最终校验失败，模型不是 {VIDEO_MODEL_LABEL}: {selected_model_text}")

        await page.keyboard.press("Escape")
        await human_delay(0.3, 0.6)
        await human_delay(0.8, 1.5)

    except Exception as e:
        logger.info(f"[Worker {worker_id}] 设置模式错误: {e}")
        try:
            await page.keyboard.press("Escape")
        except Exception:
            pass
        raise RuntimeError(f"设置视频模式失败: {e}")


class GoogleFlowVideoScraperV2(MultiBrowserScraperBase):
    """Google Flow视频生成爬虫"""

    # 类级别 MongoDB collection 缓存（延迟初始化，所有 Worker 共享）
    _mongo_col = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 初始化 MongoDB
        if GoogleFlowVideoScraperV2._mongo_col is None:
            try:
                _client = create_mongo_client()
                GoogleFlowVideoScraperV2._mongo_col = get_collection(_client)
                logger.info("[GoogleFlowVideoScraperV2] MongoDB 已连接")
            except Exception as e:
                logger.info(f"[GoogleFlowVideoScraperV2] MongoDB 连接失败，账号检测功能可能受限: {e}")

    def normalize_task(self, task_data: dict[str, Any]) -> dict[str, Any]:
        """
        在任务执行前，确保 task_data 包含 cookies。
        因为基类的 _run_single_task 在拿到 task 后就会使用 task_data['cookies'] 创建上下文。
        """
        task_copy = dict(task_data)
        if not task_copy.get("email") or not task_copy.get("cookies"):
            next_result = get_next_cookie()
            if not next_result:
                raise RuntimeError("Redis Cookie Pool 为空，无法启动任务，请先运行 login_scheduler.py")
            task_copy["email"], task_copy["cookies"] = next_result
        return task_copy

    async def _ensure_account_healthy(self, page, task_data: dict, worker) -> None:
        """
        在已导航到 Flow 首页后，检测当前账号健康状态。

        检测逻辑：
            1. 检查当前页面 URL 是否被重定向到登录页（登录态失效）
            2. 点击头像弹窗，读取 AI 点数（额度不足）
            3. 异常时：移除 Redis Cookie 并更新 MongoDB，然后从 Redis 取下一个 Cookie 重试
            4. 最多切换 MAX_SWITCHES 次，超出则抛出 RuntimeError

        Args:
            page:       当前 Playwright Page（已加载 Flow 首页）
            task_data:  任务数据（包含 email / cookies 字段）
            worker:     Worker 对象（仅用于日志打印）
        """
        MAX_SWITCHES = 3
        col = GoogleFlowVideoScraperV2._mongo_col
        current_email = task_data.get("email")

        async def _check_current_page() -> str:
            """检测当前已加载页面的账号状态，返回 'ok'/'login_expired'/'no_credits'"""
            url = page.url
            # 登录态失效
            if LOGIN_EXPIRED_PATTERN.search(url) or "/signin" in url:
                logger.info(f"[Worker {worker.worker_id}] 账号 {current_email} 登录态失效 -> status=0")
                if current_email:
                    remove_from_pool(current_email)
                    mark_pending(col, current_email, "登录态失效，访问 Flow 被重定向到登录页")
                return "login_expired"
            # 额度检测
            credits = await _click_avatar_and_get_credits(page)
            if credits is None:
                # 读取失败（弹窗未渲染/DOM 变更），记录警告但不阻断任务
                logger.info(f"[Worker {worker.worker_id}] 账号 {current_email} AI 点数读取失败，跳过额度检测继续执行")
                return "ok"
            logger.info(f"[Worker {worker.worker_id}] 账号 {current_email} AI 点数: {credits}")
            if credits < MIN_CREDITS_THRESHOLD:
                logger.info(f"[Worker {worker.worker_id}] 账号 {current_email} 额度不足({credits}) -> status=2")
                if current_email:
                    remove_from_pool(current_email)
                    mark_abnormal(col, current_email, f"AI 点数不足，当前剩余: {credits}")
                return "no_credits"
            return "ok"

        for attempt in range(MAX_SWITCHES + 1):
            status = await _check_current_page()

            if status == "ok":
                logger.info(f"[Worker {worker.worker_id}] 账号 {current_email} 检测通过，继续执行")
                return

            if attempt >= MAX_SWITCHES:
                raise RuntimeError(
                    f"[Worker {worker.worker_id}] 已切换 {MAX_SWITCHES} 次 Cookie 仍无可用账号，任务终止"
                )

            # 从 Redis 取下一个账号
            next_result = get_next_cookie()
            if not next_result:
                raise RuntimeError("Redis Cookie Pool 已空，无法切换账号")

            new_email, new_cookies = next_result
            logger.info(f"[Worker {worker.worker_id}] 切换到新账号: {new_email}")

            # 更新页面 Context Cookie
            await page.context.clear_cookies()
            await page.context.add_cookies(new_cookies)

            # 更新 task_data
            task_data["email"] = new_email
            task_data["cookies"] = new_cookies
            current_email = new_email

            # 重新加载首页（让新 Cookie 生效）
            logger.info(f"[Worker {worker.worker_id}] 重新加载 Flow 首页...")
            await page.goto(FLOW_HOME_URL, wait_until="domcontentloaded")
            await asyncio.sleep(2)

    async def _wait_for_video_status(
            self,
            page,
            media_names: set[str],
            worker_id: Any,
            timeout_ms: int,
            email: str
    ) -> dict[str, Any]:
        """等待页面自然发出的状态轮询请求返回成功。"""
        deadline = monotonic() + timeout_ms / 1000
        last_statuses: dict[str, str] = {}

        while monotonic() < deadline:
            remaining_ms = max(1000, int((deadline - monotonic()) * 1000))
            async with page.expect_response(
                    lambda r: (
                            "batchCheckAsyncVideoGenerationStatus" in r.url and
                            r.request.method == "POST"
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
                # 顺便加点随机鼠标动作，防假死判定
                if random.random() < 0.2:
                    await human_mouse_move(page)
                continue

            last_statuses = {
                item["name"]: item.get("mediaMetadata", {})
                .get("mediaStatus", {})
                .get("mediaGenerationStatus", "UNKNOWN")
                for item in tracked_items
            }
            logger.info(f"[Worker {email}-{worker_id}] 当前视频状态: {last_statuses}")

            full_api_response = json.dumps(payload, ensure_ascii=False)

            if any(status == "MEDIA_GENERATION_STATUS_FAILED" for status in last_statuses.values()):
                raise RuntimeError(f"视频生成失败: {last_statuses}")

            if media_names.issubset(last_statuses.keys()) and all(
                    status == "MEDIA_GENERATION_STATUS_SUCCESSFUL" for status in last_statuses.values()
            ):
                return {"media": tracked_items, "api_full_response": full_api_response}

            # 等待过程中加入拟人化动作
            if random.random() < 0.3:
                await human_mouse_move(page)
                await human_scroll(page)

        raise TimeoutError(f"等待视频生成超时，最后一次状态: {last_statuses}")

    async def _wait_for_video_completion(self, page, media_name: str, worker_id: Any, timeout_ms: int) -> str:
        """等待视频生成完成并返回签名URL"""
        deadline = monotonic() + timeout_ms / 1000
        redirect_url = f"https://labs.google/fx/api/trpc/media.getMediaUrlRedirect?name={media_name}"

        logger.info(f"[Worker {worker_id}] 等待视频 {media_name[:8]}... 生成完成")

        # 从页面获取user agent
        user_agent = await page.evaluate("() => navigator.userAgent")

        while monotonic() < deadline:
            try:
                # 尝试获取视频URL - 如果返回307带location，说明视频准备好了
                async with httpx.AsyncClient(timeout=10.0, follow_redirects=False) as client:
                    cookies = await page.context.cookies()
                    cookie_dict = {c["name"]: c["value"] for c in cookies}

                    response = await client.get(
                        redirect_url,
                        cookies=cookie_dict,
                        headers={
                            "User-Agent": user_agent,
                            "Referer": "https://labs.google/"
                        }
                    )

                    # 307状态码带Location头表示视频已就绪
                    if response.status_code == 307 and response.headers.get("location"):
                        video_url = response.headers["location"]
                        logger.info(f"[Worker {worker_id}] 视频已就绪! URL: {video_url[:80]}...")
                        return video_url

                    # 其他状态码可能表示还在处理中
                    logger.info(f"[Worker {worker_id}] 视频状态: {response.status_code}, 继续等待...")

            except Exception as e:
                logger.info(f"[Worker {worker_id}] 轮询错误: {e}")

            # 下次轮询前等待
            await asyncio.sleep(10)

        raise TimeoutError(f"视频生成超时 ({timeout_ms / 1000}秒后)")

    async def _capture_video_urls(self, page, media_name: str, worker_id: Any) -> tuple[str, str | None]:
        """获取视频重定向URL和CDN URL"""
        redirect_url = f"https://labs.google/fx/api/trpc/media.getMediaUrlRedirect?name={media_name}"
        try:
            user_agent = await page.evaluate("() => navigator.userAgent")
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=False) as client:
                cookies = await page.context.cookies()
                cookie_dict = {c["name"]: c["value"] for c in cookies}
                response = await client.get(
                    redirect_url,
                    cookies=cookie_dict,
                    headers={
                        "User-Agent": user_agent,
                        "Referer": "https://labs.google/",
                        "Accept": "*/*",
                    },
                )
                if response.status_code == 307:
                    return redirect_url, response.headers.get("location")
        except Exception as e:
            logger.info(f"[Worker {worker_id}] 获取视频URL失败: {e}")
        return redirect_url, None

    async def _download_video_to_local(self, page, video_url: str, worker_id: Any, cookies: list,
                                       save_path: str) -> bool:
        """下载视频直接写入本地，保留了原有的拟人化延迟和防封请求头"""
        # logger.info(f"[Worker {worker_id}] 模拟观看视频停留...")
        await human_delay(4.0, 8.0)  # 假装人在看生成的视频，防下载过快被封
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
            # 拉长下载超时时间，容忍大体积视频下载或拥堵网络，并增加 verify=False 避免 SSL 证书问题
            async with httpx.AsyncClient(timeout=180.0, verify=False) as client:
                async with client.stream("GET", video_url, cookies=cookie_dict, headers=headers) as response:
                    response.raise_for_status()
                    with open(save_path, "wb") as f:
                        async for chunk in response.aiter_bytes(chunk_size=8192):
                            if chunk:
                                f.write(chunk)

            if os.path.exists(save_path) and os.path.getsize(save_path) > 1024:
                kb_size = os.path.getsize(save_path) // 1024
                logger.info(f"[Worker {worker_id}] 视频下载成功！大小: {kb_size} KB, URL: {video_url[:50]}...")
                return True
            else:
                logger.error(f"[Worker {worker_id}] 下载文件似乎太小或不存在")
                return False
        except Exception as e:
            logger.error(f"[Worker {worker_id}] 下载视频到本地失败: {e}")
            return False

    async def _prepare_video_project(self, page, worker, prompt: str, variant_count: int, task_data) -> None:
        """按图片脚本的节奏进入项目页，并补上视频模式和提示词。"""
        # 检查并关闭底部 Cookie 弹窗
        try:
            cookie_btn = page.locator(
                '.glue-cookie-notification-bar button:has-text("Got it"), .glue-cookie-notification-bar button:has-text("同意"), .glue-cookie-notification-bar button:has-text("Accept"), .glue-cookie-notification-bar button').first
            if await cookie_btn.is_visible(timeout=2000):
                logger.info(f"[Worker {worker.worker_id}] 发现 Cookie 弹窗，正在关闭...")
                await human_click(page, cookie_btn)
                await human_delay(1.0, 2.0)
        except Exception:
            pass

        await self._ensure_account_healthy(page, task_data, worker)

        # 点击“新建项目”
        logger.info(f"[Worker {worker.worker_id}] 等待并点击新建项目...")
        try:
            new_btn = page.locator('button:has-text("新建项目")')
            # 增加新建项目按钮出现前的等待容错，最高等待 10 秒
            if await new_btn.is_visible(timeout=5000):
                await human_delay(1.0, 2.0)  # 看到按钮后，稍微停顿一下再点，模拟人类反应
                await human_click(page, new_btn)
                # 新建项目后，页面会发生整体切换并重新加载底层编辑器 DOM，
                # 在并发情况下（尤其是3并发），浏览器进程抢占 CPU 资源，这里极易出现严重的网络或渲染延迟。
                # 所以将这里的固定等待加长，以容忍极端的并发延迟。
                await human_delay(2.0, 4.0)
            else:
                raise RuntimeError("超时未找到'新建项目'按钮，页面可能未加载完成")
        except Exception as e:
            raise RuntimeError(f"点击'新建项目'失败: {e}")

        # 确保视频模式和变体数量正确
        await ensure_video_mode(page, worker.worker_id, variant_count)

        # [NEW] 上传图片逻辑 (支持 URL 和 Base64 内存直传)
        file_buffer = None
        file_name = "image.png"
        mime_type = "image/png"

        image_url = task_data.get("image_url")
        image_base64 = task_data.get("image_base64")

        if image_url:
            logger.info(f"[Worker {worker.worker_id}] 正在从 URL 下载参考图片: {image_url}")
            try:
                async with httpx.AsyncClient(verify=False, timeout=30.0) as client:
                    resp = await client.get(image_url)
                    resp.raise_for_status()
                    file_buffer = resp.content
            except Exception as e:
                logger.error(f"[Worker {worker.worker_id}] 从 URL 下载图片失败: {e}")
        elif image_base64:
            logger.info(f"[Worker {worker.worker_id}] 正在处理 Base64 格式的参考图片")
            try:
                file_buffer = base64.b64decode(image_base64)
            except Exception as e:
                logger.error(f"[Worker {worker.worker_id}] Base64 解码图片失败: {e}")

        if file_buffer:
            try:
                # Playwright 的 set_input_files 支持通过字典传入内存数据
                # 字典格式: {"name": file_name, "mimeType": mime_type, "buffer": file_buffer}
                file_input = page.locator('input[type="file"]').first
                await file_input.set_input_files({"name": file_name, "mimeType": mime_type, "buffer": file_buffer})
                logger.info(f"[Worker {worker.worker_id}] 图片已成功上传 (内存直传)")
                # 等待页面识别并上传图片，通常会在左下角出现缩略图或在输入框出现 attachment 标签
                await human_delay(10.0, 20.0)
                input('1111')
            except Exception as e:
                logger.error(f"[Worker {worker.worker_id}] 内存直传图片失败: {e}")

        # 等待输入框并输入提示词
        input_prompt = normalize_prompt_text(prompt)
        logger.info(f"[Worker {worker.worker_id}] 等待输入框并输入提示词: {input_prompt[:10]}")
        await page.wait_for_selector('[role="textbox"]', state="visible", timeout=15000)
        await human_delay(1.5, 3.0)

        textbox_loc = page.locator('[role="textbox"]')
        await fill_textbox_with_validation(page, textbox_loc, input_prompt, worker.worker_id)
        await human_delay(1.0, 2.5)

    async def _submit_video_generation_humanized(self, page, worker_id: Any) -> tuple[str, dict[str, Any]]:
        """提交生成请求；如果首个响应没抓到，则等待页面自己的状态轮询。"""
        logger.info(f"[Worker {worker_id}] 等待提交按钮出现...")
        # 并发导致输入提示词后，右侧提交按钮可能需要一小会儿才可点
        await human_delay(1.5, 3.0)

        submit_btn = page.locator("button").filter(has_text=re.compile(r"arrow_forward", re.IGNORECASE)).filter(
            has_text="创建").first
        if not await submit_btn.is_visible(timeout=2500):
            submit_btn = page.locator('button:has-text("创建")').last

        if not await submit_btn.is_visible(timeout=5000):
            raise RuntimeError("超时未找到真实可点击的创建提交按钮")

        try:
            async with page.expect_response(
                    lambda r: "batchAsyncGenerateVideoText" in r.url and r.request.method == "POST",
                    timeout=60000
            ) as response_info:
                await human_click(page, submit_btn)
                await human_delay(0.5, 1.0)
                await human_mouse_move(page)

            response = await response_info.value
            return "submit", await response.json()
        except Exception as submit_exc:
            logger.info(f"[Worker {worker_id}] 未捕获到首个生成响应: {submit_exc}，改为等待页面状态轮询...")

        async with page.expect_response(
                lambda r: "batchCheckAsyncVideoGenerationStatus" in r.url and r.request.method == "POST",
                timeout=180000
        ) as status_info:
            await human_delay(1.0, 2.0)
            await human_mouse_move(page)

        response = await status_info.value
        return "status", await response.json()

        raise RuntimeError("未捕获到首个生成响应，且未等到页面状态轮询")

    async def process_task(self, page, task_data: dict[str, Any], worker) -> dict[str, Any]:
        prompt = task_data.get("prompt")
        variant_count = int(task_data.get("variant_count", 1))
        poll_timeout_ms = int(task_data.get("poll_timeout_ms", 8 * 60 * 1000))
        task_id: str | None = task_data.get("id")  # 调用方提供的任务 ID（可选）

        # ── 写入 MongoDB：任务开始 ─────────────────────────────────────────────
        if task_id:
            try:
                await _db_create_task(task_id, prompt)
                logger.info(f"[Worker {worker.worker_id}] MongoDB 任务记录已创建: {task_id}")
            except Exception as db_err:
                logger.info(f"[Worker {worker.worker_id}] MongoDB create_task 失败（忽略）: {db_err}")

        result: dict[str, Any] = {}
        try:
            # 1. 访问 Google Flow 首页
            logger.info(f"[Worker {worker.worker_id}] 正在访问 Flow 首页...")
            await page.goto(FLOW_HOME_URL, wait_until="domcontentloaded")
            await human_delay(2.0, 4.0)
            await human_mouse_move(page)

            await self._prepare_video_project(page, worker, prompt, variant_count, task_data)

            media_name = None
            project_id = None
            response_kind, response_payload = await self._submit_video_generation_humanized(page, worker.worker_id)

            try:
                logger.info(f"[Worker {worker.worker_id}] 收到{response_kind}响应")

                if "result" in response_payload and "data" in response_payload["result"]:
                    media_items = response_payload["result"]["data"].get("media", [])
                else:
                    media_items = response_payload.get("media", [])

                if media_items:
                    primary_media_item = media_items[0] if response_kind == "project_recovery" else media_items[0]
                    media_name = primary_media_item.get("name")
                    project_id = primary_media_item.get("projectId")
                    logger.info(f"[Worker {worker.worker_id}] Media name: {media_name}, Project ID: {project_id}")
                else:
                    logger.info(f"[Worker {worker.worker_id}] 响应结构: {list(response_payload.keys())}")
            except Exception as e:
                logger.info(f"[Worker {worker.worker_id}] 捕获media name失败: {e}")

            if not media_name:
                raise RuntimeError("无法从视频生成响应中获取media name")

            await human_mouse_move(page)
            await asyncio.sleep(2)

            # 5. 等待视频生成完成
            logger.info(f"[Worker {worker.worker_id}] 等待视频生成 (可能需要4-8分钟)...")
            final_status_payload = await self._wait_for_video_status(
                page=page,
                media_names={media_name},
                worker_id=worker.worker_id,
                timeout_ms=poll_timeout_ms,
                email=task_data.get('email')

            )
            final_media_items = final_status_payload.get("media", [])
            final_media_by_name = {item["name"]: item for item in final_media_items if item.get("name")}
            primary_media = final_media_by_name.get(media_name, {"name": media_name, "projectId": project_id})
            project_id = primary_media.get("projectId") or project_id

            _, video_url = await self._capture_video_urls(page, primary_media["name"], worker.worker_id)
            if not video_url:
                raise RuntimeError("视频状态已成功，但未能获取最终视频下载地址")

            # 6. 下载视频并上传 COS（内存直传）
            local_path = None
            cos_video_url = None

            if video_url:
                cookies = await page.context.cookies()
                video_filename = f"{task_data.get('id', primary_media['name'][:8])}.mp4"

                # 创建本地 demo 存储目录
                demo_dir = os.path.join(os.getcwd(), "demo_videos")
                os.makedirs(demo_dir, exist_ok=True)
                local_path = os.path.join(demo_dir, video_filename)

                # 流式下载到本地
                is_downloaded = await self._download_video_to_local(page, video_url, worker.worker_id, cookies,
                                                                    local_path)

                if is_downloaded:
                    # 7. 上传到 COS (暂时注释)
                    pass
                    # try:
                    #     cos_key = f"flow/videos/{video_filename}"
                    #     logger.info(f"[Worker {worker.worker_id}] 正在内存直传视频到 COS: {cos_key}")
                    #     cos_video_url = upload_bytes_to_cos(video_bytes, video_filename)
                    #     logger.info(f"[Worker {worker.worker_id}] COS 上传成功: {cos_video_url}")

                    #     # 生成预签名链接作为最终 video_url（1小时有效）
                    #     signed_url = get_presigned_url(cos_key, expire_time=3600)
                    #     if signed_url:
                    #         video_url = signed_url
                    #         logger.info(f"[Worker {worker.worker_id}] 预签名链接生成成功: {signed_url}")
                    #     else:
                    #         video_url = cos_video_url  # 兜底用公开 URL
                    # except Exception as cos_err:
                    #     logger.info(f"[Worker {worker.worker_id}] COS 内存直传失败，保留原始链接: {cos_err}")
                else:
                    logger.error(f"[Worker {worker.worker_id}] 流式下载视频失败, URL是: {video_url}")
                    local_path = None

            result = {
                "status": "success",
                "prompt": prompt,
                "project_id": project_id,
                "video_url": video_url,  # COS URL（上传成功）或原始签名 URL（兜底）
                "cos_video_url": cos_video_url,  # 仅 COS 上传成功时有值
                "local_video_path": str(local_path) if local_path else None,
                "api_full_response": final_status_payload.get("api_full_response"),
            }

        except Exception as task_exc:
            # 任何异常都先记录失败，再重新抛出
            result = {
                "status": "failed",
                "error": str(task_exc),
                "prompt": prompt,
            }
            raise

        finally:
            # ── 写入 MongoDB：任务结束（成功或失败）──────────────────────────
            if task_id:
                try:
                    await _db_update_task(task_id, result)
                    logger.info(
                        f"[Worker {worker.worker_id}] MongoDB 任务结果已更新: {task_id} -> {result.get('status')}")
                except Exception as db_err:
                    logger.info(f"[Worker {worker.worker_id}] MongoDB update_task 失败（忽略）: {db_err}")

            # ── 归还账号并发槽位（每账号最多 MAX_CONCURRENT_PER_ACCOUNT 个 Worker 同时使用）
            _task_email = task_data.get("email")
            if _task_email:
                try:
                    from account_mgr.redis_utils import release_cookie
                    release_cookie(_task_email)
                    logger.info(f"[Worker {worker.worker_id}] 账号 {_task_email} 并发槽位已归还")
                except Exception as _rel_err:
                    logger.warning(f"[Worker {worker.worker_id}] 归还槽位失败（忽略）: {_rel_err}")

        return result


async def main():
    # ── 从 Redis Cookie Pool 获取账号，不再硬编码 cookie 文件路径 ──────────
    next_result = get_next_cookie()
    if not next_result:
        return logger.info("Redis Cookie Pool 为空，请先运行 login_scheduler.py 完成登录！")

    email, cookies = next_result
    logger.info(f"[main] 从 Redis 取到账号: {email}")

    scraper = GoogleFlowVideoScraperV2(
        browser_pool_size=1,
        headless=False,
        extra_flags=["--start-maximized"],
        viewport={"width": 0, "height": 0},
        task_timeout_ms=12 * 60 * 1000
    )

    medical_prompt = "基于这张图片生成关于健身的视频"

    # 为了测试 Base64 内存直传，先在外部读取文件转为 base64
    import base64
    test_image_path = r"d:\2026_SKILL\flow\1ad0fd6709f24b3ebc7ca0da7a44e698.png"
    with open(test_image_path, "rb") as f:
        img_b64 = base64.b64encode(f.read()).decode("utf-8")

    tasks = [
        {"prompt": medical_prompt, "variant_count": 1, "email": email, "cookies": cookies, "image_base64": img_b64}]
    async with scraper:
        results = await scraper.run_tasks(tasks)
        for res in results:
            if isinstance(res, Exception):
                logger.info(f"任务失败: {res}")
            else:
                logger.info(f"任务成功: {res.get('local_video_path')}")


if __name__ == "__main__":
    asyncio.run(main())
