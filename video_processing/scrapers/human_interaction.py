"""
Web 自动化的人机交互工具
===========================
此模块包含用于模拟网页人机交互的工具，包括延迟、鼠标移动、点击和文本输入。

这些工具帮助使自动化交互看起来更自然，减少被检测为自动化行为的可能性。
"""

import asyncio
import random
import logging
from typing import Any

logger = logging.getLogger(__name__)

# 配置默认值
MIN_DELAY_SECONDS = 0.5
MAX_DELAY_SECONDS = 2.0
DEFAULT_HUMAN_DELAY_STEPS = 15


async def human_delay(min_sec: float = MIN_DELAY_SECONDS, max_sec: float = MAX_DELAY_SECONDS) -> None:
    """
    模拟动作之间的人为延迟。

    此函数引入随机延迟，使自动化交互看起来更自然，模仿人类动作的可变时间。

    参数:
        min_sec: 最小延迟秒数（默认: 0.5）
        max_sec: 最大延迟秒数（默认: 2.0）

    示例:
        await human_delay(1.0, 3.0)  # 延迟 1-3 秒
    """
    await asyncio.sleep(random.uniform(min_sec, max_sec))


async def human_mouse_move(page, min_moves: int = 2, max_moves: int = 5) -> None:
    """
    模拟带有随机位置的人为鼠标移动。

    此函数将鼠标移动到页面上的随机位置，模仿自然的鼠标行为模式。

    参数:
        page: Playwright 页面对象
        min_moves: 最小鼠标移动次数（默认: 2）
        max_moves: 最大鼠标移动次数（默认: 5）

    示例:
        await human_mouse_move(page)  # 进行 2-5 次随机鼠标移动
    """
    viewport = page.viewport_size
    if not viewport:
        return

    width, height = viewport["width"], viewport["height"]
    num_moves = random.randint(min_moves, max_moves)

    for _ in range(num_moves):
        x = random.randint(100, max(120, width - 100))
        y = random.randint(100, max(120, height - 100))
        await page.mouse.move(x, y, steps=random.randint(5, 15))
        await asyncio.sleep(random.uniform(0.1, 0.3))


async def human_scroll(page, scroll_down_min: int = 100, scroll_down_max: int = 400,
                      scroll_up_chance: float = 0.3) -> None:
    """
    模拟自然的滚动行为。

    此函数以自然模式滚动页面，主要是向下滚动，但偶尔向上滚动，模仿真实用户行为。

    参数:
        page: Playwright 页面对象
        scroll_down_min: 最小向下滚动量（默认: 100）
        scroll_down_max: 最大向下滚动量（默认: 400）
        scroll_up_chance: 向上滚动的概率（默认: 0.3 = 30%）

    示例:
        await human_scroll(page)  # 自然滚动
    """
    await page.mouse.wheel(0, random.randint(scroll_down_min, scroll_down_max))
    await human_delay(0.2, 0.6)

    if random.random() < scroll_up_chance:
        await page.mouse.wheel(0, -random.randint(50, 200))
        await human_delay(0.1, 0.3)


async def human_click(page, locator_or_element, timeout: int = 5000) -> None:
    """
    模拟自然的点击行为。

    此函数使用自然鼠标移动点击元素，包括移动到元素、短暂暂停然后点击。

    参数:
        page: Playwright 页面对象
        locator_or_element: 要点击的 Playwright 定位器或元素
        timeout: 等待元素的最大时间（毫秒）（默认: 5000）

    异常:
        Exception: 如果无法找到或点击元素

    示例:
        await human_click(page, page.locator('button:text("提交")'))
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


async def fill_textbox_with_validation(
    page,
    textbox_loc,
    text: str,
    worker_id: Any,
    max_attempts: int = 2
) -> None:
    """
    填充文本框并验证输入。

    此函数模拟人类文本输入行为，包括点击文本框、输入文本并验证，如果验证失败则重试。

    参数:
        page: Playwright 页面对象
        textbox_loc: 文本框元素的定位器
        text: 要输入到文本框的文本
        worker_id: 用于日志记录的工作器标识符
        max_attempts: 最大输入尝试次数（默认: 2）

    异常:
        RuntimeError: 如果在所有尝试后文本框输入验证失败

    示例:
        await fill_textbox_with_validation(
            page,
            page.locator('textarea'),
            "你好世界",
            worker_id=1
        )
    """
    for attempt in range(1, max_attempts + 1):
        box = await textbox_loc.bounding_box()
        if box:
            await page.mouse.move(
                box["x"] + box["width"] / 3,
                box["y"] + box["height"] / 2,
                steps=15
            )

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


async def get_textbox_content(textbox_loc) -> str:
    """
    获取文本框元素的当前内容。

    此函数使用多种方法从文本框中提取文本内容，以处理不同的文本框实现。

    参数:
        textbox_loc: 文本框元素的定位器

    返回:
        str: 文本框的规范化文本内容，如果未找到则为空字符串

    示例:
        content = await get_textbox_content(page.locator('textarea'))
        print(f"文本框内容: {content}")
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

    for candidate in (
        content.get("value", ""),
        content.get("innerText", ""),
        content.get("textContent", "")
    ):
        normalized = normalize_prompt_text(candidate)
        if normalized:
            return normalized
    return ""


def normalize_prompt_text(text: str) -> str:
    """
    通过删除额外空格和换行符来规范化文本。

    此函数通过用空格替换换行符并将多个空格减少为单个空格来清理文本。

    参数:
        text: 要规范化的文本

    返回:
        str: 规范化后的文本，只有单个空格且没有换行符

    示例:
        clean_text = normalize_prompt_text("  你好   世界\\n\\n  ")
        print(clean_text)  # "你好 世界"
    """
    import re

    text = re.sub(r"\r\n?|\n", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()