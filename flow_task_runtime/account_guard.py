"""账号登录态和 AI 点数检查模块。"""

from __future__ import annotations

import asyncio
import re
from typing import Any

from .config import RuntimeSettings
from .flow_api import FLOW_HOME_URL
from .task_repository import mark_account_abnormal, mark_account_pending

LOGIN_EXPIRED_PATTERN = re.compile(r"labs\.google/fx/api/auth/signin.*error=Callback", re.IGNORECASE)


async def click_avatar_and_get_credits(page: Any) -> int | None:
    """点击右上角头像并读取 AI 点数。

    参数:
        page: 当前 Playwright 页面对象。

    返回:
        int | None: 读取成功返回点数，失败返回 None。
    """
    exclude_keywords = [
        "more_vert",
        "help_outlined",
        "help",
        "close",
        "arrow_forward",
        "discord",
        "instagram",
        "twitter",
        "flow tv",
        "search",
        "filter",
    ]

    try:
        button_center = await page.evaluate(
            """(excludeKeywords) => {
                let btn = document.querySelector('button img[src*="googleusercontent"]')?.closest('button');
                if (!btn) {
                    const width = window.innerWidth;
                    let bestRight = -1;
                    for (const b of document.querySelectorAll('button')) {
                        const rect = b.getBoundingClientRect();
                        if (!rect.width || !rect.height) continue;
                        if (rect.top > 100) continue;
                        if (rect.right < width * 0.6) continue;
                        const text = (b.innerText || '').trim().toLowerCase();
                        if (excludeKeywords.some((kw) => text.includes(kw))) continue;
                        if (rect.right > bestRight) { bestRight = rect.right; btn = b; }
                    }
                }
                if (!btn) return null;
                const rect = btn.getBoundingClientRect();
                return { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 };
            }""",
            exclude_keywords,
        )
        if not button_center:
            return None

        await page.mouse.click(button_center["x"], button_center["y"])
        credits = None
        for _ in range(10):
            await asyncio.sleep(0.5)
            credits = await page.evaluate(
                """() => {
                    const dialog = document.querySelector('dialog, [role="dialog"]');
                    if (!dialog) return null;
                    const link = dialog.querySelector('a[href*="ai%2Factivity"], a[href*="ai/activity"]');
                    if (link) {
                        const match = link.textContent.match(/([0-9,]+)/);
                        if (match) return parseInt(match[1].replace(/,/g, ''), 10);
                    }
                    for (const a of dialog.querySelectorAll('a')) {
                        if (a.textContent.includes('AI 点数')) {
                            const match = a.textContent.match(/([0-9,]+)/);
                            if (match) return parseInt(match[1].replace(/,/g, ''), 10);
                        }
                    }
                    return null;
                }"""
            )
            if credits is not None:
                break
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.3)
        return credits
    except Exception:
        try:
            await page.keyboard.press("Escape")
        except Exception:
            pass
        return None


async def ensure_account_ready(
    page: Any,
    *,
    email: str,
    account_collection: Any,
    redis_client: Any,
    settings: RuntimeSettings,
    logger: Any,
    remove_from_pool: Any,
) -> int:
    """确保账号登录态有效且 AI 点数充足。

    参数:
        page: 当前 Playwright 页面对象。
        email: 当前任务使用的账号邮箱。
        account_collection: Mongo 账号集合。
        redis_client: Redis 客户端。
        settings: 运行时配置对象。
        logger: 日志器对象。
        remove_from_pool: 从账号池移除异常账号的函数。

    返回:
        int: 当前账号可用的 AI 点数。
    """
    await page.goto(FLOW_HOME_URL, wait_until="domcontentloaded")
    current_url = page.url

    if LOGIN_EXPIRED_PATTERN.search(current_url) or "/signin" in current_url:
        remove_from_pool(redis_client, settings, email)
        mark_account_pending(account_collection, email, "登录态失效，访问 Flow 时被重定向到登录页")
        raise RuntimeError(f"账号 {email} 登录态失效")

    credits = await click_avatar_and_get_credits(page)
    if credits is None:
        raise RuntimeError(f"账号 {email} 无法读取 AI 点数")

    if credits < settings.min_credits_threshold:
        remove_from_pool(redis_client, settings, email)
        mark_account_abnormal(account_collection, email, f"AI 点数不足，当前剩余 {credits}")
        raise RuntimeError(f"账号 {email} AI 点数不足: {credits}")

    logger.info(f"账号 {email} 通过额度检查，当前 AI 点数为 {credits}")
    return int(credits)
