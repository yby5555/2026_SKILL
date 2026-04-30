"""
account_checker.py
===================
Flow 账号状态检测器（新建独立文件，不修改任何现有代码）

检测逻辑：
    1. 访问 https://labs.google/fx/zh/tools/flow
       - 若 URL 被重定向到 /api/auth/signin?error=Callback
         → 登录态失效，移除 Cookie，账号状态 → 0（重新登录）
       - 若成功进入 Flow 主页：
         → 点击头像弹窗，读取 AI 点数
         → 若点数 < MIN_CREDITS_THRESHOLD（默认 50）
           → 移除 Cookie，账号状态 → 3（额度不足）
         → 否则账号正常，不做任何改动

用法（独立运行，检测单个账号）：
    python account_checker.py email@gmail.com

用法（集成到其他脚本）：
    from account_checker import check_account_status
    result = await check_account_status(page, email, col)
"""

import asyncio
import re
import sys
from pathlib import Path
from typing import Any

# ── 路径修复 ──────────────────────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parent.parent   # 2026_SKILL/
_ACCOUNT_MGR = _ROOT / "account_mgr"

for _p in [str(_ROOT), str(_ACCOUNT_MGR)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)
# ──────────────────────────────────────────────────────────────────────────────

from mongo_utils import (                         # noqa: E402
    create_mongo_client, get_collection,
    mark_pending, mark_abnormal,
)
from redis_utils import remove_from_pool          # noqa: E402

# ── 常量 ──────────────────────────────────────────────────────────────────────
FLOW_HOME_URL       = "https://labs.google/fx/zh/tools/flow"
LOGIN_EXPIRED_PATTERN = re.compile(
    r"labs\.google/fx/api/auth/signin.*error=Callback", re.IGNORECASE
)
MIN_CREDITS_THRESHOLD = 20   # 低于此值视为额度不足

# 检测结果枚举
RESULT_OK            = "ok"            # 账号正常
RESULT_NO_CREDITS    = "no_credits"    # 额度不足 → status=2
RESULT_LOGIN_EXPIRED = "login_expired" # 登录态失效 → status=0
RESULT_ERROR         = "error"         # 检测过程出错


async def _click_avatar_and_get_credits(page) -> int | None:
    """
    点击右上角用户头像，从弹窗中读取 AI 点数。

    头像按钮查找策略（参考 automation_video_v2_click.py 的 filter+scoring 模式）：
        1. 扫描页面所有可见 button
        2. 只保留顶栏区域（y < 100px）且偏右侧（x > 60% 屏宽）的按钮
        3. 排除已知图标按钮（more_vert / help / close 等）
        4. 优先选有 googleusercontent 图片的（有头像照片）；次选最靠右的

    Returns:
        int: 当前 AI 点数，读取失败返回 None
    """
    # 已知非头像的图标按钮文字关键词
    EXCLUDE_KEYWORDS = [
        "more_vert", "help_outlined", "help", "close", "arrow_forward",
        "discord", "instagram", "twitter", "flow tv", "search", "filter",
    ]

    try:
        # ── 1. 单次 JS 调用定位头像按钮中心坐标 ──────────────────────────────
        # 所有 DOM 扫描在 JS 里一次完成，只返回坐标，避免每个按钮 N 次 await
        btn_center = await page.evaluate("""(excludeKeywords) => {
            // 方案 A：googleusercontent 图片头像（有头像照片的账号）
            let btn = document.querySelector(
                'button img[src*="googleusercontent"]'
            )?.closest('button');

            // 方案 B：字母头像账号（纯 JS 扫描，无多次 await 开销）
            if (!btn) {
                const W = window.innerWidth;
                let bestRight = -1;
                for (const b of document.querySelectorAll('button')) {
                    const rect = b.getBoundingClientRect();
                    if (!rect.width || !rect.height) continue;      // 不可见
                    if (rect.top  > 100)              continue;      // 不在顶栏
                    if (rect.right < W * 0.6)         continue;      // 不够靠右
                    const text = (b.innerText || '').trim().toLowerCase();
                    if (excludeKeywords.some(kw => text.includes(kw))) continue;
                    if (rect.right > bestRight) { bestRight = rect.right; btn = b; }
                }
            }

            if (!btn) return null;
            const r = btn.getBoundingClientRect();
            return { x: r.left + r.width / 2, y: r.top + r.height / 2 };
        }""", EXCLUDE_KEYWORDS)

        if not btn_center:
            print("[Checker] 未找到头像按钮（顶栏扫描无结果）")
            return None

        await page.mouse.click(btn_center["x"], btn_center["y"])

        # ── 2. 轮询等待弹窗出现，最多 5 秒 ─────────────────────────────────
        credits = None
        for _ in range(10):
            await asyncio.sleep(0.5)
            credits = await page.evaluate("""() => {
                const dialog = document.querySelector('dialog, [role="dialog"]');
                if (!dialog) return null;

                // href 中 / 被 URL 编码为 %2F
                const link = dialog.querySelector(
                    'a[href*="ai%2Factivity"], a[href*="ai/activity"]'
                );
                if (link) {
                    const m = link.textContent.match(/([0-9,]+)/);
                    if (m) return parseInt(m[1].replace(/,/g, ''), 10);
                }

                // 备用：找文本含 "AI 点数" 的 <a>
                for (const a of dialog.querySelectorAll('a')) {
                    if (a.textContent.includes('AI \u70b9\u6570')) {
                        const m = a.textContent.match(/([0-9,]+)/);
                        if (m) return parseInt(m[1].replace(/,/g, ''), 10);
                    }
                }
                return null;
            }""")
            if credits is not None:
                break

        print(f"[Checker] 读取到 AI 点数: {credits}")

        # ── 3. 关闭弹窗 ─────────────────────────────────────────────────────
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.3)

        return credits

    except Exception as e:
        print(f"[Checker] 读取额度异常: {e}")
        try:
            await page.keyboard.press("Escape")
        except Exception:
            pass
        return None


async def check_account_status(
    page,
    email: str,
    col,
    *,
    min_credits: int = MIN_CREDITS_THRESHOLD,
) -> dict[str, Any]:
    """
    检测单个 Flow 账号状态。

    工作流程：
        1. 访问 Flow 首页
        2. 检测是否被重定向到登录页 → 登录态失效
        3. 点击头像读取 AI 点数 → 额度检测
        4. 根据结果更新 MongoDB 状态 + 移除 Redis Cookie

    Args:
        page:       Playwright Page 对象
        email:      账号邮箱（作为 Redis key 标识）
        col:        MongoDB Collection 对象
        min_credits: 最低可用额度阈值（默认 50）

    Returns:
        {
            "result":  "ok" | "no_credits" | "login_expired" | "error",
            "email":   str,
            "credits": int | None,
            "message": str,
        }
    """
    print(f"[Checker] 开始检测账号: {email}")

    try:
        # ── 步骤 1：访问 Flow 首页 ────────────────────────────────────────────
        await page.goto(FLOW_HOME_URL, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(2)

        current_url = page.url
        print(f"[Checker] 当前 URL: {current_url}")

        # ── 步骤 2：检测登录态是否失效 ───────────────────────────────────────
        if LOGIN_EXPIRED_PATTERN.search(current_url) or "/signin" in current_url:
            print(f"[Checker] ⚠ 账号 {email} 登录态已失效，URL: {current_url}")
            # 移除 Redis Cookie
            remove_from_pool(email)
            # MongoDB 状态 → 0（待重新登录）
            mark_pending(col, email, "登录态失效，访问 Flow 被重定向到登录页")
            return {
                "result":  RESULT_LOGIN_EXPIRED,
                "email":   email,
                "credits": None,
                "message": f"登录态失效，重定向到: {current_url}",
            }

        # 确认在 Flow 主页
        if "labs.google" not in current_url:
            return {
                "result":  RESULT_ERROR,
                "email":   email,
                "credits": None,
                "message": f"意外跳转到未知页面: {current_url}",
            }

        # ── 步骤 3：点击头像，读取 AI 点数 ───────────────────────────────────
        credits = await _click_avatar_and_get_credits(page)
        print(f"[Checker] 账号 {email} 当前 AI 点数: {credits}")

        if credits is None:
            return {
                "result":  RESULT_ERROR,
                "email":   email,
                "credits": None,
                "message": "无法读取 AI 点数（DOM 结构可能已变更）",
            }

        # ── 步骤 4：额度判断 ──────────────────────────────────────────────────
        if credits < min_credits:
            print(f"[Checker] ✗ 账号 {email} 额度不足（{credits} < {min_credits}），移除 Cookie")
            # 移除 Redis Cookie
            remove_from_pool(email)
            # MongoDB 状态 → 2（账号异常：额度不足）
            mark_abnormal(col, email, f"AI 点数不足，当前剩余: {credits}")
            return {
                "result":  RESULT_NO_CREDITS,
                "email":   email,
                "credits": credits,
                "message": f"AI 点数不足（{credits} < {min_credits}），status → 2",
            }

        print(f"[Checker] ✓ 账号 {email} 状态正常，剩余 {credits} AI 点数")
        return {
            "result":  RESULT_OK,
            "email":   email,
            "credits": credits,
            "message": f"账号正常，剩余 {credits} AI 点数",
        }

    except Exception as e:
        print(f"[Checker] 检测异常: {e}")
        return {
            "result":  RESULT_ERROR,
            "email":   email,
            "credits": None,
            "message": f"检测过程异常: {e}",
        }


# ═══════════════════════════════════════════════════════════════════════════════
# 批量检测（供调度器调用）
# ═══════════════════════════════════════════════════════════════════════════════

async def check_all_active_accounts(
    *,
    min_credits: int = MIN_CREDITS_THRESHOLD,
    headless: bool = True,
) -> list[dict[str, Any]]:
    """
    批量检测所有 status=1（active）的账号。
    每个账号独立启动一个 Playwright 页面，顺序执行。

    Args:
        min_credits: 最低可用额度阈值
        headless:    是否无头模式

    Returns:
        每个账号的检测结果列表
    """
    from playwright.async_api import async_playwright

    client = create_mongo_client()
    col = get_collection(client)

    # 取所有 active 账号
    active_accounts = list(col.find({"status": 1}, {"_id": 0, "email": 1}))
    if not active_accounts:
        print("[Checker] 没有 status=1 的活跃账号")
        client.close()
        return []

    print(f"[Checker] 共 {len(active_accounts)} 个活跃账号需检测")
    results = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless)
        for account in active_accounts:
            email = account["email"]
            context = await browser.new_context()
            page = await context.new_page()
            try:
                result = await check_account_status(page, email, col, min_credits=min_credits)
                results.append(result)
            finally:
                await context.close()
        await browser.close()

    client.close()

    # 打印汇总
    ok     = sum(1 for r in results if r["result"] == RESULT_OK)
    nc     = sum(1 for r in results if r["result"] == RESULT_NO_CREDITS)
    le     = sum(1 for r in results if r["result"] == RESULT_LOGIN_EXPIRED)
    err    = sum(1 for r in results if r["result"] == RESULT_ERROR)
    print(f"\n[Checker] 检测完成: 正常={ok} | 额度不足={nc} | 登录失效={le} | 错误={err}")
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# 命令行入口（直接运行检测指定账号）
# ═══════════════════════════════════════════════════════════════════════════════

async def _cli_main():
    from playwright.async_api import async_playwright

    target_email = sys.argv[1] if len(sys.argv) > 1 else None

    client = create_mongo_client()
    col    = get_collection(client)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)

        if target_email:
            accounts = [{"email": target_email}]
        else:
            accounts = list(col.find({"status": 1}, {"_id": 0, "email": 1}))

        for account in accounts:
            context = await browser.new_page()
            result  = await check_account_status(context, account["email"], col)
            print(f"结果: {result}")
            await context.close()

        await browser.close()

    client.close()


if __name__ == "__main__":
    asyncio.run(_cli_main())
