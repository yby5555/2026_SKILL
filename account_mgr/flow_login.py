"""
account_mgr/flow_login.py

Google 登录 + Flow Cookie 一体化流程

核心优化：
    1. Google 登录完成后，直接在同一个 BrowserContext 中访问 Flow，
       不关闭浏览器再重新打开，节省 ~10s 启动时间和内存开销。
    2. 不保存任何 Cookie 文件，所有 Cookie 直接返回，由调度器推 Redis。
    3. 不修改 google_auth_flow.py 中的原始类，通过 import 复用其核心子程序。

依赖：
    google_auth_flow.py 需在 sys.path 可找到的目录中（即 goge_login/）
"""

import sys
import asyncio
from pathlib import Path
from typing import Any

# 确保能找到同包模块和 google_auth_flow.py
_pkg_dir        = Path(__file__).parent          # account_mgr/
_goge_login_dir = _pkg_dir.parent                # goge_login/

for p in [str(_pkg_dir), str(_goge_login_dir)]:
    if p not in sys.path:
        sys.path.insert(0, p)

# 复用原始文件中的类，不做任何修改
from google_auth_flow import (
    GoogleAuthScraper,
    FlowCookieExtractor,
    HumanActionSimulator,
)


async def login_and_get_flow_cookies(
    email: str,
    password: str,
    totp_key: str | None = None,
    headless: bool = True,
) -> dict[str, Any]:
    """
    一体化流程：Google 登录 → 同浏览器直接访问 Flow → 返回 Cookie

    与 google_auth_flow.full_auto_flow() 的区别：
        - 不保存任何文件（无 google_cookie / flow_cookie 文件落盘）
        - Google 登录后复用同一 context 访问 Flow，不关闭/重开浏览器
        - headless 参数动态控制，无需修改类定义

    Args:
        email:    Google 账号邮箱
        password: 账号密码
        totp_key: TOTP 2FA 密钥（无 2FA 则留 None）
        headless: True=无头（生产），False=有头（调试）

    Returns:
        {
            "success":           bool,
            "google_login":      {"success", "email", "url", "cookies"},
            "flow_extraction":   {"success", "all_cookies", "flow_cookies", "nextauth_cookies", "url"},
        }
    """
    result: dict[str, Any] = {
        "success":         False,
        "google_login":    None,
        "flow_extraction": None,
    }

    human = HumanActionSimulator()

    # ------------------------------------------------------------------ #
    #  步骤 1：Google 登录                                                  #
    # ------------------------------------------------------------------ #
    print("=" * 50)
    print(f"[{email}] 步骤 1 / 2：Google 登录...")
    print("=" * 50)

    # 动态注入 headless 参数（不修改原始类，使用子类覆盖 __init__）
    class _GoogleScraper(GoogleAuthScraper):
        def __init__(self):
            # 调用 BaseSessionContextScraper.__init__ 并传入 headless
            super(GoogleAuthScraper, self).__init__(
                headless=headless,
                locale="zh-CN",
                timezone_id="Asia/Shanghai",
                navigation_timeout_ms=30000,
                solve_cloudflare=True,
            )
            self.human = HumanActionSimulator()

    google_scraper = _GoogleScraper()
    await google_scraper.start()

    google_cookies: list[dict] = []

    try:
        google_result = await google_scraper.login_google(email, password, totp_key)
        result["google_login"] = google_result

        if not google_result["success"]:
            print(f"[FAIL] Google 登录失败: {google_result.get('url')}")
            return result

        google_cookies = google_result["cookies"]
        print(f"[OK] Google 登录成功，获取到 {len(google_cookies)} 个 Cookie")

        # ------------------------------------------------------------------ #
        #  步骤 2：复用同一浏览器实例直接访问 Flow                              #
        # ------------------------------------------------------------------ #
        print("\n" + "=" * 50)
        print(f"[{email}] 步骤 2 / 2：访问 Flow（复用同一浏览器）...")
        print("=" * 50)

        # 在同一个 scraper 实例下创建新 context，注入 Google Cookie
        context = await google_scraper.create_context()
        await context.add_cookies(google_cookies)

        page = await context.new_page()
        page.set_default_navigation_timeout(30000)
        page.set_default_timeout(30000)

        try:
            # 访问 Flow 应用
            print("[INFO] 访问 Flow 应用...")
            await page.goto(
                "https://labs.google/fx/zh/tools/flow",
                wait_until="domcontentloaded",
            )
            await human.random_pause(2.0, 3.0)

            # 尝试点击入口按钮
            try:
                btn = await page.wait_for_selector(
                    'button:has-text("Create with Flow"), button:has-text("Get Started")',
                    timeout=5000,
                )
                if btn:
                    await human.clicker.click(
                        page,
                        'button:has-text("Create with Flow"), button:has-text("Get Started")',
                    )
                    await human.random_pause(1.0, 2.0)
            except Exception:
                pass

            # 处理弹窗（复用原始类方法）
            flow_extractor = _FlowExtractor()
            await flow_extractor._auto_handle_dialogs(page)

            # 等待进入应用
            try:
                await page.wait_for_selector(
                    '[role="textbox"], textarea, [contenteditable="true"]',
                    timeout=10000,
                )
                in_app = True
            except Exception:
                in_app = False

            # 额外等待 NextAuth Cookie 被写入
            await human.random_pause(3.0, 5.0)

            all_cookies = await context.cookies()

            # 过滤 Flow Cookie
            flow_cookies = [
                c for c in all_cookies
                if "labs.google" in c.get("domain", "") or "labs-google" in c.get("domain", "")
            ]
            nextauth_cookies = [
                c for c in all_cookies
                if any(n in c.get("name", "") for n in ["next-auth", "NEXT_AUTH"])
            ]

            flow_result = {
                "success":          in_app or len(nextauth_cookies) > 0,
                "in_app":           in_app,
                "all_cookies":      all_cookies,
                "flow_cookies":     flow_cookies,
                "nextauth_cookies": nextauth_cookies,
                "url":              page.url,
            }
            result["flow_extraction"] = flow_result

            if flow_result["success"]:
                result["success"] = True
                print(f"[OK] Flow Cookie 获取成功 | "
                      f"all={len(all_cookies)} flow={len(flow_cookies)} nextauth={len(nextauth_cookies)}")
            else:
                print(f"[FAIL] Flow Cookie 获取失败，当前 URL: {page.url}")

        finally:
            await page.close()
            await context.close()

    finally:
        # 无论成功与否，关闭浏览器
        await google_scraper.close()

    return result


# ------------------------------------------------------------------ #
#  内部：薄封装 FlowCookieExtractor，仅为调用 _auto_handle_dialogs     #
# ------------------------------------------------------------------ #
class _FlowExtractor(FlowCookieExtractor):
    """仅用于调用 _auto_handle_dialogs，不启动新浏览器"""
    def __init__(self):
        # 跳过 BaseSessionContextScraper.__init__，只初始化 human
        self.human = HumanActionSimulator()


# ------------------------------------------------------------------ #
#  本地调试入口                                                         #
# ------------------------------------------------------------------ #
async def _debug():
    import sys
    sys.path.insert(0, str(_pkg_dir))

    # 直接填写调试账号
    EMAIL    = "EnnalscrAvey@gmail.com"
    PASSWORD = "edtc6qn23"
    TOTP     = "aoks5pv5ekncg6ycbau3l2f7i4pa36e4"

    result = await login_and_get_flow_cookies(
        email=EMAIL,
        password=PASSWORD,
        totp_key=TOTP,
        headless=False,   # 调试时有头
    )
    print("\n========== 结果 ==========")
    print(f"成功: {result['success']}")
    if result.get("flow_extraction"):
        fe = result["flow_extraction"]
        print(f"all_cookies 数量: {len(fe.get('all_cookies', []))}")
        print(f"nextauth 数量:    {len(fe.get('nextauth_cookies', []))}")


if __name__ == "__main__":
    asyncio.run(_debug())
