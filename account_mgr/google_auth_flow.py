"""
Google 自动登录并获取 Flow Cookie 的完整方案

基于 zdh_base 框架实现：
1. 自动登录 Google 账号（支持 TOTP 2FA）
2. 使用 Google Cookie 访问 Flow 应用
3. 自动处理 Flow 授权流程
4. 提取并保存 NextAuth Cookie 到 cookies.txt
"""

# ==================== 配置区域 - 在这里修改你的账号信息 ====================
GOOGLE_EMAIL = "s5524h24h772@mubanima26.sbs"
GOOGLE_PASSWORD = "!9U@jXKn"
GOOGLE_TOTP_KEY = "aoks5pv5ekncg6ycbau3l2f7i4pa36e4"  # 2FA 验证密钥（如果没有2FA可留空）
# {"email": "s5524h24h772@mubanima26.sbs", "password": "!9U@jXKn"},

# {"email": "EnnalscrAvey@gmail.com",      "password": "edtc6qn23",   "totp_key": "aoks5pv5ekncg6ycbau3l2f7i4pa36e4"},


OUTPUT_DIR = "D:/kuanghu-poc/flow"  # Cookie 保存目录
HEADLESS = False  # 是否无头模式运行（True=无头，False=有头）
# ============================================================================

# from __future__ import annotations

import asyncio
import json
import sys
import hmac
import hashlib
import base64
import struct
import time
import random
from pathlib import Path
from typing import Any

# 添加项目路径 - 直接添加 session_context_base 的目录
# _base_dir = Path(__file__).parent.parent / "zdh_base"
# if str(_base_dir) not in sys.path:
#     sys.path.insert(0, str(_base_dir))

# 直接导入 session_context_base（绕过 zdh_base.__init__ 的导入问题）
from session_context_base import BaseSessionContextScraper, load_cookies
from playwright.async_api import Page, BrowserContext, Browser


# ==================== 拟人化工具类 ====================

class HumanLikeTyper:
    """拟人化输入工具 - 模拟真实人类的输入行为"""

    def __init__(self, base_delay_ms: int = 80, variance_ms: int = 40):
        """
        参数:
            base_delay_ms: 基础输入延迟（毫秒）
            variance_ms: 延迟变化范围（毫秒）
        """
        self.base_delay = base_delay_ms / 1000
        self.variance = variance_ms / 1000

    def get_delay(self) -> float:
        """获取随机延迟时间"""
        return self.base_delay + random.uniform(-self.variance, self.variance)

    async def type(self, page, selector: str, text: str, clear_first: bool = True) -> None:
        """
        拟人化输入文字

        参数:
            page: Playwright 页面对象
            selector: 元素选择器
            text: 要输入的文字
            clear_first: 是否先清空输入框
        """
        element = await page.wait_for_selector(selector, timeout=10000)
        await element.click()

        if clear_first:
            # 模拟 Ctrl+A 全选，然后删除
            await page.keyboard.down('Control')
            await page.keyboard.press('a')
            await page.keyboard.up('Control')
            await asyncio.sleep(random.uniform(0.05, 0.15))
            await page.keyboard.press('Backspace')
            await asyncio.sleep(random.uniform(0.05, 0.1))

        # 逐字输入，每个字之间有随机延迟
        for i, char in enumerate(text):
            await element.type(char, delay=random.randint(50, 150))
            # 偶尔添加额外的停顿，模拟思考
            if i > 0 and random.random() < 0.1:  # 10% 概率停顿
                await asyncio.sleep(random.uniform(0.1, 0.3))

    async def fill_with_random_delay(self, element, text: str) -> None:
        """使用 fill 方法但带有随机延迟"""
        await element.click()
        await asyncio.sleep(random.uniform(0.1, 0.3))

        # 分段输入，更接近人类行为
        chunks = self._split_text_randomly(text)
        for chunk in chunks:
            await element.type(chunk)
            await asyncio.sleep(random.uniform(0.05, 0.2))

    def _split_text_randomly(self, text: str) -> list[str]:
        """随机分割文本为多个块"""
        if len(text) <= 3:
            return [text]

        chunks = []
        remaining = text
        while remaining:
            # 随机决定块大小（1-4个字符）
            chunk_size = random.randint(1, min(4, len(remaining)))
            chunks.append(remaining[:chunk_size])
            remaining = remaining[chunk_size:]
        return chunks


class HumanLikeClicker:
    """拟人化点击工具 - 模拟真实人类的点击行为"""

    @staticmethod
    async def click(page, selector: str, wait_before: float = None) -> None:
        """
        拟人化点击

        参数:
            page: Playwright 页面对象
            selector: 元素选择器
            wait_before: 点击前等待时间（秒），如果为 None 则随机生成
        """
        element = await page.wait_for_selector(selector, timeout=10000)

        # 点击前的随机等待
        if wait_before is None:
            wait_before = random.uniform(0.3, 1.0)

        await asyncio.sleep(wait_before)

        # 模拟鼠标移动到元素位置
        box = await element.bounding_box()
        if box:
            # 随机偏移点击位置，模拟不精确的点击
            x = box['x'] + box['width'] / 2 + random.uniform(-5, 5)
            y = box['y'] + box['height'] / 2 + random.uniform(-5, 5)
            await page.mouse.move(x, y)
            await asyncio.sleep(random.uniform(0.05, 0.15))

        await element.click()


class HumanActionSimulator:
    """综合拟人化行为模拟器"""

    def __init__(self):
        self.typer = HumanLikeTyper()
        self.clicker = HumanLikeClicker()

    async def random_pause(self, min_sec: float = 0.5, max_sec: float = 2.0) -> None:
        """随机暂停"""
        await asyncio.sleep(random.uniform(min_sec, max_sec))

    async def simulate_reading(self, word_count: int = 50) -> None:
        """模拟阅读时间 - 基于字数计算"""
        # 人类阅读速度约为 200-300 字/分钟
        read_time = word_count / random.uniform(200, 300) * 60
        await asyncio.sleep(read_time)

    async def simulate_thinking(self) -> None:
        """模拟思考"""
        await asyncio.sleep(random.uniform(0.8, 2.0))


class TOTPGenerator:
    """TOTP (Time-based One-Time Password) 生成器"""

    def __init__(self, secret: str, digits: int = 6, interval: int = 30):
        self.secret = self._normalize_secret(secret)
        self.digits = digits
        self.interval = interval

    @staticmethod
    def _normalize_secret(secret: str) -> bytes:
        secret = secret.strip().replace(' ', '').replace('\n', '').replace('\r', '').upper()
        padding = len(secret) % 8
        if padding:
            secret += '=' * (8 - padding)
        return base64.b32decode(secret, casefold=True)

    def generate(self) -> str:
        counter = int(time.time()) // self.interval
        counter_bytes = struct.pack('>Q', counter)
        hmac_hash = hmac.new(self.secret, counter_bytes, hashlib.sha1).digest()
        offset = hmac_hash[-1] & 0x0f
        code = struct.unpack('>I', hmac_hash[offset:offset + 4])[0]
        code &= 0x7fffffff
        code %= 10 ** self.digits
        return f"{code:0{self.digits}d}"


class GoogleAuthScraper(BaseSessionContextScraper):
    """
    Google 账号自动登录爬虫（拟人化版本）

    功能：
    1. 拟人化自动登录 Google 账号
    2. 处理 TOTP 两步验证
    3. 保存 Google Cookie
    """

    def __init__(self, **kwargs):
        super().__init__(
            headless=False,  # 默认有头模式，方便调试
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            navigation_timeout_ms=30000,
            solve_cloudflare=True,
            **kwargs
        )
        self.human = HumanActionSimulator()

    async def login_google(
        self,
        email: str,
        password: str,
        totp_key: str | None = None,
    ) -> dict[str, Any]:
        """
        登录 Google 账号（拟人化版本）

        返回: 登录结果和 Cookie
        """
        context = await self.create_context()
        page = await context.new_page()
        page.set_default_navigation_timeout(30000)
        page.set_default_timeout(30000)

        try:
            print("[INFO] 开始登录流程...")

            # 1. 访问登录页面
            await page.goto("https://accounts.google.com/signin", wait_until="networkidle")
            await self.human.random_pause(1.0, 2.0)

            # 2. 输入邮箱（拟人化）
            print("[INFO] 输入邮箱...")
            email_input = await page.wait_for_selector('input[type="email"], input[name="identifier"]', timeout=15000)

            # 使用拟人化输入
            await self.human.typer.type(page, 'input[type="email"], input[name="identifier"]', email)
            await self.human.random_pause(0.5, 1.5)

            # 点击"下一步"
            await self.human.clicker.click(page, 'button:has-text("下一步"), button:has-text("Next"), #identifierNext')
            print("[INFO] 已点击下一步（邮箱）")

            # 等待跳转
            await page.wait_for_load_state("networkidle", timeout=10000)
            await self.human.random_pause(1.0, 2.0)

            # 3. 输入密码（拟人化）
            print("[INFO] 输入密码...")
            password_input = await page.wait_for_selector('input[type="password"]', timeout=15000)

            await self.human.typer.type(page, 'input[type="password"]', password)
            await self.human.random_pause(0.5, 1.5)

            # 点击"下一步"
            await self.human.clicker.click(page, 'button:has-text("下一步"), button:has-text("Next"), #passwordNext')
            print("[INFO] 已点击下一步（密码）")

            # 等待页面跳转
            await self.human.random_pause(8.0, 15.0)

            # 4. 处理 2FA（如果有）
            if totp_key:
                await self.human.random_pause(2.0, 3.0)

                # 尝试多种方式查找 2FA 输入框
                totp_input = None
                selectors = [
                    'input[type="tel"]',
                    'input[type="text"][name="Pin"]',
                    'input[aria-label*="验证码"]',
                    'input[name="Pin"]',
                    'input[id*="totp"]',
                    'input[id*="Pin"]',
                ]

                for selector in selectors:
                    try:
                        totp_input = await page.wait_for_selector(selector, timeout=3000)
                        if totp_input:
                            break
                    except:
                        continue

                if totp_input:
                    # 生成验证码
                    totp = TOTPGenerator(totp_key)
                    code = totp.generate()
                    print(f"[INFO] Generated TOTP: {code}")

                    # 拟人化输入验证码
                    await self.human.typer.type(page, selectors[0], code)
                    await self.human.random_pause(0.5, 1.5)

                    # 尝试多种方式点击下一步
                    next_clicked = False
                    next_selectors = [
                        'button:has-text("下一步")',
                        'button:has-text("Next")',
                        '#totpNext',
                        'button[type="submit"]',
                    ]

                    for next_sel in next_selectors:
                        try:
                            btn = await page.query_selector(next_sel)
                            if btn and await btn.is_visible():
                                await self.human.clicker.click(page, next_sel)
                                next_clicked = True
                                break
                        except:
                            continue

                    if not next_clicked:
                        # 尝试按 Enter 键
                        await page.keyboard.press('Enter')

                    await self.human.random_pause(2.0, 3.0)
                else:
                    print("[INFO] TOTP input not found, may not need 2FA")

            # 5. 验证登录成功
            await self.human.random_pause(2.0, 3.0)

            try:
                print("[INFO] Refreshing page before login verification...")
                await page.reload(wait_until="domcontentloaded")
                await self.human.random_pause(1.0, 2.0)
            except Exception as e:
                print(f"[WARN] Page refresh failed, continuing verification: {e}")

            current_url = page.url
            print(f"[INFO] Current URL after login: {current_url}")

            success = any([
                "myaccount.google.com" in current_url,
                "accounts.google.com/ManageAccount" in current_url,
            ])

            # 获取所有 Google Cookie
            cookies = await context.cookies()

            # 过滤 Google 相关的 Cookie
            # 注意：排除 PSIDRTS（会话轮转短期令牌，仅 ~10 分钟有效期，保存后必定过期）
            # 注意：排除地区域名 cookie（如 .google.com.br），只保留主域 .google.com
            _SKIP_COOKIE_NAMES = {'__Secure-1PSIDRTS', '__Secure-3PSIDRTS'}
            _SKIP_DOMAIN_SUFFIXES = ('.google.com.br', '.google.co.jp', '.google.co.uk',
                                     '.google.de', '.google.fr', '.google.com.au')
            google_cookies = [
                c for c in cookies
                if any(domain in c.get('domain', '') for domain in ['google.com', 'googleapis.com', 'googleusercontent.com', 'google-analytics.com'])
                and c.get('name', '') not in _SKIP_COOKIE_NAMES
                and not any(c.get('domain', '').endswith(suffix) for suffix in _SKIP_DOMAIN_SUFFIXES)
            ]

            return {
                "success": success,
                "email": email,
                "url": current_url,
                "cookies": google_cookies,
            }

        finally:
            await page.close()
            await context.close()


class FlowCookieExtractor(BaseSessionContextScraper):
    """
    Flow Cookie 提取器（拟人化版本）

    使用 Google Cookie 访问 Flow 应用，拟人化处理授权流程，提取 NextAuth Cookie
    """

    def __init__(self, **kwargs):
        super().__init__(
            headless=False,
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            navigation_timeout_ms=30000,
            **kwargs
        )
        self.human = HumanActionSimulator()

    async def extract_flow_cookies(
        self,
        google_cookies: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """
        使用 Google Cookie 提取 Flow NextAuth Cookie（拟人化版本）

        参数:
            google_cookies: Google 登录后的 Cookie 列表

        返回: 提取结果和 Flow Cookie
        """
        # 创建带有 Google Cookie 的上下文
        context = await self.create_context()
        page = await context.new_page()
        page.set_default_navigation_timeout(30000)
        page.set_default_timeout(30000)

        try:
            # 先注入 Google Cookie
            await context.add_cookies(google_cookies)

            # 访问 Flow 应用
            print("[INFO] 访问 Flow 应用...")
            await page.goto("https://labs.google/fx/zh/tools/flow", wait_until="domcontentloaded")

            # 等待页面加载（拟人化）
            await self.human.random_pause(2.0, 3.0)

            # 尝试点击 "Create with Flow" 或 "Get Started" 按钮进入应用
            try:
                create_btn = await page.wait_for_selector('button:has-text("Create with Flow"), button:has-text("Get Started")', timeout=5000)
                if create_btn:
                    await self.human.clicker.click(page, 'button:has-text("Create with Flow"), button:has-text("Get Started")')
                    await self.human.random_pause(1.0, 2.0)
            except:
                pass

            # 自动处理所有对话框
            print("[INFO] 处理 Flow 弹窗...")
            await self._auto_handle_dialogs(page)

            # 等待进入应用（检查是否有输入框）
            try:
                await page.wait_for_selector('[role="textbox"], textarea, [contenteditable="true"]', timeout=10000)
                in_app = True
            except:
                in_app = False

            # 额外等待，确保 NextAuth Cookie 被设置
            await self.human.random_pause(3.0, 5.0)

            # 获取所有 Cookie
            all_cookies = await context.cookies()

            # 调试：打印 NextAuth 相关 Cookie
            nextauth_debug = [c for c in all_cookies if 'next-auth' in c.get('name', '').lower() or 'session' in c.get('name', '').lower()]
            print(f"[DEBUG] NextAuth related cookies: {len(nextauth_debug)}")
            for c in nextauth_debug:
                print(f"  - {c.get('name')}: {c.get('value')[:50]}..." if len(c.get('value', '')) > 50 else f"  - {c.get('name')}: {c.get('value')}")

            # 过滤 Flow 相关的 Cookie (labs.google 域名)
            flow_cookies = [
                c for c in all_cookies
                if 'labs.google' in c.get('domain', '') or 'labs-google' in c.get('domain', '')
            ]

            # 重点检查 NextAuth 相关 Cookie
            nextauth_cookies = [
                c for c in all_cookies
                if any(name in c.get('name', '') for name in ['next-auth', 'NEXT_AUTH'])
            ]

            return {
                "success": in_app or len(nextauth_cookies) > 0,
                "in_app": in_app,
                "flow_cookies": flow_cookies,
                "nextauth_cookies": nextauth_cookies,
                "all_cookies": all_cookies,
                "url": page.url,
            }

        finally:
            await page.close()
            await context.close()

    async def _auto_handle_dialogs(self, page: Page) -> None:
        """
        自动处理 Flow 的各种对话框

        首次访问时的弹窗流程：
        1. "Flow 最新更新" - 点击"开始使用"
        2. "体验 AI 工具的创造力" - 点击"下一步"
        3. "隐私权政策" - 必须滚动到底部才能点击"继续"

        关键：隐私权政策弹窗需要真正滚动到底部，否则下次访问还会弹出！
        """
        max_attempts = 20
        attempt = 0

        while attempt < max_attempts:
            await asyncio.sleep(1)

            # 检查是否有对话框
            dialog_info = await page.evaluate("""
                () => {
                    const dialog = document.querySelector('dialog[open], [role="dialog"]');
                    if (!dialog) return { has_dialog: false };

                    // 获取对话框内容判断是哪种弹窗
                    const text = dialog.innerText || '';
                    const hasPrivacyPolicy = text.includes('隐私权政策') || 
                                             text.includes('Privacy') || 
                                             text.includes('Review our privacy policy') ||
                                             text.includes('Your data and Google Flow');
                    const hasExperienceAI = text.includes('体验 AI 工具') || 
                                            text.includes('labs.google/fx') ||
                                            text.includes('Experience and shape AI');
                    const hasChangelog = text.includes('最新更新') || 
                                         text.includes('Share your creations') ||
                                         text.includes('Latest updates') ||
                                         text.includes("What's new");

                    const buttons = Array.from(dialog.querySelectorAll('button'));
                    const findBtn = (txts) => buttons.find(b => txts.some(t => b.textContent.includes(t)));

                    const continueBtn = findBtn(['继续', 'Continue', 'Accept', 'I agree']);
                    const nextBtn = findBtn(['下一步', 'Next']);
                    const startBtn = findBtn(['开始使用', 'Get Started', 'Get started']);

                    return {
                        has_dialog: true,
                        hasPrivacyPolicy,
                        hasExperienceAI,
                        hasChangelog,
                        hasContinueBtn: !!continueBtn,
                        hasNextBtn: !!nextBtn,
                        hasStartBtn: !!startBtn,
                        scrollHeight: dialog.scrollHeight,
                        scrollTop: dialog.scrollTop
                    };
                }
            """)

            if not dialog_info['has_dialog']:
                # 检查是否进入应用
                in_app = await page.evaluate("""
                    () => {
                        const hasInput = !!(
                            document.querySelector('[role="textbox"]') ||
                            document.querySelector('textarea') ||
                            document.querySelector('[contenteditable="true"]')
                        );
                        const buttons = Array.from(document.querySelectorAll('button'));
                        const hasCreateBtn = buttons.some(b => b.textContent.includes('新建项目') || b.textContent.includes('Create'));
                        return hasInput || hasCreateBtn;
                    }
                """)
                if in_app:
                    break

            # 根据弹窗类型处理
            if dialog_info.get('hasChangelog') or dialog_info.get('hasStartBtn'):
                # 第一个弹窗：最新更新 - 点击"开始使用"
                # 添加拟人化延迟
                await self.human.random_pause(0.5, 1.5)
                await page.evaluate("""
                    () => {
                        const dialog = document.querySelector('dialog[open], [role="dialog"]');
                        if (!dialog) return;
                        const buttons = Array.from(dialog.querySelectorAll('button'));
                        const btn = buttons.find(b => 
                            b.textContent.includes('开始使用') || 
                            b.textContent.includes('Get Started') || 
                            b.textContent.includes('Get started') || 
                            b.textContent.includes('Next') ||
                            b.textContent.includes('下一步')
                        );
                        if (btn) {
                            btn.disabled = false;
                            btn.click();
                        }
                    }
                """)

            elif dialog_info.get('hasExperienceAI') or dialog_info.get('hasNextBtn'):
                # 第二个弹窗：体验 AI 工具 - 点击"下一步"
                # 添加拟人化延迟
                await self.human.random_pause(0.5, 1.5)
                await page.evaluate("""
                    () => {
                        const dialog = document.querySelector('dialog[open], [role="dialog"]');
                        if (!dialog) return;
                        const buttons = Array.from(dialog.querySelectorAll('button'));
                        const btn = buttons.find(b => 
                            b.textContent.includes('下一步') || 
                            b.textContent.includes('Next') ||
                            b.textContent.includes('Continue') ||
                            b.textContent.includes('继续')
                        );
                        if (btn) {
                            btn.disabled = false;
                            btn.click();
                        }
                    }
                """)
                await asyncio.sleep(1)  # 等待下一个弹窗出现

            elif dialog_info.get('hasPrivacyPolicy'):
                # 第三个弹窗：隐私权政策 - 必须滚动到底部！
                # 关键发现：需要滚动的是 className 包含 "sc-defdda7d-2" 的元素
                # 添加拟人化阅读延迟
                await self.human.simulate_reading(100)

                scroll_result = await page.evaluate("""
                    () => {
                        const dialog = document.querySelector('dialog[open], [role="dialog"]');
                        if (!dialog) return { scrolled: false, elementFound: false };

                        // 查找滚动容器 - Flow 使用 styled-components，className 格式为 "sc-defdda7d-2 XXXX"
                        let scrollableDiv = dialog.querySelector('[class*="sc-defdda7d-2"]');

                        if (!scrollableDiv) {
                            // 备用方法：查找所有 div，找出可滚动的那个
                            const allDivs = dialog.querySelectorAll('div');
                            for (const div of allDivs) {
                                const style = window.getComputedStyle(div);
                                if ((style.overflowY === 'scroll' || style.overflow === 'hidden scroll') &&
                                    div.scrollHeight > div.clientHeight) {
                                    scrollableDiv = div;
                                    break;
                                }
                            }
                        }

                        if (!scrollableDiv) {
                            return { scrolled: false, elementFound: false, reason: 'no scrollable div' };
                        }

                        const scrollHeight = scrollableDiv.scrollHeight;
                        const clientHeight = scrollableDiv.clientHeight;
                        const currentScroll = scrollableDiv.scrollTop;

                        // 检查是否已经到底部
                        const isAtBottom = currentScroll >= scrollHeight - clientHeight - 10;

                        if (!isAtBottom) {
                            // 渐进式滚动到底部 - 模拟人类分段阅读滚动
                            const scrollDistance = scrollHeight - currentScroll;
                            const scrollStep = Math.max(100, Math.floor(scrollDistance / 15));  // 分更多步，更平滑

                            // 分15次渐进滚动，模拟阅读
                            for (let i = 0; i < 15; i++) {
                                scrollableDiv.scrollTop = Math.min(scrollableDiv.scrollTop + scrollStep, scrollHeight);
                                scrollableDiv.dispatchEvent(new Event('scroll', { bubbles: true }));
                                scrollableDiv.dispatchEvent(new WheelEvent('wheel', { deltaY: scrollStep, bubbles: true, cancelable: true }));
                            }

                            // 最后确保滚动到绝对底部
                            scrollableDiv.scrollTop = scrollHeight;
                        }

                        // 触发滚动事件
                        scrollableDiv.dispatchEvent(new Event('scroll', { bubbles: true }));
                        scrollableDiv.dispatchEvent(new WheelEvent('wheel', { deltaY: 100 }));

                        return {
                            scrolled: true,
                            elementFound: true,
                            className: scrollableDiv.className,
                            scrollHeight,
                            clientHeight,
                            finalScrollTop: scrollableDiv.scrollTop,
                            atBottom: scrollableDiv.scrollTop >= scrollHeight - clientHeight - 10
                        };
                    }
                """)

                if scroll_result.get('scrolled') and scroll_result.get('atBottom'):
                    # 已滚动到底部，等待系统检测 - 添加拟人化延迟
                    await self.human.random_pause(1.0, 2.0)

                    # 点击"继续"按钮
                    await page.evaluate("""
                        () => {
                            const dialog = document.querySelector('dialog[open], [role="dialog"]');
                            if (!dialog) return;

                            const buttons = Array.from(dialog.querySelectorAll('button'));
                            const continueBtn = buttons.find(b => 
                                b.textContent.includes('继续') || 
                                b.textContent.includes('Continue') ||
                                b.textContent.includes('Accept') ||
                                b.textContent.includes('I agree')
                            );

                            if (continueBtn) {
                                continueBtn.disabled = false;
                                continueBtn.click();
                            }
                        }
                    """)
                    await asyncio.sleep(2)  # 等待对话框关闭
                else:
                    # 还没完成滚动，继续下一次循环
                    await asyncio.sleep(0.3)

            attempt += 1

        # 最后检查并强制移除所有对话框
        await page.evaluate("""
            () => {
                const dialogs = document.querySelectorAll('dialog[open], [role="dialog"]');
                dialogs.forEach(d => {
                    try { d.close(); } catch(e) {}
                    try { d.remove(); } catch(e) {}
                });
            }
        """)


def save_cookies_to_file(cookies: list[dict[str, Any]], output_path: Path) -> None:
    """
    保存 Cookie 到文件

    保存为 JSON 格式（与 automation.py 兼容）
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # 保存为 JSON 格式
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(cookies, f, indent=2, ensure_ascii=False)


async def full_auto_flow(
    email: str,
    password: str,
    totp_key: str | None = None,
    output_dir: str | Path = "D:/kuanghu-poc/flow",
) -> dict[str, Any]:
    """
    完整的自动化流程：
    1. 登录 Google
    2. 使用 Google Cookie 访问 Flow
    3. 提取并保存 Flow Cookie

    返回: 执行结果
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    result = {
        "google_login": None,
        "flow_extraction": None,
        "success": False,
        "cookie_file": None,
    }

    # 步骤 1: 登录 Google
    print("=" * 50)
    print("步骤 1: 登录 Google 账号...")
    print("=" * 50)

    google_scraper = GoogleAuthScraper()
    await google_scraper.start()

    try:
        google_result = await google_scraper.login_google(email, password, totp_key)
        result["google_login"] = google_result

        if not google_result["success"]:
            print(f"[FAIL] Google 登录失败: {google_result.get('url')}")
            return result

        print(f"[OK] Google 登录成功!")
        print(f"   获取到 {len(google_result['cookies'])} 个 Google Cookie")

        # 保存 Google Cookie（备用）
        google_cookie_path = output_dir /'gogle_cookie'/ f"{GOOGLE_EMAIL}.json"
        with open(google_cookie_path, 'w', encoding='utf-8') as f:
            json.dump(google_result["cookies"], f, indent=2)
        print(f"   Google Cookie 已保存到: {google_cookie_path}")

    finally:
        await google_scraper.close()

    # 步骤 2: 访问 Flow 并提取 Cookie
    print("\n" + "=" * 50)
    print("步骤 2: 访问 Flow 应用并提取 Cookie...")
    print("=" * 50)

    flow_scraper = FlowCookieExtractor()
    await flow_scraper.start()

    try:
        flow_result = await flow_scraper.extract_flow_cookies(google_result["cookies"])
        result["flow_extraction"] = flow_result

        if flow_result["success"]:
            print(f"[OK] 成功进入 Flow 应用!")
            print(f"   获取到 {len(flow_result['flow_cookies'])} 个 Flow Cookie")
            print(f"   获取到 {len(flow_result['nextauth_cookies'])} 个 NextAuth Cookie")

            # 保存所有 Cookie（用于 Flow 访问）
            all_flow_cookies = flow_result["all_cookies"]
            cookie_file = output_dir /'flow_cookie'/ f'{GOOGLE_EMAIL}.txt'
            save_cookies_to_file(all_flow_cookies, cookie_file)

            result["success"] = True
            result["cookie_file"] = str(cookie_file)
            print(f"\n[OK] Cookie 已保存到: {cookie_file}")

        else:
            print(f"[FAIL] Flow Cookie 提取失败")
            print(f"   当前 URL: {flow_result.get('url')}")

    finally:
        await flow_scraper.close()

    return result


async def main():
    """主入口 - 使用顶部配置区域的设置"""
    global GOOGLE_EMAIL, GOOGLE_PASSWORD, GOOGLE_TOTP_KEY, OUTPUT_DIR, HEADLESS

    print(f"使用账号: {GOOGLE_EMAIL}")
    print(f"输出目录: {OUTPUT_DIR}")
    print(f"运行模式: {'无头' if HEADLESS else '有头'}")
    print("=" * 50)

    # 如果需要无头模式，动态修改类初始化
    if HEADLESS:
        GoogleAuthScraper.__init__ = lambda self, **kw: BaseSessionContextScraper.__init__(
            self, headless=True, **kw
        )
        FlowCookieExtractor.__init__ = lambda self, **kw: BaseSessionContextScraper.__init__(
            self, headless=True, **kw
        )

    result = await full_auto_flow(
        email=GOOGLE_EMAIL,
        password=GOOGLE_PASSWORD,
        totp_key=GOOGLE_TOTP_KEY,
        output_dir=OUTPUT_DIR,
    )

    print("\n" + "=" * 50)
    print("执行结果")
    print("=" * 50)
    print(f"Google 登录: {'[OK] 成功' if result['google_login'] and result['google_login'].get('success') else '[FAIL] 失败'}")
    print(f"Flow Cookie: {'[OK] 成功' if result['success'] else '[FAIL] 失败'}")
    if result.get('cookie_file'):
        print(f"Cookie 文件: {result['cookie_file']}")
    print("=" * 50)

    return 0 if result["success"] else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
