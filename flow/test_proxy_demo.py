"""代理验证 Demo：通过 GoogleFlowVideoScraperV2 同时打开 6 个 context，验证代理是否生效。

用法:
    python test_proxy_demo.py

运行后会启动浏览器池，创建 6 个带独立代理的 context，
每个 context 访问 httpbin.org/ip 显示当前出口 IP。
控制台 input() 断住，你可以切换到浏览器逐一查看。
按回车后关闭所有浏览器。
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.append(str(_ROOT))

_ACCOUNT_MGR = _ROOT / "account_mgr"
if str(_ACCOUNT_MGR) not in sys.path:
    sys.path.insert(0, str(_ACCOUNT_MGR))

from automation_video_v2_click_consumer import GoogleFlowVideoScraperV2

IP_CHECK_URL = "https://ip138.com/"

# 6 个 context = 3 浏览器 × 2 context/浏览器
BROWSER_POOL_SIZE = 1
CONTEXTS_PER_BROWSER = 1
TOTAL_CONTEXTS = BROWSER_POOL_SIZE * CONTEXTS_PER_BROWSER

# 全局事件：当所有 context 都打开后，等待用户按回车
_all_ready = asyncio.Event()
_stop = asyncio.Event()
_ready_count = 0
_ready_lock = asyncio.Lock()


class ProxyTestScraper(GoogleFlowVideoScraperV2):
    """仅用于测试代理的子类，覆盖 process_task 让页面保持打开。"""

    async def process_task(self, page, task_data: dict[str, Any], worker) -> dict[str, Any]:
        global _ready_count
        proxy = task_data.get("proxy", "无代理")
        # 只显示 IP 部分
        display_proxy = proxy.split("@")[-1] if "@" in str(proxy) else str(proxy)

        print(f"  [Worker {worker.worker_id}] 代理: {display_proxy}")
        print(f"  [Worker {worker.worker_id}] 正在访问 {IP_CHECK_URL} ...")

        try:
            await page.goto(IP_CHECK_URL, wait_until="domcontentloaded", timeout=30000)
            content = await page.text_content("body")
            print(f"  [Worker {worker.worker_id}] 页面返回 IP: {(content or '').strip()}")
        except Exception as exc:
            print(f"  [Worker {worker.worker_id}] 导航失败: {exc}")

        # 标记就绪
        async with _ready_lock:
            _ready_count += 1
            if _ready_count >= TOTAL_CONTEXTS:
                _all_ready.set()

        # 保持页面打开，等待用户按回车
        await _stop.wait()
        return {"status": "done"}


async def main() -> None:
    print("=" * 60)
    print("代理验证 Demo (使用 GoogleFlowVideoScraperV2)")
    print(f"浏览器数: {BROWSER_POOL_SIZE}, 每浏览器 context: {CONTEXTS_PER_BROWSER}")
    print(f"总 context 数: {TOTAL_CONTEXTS}")
    print("=" * 60)

    scraper = ProxyTestScraper(
        browser_pool_size=BROWSER_POOL_SIZE,
        max_contexts_per_browser=CONTEXTS_PER_BROWSER,
        headless=False,
        extra_flags=["--start-maximized"],
        viewport={"width": 0, "height": 0},
        task_timeout_ms=10 * 60 * 1000,  # 10 分钟足够手动检查
    )

    # 构建 6 个假任务，每个都会经过 normalize_task 获取独立代理
    fake_tasks = [
        {"_id": f"proxy-test-{i + 1}", "prompt": "test", "type": 1}
        for i in range(TOTAL_CONTEXTS)
    ]

    print(f"\n即将提交 {TOTAL_CONTEXTS} 个测试任务，每个任务会自动获取独立代理...")
    print("-" * 60)

    async with scraper:
        # 提交所有任务（异步并发执行）
        task_future = asyncio.create_task(scraper.run_tasks(fake_tasks))

        # 等待所有 context 就绪
        await _all_ready.wait()

        print()
        print("=" * 60)
        print("所有页面已打开！请切换到浏览器查看各页面的 IP 地址。")
        print("确认完毕后，在此处按回车关闭所有浏览器...")
        print("=" * 60)

        # 阻塞等待用户输入
        await asyncio.to_thread(input)

        # 通知所有 process_task 退出
        _stop.set()

        # 等待任务完成
        results = await task_future
        print(f"\n任务结果: {len(results)} 个已完成")

    print("所有浏览器已关闭")


if __name__ == "__main__":
    asyncio.run(main())
