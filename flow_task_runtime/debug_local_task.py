"""PyCharm 本地调试脚本。

功能说明：
1. 不从 Redis 任务队列获取任务
2. 直接在脚本内构造一个本地任务对象
3. 方便在 PyCharm 中右键运行、下断点、单步调试
4. 支持两种账号来源：
   - 方式 A：不提供 email/cookies，交给抓取器自动从 Redis cookie 池取
   - 方式 B：手动在 LOCAL_TASK 中填入 email 和 cookies，完全跳过 Redis 账号分配
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import sys
from pathlib import Path
from typing import Any

from playwright_stealth.stealth import Stealth

if __package__ in (None, ""):
    ROOT_DIR = Path(__file__).resolve().parent.parent
    if str(ROOT_DIR) not in sys.path:
        sys.path.insert(0, str(ROOT_DIR))
    from flow_task_runtime.config import load_settings
    from flow_task_runtime.logging_utils import get_logger
    from flow_task_runtime.scraper import CreditCheckedFlowScraper
    from flow_task_runtime.storage import create_account_collection, create_redis_client
else:
    from .config import load_settings
    from .logging_utils import get_logger
    from .scraper import CreditCheckedFlowScraper
    from .storage import create_account_collection, create_redis_client


class StealthCreditCheckedFlowScraper(CreditCheckedFlowScraper):
    """仅用于本地调试脚本的 stealth 版本抓取器。"""

    async def initialize_page(self, page: Any, task_data: dict[str, Any], worker: Any) -> None:
        """在业务逻辑执行前给页面注入 playwright_stealth。"""
        await super().initialize_page(page, task_data, worker)
        await Stealth().apply_stealth_async(page)
        try:
            fingerprint = await page.evaluate(
                """() => ({
                    userAgent: navigator.userAgent,
                    webdriver: navigator.webdriver,
                    languages: navigator.languages,
                    platform: navigator.platform,
                    plugins: navigator.plugins ? navigator.plugins.length : null,
                    hardwareConcurrency: navigator.hardwareConcurrency,
                    deviceMemory: navigator.deviceMemory || null,
                    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
                    viewport: { width: window.innerWidth, height: window.innerHeight },
                    screen: { width: screen.width, height: screen.height },
                })"""
            )
            self.logger.info(
                "[debug] browser fingerprint "
                f"worker={worker.worker_id}: {json.dumps(fingerprint, ensure_ascii=False)}"
            )
        except Exception as exc:
            self.logger.warning(f"[debug] browser fingerprint collection failed: {exc!r}")
        self.logger.info(f"[debug] playwright_stealth 已注入页面，worker={worker.worker_id}")

def read_debug_bool(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def is_recaptcha_unusual_activity(result: Any) -> bool:
    if not isinstance(result, Exception):
        return False
    text = f"{type(result).__name__}: {result}"
    return "reCAPTCHA evaluation failed" in text or "PUBLIC_ERROR_UNUSUAL_ACTIVITY" in text


def create_debug_scraper(
    *,
    settings: Any,
    redis_client: Any,
    account_collection: Any,
    logger: Any,
    headless: bool,
) -> StealthCreditCheckedFlowScraper:
    logger.info(f"[debug] creating scraper, headless={headless}")
    return StealthCreditCheckedFlowScraper(
        settings=settings,
        redis_client=redis_client,
        account_collection=account_collection,
        logger=logger,
        browser_pool_size=1,
        max_contexts_per_browser=1,
        headless=headless,
        extra_flags=[
            "--start-maximized",
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars",
            "--window-size=1920,1080",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--no-first-run",
        ],
        viewport={"width": 1920, "height": 1080},
        navigation_timeout_ms=settings.navigation_timeout_ms,
        task_timeout_ms=settings.task_timeout_ms,
        default_cookie_domain=".google.com",
        recycle_browser_after_tasks=20,
        recycle_browser_after_failures=3,
    )


LOCAL_TASK: dict[str, Any] = {
    "_id": "debug-local-task-001",  # 本地调试任务 ID
    "type": 1,  # 视频任务类型，固定为 1
    "prompt": "使用第一张图中的人物作为主角，结合第二张图的代码工作区，生成一段竖版视频：人物在现代办公室里一边做轻度有氧动作一边持续编程，镜头稳定，动作自然，适合短视频展示。",  # 本地调试 prompt
    "image_value": [
        str(Path(__file__).resolve().parent.parent / "flow" / "otter.png"),  # 第一张参考图
        str(Path(__file__).resolve().parent.parent / "flow" / "videos" / "pending" / "image.png"),  # 第二张参考图
    ],
    "image_type": "",  # 留空表示自动识别图片类型
    "poll_timeout_ms": 8 * 60 * 1000,  # 本地调试轮询超时
    # 你如果想完全跳过 Redis cookie 池，可以手动加上这两个字段：
    # "email": "your_email@example.com",
    # "cookies": [...],
}


def normalize_local_task(task: dict[str, Any]) -> dict[str, Any]:
    """对本地调试任务做一层标准化，便于本地文件直接调试。

    功能说明：
    1. 对输入任务做浅拷贝，避免污染模块级常量
    2. 如果 `image_value` 中放的是本地文件路径，则自动读取文件并转成 Base64
    3. 最终把图片字段改写成抓取器稳定支持的 `image_base64` / `image_base64_list`

    参数:
        task: 原始本地调试任务字典。

    返回:
        dict[str, Any]: 标准化后的调试任务字典。
    """
    normalized_task = dict(task)
    raw_image_value = normalized_task.get("image_value")
    if raw_image_value in (None, "", []):
        return normalized_task

    raw_items = raw_image_value if isinstance(raw_image_value, list) else [raw_image_value]
    resolved_items: list[str] = []
    converted_all_local_files = True

    for raw_item in raw_items:
        item_text = str(raw_item)
        local_path = Path(item_text)
        if local_path.exists() and local_path.is_file():
            image_bytes = local_path.read_bytes()
            resolved_items.append(base64.b64encode(image_bytes).decode("utf-8"))
        else:
            converted_all_local_files = False
            break

    if converted_all_local_files and resolved_items:
        normalized_task.pop("image_value", None)
        normalized_task.pop("image_type", None)
        if len(resolved_items) == 1:
            normalized_task["image_base64"] = resolved_items[0]
        else:
            normalized_task["image_base64_list"] = resolved_items

    return normalized_task


async def run_local_debug_task_legacy() -> list[Any]:
    """执行本地调试任务并返回抓取结果列表。

    返回:
        list[Any]: 抓取器返回的结果列表。
    """
    settings = load_settings()  # 读取运行时配置
    logger = get_logger("FlowTaskRuntimeDebug", settings.log_file)  # 初始化日志器
    redis_client = create_redis_client(settings)  # 创建 Redis 客户端
    account_collection = create_account_collection(settings)  # 创建账号集合

    scraper = StealthCreditCheckedFlowScraper(
        settings=settings,  # 运行配置
        redis_client=redis_client,  # Redis 客户端
        account_collection=account_collection,  # Mongo 账号集合
        logger=logger,  # 日志器
        browser_pool_size=1,  # 本地调试固定只开 1 个浏览器
        max_contexts_per_browser=1,  # 本地调试固定只开 1 个 context
        headless=False,  # 🔥 改为无头模式进行测试
        extra_flags=[
            "--start-maximized",  # 浏览器最大化启动
            "--disable-blink-features=AutomationControlled",  # 防检测
            "--disable-infobars",  # 禁用信息栏
            "--window-size=1920,1080",  # 设置窗口大小
            "--disable-dev-shm-usage",  # 避免共享内存问题
            "--disable-gpu",  # 无头模式禁用GPU
            "--no-first-run",  # 跳过首次运行
        ],  # 增强的无头模式参数
        viewport={"width": 1920, "height": 1080},  # 🆕 配合window-size设置固定视口
        navigation_timeout_ms=settings.navigation_timeout_ms,  # 页面导航超时
        task_timeout_ms=settings.task_timeout_ms,  # 单任务总超时
        default_cookie_domain=".google.com",  # 缺省 cookie 域名
        recycle_browser_after_tasks=20,  # 执行 20 次任务后回收 browser
        recycle_browser_after_failures=3,  # 连续失败 3 次后回收 browser
    )

    local_task = normalize_local_task(LOCAL_TASK)
    logger.info(f"[debug] 开始执行本地调试任务: {local_task.get('_id')}")

    async with scraper:
        results = await scraper.run_tasks([local_task])

    return results


async def run_local_debug_task() -> list[Any]:
    settings = load_settings()
    logger = get_logger("FlowTaskRuntimeDebug", settings.log_file)
    redis_client = create_redis_client(settings)
    account_collection = create_account_collection(settings)

    local_task = normalize_local_task(LOCAL_TASK)
    logger.info(f"[debug] starting local debug task: {local_task.get('_id')}")

    headless_first = read_debug_bool("FLOW_TASK_DEBUG_HEADLESS", True)
    fallback_to_headed = read_debug_bool("FLOW_TASK_DEBUG_FALLBACK_HEADED", True)
    scraper = create_debug_scraper(
        settings=settings,
        redis_client=redis_client,
        account_collection=account_collection,
        logger=logger,
        headless=headless_first,
    )
    async with scraper:
        results = await scraper.run_tasks([local_task])

    if headless_first and fallback_to_headed and any(is_recaptcha_unusual_activity(item) for item in results):
        logger.warning(
            "[debug] headless run hit reCAPTCHA unusual activity; "
            "retrying once with headed browser"
        )
        headed_scraper = create_debug_scraper(
            settings=settings,
            redis_client=redis_client,
            account_collection=account_collection,
            logger=logger,
            headless=False,
        )
        async with headed_scraper:
            results = await headed_scraper.run_tasks([local_task])

    return results


def main() -> None:
    """同步入口，适合 PyCharm 直接右键运行。"""
    results = asyncio.run(run_local_debug_task())
    for index, result in enumerate(results, start=1):
        if isinstance(result, Exception):
            print(f"RESULT {index}: FAIL {result!r}")
        else:
            print(f"RESULT {index}: OK {json.dumps(result, ensure_ascii=False, indent=2)}")


if __name__ == "__main__":
    main()
