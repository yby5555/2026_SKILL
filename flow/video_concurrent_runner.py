"""
video_concurrent_runner.py
===========================
并发视频生成入口脚本（独立新文件，不修改 automation_video_v2_click.py）

功能：
    - 并发 6（browser_pool_size=2 × max_contexts_per_browser=3）
    - Cookie 从 Redis Pool 中 Round-Robin 获取（由 login_scheduler.py 写入）
    - 支持命令行传入关键词：python video_concurrent_runner.py "your prompt here"

用法：
    python video_concurrent_runner.py
    python video_concurrent_runner.py "A doctor explains prostate anatomy"
"""

import asyncio
import sys
from pathlib import Path
from typing import Any

# ── 路径修复 ──────────────────────────────────────────────────────────────────
_FLOW_DIR = Path(__file__).resolve().parent          # 2026_SKILL/flow/
_ROOT = _FLOW_DIR.parent                             # 2026_SKILL/
_ACCOUNT_MGR = _ROOT / "account_mgr"

for _p in [str(_ROOT), str(_FLOW_DIR), str(_ACCOUNT_MGR)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)
# ──────────────────────────────────────────────────────────────────────────────

from automation_video_v2_click import GoogleFlowVideoScraperV2  # noqa: E402
from redis_utils import get_next_cookie                          # noqa: E402

# ── 并发参数 ──────────────────────────────────────────────────────────────────
BROWSER_POOL_SIZE      = 2   # 浏览器进程数
CONTEXTS_PER_BROWSER   = 3   # 每个浏览器最多并发 context 数
MAX_CONCURRENT         = BROWSER_POOL_SIZE * CONTEXTS_PER_BROWSER  # = 6
TASK_TIMEOUT_MS        = 12 * 60 * 1000  # 单任务最长 12 分钟

# ── 输出目录 ──────────────────────────────────────────────────────────────────
VIDEO_OUTPUT_DIR = Path("d:/2026_SKILL/flow/videos")

# ── 默认 prompt（直接运行时使用）────────────────────────────────────────────
DEFAULT_PROMPT = """
Realistic medical education video in a clean clinical examination room.
A 50-year-old American male doctor with short curly hair wears a white lab coat over green surgical scrubs and blue medical gloves.
He stands beside the examination bed and explains the anatomical location of the prostate to a male patient,
pointing to the lower abdominal area and an anatomical chart for reference.
Medium shot, stable camera, slight forward push during the explanation,
bright hospital lighting, blue privacy curtains in the background,
highly realistic medical documentary style, non-sexual, educational, professional.
""".strip()


async def run_concurrent_videos(prompt: str, count: int = MAX_CONCURRENT) -> list[Any]:
    """
    并发生成视频。

    Args:
        prompt: 视频生成提示词
        count:  并发任务数，最多 MAX_CONCURRENT（默认 6）

    Returns:
        结果列表，每项为 dict（成功）或 Exception（失败）
    """
    count = min(count, MAX_CONCURRENT)

    # 1. 从 Redis 取 Cookie（每个任务取一个账号）
    tasks: list[dict[str, Any]] = []
    for i in range(count):
        result = get_next_cookie()
        if result is None:
            print(
                f"[Runner] Redis Cookie Pool 不足，只取到 {i} 个账号，"
                "请先运行 login_scheduler.py"
            )
            break
        email, cookies = result
        print(f"[Runner] 任务 {i + 1}/{count} → 账号: {email}")
        tasks.append(
            {
                "prompt": prompt,
                "variant_count": 1,
                "email": email,      # 账号标识，供健康检测使用
                "cookies": cookies,  # 任务级 Cookie，基类 _build_cookie_payload() 自动读取
            }
        )

    if not tasks:
        print("[Runner] 没有可用的 Cookie，退出")
        return []

    # 2. 初始化 Scraper 并并发执行
    VIDEO_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    scraper = GoogleFlowVideoScraperV2(
        browser_pool_size=BROWSER_POOL_SIZE,
        max_contexts_per_browser=CONTEXTS_PER_BROWSER,
        headless=True,
        extra_flags=["--start-maximized"],
        default_cookie_domain="labs.google",
        task_timeout_ms=TASK_TIMEOUT_MS,
    )

    async with scraper:
        results = await scraper.run_tasks(tasks)

    # 3. 打印结果摘要
    for idx, res in enumerate(results):
        if isinstance(res, Exception):
            print(f"[Runner] ✗ 任务 {idx + 1} 失败: {res}")
        else:
            print(f"[Runner] ✓ 任务 {idx + 1} 成功: {res.get('local_video_path')}")

    return results


async def main():
    prompt = " ".join(sys.argv[1:]).strip() or DEFAULT_PROMPT
    print(f"[Runner] 使用提示词: {prompt[:60]}{'...' if len(prompt) > 60 else ''}")
    print(f"[Runner] 并发: {MAX_CONCURRENT}（browser×{BROWSER_POOL_SIZE}, context×{CONTEXTS_PER_BROWSER}）")
    await run_concurrent_videos(prompt)


if __name__ == "__main__":
    asyncio.run(main())
