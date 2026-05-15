"""Run run_40_task_audit.py's task matrix through the CloakBrowser replacement runner.

This stays inside cloakbrowser_replacement and does not modify existing consumer code.
It imports the 40-task manifest builder from video_processing.consumers.run_40_task_audit,
then executes each task using the same minimal CloakBrowser path as
run_one_video_task_headless.py.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import mimetypes
import os
import random
import sys
import time
import traceback
from dataclasses import asdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CLOAKBROWSER_REPO = Path(os.getenv("CLOAKBROWSER_REPO", r"D:\CloakBrowser"))
if CLOAKBROWSER_REPO.exists() and str(CLOAKBROWSER_REPO) not in sys.path:
    sys.path.insert(0, str(CLOAKBROWSER_REPO))

from cloak_browser_runner import CloakBrowserRunner, CloakBrowserRunnerConfig, get_cloakbrowser_status, safe_slug  # noqa: E402
from driver_base.multi_browser_scraper_base import load_cookies  # noqa: E402
from video_processing.consumers.run_40_task_audit import (  # noqa: E402
    SAMPLE_IMAGE_A,
    SAMPLE_IMAGE_B,
    build_task_specs_with_images,
)
from video_processing.scrapers.automation_video_v2_click_consumer import GoogleFlowVideoScraperV2  # noqa: E402
from video_processing.utils.task_common import build_scraper_task, now_local  # noqa: E402
from run_one_video_task_headless import ProbeWorkerContext, build_runner_config, filter_flow_cookies, validate_video_task  # noqa: E402

ARTIFACT_DIR = Path(__file__).resolve().parent / "artifacts" / "audit40_cloak"
ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)


# 右键运行/不传 CLI 参数时使用这些默认值；想调速度/并发优先改这里。
# 是否无头运行：True=后台无窗口；False=显示窗口。
DEFAULT_HEADLESS = True
# 是否只跑需要传图片的任务。
DEFAULT_IMAGE_ONLY = True
# 浏览器进程数量。
DEFAULT_BROWSER_POOL_SIZE = 4
# 每个浏览器里开的隔离 context/window 数。
DEFAULT_CONTEXTS_PER_BROWSER = 1
# 单个 context 跑完一个任务后的冷却秒数。
DEFAULT_COOLDOWN_SECONDS = 8.0
# 限制任务数；0 表示跑所选全部任务。
DEFAULT_LIMIT = 0
# 视频生成轮询超时。
DEFAULT_POLL_TIMEOUT_MS = 6 * 60 * 1000
# 单个任务整体超时。
DEFAULT_TASK_TIMEOUT_MS = 9 * 60 * 1000
# 是否开启 CloakBrowser humanize 行为层。
DEFAULT_HUMANIZE = True
# humanize 预设：default 较快；careful 更慢更稳。
DEFAULT_HUMAN_PRESET = "careful"
# 各 context 启动错峰秒数。
DEFAULT_STAGGER_SECONDS = 8.0
# True=使用临时隔离 context，cookie/localStorage 隔离。
DEFAULT_INCOGNITO_CONTEXTS = True
# True=同一 browser 里开多个隔离 context。
DEFAULT_SHARED_BROWSER_CONTEXTS = True
# profile 模式：incognito=临时隔离 context；fixed=固定用户目录 persistent profile。
DEFAULT_PROFILE_MODE = "fixed"
# 固定用户目录根目录；fixed 模式且未指定 --profile-dir 时自动分子目录。
DEFAULT_PROFILE_BASE_DIR = str(Path(__file__).resolve().parent / "profiles")
# True=每次浏览器打开/断线重启时都换 fingerprint_seed；fixed profile 模式下也换新的用户目录子目录。
DEFAULT_RANDOMIZE_BROWSER_IDENTITY = True
# DEFAULT_PROFILE_MODE = "fixed"
# DEFAULT_PROFILE_BASE_DIR = r"D:\2026_SKILL\cloakbrowser_replacement\profiles"


class RotatingIdentityCloakBrowserRunner(CloakBrowserRunner):
    """在每次浏览器真正启动前刷新浏览器身份。

    这里的“浏览器身份”包含两部分：
    1. fingerprint_seed：传给 CloakBrowser 的 --fingerprint 参数，影响浏览器指纹。
    2. persistent_profile_dir：fixed profile 模式下的用户目录，影响 cookie/localStorage/cache 等本地状态。

    CloakBrowserRunner.ensure_started() 在浏览器断开后会调用 close() + start()，
    所以只要重写 start()，首次打开和断线重启都会自动换一套身份。
    """

    def __init__(
        self,
        config: CloakBrowserRunnerConfig,
        *,
        enabled: bool,
        run_id: str,
        suffix: str,
        profile_mode: str,
        profile_dir: str,
        profile_base_dir: str,
        identity_events: list[dict[str, Any]],
    ) -> None:
        super().__init__(config)
        self._enabled = enabled
        self._run_id = run_id
        self._suffix = suffix
        self._profile_mode = profile_mode
        self._profile_dir = profile_dir
        self._profile_base_dir = profile_base_dir
        self._identity_events = identity_events
        self._launch_counter = 0

    def _next_profile_base_dir(self) -> Path:
        """返回随机用户目录的父目录；显式 --profile-dir 会被当作父目录使用。"""
        if self._profile_dir:
            return Path(self._profile_dir)
        return Path(self._profile_base_dir)

    def _apply_new_browser_identity(self) -> None:
        """生成并应用本次浏览器启动使用的随机指纹和用户目录。"""
        self._launch_counter += 1
        fingerprint_seed = random.randint(1, 0x7FFFFFFF)
        launch_token = safe_slug(
            f"{self._run_id}-{self._suffix}-l{self._launch_counter}-{int(time.time() * 1000)}-{fingerprint_seed}"
        )

        self.config.fingerprint_seed = fingerprint_seed
        profile_dir = ""
        if self._profile_mode == "fixed":
            profile_path = self._next_profile_base_dir() / launch_token
            self.config.use_persistent_context = True
            self.config.persistent_profile_dir = profile_path
            profile_dir = str(profile_path)

        event = {
            "run_id": self._run_id,
            "suffix": self._suffix,
            "launch_counter": self._launch_counter,
            "fingerprint_seed": fingerprint_seed,
            "profile_mode": self._profile_mode,
            "persistent_profile_dir": profile_dir,
            "created_at": now_local().isoformat(),
        }
        self._identity_events.append(event)
        print(
            "[browser_identity] "
            f"suffix={self._suffix} launch={self._launch_counter} "
            f"fingerprint_seed={fingerprint_seed} profile_dir={profile_dir or '<incognito>'}"
        )

    async def start(self) -> None:
        """首次打开/断线重启前刷新身份；已启动时不重复刷新。"""
        if self.browser is not None or self.persistent_context is not None:
            return
        if self._enabled:
            self._apply_new_browser_identity()
        await super().start()

def image_file_to_data_uri(path: Path) -> str:
    mime_type = mimetypes.guess_type(str(path))[0] or "image/png"
    return f"data:{mime_type};base64," + base64.b64encode(path.read_bytes()).decode("ascii")


def task_success_from_path(local_video_path: str) -> bool:
    if not local_video_path:
        return False
    path = Path(local_video_path)
    return path.exists() and path.stat().st_size > 1024


def summarize(results: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(results)
    success_count = sum(1 for row in results if row.get("success"))
    by_family: dict[str, dict[str, int]] = {}
    for row in results:
        family = str(row.get("family"))
        bucket = by_family.setdefault(family, {"success": 0, "total": 0})
        bucket["total"] += 1
        if row.get("success"):
            bucket["success"] += 1
    return {
        "success_count": success_count,
        "total_count": total,
        "success_rate": success_count / total if total else 0.0,
        "by_family": {
            family: {**bucket, "success_rate": bucket["success"] / bucket["total"] if bucket["total"] else 0.0}
            for family, bucket in by_family.items()
        },
    }


def runner_config_report(config: CloakBrowserRunnerConfig) -> dict[str, Any]:
    return {
        "headless": config.headless,
        "humanize": config.humanize,
        "human_preset": config.human_preset,
        "geoip": config.geoip,
        "backend": config.backend,
        "timezone": config.timezone,
        "locale": config.locale,
        "fingerprint_seed": config.fingerprint_seed,
        "persistent_profile_dir": str(config.persistent_profile_dir or ""),
        "use_persistent_context": config.use_persistent_context,
        "inject_cookies_into_persistent_profile": config.inject_cookies_into_persistent_profile,
        "extra_args": list(config.extra_args),
        "proxy_configured": bool(config.default_proxy),
        "context_proxy_configured": bool(config.context_proxy),
        "restart_on_disconnect": config.restart_on_disconnect,
    }


def load_context_proxy_map(raw: str) -> dict[int, str]:
    """Parse a simple context proxy map: "0=socks5://a,1=http://b"."""
    result: dict[int, str] = {}
    for part in (raw or "").split(","):
        item = part.strip()
        if not item or "=" not in item:
            continue
        key, value = item.split("=", 1)
        try:
            result[int(key.strip())] = value.strip()
        except ValueError:
            continue
    return result


def resolve_context_proxy(args: argparse.Namespace, context_id: int) -> str:
    proxy_map = load_context_proxy_map(args.context_proxy_map)
    if context_id in proxy_map:
        return proxy_map[context_id]
    if args.context_proxy:
        return args.context_proxy
    return ""


def apply_profile_args(config: CloakBrowserRunnerConfig, args: argparse.Namespace, suffix: str) -> None:
    """根据 --profile-mode/--profile-dir 参数覆盖 runner 的 profile 行为。"""
    if args.profile_mode == "incognito":
        config.use_persistent_context = False
        config.persistent_profile_dir = None
        return

    if args.profile_mode == "fixed":
        config.use_persistent_context = True
        if args.profile_dir:
            # 明确指定 --profile-dir 时，直接使用这个固定用户目录。
            config.persistent_profile_dir = Path(args.profile_dir)
        else:
            # 未指定 --profile-dir 时，在 --profile-base-dir 下按 browser/context 自动分目录，避免多个 worker 抢同一个 profile。
            config.persistent_profile_dir = Path(args.profile_base_dir) / suffix
        return

    raise ValueError(f"不支持的 profile_mode: {args.profile_mode}")


async def run_one_with_cloak(
    runner: CloakBrowserRunner,
    scraper: GoogleFlowVideoScraperV2,
    spec: Any,
    index: int,
    total: int,
    worker_id: int,
    context_id: int,
    context_proxy: str = "",
) -> dict[str, Any]:
    task = dict(spec.queue_payload)
    task_id, _ = validate_video_task(task)
    scraper_task = build_scraper_task(task)
    normalized_task = scraper.normalize_task(scraper_task)
    raw_cookies = load_cookies(normalized_task.get("cookies"), default_domain=".google.com")
    filtered_cookies, dropped_cookies = filter_flow_cookies(raw_cookies)
    normalized_task["cookies"] = filtered_cookies
    if context_proxy:
        normalized_task["context_proxy"] = context_proxy
    worker = ProbeWorkerContext(worker_id=worker_id, context_id=context_id)
    failure_prefix = ARTIFACT_DIR / f"{task_id}.failure"

    async def handler(page: Any, task_data: dict[str, Any]) -> dict[str, Any]:
        try:
            result = await scraper.process_task(page, task_data, worker)
            return {"ok": True, "result": result}
        except Exception as exc:
            screenshot = f"{failure_prefix}.png"
            html = f"{failure_prefix}.html"
            try:
                await page.screenshot(path=screenshot, full_page=True)
            except Exception:
                screenshot = ""
            try:
                Path(html).write_text(await page.content(), encoding="utf-8")
            except Exception:
                html = ""
            return {
                "ok": False,
                "error_type": type(exc).__name__,
                "error": str(exc),
                "error_msg": str(exc) or "unknown_error",
                "traceback": traceback.format_exc(),
                "failure_screenshot": screenshot,
                "failure_html": html,
            }

    started = time.time()
    execution = await runner.run_task(normalized_task, handler)
    elapsed = time.time() - started
    result = execution.get("result") if isinstance(execution, dict) else None
    local_video_path = str(result.get("local_video_path") or "") if isinstance(result, dict) else ""
    success = bool(execution.get("ok")) and task_success_from_path(local_video_path)
    row = {
        "task_id": task_id,
        "index": index,
        "total": total,
        "worker_id": worker_id,
        "context_id": context_id,
        "family": spec.family,
        "gen_type": spec.gen_type,
        "image_count": spec.image_count,
        "model_type": spec.model_type,
        "proportion": spec.proportion,
        "success": success,
        "elapsed_seconds": round(elapsed, 2),
        "local_video_path": local_video_path,
        "video_file_size_bytes": Path(local_video_path).stat().st_size if success else 0,
        "cookie_count_raw": len(raw_cookies),
        "cookie_count": len(filtered_cookies),
        "cookie_dropped_count": len(dropped_cookies),
        "error": "" if success else execution.get("error", "unknown_failure"),
        "error_msg": "" if success else execution.get("error_msg", execution.get("error", "unknown_failure")),
        "execution": execution if not success else {"ok": True, "result": result},
    }
    print(f"[{index:02d}/{total}] {'SUCCESS' if success else 'FAILED '} {task_id} elapsed={elapsed:.1f}s")
    if not success:
        print(f"    error={row['error_msg']}")
    return row


async def run_audit(args: argparse.Namespace) -> dict[str, Any]:
    run_id = args.run_id or time.strftime("%Y%m%d-%H%M%S")
    image_values = [image_file_to_data_uri(Path(args.image_file_a)), image_file_to_data_uri(Path(args.image_file_b))]
    specs = build_task_specs_with_images(
        run_id,
        image_values=image_values,
        image_mode="base64",
        poll_timeout_ms=args.poll_timeout_ms,
    )
    if args.image_only:
        specs = [spec for spec in specs if spec.image_count > 0]
    if args.limit:
        specs = specs[: args.limit]

    manifest_path = ARTIFACT_DIR / f"manifest-{run_id}.json"
    manifest_path.write_text(json.dumps([asdict(spec) for spec in specs], ensure_ascii=False, indent=2), encoding="utf-8")

    scraper = GoogleFlowVideoScraperV2(
        browser_pool_size=args.browser_pool_size,
        max_contexts_per_browser=args.contexts_per_browser,
        headless=args.headless,
        task_timeout_ms=args.task_timeout_ms,
        recycle_browser_after_failures=1,
    )
    base_runner_config = build_runner_config(
        {"_id": run_id, "email": "audit40"},
        suffix="base",
    )
    base_runner_config.headless = args.headless
    base_runner_config.humanize = not args.no_humanize
    base_runner_config.human_preset = args.human_preset
    apply_profile_args(base_runner_config, args, "base")


    results: list[dict[str, Any]] = []
    result_lock = asyncio.Lock()
    progress_counter = {"next_index": 0}
    queue: asyncio.Queue[tuple[int, Any] | None] = asyncio.Queue()
    for index, spec in enumerate(specs, start=1):
        queue.put_nowait((index, spec))

    started_at = now_local().isoformat()
    browser_identity_events: list[dict[str, Any]] = []

    async def write_partial_report() -> None:
        partial_report = {
            "run_id": run_id,
            "started_at": started_at,
            "updated_at": now_local().isoformat(),
            "headless": args.headless,
            "browser_pool_size": args.browser_pool_size,
            "contexts_per_browser": args.contexts_per_browser,
            "concurrency": args.browser_pool_size * args.contexts_per_browser,
            "incognito_contexts": args.incognito_contexts,
            "shared_browser_contexts": args.shared_browser_contexts,
            "effective_shared_browser_contexts": args.shared_browser_contexts and args.profile_mode != "fixed",
            "profile_mode": args.profile_mode,
            "profile_dir": args.profile_dir,
            "profile_base_dir": args.profile_base_dir,
            "randomize_browser_identity": args.randomize_browser_identity,
            "browser_identity_events": browser_identity_events,
            "limit": args.limit,
            "image_only": args.image_only,
            "cloakbrowser": get_cloakbrowser_status(),
            "runner_config_redacted": runner_config_report(base_runner_config),
            **summarize(results),
            "manifest_path": str(manifest_path),
            "tasks": results,
        }
        (ARTIFACT_DIR / "last_audit40_cloak_report.json").write_text(
            json.dumps(partial_report, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

    async def context_loop(runner: CloakBrowserRunner, worker_id: int, context_id: int) -> None:
        if args.stagger_seconds > 0:
            await asyncio.sleep((worker_id - 1 + context_id) * args.stagger_seconds)
        context_proxy = resolve_context_proxy(args, context_id)
        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                return
            index, spec = item
            try:
                row = await run_one_with_cloak(
                    runner,
                    scraper,
                    spec,
                    index,
                    len(specs),
                    worker_id,
                    context_id,
                    context_proxy,
                )
                async with result_lock:
                    results.append(row)
                    results.sort(key=lambda row: int(row.get("index", 0)))
                    await write_partial_report()
            finally:
                queue.task_done()
                if args.cooldown_seconds > 0:
                    await asyncio.sleep(args.cooldown_seconds)

    def worker_suffix(browser_id: int, *, context_id: int | None = None) -> str:
        """生成 browser/context 对应的可读后缀，用于日志和用户目录命名。"""
        return f"b{browser_id}" if context_id is None else f"b{browser_id}-c{context_id}"

    def make_worker_config(browser_id: int, *, context_id: int | None = None) -> CloakBrowserRunnerConfig:
        suffix = worker_suffix(browser_id, context_id=context_id)
        worker_config = build_runner_config(
            {"_id": run_id, "email": "audit40"},
            suffix=suffix,
        )
        worker_config.headless = args.headless
        worker_config.humanize = not args.no_humanize
        worker_config.human_preset = args.human_preset
        apply_profile_args(worker_config, args, suffix)
        return worker_config

    def make_runner(browser_id: int, *, context_id: int | None = None) -> RotatingIdentityCloakBrowserRunner:
        """创建 runner；默认每次 start/restart 都会随机化指纹和 fixed profile 子目录。"""
        suffix = worker_suffix(browser_id, context_id=context_id)
        return RotatingIdentityCloakBrowserRunner(
            make_worker_config(browser_id, context_id=context_id),
            enabled=args.randomize_browser_identity,
            run_id=run_id,
            suffix=suffix,
            profile_mode=args.profile_mode,
            profile_dir=args.profile_dir,
            profile_base_dir=args.profile_base_dir,
            identity_events=browser_identity_events,
        )

    async def browser_loop(browser_id: int) -> None:
        async with make_runner(browser_id) as runner:
            context_tasks = [
                asyncio.create_task(context_loop(runner, browser_id, context_index))
                for context_index in range(args.contexts_per_browser)
            ]
            await asyncio.gather(*context_tasks)

    async def standalone_context_loop(browser_id: int, context_id: int) -> None:
        async with make_runner(browser_id, context_id=context_id) as runner:
            await context_loop(runner, browser_id, context_id)

    concurrency = args.browser_pool_size * args.contexts_per_browser
    shared_browser_contexts = args.shared_browser_contexts and args.profile_mode != "fixed"
    if shared_browser_contexts:
        workers = [asyncio.create_task(browser_loop(browser_index + 1)) for browser_index in range(args.browser_pool_size)]
    else:
        workers = [
            asyncio.create_task(standalone_context_loop(browser_index + 1, context_index))
            for browser_index in range(args.browser_pool_size)
            for context_index in range(args.contexts_per_browser)
        ]
    for _ in range(concurrency):
        queue.put_nowait(None)
    await queue.join()
    await asyncio.gather(*workers)

    report = {
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": now_local().isoformat(),
        "headless": args.headless,
        "browser_pool_size": args.browser_pool_size,
        "contexts_per_browser": args.contexts_per_browser,
        "concurrency": args.browser_pool_size * args.contexts_per_browser,
        "incognito_contexts": args.incognito_contexts,
        "shared_browser_contexts": args.shared_browser_contexts,
        "effective_shared_browser_contexts": args.shared_browser_contexts and args.profile_mode != "fixed",
        "profile_mode": args.profile_mode,
        "profile_dir": args.profile_dir,
        "profile_base_dir": args.profile_base_dir,
        "randomize_browser_identity": args.randomize_browser_identity,
        "browser_identity_events": browser_identity_events,
        "limit": args.limit,
        "image_only": args.image_only,
        "cloakbrowser": get_cloakbrowser_status(),
        "runner_config_redacted": runner_config_report(base_runner_config),
        **summarize(results),
        "manifest_path": str(manifest_path),
        "tasks": results,
    }
    report_path = ARTIFACT_DIR / f"report-{run_id}.json"
    report["report_path"] = str(report_path)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    (ARTIFACT_DIR / "last_audit40_cloak_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    print(json.dumps({k: report[k] for k in ("run_id", "success_count", "total_count", "success_rate", "by_family", "report_path")}, ensure_ascii=False, indent=2))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="使用 CloakBrowser 跑 run_40_task_audit 任务矩阵")
    parser.add_argument("--run-id", default="", help="本次运行 ID；空则自动使用时间戳")
    parser.add_argument("--headless", action=argparse.BooleanOptionalAction, default=DEFAULT_HEADLESS, help="是否无头运行；--no-headless 会显示浏览器窗口")
    parser.add_argument("--no-humanize", action="store_true", default=not DEFAULT_HUMANIZE, help="关闭 CloakBrowser humanize 行为层")
    parser.add_argument("--human-preset", default=DEFAULT_HUMAN_PRESET, choices=["default", "careful"], help="humanize 预设：default 较快，careful 较慢")
    parser.add_argument("--poll-timeout-ms", type=int, default=DEFAULT_POLL_TIMEOUT_MS, help="视频生成状态轮询超时毫秒")
    parser.add_argument("--task-timeout-ms", type=int, default=DEFAULT_TASK_TIMEOUT_MS, help="单个任务整体超时毫秒")
    parser.add_argument("--cooldown-seconds", type=float, default=DEFAULT_COOLDOWN_SECONDS, help="每个 context 完成任务后的冷却秒数")
    parser.add_argument("--stagger-seconds", type=float, default=DEFAULT_STAGGER_SECONDS, help="多个 context 启动错峰秒数")
    parser.add_argument("--incognito-contexts", action=argparse.BooleanOptionalAction, default=DEFAULT_INCOGNITO_CONTEXTS, help="使用临时隔离 context；cookie/localStorage 隔离")
    parser.add_argument("--shared-browser-contexts", action=argparse.BooleanOptionalAction, default=DEFAULT_SHARED_BROWSER_CONTEXTS, help="同一 browser 内开多个隔离 context")
    parser.add_argument("--profile-mode", choices=["incognito", "fixed"], default=os.getenv("CLOAK_VIDEO_PROFILE_MODE", DEFAULT_PROFILE_MODE), help="profile 模式：incognito=临时隔离 context；fixed=固定用户目录")
    parser.add_argument("--profile-dir", default=os.getenv("CLOAK_VIDEO_PROFILE_DIR", ""), help="固定用户目录。仅 --profile-mode fixed 时生效；指定后使用该目录")
    parser.add_argument("--profile-base-dir", default=os.getenv("CLOAK_VIDEO_PROFILE_BASE_DIR", DEFAULT_PROFILE_BASE_DIR), help="固定用户目录根目录。fixed 模式且未指定 --profile-dir 时自动分子目录")
    parser.add_argument(
        "--randomize-browser-identity",
        action=argparse.BooleanOptionalAction,
        default=DEFAULT_RANDOMIZE_BROWSER_IDENTITY,
        help="每次打开/断线重启浏览器都随机 fingerprint_seed；fixed 模式下同时换新的用户目录子目录",
    )
    parser.add_argument("--context-proxy", default=os.getenv("CLOAK_VIDEO_CONTEXT_PROXY", ""), help="所有无痕 context 使用同一代理")
    parser.add_argument("--context-proxy-map", default=os.getenv("CLOAK_VIDEO_CONTEXT_PROXY_MAP", ""), help="按 context_id 设置代理，例如 0=socks5://a:1,1=http://b:2")
    parser.add_argument("--browser-pool-size", type=int, default=DEFAULT_BROWSER_POOL_SIZE, help="浏览器进程数量")
    parser.add_argument("--contexts-per-browser", type=int, default=DEFAULT_CONTEXTS_PER_BROWSER, help="每个浏览器内的并发 context 数")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="限制任务数；0 表示所选全部")
    parser.add_argument("--image-only", action=argparse.BooleanOptionalAction, default=DEFAULT_IMAGE_ONLY, help="只跑需要传图片的任务")
    parser.add_argument("--image-file-a", default=str(SAMPLE_IMAGE_A), help="第一张样例图片")
    parser.add_argument("--image-file-b", default=str(SAMPLE_IMAGE_B), help="第二张样例图片")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = asyncio.run(run_audit(args))
    return 0 if report["success_count"] == report["total_count"] else 1


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    raise SystemExit(main())
