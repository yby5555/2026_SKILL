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

from cloak_browser_runner import CloakBrowserRunner, CloakBrowserRunnerConfig, get_cloakbrowser_status  # noqa: E402
from driver_base.multi_browser_scraper_base import load_cookies  # noqa: E402
from video_processing.consumers.run_40_task_audit import (  # noqa: E402
    SAMPLE_IMAGE_A,
    SAMPLE_IMAGE_B,
    build_task_specs_with_images,
)
from video_processing.scrapers.automation_video_v2_click_consumer import GoogleFlowVideoScraperV2  # noqa: E402
from video_processing.utils.task_common import build_scraper_task, now_local  # noqa: E402
from run_one_video_task_headless import ProbeWorkerContext, filter_flow_cookies, validate_video_task  # noqa: E402

ARTIFACT_DIR = Path(__file__).resolve().parent / "artifacts" / "audit40_cloak"
ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)


# Defaults used when running by right-click / without CLI arguments.
# Edit these values directly if you want different local behavior.
DEFAULT_HEADLESS = True
DEFAULT_IMAGE_ONLY = True
DEFAULT_BROWSER_POOL_SIZE = 2
DEFAULT_CONTEXTS_PER_BROWSER = 1
DEFAULT_COOLDOWN_SECONDS = 5.0
DEFAULT_LIMIT = 0  # 0 means all selected tasks; with DEFAULT_IMAGE_ONLY=True this is 32 tasks.
DEFAULT_POLL_TIMEOUT_MS = 6 * 60 * 1000
DEFAULT_TASK_TIMEOUT_MS = 9 * 60 * 1000
DEFAULT_HUMANIZE = True


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


async def run_one_with_cloak(
    runner: CloakBrowserRunner,
    scraper: GoogleFlowVideoScraperV2,
    spec: Any,
    index: int,
    total: int,
    worker_id: int,
    context_id: int,
) -> dict[str, Any]:
    task = dict(spec.queue_payload)
    task_id, _ = validate_video_task(task)
    scraper_task = build_scraper_task(task)
    normalized_task = scraper.normalize_task(scraper_task)
    raw_cookies = load_cookies(normalized_task.get("cookies"), default_domain=".google.com")
    filtered_cookies, dropped_cookies = filter_flow_cookies(raw_cookies)
    normalized_task["cookies"] = filtered_cookies
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
                "error_msg": str(exc) or "????",
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
    runner_config = CloakBrowserRunnerConfig(headless=args.headless, humanize=not args.no_humanize)

    results: list[dict[str, Any]] = []
    result_lock = asyncio.Lock()
    progress_counter = {"next_index": 0}
    queue: asyncio.Queue[tuple[int, Any] | None] = asyncio.Queue()
    for index, spec in enumerate(specs, start=1):
        queue.put_nowait((index, spec))

    started_at = now_local().isoformat()

    async def write_partial_report() -> None:
        partial_report = {
            "run_id": run_id,
            "started_at": started_at,
            "updated_at": now_local().isoformat(),
            "headless": args.headless,
            "browser_pool_size": args.browser_pool_size,
            "contexts_per_browser": args.contexts_per_browser,
            "concurrency": args.browser_pool_size * args.contexts_per_browser,
            "limit": args.limit,
            "image_only": args.image_only,
            "cloakbrowser": get_cloakbrowser_status(),
            **summarize(results),
            "manifest_path": str(manifest_path),
            "tasks": results,
        }
        (ARTIFACT_DIR / "last_audit40_cloak_report.json").write_text(
            json.dumps(partial_report, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

    async def worker_loop(worker_id: int, context_id: int) -> None:
        async with CloakBrowserRunner(runner_config) as runner:
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
                    )
                    async with result_lock:
                        results.append(row)
                        results.sort(key=lambda row: int(row.get("index", 0)))
                        await write_partial_report()
                finally:
                    queue.task_done()
                    if args.cooldown_seconds > 0:
                        await asyncio.sleep(args.cooldown_seconds)

    concurrency = args.browser_pool_size * args.contexts_per_browser
    workers = [
        asyncio.create_task(worker_loop(browser_index + 1, context_index))
        for browser_index in range(args.browser_pool_size)
        for context_index in range(args.contexts_per_browser)
    ]
    for _ in workers:
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
        "limit": args.limit,
        "image_only": args.image_only,
        "cloakbrowser": get_cloakbrowser_status(),
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
    parser = argparse.ArgumentParser(description="Run run_40_task_audit task matrix with CloakBrowser runner")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--headless", action=argparse.BooleanOptionalAction, default=DEFAULT_HEADLESS)
    parser.add_argument("--no-humanize", action="store_true", default=not DEFAULT_HUMANIZE)
    parser.add_argument("--poll-timeout-ms", type=int, default=DEFAULT_POLL_TIMEOUT_MS)
    parser.add_argument("--task-timeout-ms", type=int, default=DEFAULT_TASK_TIMEOUT_MS)
    parser.add_argument("--cooldown-seconds", type=float, default=DEFAULT_COOLDOWN_SECONDS)
    parser.add_argument("--browser-pool-size", type=int, default=DEFAULT_BROWSER_POOL_SIZE)
    parser.add_argument("--contexts-per-browser", type=int, default=DEFAULT_CONTEXTS_PER_BROWSER)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="For smoke testing; 0 means all selected tasks")
    parser.add_argument("--image-only", action=argparse.BooleanOptionalAction, default=DEFAULT_IMAGE_ONLY, help="Only run tasks that require images (exclude text family)")
    parser.add_argument("--image-file-a", default=str(SAMPLE_IMAGE_A))
    parser.add_argument("--image-file-b", default=str(SAMPLE_IMAGE_B))
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
