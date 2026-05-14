"""Run one Google Flow video generation task with the isolated CloakBrowser runner.

This script mirrors the task flow of the Redis consumer without reusing its
browser launch/context handling and without touching existing code:

1. Build a task dict (`_id`, `prompt`, `type=1`).
2. Validate and convert it with `build_scraper_task()`.
3. Let `GoogleFlowVideoScraperV2.normalize_task()` allocate a cookie from the
   same account pool used by the Redis consumer.
4. Execute `GoogleFlowVideoScraperV2.process_task()` on a page created by the
   new CloakBrowser-backed runner instead of `driver_base.BrowserWorker`.
5. Persist a JSON artifact with the local video path, md5, file size, MIME type,
   and any failure diagnostics.

Run from repo root:
    python cloakbrowser_replacement\run_one_video_task_headless.py

Environment overrides:
    CLOAK_VIDEO_TASK_JSON='{"_id":"...","prompt":"...","type":1}'
    CLOAK_VIDEO_PROMPT='A short cinematic shot of clouds over a city skyline.'
    CLOAK_VIDEO_POLL_TIMEOUT_MS=600000
    CLOAK_VIDEO_HEADLESS=1
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Keep local CloakBrowser checkout importable without installing into this repo.
CLOAKBROWSER_REPO = Path(os.getenv("CLOAKBROWSER_REPO", r"D:\CloakBrowser"))
if CLOAKBROWSER_REPO.exists() and str(CLOAKBROWSER_REPO) not in sys.path:
    sys.path.insert(0, str(CLOAKBROWSER_REPO))

from cloak_browser_runner import CloakBrowserRunner, CloakBrowserRunnerConfig, get_cloakbrowser_status  # noqa: E402
from driver_base.multi_browser_scraper_base import load_cookies  # noqa: E402
from video_processing.scrapers.automation_video_v2_click_consumer import GoogleFlowVideoScraperV2  # noqa: E402
from video_processing.utils.task_common import build_scraper_task, now_local  # noqa: E402

ARTIFACT_DIR = Path(__file__).resolve().parent / "artifacts" / "video_task"
ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)


COOKIE_DROP_EXACT_NAMES = {
    # Preference / chooser / explicit account-display cookies, not required for
    # the Flow session and unnecessarily identifying for this one-task probe.
    "ACCOUNT_CHOOSER",
    "EMAIL",
    "email",
    "NID",
    "OTZ",
}
COOKIE_DROP_PREFIXES = (
    # Google Analytics / measurement cookies.
    "_ga",
)
COOKIE_DROP_DOMAINS = {
    # Region/account-management side cookies observed in the pool that are not
    # needed for labs.google Flow generation.
    ".google.com.vn",
    "myaccount.google.com",
    "ogs.google.com",
}


def filter_flow_cookies(cookies: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Keep only cookies needed for the Flow session; return kept and redacted drop report."""
    kept: list[dict[str, Any]] = []
    dropped_report: list[dict[str, Any]] = []
    for cookie in cookies:
        name = str(cookie.get("name", ""))
        domain = str(cookie.get("domain", ""))
        reason = ""
        if domain in COOKIE_DROP_DOMAINS:
            reason = "unneeded_domain"
        elif name in COOKIE_DROP_EXACT_NAMES:
            reason = "nonessential_identifier_or_preference"
        elif any(name.startswith(prefix) for prefix in COOKIE_DROP_PREFIXES):
            reason = "analytics"

        if reason:
            dropped_report.append({"name": name, "domain": domain, "reason": reason})
            continue
        kept.append(cookie)
    return kept, dropped_report


@dataclass(slots=True)
class ProbeWorkerContext:
    worker_id: int = 1
    browser_task_capacity: int = 1
    active_tasks: int = 1
    tasks_since_recycle: int = 0
    consecutive_failures: int = 0
    context_id: int = 0


def validate_video_task(task: dict[str, Any]) -> tuple[str, str]:
    """Local validation only; do not import redis_task_consumer browser helpers."""
    task_id = str(task.get("_id", "")).strip()
    prompt = str(task.get("prompt", "")).strip()
    if not task_id:
        raise ValueError("missing _id")
    if not prompt:
        raise ValueError("missing prompt")
    if int(task.get("type", -1)) != 1:
        raise ValueError(f"only type=1 video tasks are supported, got type={task.get('type')}")
    return task_id, prompt


def build_input_task() -> dict[str, Any]:
    raw_json = os.getenv("CLOAK_VIDEO_TASK_JSON", "").strip()
    if raw_json:
        task = json.loads(raw_json)
        if not isinstance(task, dict):
            raise ValueError("CLOAK_VIDEO_TASK_JSON must be a JSON object")
        return task

    task_id = os.getenv("CLOAK_VIDEO_TASK_ID") or f"cloak-headless-{int(time.time())}"
    prompt = os.getenv(
        "CLOAK_VIDEO_PROMPT",
        "A five-second cinematic shot of soft morning clouds drifting above a futuristic city skyline, smooth camera motion.",
    )
    return {
        "_id": task_id,
        "prompt": prompt,
        "type": 1,
        "gen_type": int(os.getenv("CLOAK_VIDEO_GEN_TYPE", "1")),
        "model_type": int(os.getenv("CLOAK_VIDEO_MODEL_TYPE", "0")),
        "proportion": int(os.getenv("CLOAK_VIDEO_PROPORTION", "0")),
        "poll_timeout_ms": int(os.getenv("CLOAK_VIDEO_POLL_TIMEOUT_MS", "600000")),
        "retry_count": 0,
    }


async def run_one_task() -> dict[str, Any]:
    input_task = build_input_task()
    task_id, prompt = validate_video_task(input_task)
    scraper_task = build_scraper_task(input_task)
    #
    # headless = os.getenv("CLOAK_VIDEO_HEADLESS", "1").strip().lower() in {"1", "true", "yes", "on", "y"}

    scraper = GoogleFlowVideoScraperV2(
        browser_pool_size=1,
        max_contexts_per_browser=1,
        # headless=False,
        task_timeout_ms=int(os.getenv("CLOAK_VIDEO_TASK_TIMEOUT_MS", "900000")),
        recycle_browser_after_failures=1,
    )

    normalized_task = scraper.normalize_task(scraper_task)
    raw_cookies = load_cookies(normalized_task.get("cookies"), default_domain=".google.com")
    filtered_cookies, dropped_cookies = filter_flow_cookies(raw_cookies)
    normalized_task["cookies"] = filtered_cookies
    worker = ProbeWorkerContext()
    artifact_prefix = ARTIFACT_DIR / f"failure_{int(time.time())}"

    runner_config = CloakBrowserRunnerConfig(
        headless=False,
        humanize=True
    )

    started_at = now_local().isoformat()
    async with CloakBrowserRunner(runner_config) as runner:
        async def handler(page: Any, task_data: dict[str, Any]) -> dict[str, Any]:
            try:
                result = await scraper.process_task(page, task_data, worker)
                return {"ok": True, "result": result}
            except Exception as exc:
                screenshot = f"{artifact_prefix}.failure.png"
                html = f"{artifact_prefix}.failure.html"
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
                    "traceback": traceback.format_exc(),
                    "failure_screenshot": screenshot,
                    "failure_html": html,
                }

        execution = await runner.run_task(
            normalized_task,
            handler,
        )

    finished_at = now_local().isoformat()
    artifact: dict[str, Any] = {
        "started_at": started_at,
        "finished_at": finished_at,
        # "headless": headless,
        "input_task": input_task,
        "task_id": task_id,
        "prompt": prompt,
        "scraper_task": scraper_task,
        "normalized_task_redacted": {
            key: ("<redacted>" if key in {"cookies"} else value)
            for key, value in normalized_task.items()
        },
        "cookie_count_raw": len(raw_cookies),
        "cookie_count": len(normalized_task.get("cookies") or []),
        "cookie_dropped_count": len(dropped_cookies),
        "cookie_dropped_redacted": dropped_cookies,
        "cloakbrowser": get_cloakbrowser_status(),
        "execution": execution,
    }

    result = execution.get("result") if isinstance(execution, dict) else None
    if isinstance(result, dict):
        video_path = result.get("local_video_path")
        artifact["video_path_exists"] = bool(video_path and Path(video_path).exists())
        artifact["video_file_size_bytes"] = Path(video_path).stat().st_size if video_path and Path(video_path).exists() else 0

    output_path = ARTIFACT_DIR / "last_video_task_result.json"
    output_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(artifact, ensure_ascii=False, indent=2, default=str))
    return artifact


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    asyncio.run(run_one_task())
