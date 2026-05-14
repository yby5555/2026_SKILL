from __future__ import annotations

import argparse
import asyncio
import base64
import json
import mimetypes
import os
import signal
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from video_processing.consumers.redis_task_consumer import (  # noqa: E402
    DEFAULT_TASK_PRIORITY,
    TASK_CREATE_PROCESSING_QUEUE,
    TASK_CREATE_QUEUE,
    GoogleFlowVideoScraperV2,
    _convert_cos_urls_in_task,
    _encode_queue_score,
    extract_error_message,
    handle_single_task,
    mark_task_failed,
    mark_task_processing,
)
from video_processing.utils.task_common import (  # noqa: E402
    create_redis_client,
    create_task_collection,
    dumps_queue_payload,
)

SAMPLE_IMAGE_A = ROOT / "flow" / "fb002687-fa5a-453f-85e0-00362b9af9bc.png"
SAMPLE_IMAGE_B = ROOT / "flow" / "6f31785f-9634-4a16-bf5b-f741a7acd617.jpg"

DEFAULT_IMAGE_URLS = [
    "https://sucai-hw-cms-test-1392049403.cos.ap-guangzhou.myqcloud.com/7/read_image/fb002687-fa5a-453f-85e0-00362b9af9bc.png?sign=q-sign-algorithm%3Dsha1%26q-ak%3DAKIDbBbnmaSHvqcBfy74oMhohNr8D5mFUVP9%26q-sign-time%3D1778569509%3B1778612769%26q-key-time%3D1778569509%3B1778612769%26q-header-list%3Dhost%26q-url-param-list%3D%26q-signature%3Dd36bf4f29cc61d377ddfc75625b7e70f04f44c7b&",
    "https://sucai-hw-cms-test-1392049403.cos.ap-guangzhou.myqcloud.com/7/read_image/6f31785f-9634-4a16-bf5b-f741a7acd617.jpg?sign=q-sign-algorithm%3Dsha1%26q-ak%3DAKIDbBbnmaSHvqcBfy74oMhohNr8D5mFUVP9%26q-sign-time%3D1778569509%3B1778612769%26q-key-time%3D1778569509%3B1778612769%26q-header-list%3Dhost%26q-url-param-list%3D%26q-signature%3Da5ec5e0321d7cb2ef8eee624085b5d80a6ac07af&",
]

TEXT_PROMPTS = [
    "Create a cinematic cyberpunk city night video with slow forward camera motion, wet reflective streets, and neon signage.",
    "Create a golden-hour coastal road trip video from an aerial perspective with waves crashing below and soft filmic light.",
]

REFERENCE_PROMPTS = [
    "Use the provided reference image and create a premium lifestyle video with subtle camera motion and preserved subject identity.",
    "Use the provided reference image and create a polished product showcase video with soft studio lighting and clean cinematic motion.",
]

FRAME_PROMPTS = [
    "Animate a smooth cinematic shot starting from the provided frame and ending naturally while preserving the subject and scene style.",
    "Create a coherent motion sequence that transitions between the provided frame images with stable identity and filmic movement.",
]

MODE_FAMILIES = [
    ("text", 1, 0),
    ("reference_single", 1, 1),
    ("reference_double", 1, 2),
    ("frame_single", 0, 1),
    ("frame_double", 0, 2),
]


@dataclass(frozen=True)
class AuditTaskSpec:
    task_id: str
    family: str
    gen_type: int
    image_count: int
    model_type: int
    proportion: int
    prompt: str
    queue_payload: dict[str, Any]


def build_prompt(family: str, variant_index: int) -> str:
    if family == "text":
        return TEXT_PROMPTS[variant_index % len(TEXT_PROMPTS)]
    if family.startswith("reference"):
        return REFERENCE_PROMPTS[variant_index % len(REFERENCE_PROMPTS)]
    return FRAME_PROMPTS[variant_index % len(FRAME_PROMPTS)]


def build_task_specs(run_id: str) -> list[AuditTaskSpec]:
    return build_task_specs_with_images(
        run_id,
        image_values=list(DEFAULT_IMAGE_URLS),
        image_mode="url",
        poll_timeout_ms=6 * 60 * 1000,
    )


def build_task_specs_with_images(
    run_id: str,
    *,
    image_values: list[str],
    image_mode: str,
    poll_timeout_ms: int,
) -> list[AuditTaskSpec]:
    specs: list[AuditTaskSpec] = []
    if len(image_values) < 2:
        raise ValueError("40-task audit needs at least two images for single/double image cases")
    image_a, image_b = image_values[:2]
    for family, gen_type, image_count in MODE_FAMILIES:
        for model_type in (0, 1):
            for proportion in (0, 1):
                for variant_index in range(2):
                    task_id = (
                        f"audit40-{run_id}-{family}-g{gen_type}-i{image_count}"
                        f"-m{model_type}-p{proportion}-v{variant_index + 1}"
                    )
                    payload: dict[str, Any] = {
                        "_id": task_id,
                        "type": 1,
                        "prompt": build_prompt(family, variant_index),
                        "gen_type": gen_type,
                        "model_type": model_type,
                        "proportion": proportion,
                        "poll_timeout_ms": poll_timeout_ms,
                    }
                    if image_count == 1:
                        if image_mode == "base64":
                            payload["image_base64"] = image_a
                        else:
                            payload["image_url"] = image_a
                    elif image_count == 2:
                        if image_mode == "base64":
                            payload["image_base64_list"] = [image_a, image_b]
                        else:
                            payload["image_url_list"] = [image_a, image_b]
                    specs.append(
                        AuditTaskSpec(
                            task_id=task_id,
                            family=family,
                            gen_type=gen_type,
                            image_count=image_count,
                            model_type=model_type,
                            proportion=proportion,
                            prompt=payload["prompt"],
                            queue_payload=payload,
                        )
                    )
    if len(specs) != 40:
        raise RuntimeError(f"Expected 40 audit tasks, got {len(specs)}")
    return specs


def image_file_to_data_uri(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Sample image not found: {path}")
    mime_type = mimetypes.guess_type(str(path))[0] or "image/png"
    return f"data:{mime_type};base64," + base64.b64encode(path.read_bytes()).decode("ascii")


def parse_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() not in {"0", "false", "no", "off", "n"}


def parse_viewport(raw_value: str) -> dict[str, int] | None:
    raw_value = str(raw_value or "").strip().lower()
    if raw_value in {"", "0", "none", "auto", "native"}:
        return None
    if "x" not in raw_value:
        raise ValueError(f"Invalid viewport {raw_value!r}; expected WIDTHxHEIGHT or 0")
    width_raw, height_raw = raw_value.split("x", 1)
    return {"width": int(width_raw), "height": int(height_raw)}


def resolve_image_values(args: argparse.Namespace) -> tuple[list[str], str]:
    if args.image_source == "url":
        values = args.image_url or list(DEFAULT_IMAGE_URLS)
        return values, "url"

    image_files = [Path(item) for item in (args.image_file or [])]
    if not image_files:
        image_files = [SAMPLE_IMAGE_A, SAMPLE_IMAGE_B]
    return [image_file_to_data_uri(path) for path in image_files], "base64"


def wait_for_empty_queue(redis_client: Any) -> None:
    queue_count = redis_client.zcard(TASK_CREATE_QUEUE)
    processing_count = redis_client.zcard(TASK_CREATE_PROCESSING_QUEUE)
    if queue_count or processing_count:
        raise RuntimeError(
            f"Redis queue is not empty: queue={queue_count}, processing={processing_count}"
        )


def enqueue_tasks(redis_client: Any, specs: list[AuditTaskSpec], priority: int = DEFAULT_TASK_PRIORITY) -> None:
    mapping = {
        dumps_queue_payload(spec.queue_payload): _encode_queue_score(priority)
        for spec in specs
    }
    redis_client.zadd(TASK_CREATE_QUEUE, mapping)


def build_consumer_env(args: argparse.Namespace) -> dict[str, str]:
    env = {
        "FLOW_VIDEO_LOCALE": args.locale,
        "FLOW_VIDEO_TIMEZONE_ID": args.timezone_id,
        "FLOW_CONSUMER_HEADLESS": "1" if parse_bool(args.headless) else "0",
        "FLOW_CONSUMER_BROWSER_POOL_SIZE": str(args.browser_pool_size),
        "FLOW_CONSUMER_CONTEXTS_PER_BROWSER": str(args.contexts_per_browser),
        "FLOW_CONSUMER_COOLDOWN_MIN_SEC": str(args.cooldown_min_seconds),
        "FLOW_CONSUMER_COOLDOWN_MAX_SEC": str(args.cooldown_max_seconds),
        "FLOW_BROWSER_VIEWPORT": args.viewport,
    }
    if args.extra_flag:
        env["FLOW_CONSUMER_EXTRA_FLAGS"] = ",".join(args.extra_flag)
    return env


def start_consumer(log_path: Path, env_overrides: dict[str, str]) -> subprocess.Popen[str]:
    python_exe = sys.executable
    env = os.environ.copy()
    env.update(env_overrides)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = open(log_path, "w", encoding="utf-8")
    return subprocess.Popen(
        [python_exe, "-m", "video_processing.consumers.redis_task_consumer"],
        cwd=str(ROOT),
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )


def task_success_from_path(local_video_path: str) -> bool:
    if not local_video_path:
        return False
    path = Path(local_video_path)
    return path.exists() and path.stat().st_size > 1024


def poll_results(
    collection: Any,
    specs: list[AuditTaskSpec],
    timeout_seconds: int,
    poll_interval_seconds: int,
) -> dict[str, dict[str, Any]]:
    task_ids = [spec.task_id for spec in specs]
    deadline = time.time() + timeout_seconds
    results: dict[str, dict[str, Any]] = {}
    while time.time() < deadline:
        docs = {
            str(doc["_id"]): doc
            for doc in collection.find({"_id": {"$in": task_ids}})
        }
        for spec in specs:
            doc = docs.get(spec.task_id, {})
            local_video_path = str(doc.get("local_video_path") or "")
            task_status = str(doc.get("task_status") or "")
            error_msg = str(doc.get("error_msg") or "")
            success = task_success_from_path(local_video_path)
            failed = task_status == "failed"
            if success or failed:
                results[spec.task_id] = {
                    "task_id": spec.task_id,
                    "family": spec.family,
                    "gen_type": spec.gen_type,
                    "image_count": spec.image_count,
                    "model_type": spec.model_type,
                    "proportion": spec.proportion,
                    "task_status": task_status,
                    "local_video_path": local_video_path,
                    "error_msg": error_msg,
                    "success": success,
                }
        if len(results) == len(specs):
            return results
        time.sleep(poll_interval_seconds)
    return results


async def run_specs_in_process(args: argparse.Namespace, specs: list[AuditTaskSpec]) -> dict[str, dict[str, Any]]:
    """Run the 40 specs in this Python process using redis_task_consumer generation helpers.

    This bypasses the Redis queue/subprocess wrapper but intentionally reuses
    redis_task_consumer.handle_single_task(), mark_task_processing(), mark_task_failed(),
    COS conversion, and error extraction.  With browser_pool_size=1 and
    contexts_per_browser=1 this is the simplest single-process/single-worker path.
    """
    collection = create_task_collection()
    headless = parse_bool(args.headless)
    extra_flags = list(args.extra_flag or ["--start-maximized"])
    viewport = parse_viewport(args.viewport)
    scraper = GoogleFlowVideoScraperV2(
        browser_pool_size=args.browser_pool_size,
        max_contexts_per_browser=args.contexts_per_browser,
        headless=headless,
        locale=args.locale,
        timezone_id=args.timezone_id,
        extra_flags=extra_flags,
        viewport=viewport,
        task_timeout_ms=args.task_timeout_ms,
    )

    results: dict[str, dict[str, Any]] = {}
    async with scraper:
        for index, spec in enumerate(specs, start=1):
            task = dict(spec.queue_payload)
            task_id = spec.task_id
            try:
                _convert_cos_urls_in_task(task)
                mark_task_processing(collection, task)
                local_path = await handle_single_task(scraper, collection, task)
                local_video_path = str(local_path)
                results[task_id] = {
                    "task_id": task_id,
                    "family": spec.family,
                    "gen_type": spec.gen_type,
                    "image_count": spec.image_count,
                    "model_type": spec.model_type,
                    "proportion": spec.proportion,
                    "task_status": "completed",
                    "local_video_path": local_video_path,
                    "error_msg": "",
                    "success": task_success_from_path(local_video_path),
                }
                print(f"[{index:02d}/{len(specs)}] success task_id={task_id} path={local_video_path}")
            except Exception as exc:
                error_msg = extract_error_message(exc, "采集异常")
                mark_task_failed(collection, task, error_msg)
                results[task_id] = {
                    "task_id": task_id,
                    "family": spec.family,
                    "gen_type": spec.gen_type,
                    "image_count": spec.image_count,
                    "model_type": spec.model_type,
                    "proportion": spec.proportion,
                    "task_status": "failed",
                    "local_video_path": "",
                    "error_msg": error_msg,
                    "success": False,
                }
                print(f"[{index:02d}/{len(specs)}] failed task_id={task_id} error={error_msg}")

            if index < len(specs):
                cooldown = max(0.0, float(args.cooldown_min_seconds))
                if cooldown:
                    await asyncio.sleep(cooldown)

    return results


def stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        process.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
        process.wait(timeout=10)
        return
    except Exception:
        pass
    process.terminate()
    try:
        process.wait(timeout=10)
    except Exception:
        process.kill()
        process.wait(timeout=10)


def build_report(
    run_id: str,
    specs: list[AuditTaskSpec],
    results: dict[str, dict[str, Any]],
    log_path: Path,
) -> dict[str, Any]:
    task_rows = []
    for spec in specs:
        row = results.get(spec.task_id) or {
            "task_id": spec.task_id,
            "family": spec.family,
            "gen_type": spec.gen_type,
            "image_count": spec.image_count,
            "model_type": spec.model_type,
            "proportion": spec.proportion,
            "task_status": "timeout",
            "local_video_path": "",
            "error_msg": "timeout_or_incomplete",
            "success": False,
        }
        task_rows.append(row)
    success_count = sum(1 for row in task_rows if row["success"])
    total_count = len(task_rows)
    success_rate = success_count / total_count if total_count else 0.0
    return {
        "run_id": run_id,
        "success_count": success_count,
        "total_count": total_count,
        "success_rate": success_rate,
        "consumer_log_path": str(log_path),
        "tasks": task_rows,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the 40-task audit against redis_task_consumer logic.")
    parser.add_argument(
        "--runner",
        choices=["in-process", "subprocess"],
        default="in-process",
        help="in-process=本进程内顺序调用 redis_task_consumer 生成逻辑；subprocess=旧模式启动消费者子进程。",
    )
    parser.add_argument("--timeout-seconds", type=int, default=4 * 60 * 60)
    parser.add_argument("--poll-interval-seconds", type=int, default=15)
    parser.add_argument("--locale", default="en-US")
    parser.add_argument("--timezone-id", default="America/Los_Angeles")
    parser.add_argument("--headless", default="true", help="true/false；默认 false，方便单进程有头观察。")
    parser.add_argument("--browser-pool-size", type=int, default=1)
    parser.add_argument("--contexts-per-browser", type=int, default=1)
    parser.add_argument("--cooldown-min-seconds", type=int, default=5)
    parser.add_argument("--cooldown-max-seconds", type=int, default=10)
    parser.add_argument("--task-timeout-ms", type=int, default=8 * 60 * 1000)
    parser.add_argument("--poll-timeout-ms", type=int, default=6 * 60 * 1000)
    parser.add_argument("--viewport", default="1920x1080")
    parser.add_argument("--extra-flag", action="append", default=[], help="浏览器启动参数，可传多次。")
    parser.add_argument(
        "--image-source",
        choices=["local", "url"],
        default="local",
        help="local=使用 flow 目录两张本地样图转 base64；url=使用脚本内置/传入 COS URL。",
    )
    parser.add_argument("--image-file", action="append", default=[], help="本地图片路径，可传两次覆盖默认样图。")
    parser.add_argument("--image-url", action="append", default=[], help="图片 URL，可传两次；需配合 --image-source url。")
    parser.add_argument("--queue-priority", type=int, default=DEFAULT_TASK_PRIORITY)
    parser.add_argument("--allow-non-empty-queue", action="store_true", help="subprocess 旧模式下允许 Redis 队列非空。")
    parser.add_argument("--dry-run", action="store_true", help="只生成 manifest，不启动浏览器/消费者。")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_id = time.strftime("%Y%m%d-%H%M%S")
    image_values, image_mode = resolve_image_values(args)
    specs = build_task_specs_with_images(
        run_id,
        image_values=image_values,
        image_mode=image_mode,
        poll_timeout_ms=args.poll_timeout_ms,
    )
    manifest_dir = ROOT / ".omx" / "logs"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / f"audit40-manifest-{run_id}.json"
    report_path = manifest_dir / f"audit40-report-{run_id}.json"
    consumer_log_path = manifest_dir / f"audit40-consumer-{run_id}.log"

    manifest_path.write_text(
        json.dumps([asdict(spec) for spec in specs], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    if args.dry_run:
        results: dict[str, dict[str, Any]] = {}
    elif args.runner == "in-process":
        results = asyncio.run(run_specs_in_process(args, specs))
    else:
        redis_client = create_redis_client()
        collection = create_task_collection()
        if not args.allow_non_empty_queue:
            wait_for_empty_queue(redis_client)

        consumer_process = start_consumer(consumer_log_path, build_consumer_env(args))
        try:
            time.sleep(5)
            if consumer_process.poll() is not None:
                raise RuntimeError(f"consumer exited early with code {consumer_process.returncode}")
            enqueue_tasks(redis_client, specs, priority=args.queue_priority)
            results = poll_results(
                collection=collection,
                specs=specs,
                timeout_seconds=args.timeout_seconds,
                poll_interval_seconds=args.poll_interval_seconds,
            )
        finally:
            stop_process(consumer_process)

    report = build_report(run_id, specs, results, consumer_log_path)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(
        {
            "run_id": run_id,
            "success_count": report["success_count"],
            "total_count": report["total_count"],
            "success_rate": report["success_rate"],
            "manifest_path": str(manifest_path),
            "report_path": str(report_path),
            "consumer_log_path": str(consumer_log_path),
        },
        ensure_ascii=False,
        indent=2,
    ))
    return 0 if report["success_rate"] >= 0.80 and report["total_count"] == 40 else 1


if __name__ == "__main__":
    raise SystemExit(main())
