from __future__ import annotations

import argparse
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
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from video_processing.consumers.redis_task_consumer import (  # noqa: E402
    DEFAULT_TASK_PRIORITY,
    TASK_CREATE_PROCESSING_QUEUE,
    TASK_CREATE_QUEUE,
    _encode_queue_score,
)
from video_processing.utils.task_common import (  # noqa: E402
    create_redis_client,
    create_task_collection,
    dumps_queue_payload,
)

SAMPLE_IMAGE_A = ROOT / "flow" / "fb002687-fa5a-453f-85e0-00362b9af9bc.png"
SAMPLE_IMAGE_B = ROOT / "flow" / "6f31785f-9634-4a16-bf5b-f741a7acd617.jpg"

TEXT_PROMPTS = [
    "Create a cinematic cyberpunk city night video with slow forward camera motion, wet reflective streets, and neon signage.",
    "Create a golden-hour coastal road trip video from an aerial perspective with waves below and soft filmic light.",
    "Create a cozy coffee shop morning video with steam rising, soft window light, and gentle handheld camera motion.",
    "Create a futuristic product reveal video with smooth dolly movement, premium reflections, and clean studio lighting.",
    "Create a peaceful mountain lake video at sunrise with mist, slow camera push-in, and realistic atmospheric depth.",
]

REFERENCE_PROMPTS = [
    "Use the provided reference image to create a premium lifestyle video with subtle camera motion and preserved subject identity.",
    "Use the provided reference image to create a polished product showcase video with soft studio lighting and clean cinematic motion.",
    "Use the provided reference image and animate a slow parallax camera move while preserving the main subject and style.",
    "Use the provided reference image to create a commercial-style reveal with stable details, natural movement, and filmic color.",
    "Use the provided reference image to make a short cinematic clip with gentle motion and consistent composition.",
]

FRAME_PROMPTS = [
    "Animate a smooth cinematic shot from the provided frame while preserving the subject, scene layout, and visual style.",
    "Create a coherent motion sequence using the provided frame image with stable identity and filmic movement.",
    "Use the frame image as the starting point and generate natural camera motion with consistent details.",
    "Animate the supplied frame into a realistic short video with smooth movement and no abrupt subject changes.",
    "Generate a stable cinematic transition from the supplied frame, preserving lighting, composition, and subject identity.",
]

MODE_FAMILIES: dict[str, dict[str, int]] = {
    "text": {"gen_type": 1, "image_count": 0},
    "reference_single": {"gen_type": 1, "image_count": 1},
    "reference_double": {"gen_type": 1, "image_count": 2},
    "frame_single": {"gen_type": 0, "image_count": 1},
    "frame_double": {"gen_type": 0, "image_count": 2},
}

MODEL_PROPORTION_CYCLE = [(0, 0), (0, 1), (1, 0), (1, 1), (0, 0)]


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


def image_file_to_data_uri(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Sample image not found: {path}")
    mime_type = mimetypes.guess_type(str(path))[0] or "image/png"
    return f"data:{mime_type};base64," + base64.b64encode(path.read_bytes()).decode("ascii")


def build_prompt(family: str, variant_index: int) -> str:
    prompts = TEXT_PROMPTS if family == "text" else FRAME_PROMPTS if family.startswith("frame") else REFERENCE_PROMPTS
    return prompts[variant_index % len(prompts)]


def parse_families(raw_families: str | None) -> list[str]:
    if not raw_families:
        return list(MODE_FAMILIES)
    families = [item.strip() for item in raw_families.split(",") if item.strip()]
    unknown = sorted(set(families) - set(MODE_FAMILIES))
    if unknown:
        raise ValueError(f"Unknown families: {unknown}; supported={sorted(MODE_FAMILIES)}")
    return families


def build_task_specs(
    run_id: str,
    *,
    families: Iterable[str] | None = None,
    tasks_per_family: int = 5,
    image_data_uris: list[str] | None = None,
    poll_timeout_ms: int = 8 * 60 * 1000,
) -> list[AuditTaskSpec]:
    selected_families = list(families or MODE_FAMILIES)
    if tasks_per_family < 1:
        raise ValueError("tasks_per_family must be >= 1")
    image_data_uris = image_data_uris or []
    if len(image_data_uris) < 2 and any(MODE_FAMILIES[f]["image_count"] > 0 for f in selected_families):
        raise ValueError("At least two image data URIs are required for image/frame audit families")

    specs: list[AuditTaskSpec] = []
    for family in selected_families:
        family_cfg = MODE_FAMILIES[family]
        gen_type = family_cfg["gen_type"]
        image_count = family_cfg["image_count"]
        for variant_index in range(tasks_per_family):
            model_type, proportion = MODEL_PROPORTION_CYCLE[variant_index % len(MODEL_PROPORTION_CYCLE)]
            task_id = (
                f"type-audit-{run_id}-{family}-g{gen_type}-i{image_count}"
                f"-m{model_type}-p{proportion}-n{variant_index + 1}"
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
                payload["image_base64"] = image_data_uris[variant_index % len(image_data_uris)]
            elif image_count == 2:
                payload["image_base64_list"] = [
                    image_data_uris[variant_index % len(image_data_uris)],
                    image_data_uris[(variant_index + 1) % len(image_data_uris)],
                ]
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
    return specs


def wait_for_empty_queue(redis_client: Any) -> None:
    queue_count = redis_client.zcard(TASK_CREATE_QUEUE)
    processing_count = redis_client.zcard(TASK_CREATE_PROCESSING_QUEUE)
    if queue_count or processing_count:
        raise RuntimeError(f"Redis queue is not empty: queue={queue_count}, processing={processing_count}")


def enqueue_tasks(redis_client: Any, specs: list[AuditTaskSpec], priority: int = DEFAULT_TASK_PRIORITY) -> None:
    mapping = {dumps_queue_payload(spec.queue_payload): _encode_queue_score(priority) for spec in specs}
    redis_client.zadd(TASK_CREATE_QUEUE, mapping)


def default_consumer_env(args: argparse.Namespace) -> dict[str, str]:
    return {
        # Values observed via browser MCP on the user's active Flow tab on 2026-05-14.
        "FLOW_VIDEO_LOCALE": args.locale,
        "FLOW_VIDEO_TIMEZONE_ID": args.timezone_id,
        "FLOW_BROWSER_MAJOR": args.browser_major,
        "FLOW_CONSUMER_HEADLESS": "1" if args.headless else "0",
        "FLOW_BROWSER_VIEWPORT": args.viewport,
        "FLOW_ENABLE_CONTEXT_STEALTH_SCRIPT": "1",
        "FLOW_FORCE_CONTEXT_FINGERPRINT": "1",
        "FLOW_CONSUMER_BROWSER_POOL_SIZE": str(args.browser_pool_size),
        "FLOW_CONSUMER_CONTEXTS_PER_BROWSER": str(args.contexts_per_browser),
        "FLOW_CONSUMER_COOLDOWN_MIN_SEC": str(args.cooldown_min_seconds),
        "FLOW_CONSUMER_COOLDOWN_MAX_SEC": str(args.cooldown_max_seconds),
    }


def start_consumer(log_path: Path, env_overrides: dict[str, str]) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env.update(env_overrides)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = open(log_path, "w", encoding="utf-8")
    return subprocess.Popen(
        [sys.executable, "-m", "video_processing.consumers.redis_task_consumer"],
        cwd=str(ROOT),
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )


def is_success_doc(doc: dict[str, Any]) -> bool:
    local_video_path = str(doc.get("local_video_path") or "")
    if not local_video_path:
        return False
    path = Path(local_video_path)
    if not path.exists() or path.stat().st_size <= 1024:
        return False
    mime = str(doc.get("mime") or doc.get("video_mime_type") or "")
    return not mime or mime.startswith("video/") or path.suffix.lower() in {".mp4", ".webm", ".mov"}


def poll_results(collection: Any, specs: list[AuditTaskSpec], timeout_seconds: int, poll_interval_seconds: int) -> dict[str, dict[str, Any]]:
    task_ids = [spec.task_id for spec in specs]
    deadline = time.time() + timeout_seconds
    results: dict[str, dict[str, Any]] = {}
    while time.time() < deadline:
        docs = {str(doc["_id"]): doc for doc in collection.find({"_id": {"$in": task_ids}})}
        for spec in specs:
            doc = docs.get(spec.task_id, {})
            fallback_video_path = ROOT / "video_processing" / "downloaded_videos" / f"{spec.task_id}.mp4"
            if not doc and fallback_video_path.exists():
                doc = {"local_video_path": str(fallback_video_path), "mime": "video/mp4"}
            task_status = str(doc.get("task_status") or "")
            local_video_path = str(doc.get("local_video_path") or "")
            error_msg = str(doc.get("error_msg") or "")
            success = is_success_doc(doc)
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
                    "file_size_bytes": Path(local_video_path).stat().st_size if local_video_path and Path(local_video_path).exists() else 0,
                    "mime": str(doc.get("mime") or doc.get("video_mime_type") or ""),
                    "error_msg": error_msg,
                    "success": success,
                }
        if len(results) == len(specs):
            return results
        time.sleep(poll_interval_seconds)
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
    env_overrides: dict[str, str],
    success_threshold: float,
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
            "file_size_bytes": 0,
            "mime": "",
            "error_msg": "timeout_or_incomplete",
            "success": False,
        }
        task_rows.append(row)

    family_metrics: dict[str, dict[str, Any]] = {}
    for family in sorted({row["family"] for row in task_rows}):
        family_rows = [row for row in task_rows if row["family"] == family]
        success_count = sum(1 for row in family_rows if row["success"])
        family_metrics[family] = {
            "success_count": success_count,
            "total_count": len(family_rows),
            "success_rate": success_count / len(family_rows) if family_rows else 0.0,
            "passed": bool(family_rows) and (success_count / len(family_rows)) >= success_threshold,
        }

    success_count = sum(1 for row in task_rows if row["success"])
    total_count = len(task_rows)
    success_rate = success_count / total_count if total_count else 0.0
    return {
        "run_id": run_id,
        "success_count": success_count,
        "total_count": total_count,
        "success_rate": success_rate,
        "success_threshold": success_threshold,
        "passed_global_threshold": success_rate >= success_threshold,
        "passed_each_family_threshold": all(item["passed"] for item in family_metrics.values()),
        "consumer_log_path": str(log_path),
        "consumer_env_overrides": env_overrides,
        "family_metrics": family_metrics,
        "tasks": task_rows,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a Redis consumer audit with 5 tasks per video task family and require >=80% generated-video success."
    )
    parser.add_argument("--tasks-per-family", type=int, default=5)
    parser.add_argument("--families", default="", help=f"Comma-separated subset. Supported: {','.join(MODE_FAMILIES)}")
    parser.add_argument("--success-threshold", type=float, default=0.80)
    parser.add_argument("--timeout-seconds", type=int, default=4 * 60 * 60)
    parser.add_argument("--poll-interval-seconds", type=int, default=15)
    parser.add_argument("--poll-timeout-ms", type=int, default=8 * 60 * 1000)
    parser.add_argument("--locale", default=os.getenv("FLOW_VIDEO_LOCALE") or os.getenv("FLOW_BROWSER_LOCALE") or "en-US")
    parser.add_argument("--timezone-id", default=os.getenv("FLOW_VIDEO_TIMEZONE_ID") or os.getenv("FLOW_BROWSER_TIMEZONE_ID") or "America/Los_Angeles")
    parser.add_argument("--browser-major", default=os.getenv("FLOW_BROWSER_MAJOR") or "145")
    parser.add_argument("--viewport", default="1920x1080")
    parser.add_argument("--headless", action="store_true", help="Run consumer headless. Default is headed to match the MCP-observed browser.")
    parser.add_argument("--browser-pool-size", type=int, default=1, help="Default 1 for VPN stability; increase only after success rate is stable.")
    parser.add_argument("--contexts-per-browser", type=int, default=1, help="Default 1 to avoid concurrent tasks sharing a shifting VPN route.")
    parser.add_argument("--cooldown-min-seconds", type=int, default=8)
    parser.add_argument("--cooldown-max-seconds", type=int, default=15)
    parser.add_argument("--image-a", type=Path, default=SAMPLE_IMAGE_A)
    parser.add_argument("--image-b", type=Path, default=SAMPLE_IMAGE_B)
    parser.add_argument("--allow-non-empty-queue", action="store_true")
    parser.add_argument("--queue-priority", type=int, default=100, help="Use a higher priority so the current audit runs before stale test entries.")
    parser.add_argument("--dry-run", action="store_true", help="Write manifest/report inputs without starting the consumer.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not 0 < args.success_threshold <= 1:
        raise ValueError("--success-threshold must be in (0, 1]")

    run_id = time.strftime("%Y%m%d-%H%M%S")
    families = parse_families(args.families)
    image_data_uris = [image_file_to_data_uri(args.image_a), image_file_to_data_uri(args.image_b)]
    specs = build_task_specs(
        run_id,
        families=families,
        tasks_per_family=args.tasks_per_family,
        image_data_uris=image_data_uris,
        poll_timeout_ms=args.poll_timeout_ms,
    )

    artifact_dir = ROOT / ".omx" / "logs"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = artifact_dir / f"task-type-audit-manifest-{run_id}.json"
    report_path = artifact_dir / f"task-type-audit-report-{run_id}.json"
    consumer_log_path = artifact_dir / f"task-type-audit-consumer-{run_id}.log"
    env_overrides = default_consumer_env(args)

    manifest_path.write_text(
        json.dumps([asdict(spec) for spec in specs], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    if args.dry_run:
        dry_report = build_report(run_id, specs, {}, consumer_log_path, env_overrides, args.success_threshold)
        dry_report["dry_run"] = True
        report_path.write_text(json.dumps(dry_report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"run_id": run_id, "total_count": len(specs), "manifest_path": str(manifest_path), "report_path": str(report_path), "dry_run": True}, ensure_ascii=False, indent=2))
        return 0

    redis_client = create_redis_client()
    collection = create_task_collection()
    if not args.allow_non_empty_queue:
        wait_for_empty_queue(redis_client)

    enqueue_tasks(redis_client, specs, priority=args.queue_priority)
    consumer_process = start_consumer(consumer_log_path, env_overrides)
    try:
        time.sleep(5)
        if consumer_process.poll() is not None:
            raise RuntimeError(f"consumer exited early with code {consumer_process.returncode}; see {consumer_log_path}")
        results = poll_results(collection, specs, args.timeout_seconds, args.poll_interval_seconds)
    finally:
        stop_process(consumer_process)

    report = build_report(run_id, specs, results, consumer_log_path, env_overrides, args.success_threshold)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    summary = {
        "run_id": run_id,
        "success_count": report["success_count"],
        "total_count": report["total_count"],
        "success_rate": report["success_rate"],
        "passed_global_threshold": report["passed_global_threshold"],
        "passed_each_family_threshold": report["passed_each_family_threshold"],
        "manifest_path": str(manifest_path),
        "report_path": str(report_path),
        "consumer_log_path": str(consumer_log_path),
        "family_metrics": report["family_metrics"],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if report["passed_global_threshold"] and report["passed_each_family_threshold"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
