from __future__ import annotations

import json
import logging
import logging.handlers
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pymongo import MongoClient
from redis import Redis

_FLOW_DIR = Path(__file__).resolve().parent
_ROOT = _FLOW_DIR.parent
_ACCOUNT_MGR = _ROOT / "account_mgr"

for _path in (str(_ROOT), str(_FLOW_DIR), str(_ACCOUNT_MGR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from api_config import TASK_COLLECTION, TASK_DB_NAME
from account_mgr.config import MONGO_URI, REDIS_DB, REDIS_HOST, REDIS_PASSWORD, REDIS_PORT


TASK_CREATE_QUEUE = "task:create:queue"
TASK_CREATE_PROCESSING_QUEUE = "task:create:processing"

DEFAULT_MAX_RETRIES = 3
DEFAULT_POLL_TIMEOUT_MS = 4 * 60 * 1000
DEFAULT_UPLOAD_POLL_SECONDS = 10
SHANGHAI_TZ = timezone(timedelta(hours=8))

VIDEO_PENDING_DIR = _FLOW_DIR / "videos" / "pending"
VIDEO_PENDING_DIR.mkdir(parents=True, exist_ok=True)

LOG_DIR = _FLOW_DIR / "log"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "task_pipeline.log"


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        handler = logging.handlers.RotatingFileHandler(
            LOG_FILE,
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False
    return logger


def now_local() -> datetime:
    return datetime.now(SHANGHAI_TZ)


def create_redis_client() -> Redis:
    client = Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,
        socket_timeout=10,
        socket_connect_timeout=10,
    )
    client.ping()
    return client


def create_task_collection() -> Any:
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        socketTimeoutMS=10000,
    )
    return client[TASK_DB_NAME][TASK_COLLECTION]


def parse_queue_payload(raw_payload: str) -> dict[str, Any]:
    task = json.loads(raw_payload)
    if not isinstance(task, dict):
        raise ValueError("队列任务必须是 JSON Object")
    return task


def dumps_queue_payload(task: dict[str, Any]) -> str:
    return json.dumps(task, ensure_ascii=False, separators=(",", ":"))


def is_http_url(value: str) -> bool:
    return bool(re.match(r"^https?://", value, re.IGNORECASE))


def _normalize_single_image_text(image_value: Any, image_type: str | None = None) -> tuple[str | None, str | None]:
    if image_value in (None, ""):
        return None, None

    image_text = str(image_value).strip()
    if not image_text:
        return None, None

    normalized_type = (image_type or "").strip().lower()
    if normalized_type == "url":
        return "url", image_text

    if normalized_type == "base64":
        if image_text.startswith("data:") and "," in image_text:
            image_text = image_text.split(",", 1)[1].strip()
        return "base64", image_text

    if is_http_url(image_text):
        return "url", image_text

    if image_text.startswith("data:") and "," in image_text:
        image_text = image_text.split(",", 1)[1].strip()

    return "base64", image_text


def normalize_image_payload(image_value: Any, image_type: str | None = None) -> dict[str, Any]:
    if image_value in (None, "", []):
        return {}

    raw_items = image_value if isinstance(image_value, list) else [image_value]
    normalized_items: list[tuple[str, str]] = []

    for item in raw_items:
        normalized_kind, normalized_value = _normalize_single_image_text(item, image_type)
        if normalized_kind and normalized_value:
            normalized_items.append((normalized_kind, normalized_value))

    if not normalized_items:
        return {}

    if len(normalized_items) == 1:
        normalized_kind, normalized_value = normalized_items[0]
        if normalized_kind == "url":
            return {"image_url": normalized_value}
        return {"image_base64": normalized_value}

    if all(kind == "url" for kind, _ in normalized_items):
        return {"image_url_list": [value for _, value in normalized_items]}

    if all(kind == "base64" for kind, _ in normalized_items):
        return {"image_base64_list": [value for _, value in normalized_items]}

    raise ValueError("image_value 中不能混传 url 和 base64，请拆分后重试")


def build_scraper_task(task: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "_id": str(task.get("_id", "")).strip(),
        "prompt": str(task.get("prompt", "")).strip(),
        "variant_count": 1,
        "poll_timeout_ms": int(task.get("poll_timeout_ms", DEFAULT_POLL_TIMEOUT_MS)),
    }
    if "image_value" in task:
        payload.update(normalize_image_payload(task.get("image_value"), str(task.get("image_type", "") or "")))
    else:
        payload.update(normalize_image_payload(task.get("image")))
    return payload


def make_local_video_path(task_id: str, suffix: str = ".mp4") -> Path:
    safe_task_id = re.sub(r"[^a-zA-Z0-9._-]+", "_", task_id).strip("._") or "task"
    normalized_suffix = suffix if suffix.startswith(".") else f".{suffix}"
    return VIDEO_PENDING_DIR / f"{safe_task_id}{normalized_suffix}"


def recover_processing_queue(redis_client: Redis, logger: logging.Logger) -> int:
    recovered = 0
    while True:
        moved = redis_client.rpoplpush(TASK_CREATE_PROCESSING_QUEUE, TASK_CREATE_QUEUE)
        if moved is None:
            break
        recovered += 1
    if recovered:
        logger.info(f"[recover] 已将 {recovered} 条遗留 processing 任务恢复回主队列")
    return recovered


def remove_processing_payload(redis_client: Redis, raw_payload: str) -> None:
    redis_client.lrem(TASK_CREATE_PROCESSING_QUEUE, 1, raw_payload)


def requeue_with_higher_priority(redis_client: Redis, raw_payload: str, task: dict[str, Any]) -> int:
    remove_processing_payload(redis_client, raw_payload)
    task["retry_count"] = int(task.get("retry_count", 0)) + 1
    redis_client.lpush(TASK_CREATE_QUEUE, dumps_queue_payload(task))
    return int(task["retry_count"])
