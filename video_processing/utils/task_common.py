"""
视频处理任务公共工具
====================
此模块提供视频处理系统中各子模块共享的公共工具函数，
包括日志记录器创建、Redis/MongoDB 客户端构建、队列载荷序列化、
图片载荷规范化以及爬虫任务构建等。
"""

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

# Update path references for new location
_VIDEO_PROCESSING = Path(__file__).resolve().parent.parent.parent
_ACCOUNT_MGR = _VIDEO_PROCESSING / "account_mgr"
_FLOW_DIR = _VIDEO_PROCESSING / "flow"

for _path in (str(_VIDEO_PROCESSING), str(_FLOW_DIR), str(_ACCOUNT_MGR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

# Import directly from api_config module to avoid flow package issues
import importlib.util
spec = importlib.util.spec_from_file_location("api_config", str(_FLOW_DIR / "api_config.py"))
api_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api_config)

TASK_COLLECTION = api_config.TASK_COLLECTION
TASK_DB_NAME = api_config.TASK_DB_NAME
from account_mgr.config import MONGO_URI, REDIS_DB, REDIS_HOST, REDIS_PASSWORD, REDIS_PORT


TASK_CREATE_QUEUE = "task:create:queue"
TASK_CREATE_PROCESSING_QUEUE = "task:create:processing"

DEFAULT_MAX_RETRIES = 3
DEFAULT_POLL_TIMEOUT_MS = 4 * 60 * 1000
DEFAULT_UPLOAD_POLL_SECONDS = 10
SHANGHAI_TZ = timezone(timedelta(hours=8))

# 视频待处理目录（指向新的视频处理模块目录）
VIDEO_PENDING_DIR = _VIDEO_PROCESSING / "videos" / "pending"
VIDEO_PENDING_DIR.mkdir(parents=True, exist_ok=True)

# 日志目录指向新的视频处理模块目录
_LOG_PROCESSING_DIR = Path(__file__).resolve().parent.parent  # video_processing目录
LOG_DIR = _LOG_PROCESSING_DIR / "log"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "task_pipeline.log"


def get_logger(name: str) -> logging.Logger:
    """
    创建并配置一个日志记录器。

    此函数创建一个具有文件轮转功能的日志记录器，用于记录应用程序日志。

    参数:
        name: 日志记录器名称

    返回:
        logging.Logger: 配置好的日志记录器实例

    示例:
        logger = get_logger("MyModule")
        logger.info("启动应用程序")
    """
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
    """
    获取当前本地时间（上海时区）。

    返回:
        datetime: 上海时区的当前时间

    示例:
        current_time = now_local()
        print(f"当前时间: {current_time}")
    """
    return datetime.now(SHANGHAI_TZ)


def create_redis_client() -> Redis:
    """
    创建并配置 Redis 客户端。

    此函数创建一个连接到 Redis 服务器的客户端，并验证连接。

    返回:
        Redis: 配置好的 Redis 客户端实例

    异常:
        ConnectionError: 如果无法连接到 Redis 服务器

    示例:
        redis_client = create_redis_client()
        redis_client.set("key", "value")
    """
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
    """
    创建 MongoDB 任务集合。

    此函数连接到 MongoDB 并返回任务集合对象。

    返回:
        Any: MongoDB 集合对象

    示例:
        collection = create_task_collection()
        collection.find_one({"_id": "task123"})
    """
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        socketTimeoutMS=10000,
    )
    return client[TASK_DB_NAME][TASK_COLLECTION]


def parse_queue_payload(raw_payload: str) -> dict[str, Any]:
    """
    解析队列载荷字符串。

    将 JSON 字符串解析为字典，用于队列任务数据。

    参数:
        raw_payload: JSON 格式的载荷字符串

    返回:
        dict[str, Any]: 解析后的任务字典

    异常:
        ValueError: 如果载荷不是有效的 JSON 对象

    示例:
        task = parse_queue_payload('{"_id": "task123", "prompt": "创建视频"}')
    """
    task = json.loads(raw_payload)
    if not isinstance(task, dict):
        raise ValueError("队列任务必须是 JSON Object")
    return task


def dumps_queue_payload(task: dict[str, Any]) -> str:
    """
    将任务字典序列化为 JSON 字符串。

    参数:
        task: 要序列化的任务字典

    返回:
        str: JSON 格式的载荷字符串

    示例:
        payload = dumps_queue_payload({"_id": "task123", "prompt": "创建视频"})
    """
    return json.dumps(task, ensure_ascii=False, separators=(",", ":"))


def is_http_url(value: str) -> bool:
    """
    检查字符串是否为 HTTP/HTTPS URL。

    参数:
        value: 要检查的字符串

    返回:
        bool: 如果是 HTTP/HTTPS URL 则为 True

    示例:
        if is_http_url("https://example.com"):
            print("这是一个 URL")
    """
    return bool(re.match(r"^https?://", value, re.IGNORECASE))


def _normalize_single_image_text(image_value: Any, image_type: str | None = None) -> tuple[str | None, str | None]:
    """
    规范化单条图片数据，返回 (类型, 值) 元组。

    自动识别 URL 或 Base64 格式；若指定 image_type 则按指定类型处理。

    参数:
        image_value: 图片数据（URL 字符串、Base64 字符串或 None）
        image_type: 可选类型提示（"url" 或 "base64"）

    返回:
        tuple[str | None, str | None]: ("url"/"base64", 值) 或 (None, None)
    """
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
    """
    规范化图片载荷数据。

    此函数将各种格式的图片数据（URL、base64、列表等）转换为统一的格式。

    参数:
        image_value: 图片数据，可以是 URL、base64 字符串或列表
        image_type: 可选的图片类型提示（"url" 或 "base64"）

    返回:
        dict[str, Any]: 规范化后的图片数据字典

    示例:
        normalized = normalize_image_payload("https://example.com/image.jpg", "url")
        # 或
        normalized = normalize_image_payload(["url1.jpg", "url2.jpg"])
    """
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
    """
    构建爬虫任务数据。

    此函数将通用任务格式转换为爬虫特定的任务格式，处理各种参数和图片数据。

    参数:
        task: 原始任务数据字典，必须包含 "_id" 和 "prompt" 字段

    返回:
        dict[str, Any]: 构建好的爬虫任务字典

    示例:
        scraper_task = build_scraper_task({
            "_id": "task123",
            "prompt": "美丽的日落",
            "gen_type": 1,
            "image_url": "https://example.com/image.jpg"
        })
    """
    payload = {
        "_id": str(task.get("_id", "")).strip(),
        "prompt": str(task.get("prompt", "")).strip(),
        "variant_count": 1,
        "poll_timeout_ms": int(task.get("poll_timeout_ms", DEFAULT_POLL_TIMEOUT_MS)),
    }
    for passthrough_key in ("gen_type", "proportion", "model_type"):
        if passthrough_key in task:
            payload[passthrough_key] = task[passthrough_key]
    if "image_value" in task:
        payload.update(normalize_image_payload(task.get("image_value"), str(task.get("image_type", "") or "")))
    else:
        payload.update(normalize_image_payload(task.get("image")))
    return payload


def make_local_video_path(task_id: str, suffix: str = ".mp4") -> Path:
    """
    根据任务 ID 生成本地视频保存路径。

    将任务 ID 中的特殊字符替换为下划线，拼接后缀名，
    返回 VIDEO_PENDING_DIR 下的完整路径。

    参数:
        task_id: 任务唯一标识
        suffix: 文件后缀名（默认 ".mp4"）

    返回:
        Path: 本地视频文件的完整路径
    """
    safe_task_id = re.sub(r"[^a-zA-Z0-9._-]+", "_", task_id).strip("._") or "task"
    normalized_suffix = suffix if suffix.startswith(".") else f".{suffix}"
    return VIDEO_PENDING_DIR / f"{safe_task_id}{normalized_suffix}"


def recover_processing_queue(redis_client: Redis, logger: logging.Logger) -> int:
    """
    恢复处理中队列中的遗留任务。

    将 TASK_CREATE_PROCESSING_QUEUE 中残留的任务逐条移回
    TASK_CREATE_QUEUE，通常在服务启动时调用。

    参数:
        redis_client: Redis 客户端
        logger: 日志记录器

    返回:
        int: 恢复的任务数量
    """
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
    """
    从处理中队列中移除指定的任务载荷。

    参数:
        redis_client: Redis 客户端
        raw_payload: 要移除的载荷字符串
    """
    redis_client.lrem(TASK_CREATE_PROCESSING_QUEUE, 1, raw_payload)


def requeue_with_higher_priority(redis_client: Redis, raw_payload: str, task: dict[str, Any]) -> int:
    """
    将失败任务从处理中队列移除，递增重试计数后重新推入主队列头部。

    参数:
        redis_client: Redis 客户端
        raw_payload: 原始载荷字符串
        task: 任务字典（会被原地修改 retry_count）

    返回:
        int: 更新后的重试次数
    """
    remove_processing_payload(redis_client, raw_payload)
    task["retry_count"] = int(task.get("retry_count", 0)) + 1
    redis_client.lpush(TASK_CREATE_QUEUE, dumps_queue_payload(task))
    return int(task["retry_count"])
