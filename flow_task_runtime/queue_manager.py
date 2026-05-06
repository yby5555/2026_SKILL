"""Redis 队列和任务构建工具。"""

from __future__ import annotations

import asyncio
import json
import re
import time
from typing import Any

from .config import TASK_CREATE_PROCESSING_QUEUE, TASK_CREATE_QUEUE

DEFAULT_POLL_TIMEOUT_MS = 8 * 60 * 1000
SCORE_TIME_FACTOR = 10**13
MAX_TIMESTAMP_MS = 9_999_999_999_999


def parse_task_payload(raw_payload: str) -> dict[str, Any]:
    """把队列字符串解析为任务字典。

    参数:
        raw_payload: Redis 队列中取出的 JSON 字符串。

    返回:
        dict[str, Any]: 解析后的任务字典。
    """
    task = json.loads(raw_payload)
    if not isinstance(task, dict):
        raise ValueError("队列任务必须是 JSON Object")
    return task


def dumps_task_payload(task: dict[str, Any]) -> str:
    """把任务字典编码成紧凑 JSON。

    参数:
        task: 要编码的任务字典。

    返回:
        str: 紧凑 JSON 字符串。
    """
    return json.dumps(task, ensure_ascii=False, separators=(",", ":"))


def is_http_url(value: str) -> bool:
    """判断文本是否为 http / https URL。

    参数:
        value: 待判断的字符串。

    返回:
        bool: 是否为 URL。
    """
    return bool(re.match(r"^https?://", value, re.IGNORECASE))


def _normalize_single_image_text(image_value: Any, image_type: str | None = None) -> tuple[str | None, str | None]:
    """把单张图片输入规范化为 url 或 base64。

    参数:
        image_value: 原始图片输入，可以是 URL、Base64 或空值。
        image_type: 调用方显式声明的图片类型。

    返回:
        tuple[str | None, str | None]:
            返回 (类型, 值)，类型为 url/base64，失败时返回空值。
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
    """把旧任务里的图片字段转换成统一结构。

    参数:
        image_value: 原始图片字段，可以是单张或多张。
        image_type: 图片类型提示。

    返回:
        dict[str, Any]: 统一后的图片字段字典。
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

    raise ValueError("image_value 中不能混合 url 和 base64，请拆分后重试")


def build_scraper_task(task: dict[str, Any]) -> dict[str, Any]:
    """把队列任务转换成抓取器真正执行所需的结构。

    参数:
        task: 原始队列任务字典。

    返回:
        dict[str, Any]: 供抓取器直接消费的任务结构。
    """
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


def validate_task(task: dict[str, Any]) -> tuple[str, str]:
    """校验队列任务结构并返回关键字段。

    参数:
        task: 待校验任务字典。

    返回:
        tuple[str, str]: 返回 (task_id, prompt)。
    """
    task_id = str(task.get("_id", "")).strip()
    prompt = str(task.get("prompt", "")).strip()
    if not task_id:
        raise ValueError("缺少 _id")
    if not prompt:
        raise ValueError("缺少 prompt")
    if int(task.get("type", -1)) != 1:
        raise ValueError(f"只支持处理 type=1 的视频任务，当前 type={task.get('type')}")
    return task_id, prompt


def compute_retry_priority(default_task_priority: int, retry_priority_step: int, retry_count: int) -> int:
    """根据重试次数计算新的队列优先级。

    参数:
        default_task_priority: 基础优先级。
        retry_priority_step: 每次重试增加的优先级步长。
        retry_count: 当前重试次数。

    返回:
        int: 计算后的优先级。
    """
    return int(default_task_priority) + max(0, int(retry_count)) * int(retry_priority_step)


def encode_queue_score(priority: int, timestamp_ms: int | None = None) -> int:
    """把优先级和时间编码成 Sorted Set 的 score。

    参数:
        priority: 优先级数值。
        timestamp_ms: 毫秒时间戳；不传则使用当前时间。

    返回:
        int: Redis Sorted Set 使用的 score。
    """
    if timestamp_ms is None:
        timestamp_ms = int(time.time() * 1000)
    bounded_priority = max(0, int(priority))
    inverted_time = max(0, MAX_TIMESTAMP_MS - int(timestamp_ms))
    return bounded_priority * SCORE_TIME_FACTOR + inverted_time


def decode_queue_priority(score: float | int) -> int:
    """从 score 中反推出原始优先级。

    参数:
        score: Redis Sorted Set 中的 score。

    返回:
        int: 解码后的优先级。
    """
    return int(float(score)) // SCORE_TIME_FACTOR


def recover_processing_queue(redis_client: Any, logger: Any) -> int:
    """把遗留的 processing 任务恢复回主队列。

    参数:
        redis_client: Redis 客户端。
        logger: 日志器对象。

    返回:
        int: 恢复回主队列的任务数量。
    """
    recovered = 0
    while True:
        moved_items = redis_client.zpopmax(TASK_CREATE_PROCESSING_QUEUE, count=100)
        if not moved_items:
            break
        for raw_payload, score in moved_items:
            redis_client.zadd(TASK_CREATE_QUEUE, {raw_payload: score})
            recovered += 1
    if recovered:
        logger.info(f"[recover] 已将 {recovered} 条遗留 processing 任务恢复回主队列")
    return recovered


async def pop_highest_priority_task(redis_client: Any, block_timeout_seconds: int) -> tuple[str, float] | tuple[None, None]:
    """从主队列阻塞式取出最高优先级任务并转入 processing 队列。

    参数:
        redis_client: Redis 客户端。
        block_timeout_seconds: 阻塞等待秒数。

    返回:
        tuple[str, float] | tuple[None, None]:
            成功时返回 (payload, score)，失败时返回 (None, None)。
    """
    result = await asyncio.to_thread(redis_client.bzpopmax, TASK_CREATE_QUEUE, int(block_timeout_seconds))
    if not result:
        return None, None

    _, raw_payload, score = result
    raw_score = float(score)
    redis_client.zadd(TASK_CREATE_PROCESSING_QUEUE, {raw_payload: raw_score})
    return raw_payload, raw_score


def ack_processing_payload(redis_client: Any, raw_payload: str) -> None:
    """确认任务已处理完成并从 processing 队列移除。

    参数:
        redis_client: Redis 客户端。
        raw_payload: 要确认移除的原始任务字符串。
    """
    redis_client.zrem(TASK_CREATE_PROCESSING_QUEUE, raw_payload)


def requeue_task(
    redis_client: Any,
    raw_payload: str,
    task: dict[str, Any],
    *,
    default_task_priority: int,
    retry_priority_step: int,
) -> tuple[int, int]:
    """把任务提升重试优先级后重新放回主队列。

    参数:
        redis_client: Redis 客户端。
        raw_payload: processing 队列中的原始任务字符串。
        task: 当前任务字典。
        default_task_priority: 基础优先级。
        retry_priority_step: 每次重试增加的优先级步长。

    返回:
        tuple[int, int]: 返回 (新的重试次数, 新的 score)。
    """
    ack_processing_payload(redis_client, raw_payload)
    task["retry_count"] = int(task.get("retry_count", 0)) + 1
    retry_priority = compute_retry_priority(default_task_priority, retry_priority_step, task["retry_count"])
    retry_score = encode_queue_score(retry_priority)
    redis_client.zadd(TASK_CREATE_QUEUE, {dumps_task_payload(task): retry_score})
    return int(task["retry_count"]), retry_score
