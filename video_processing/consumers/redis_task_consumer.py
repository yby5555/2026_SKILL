from __future__ import annotations

import asyncio
import base64
import json
import os
import random
import re
import time
from pathlib import Path
from typing import Any

import httpx

import sys
from pathlib import Path

# 添加父目录到导入路径
_video_processing = Path(__file__).resolve().parent.parent.parent
if str(_video_processing) not in sys.path:
    sys.path.insert(0, str(_video_processing))

from video_processing.scrapers.automation_video_v2_click_consumer import (
    GoogleFlowVideoScraperV2,
    resolve_flow_locale,
    resolve_flow_timezone_id,
)
from video_processing.utils.task_common import (
    DEFAULT_MAX_RETRIES,
    TASK_CREATE_PROCESSING_QUEUE,
    TASK_CREATE_QUEUE,
    build_scraper_task,
    create_redis_client,
    create_task_collection,
    dumps_queue_payload,
    get_logger,
    now_local,
    parse_queue_payload,
)

logger = get_logger("RedisTaskVideoConsumer")

from account_mgr.cos_utils import download_cos_image, is_cos_url

MAX_RETRIES = DEFAULT_MAX_RETRIES  # 单个任务最大重试次数
REDIS_BLOCK_TIMEOUT_SECONDS = 5  # Redis 阻塞等待新任务的超时秒数
DOWNLOAD_TIMEOUT_SECONDS = 300  # 文件下载超时秒数（5分钟）
DEFAULT_TASK_PRIORITY = 10  # 新任务插入时的默认优先级分值
RETRY_PRIORITY_STEP = 10  # 每次重试时优先级分值的递增步长，分值越高越晚被消费
SCORE_TIME_FACTOR = 10**13  # 时间戳转分值所用的除数因子（13位毫秒级 → 约4位分值）
MAX_TIMESTAMP_MS = 9_999_999_999_999  # 13位毫秒时间戳的最大合法值，用于分值逆序计算
BROWSER_POOL_SIZE = 2  # 浏览器实例池大小
CONTEXTS_PER_BROWSER = 2  # 每个浏览器实例下的并发上下文数
CONSUMER_WORKERS = BROWSER_POOL_SIZE * CONTEXTS_PER_BROWSER  # 消费者工作协程总数
COOLDOWN_MIN_SEC = 5  # 任务间冷却最小秒数
COOLDOWN_MAX_SEC = 10  # 任务间冷却最大秒数



def _env_bool(name: str, default: bool) -> bool:
    """Parse a bool-like environment variable without adding runtime dependencies."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "y"}


def _parse_viewport_env(raw_value: str | None) -> dict[str, int] | None:
    """Parse WIDTHxHEIGHT viewport strings, returning None for invalid/disabled input."""
    if not raw_value:
        return None
    raw_value = raw_value.strip().lower()
    if raw_value in {"0", "none", "auto", "native"}:
        return None
    match = re.fullmatch(r"(\d{3,5})x(\d{3,5})", raw_value)
    if not match:
        raise ValueError(f"Invalid viewport value {raw_value!r}; expected WIDTHxHEIGHT")
    return {"width": int(match.group(1)), "height": int(match.group(2))}


def _resolve_consumer_extra_flags(headless: bool) -> list[str]:
    """Build browser launch flags while keeping headed audit runs close to the user's browser."""
    flags = ["--start-maximized"]
    configured = os.getenv("FLOW_CONSUMER_EXTRA_FLAGS", "").strip()
    if configured:
        flags.extend(part.strip() for part in configured.split(",") if part.strip())
    if headless and not any(flag.startswith("--headless") for flag in flags):
        flags.append("--headless=new")
    if not headless:
        flags = [flag for flag in flags if not flag.startswith("--headless")]
    return flags

def _extract_json_objects(raw: str) -> list[dict]:
    """
    从原始字符串中提取所有顶层 JSON 对象（花括号平衡匹配）。
    比贪婪正则更可靠，不会截断或错误匹配嵌套结构。
    """
    results = []
    i = 0
    while i < len(raw):
        if raw[i] == '{':
            depth = 0
            start = i
            in_str = False
            escape = False
            while i < len(raw):
                ch = raw[i]
                if escape:
                    escape = False
                elif ch == '\\' and in_str:
                    escape = True
                elif ch == '"' and not escape:
                    in_str = not in_str
                elif not in_str:
                    if ch == '{':
                        depth += 1
                    elif ch == '}':
                        depth -= 1
                        if depth == 0:
                            try:
                                results.append(json.loads(raw[start:i + 1]))
                            except json.JSONDecodeError:
                                pass
                            break
                i += 1
        i += 1
    return results


def extract_error_message(exc: Exception, fallback: str = "采集异常") -> str:
    """
    从异常中提取有意义的错误消息。

    基于 Google Flow 实际 API 响应结构：

    路径 1 - 生成失败（poll/submit 200 响应中）：
        media[].mediaMetadata.mediaStatus.mediaGenerationStatus = FAILED
        media[].mediaMetadata.mediaStatus.error.message          = "PUBLIC_ERROR_IP_INPUT_IMAGE"
        media[].mediaMetadata.mediaStatus.failureReasons         = ["IP_PROHIBITED"]

    路径 2 - API 级别拒绝（非 200 响应）：
        error.code    = 403
        error.message = "reCAPTCHA evaluation failed"
        error.details[].reason = "PUBLIC_ERROR_UNUSUAL_ACTIVITY"

    路径 3 - 其他页面级异常：返回 fallback
    """
    raw = str(exc)
    if isinstance(exc, TimeoutError):
        return "采集超时"

    for obj in _extract_json_objects(raw):
        # ── 路径 1：Flow 生成失败 ──
        media_list = obj.get("media")
        if isinstance(media_list, list):
            for item in media_list:
                media_status = (
                    item.get("mediaMetadata", {})
                    .get("mediaStatus", {})
                )
                error_info = media_status.get("error")
                if isinstance(error_info, dict) and error_info.get("message"):
                    parts = [error_info["message"]]
                    reasons = media_status.get("failureReasons")
                    if isinstance(reasons, list) and reasons:
                        parts.append(f"({', '.join(reasons)})")
                    return " ".join(parts)

        # ── 路径 2：API 级别错误 ──
        api_error = obj.get("error")
        if isinstance(api_error, dict) and api_error.get("code"):
            parts = []
            details = api_error.get("details")
            if isinstance(details, list):
                for detail in details:
                    reason = detail.get("reason")
                    if reason:
                        parts.append(reason)
            msg = api_error.get("message")
            if msg:
                parts.append(msg)
            if parts:
                return ": ".join(parts)

    # ── 路径 3：页面级操作异常（无法提取 JSON）──
    status_match = re.search(r"'(MEDIA_GENERATION_STATUS_\w+)'", raw)
    if status_match:
        return status_match.group(1)
    return fallback


def mark_task_processing(collection: Any, task: dict[str, Any]) -> None:
    """
    在数据库中标记任务为处理中。

    此函数在 MongoDB 中将任务状态更新为"processing"，
    表示该任务正在处理中。

    参数:
        collection: MongoDB 集合对象
        task: 包含至少 "_id" 字段的任务字典

    示例:
        mark_task_processing(mongo_collection, {"_id": "task123"})
    """
    task_id = str(task.get("_id", ""))
    collection.update_one(
        {"_id": task_id},
        {
            "$set": {
                "msg": "执行中",
                "task_status": "processing",
                "updated_at": now_local(),
            },
        },
    )


def upsert_task_generation_result(
    collection: Any,
    task: dict[str, Any],
    local_video_path: str,
    api_full_response: str,
    file_md5: str | None = None,
    filesize: int | None = None,
    mime: str | None = None,
) -> None:
    """
    用视频生成信息更新任务结果。

    此函数在 MongoDB 中更新任务，包含视频生成的结果，
    包括本地视频路径、API 响应和可选的文件元数据。

    参数:
        collection: MongoDB 集合对象
        task: 包含 "_id" 字段的任务字典
        local_video_path: 生成的视频文件路径
        api_full_response: 完整的 API 响应字符串
        file_md5: 可选的视频文件 MD5 哈希值
        filesize: 可选的文件大小（KB）
        mime: 可选的视频 MIME 类型

    示例:
        upsert_task_generation_result(
            mongo_collection,
            task_dict,
            "/path/to/video.mp4",
            api_response_json,
            file_md5="abc123",
            filesize=1024
        )
    """
    task_id = str(task["_id"])
    update_fields: dict[str, Any] = {
        "local_video_path": local_video_path,
        "api_full_response": api_full_response,
    }
    if file_md5 is not None:
        update_fields["file_md5"] = file_md5
    if filesize is not None:
        update_fields["file_size"] = filesize
    if mime is not None:
        update_fields["mime"] = mime

    collection.update_one(
        {"_id": task_id},
        {"$set": update_fields},
        upsert=True,
    )


def mark_task_failed(collection: Any, task: dict[str, Any], error_message: str) -> None:
    """
    在数据库中标记任务为失败。

    此函数在 MongoDB 中将任务状态更新为"failed"并记录错误消息。

    参数:
        collection: MongoDB 集合对象
        task: 包含至少 "_id" 字段的任务字典
        error_message: 错误消息描述

    示例:
        mark_task_failed(mongo_collection, {"_id": "task123"}, "网络连接失败")
    """
    task_id = str(task.get("_id", ""))
    collection.update_one(
        {"_id": task_id},
        {
            "$set": {
                "msg": "失败",
                "task_status": "failed",
                "error_msg": error_message,
                "updated_at": now_local(),
            },
        },
        upsert=True,
    )


def _compute_retry_priority(retry_count: int) -> int:
    """
    计算重试优先级。

    根据重试次数计算任务的优先级，重试次数越多优先级越高。

    参数:
        retry_count: 当前重试次数

    返回:
        int: 计算后的优先级值
    """
    return DEFAULT_TASK_PRIORITY + max(0, retry_count) * RETRY_PRIORITY_STEP


def _encode_queue_score(priority: int, timestamp_ms: int | None = None) -> int:
    """
    编码队列分数。

    将优先级和时间戳编码为单个分数值，用于 Redis 有序集合。

    参数:
        priority: 任务优先级
        timestamp_ms: 时间戳（毫秒），如果为 None 则使用当前时间

    返回:
        int: 编码后的分数值
    """
    if timestamp_ms is None:
        timestamp_ms = int(time.time() * 1000)
    bounded_priority = max(0, int(priority))
    inverted_time = max(0, MAX_TIMESTAMP_MS - int(timestamp_ms))
    return bounded_priority * SCORE_TIME_FACTOR + inverted_time


def _decode_queue_priority(score: float | int) -> int:
    """
    解码队列分数。

    从编码的分数值中提取优先级。

    参数:
        score: 编码的分数值

    返回:
        int: 解码后的优先级
    """
    return int(float(score)) // SCORE_TIME_FACTOR


def _recover_processing_queue(redis_client: Any) -> int:
    """
    恢复处理中队列中的任务。

    将在处理中队列中的遗留任务恢复回主队列，通常在启动时调用。

    参数:
        redis_client: Redis 客户端对象

    返回:
        int: 恢复的任务数量
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


async def _pop_highest_priority_task(redis_client: Any) -> tuple[str, float] | tuple[None, None]:
    """
    从队列中弹出最高优先级的任务。

    从 Redis 有序集合中获取优先级最高的任务，并移动到处理中队列。

    参数:
        redis_client: Redis 客户端对象

    返回:
        tuple: (任务载荷字符串, 分数) 或 (None, None) 如果队列为空
    """
    result = await asyncio.to_thread(
        redis_client.bzpopmax,
        TASK_CREATE_QUEUE,
        REDIS_BLOCK_TIMEOUT_SECONDS,
    )
    if not result:
        return None, None

    _, raw_payload, score = result
    raw_score = float(score)
    redis_client.zadd(TASK_CREATE_PROCESSING_QUEUE, {raw_payload: raw_score})
    return raw_payload, raw_score


def _remove_processing_payload(redis_client: Any, raw_payload: str) -> None:
    """
    从处理中队列中移除任务载荷。

    参数:
        redis_client: Redis 客户端对象
        raw_payload: 要移除的任务载荷字符串
    """
    redis_client.zrem(TASK_CREATE_PROCESSING_QUEUE, raw_payload)


def _convert_cos_urls_in_task(task: dict[str, Any]) -> None:
    image_urls = task.get("image_url_list") or []
    image_url = task.get("image_url")

    if image_urls and all(is_cos_url(u) for u in image_urls if u):
        b64_list = [base64.b64encode(download_cos_image(u)).decode("utf-8") for u in image_urls if u]
        task["image_base64_list"] = b64_list
        task.pop("image_url_list", None)
        logger.info(f"[任务:{task.get('_id')}] 已将 {len(b64_list)} 张 COS 图片转为 base64")
    elif image_url and is_cos_url(image_url):
        task["image_base64"] = base64.b64encode(download_cos_image(image_url)).decode("utf-8")
        task.pop("image_url", None)
        logger.info(f"[任务:{task.get('_id')}] 已将单张 COS 图片转为 base64")


def validate_task(task: dict[str, Any]) -> tuple[str, str]:
    """
    验证任务数据的有效性。

    检查任务是否包含必需的字段，并提取任务 ID 和提示词。

    参数:
        task: 要验证的任务字典

    返回:
        tuple: (任务_id, 提示词)

    异常:
        ValueError: 如果任务缺少必需字段或类型不正确

    示例:
        task_id, prompt = validate_task({"_id": "task123", "prompt": "创建视频"})
    """
    task_id = str(task.get("_id", "")).strip()
    prompt = str(task.get("prompt", "")).strip()
    if not task_id:
        raise ValueError("缺少 _id")
    if not prompt:
        raise ValueError("缺少 prompt")
    if int(task.get("type", -1)) != 1:
        raise ValueError(f"只支持处理 type=1 的视频生成任务, 当前 type={task.get('type')}")
    return task_id, prompt


async def handle_single_task(scraper: GoogleFlowVideoScraperV2, collection: Any, task: dict[str, Any]) -> Path:
    """
    处理单个视频生成任务。

    此函数使用爬虫执行单个视频生成任务，并更新数据库中的结果。

    参数:
        scraper: 视频爬虫实例
        collection: MongoDB 集合对象
        task: 任务数据字典

    返回:
        Path: 下载的视频文件路径

    异常:
        Exception: 如果视频生成失败

    示例:
        video_path = await handle_single_task(scraper, mongo_collection, task_data)
    """
    task_id, _ = validate_task(task)
    scraper_task = build_scraper_task(task)
    raw_results = await scraper.run_tasks([scraper_task])
    raw_result = raw_results[0]
    if isinstance(raw_result, Exception):
        # 如果是异常，向外抛出，让外层捕获并更新失败状态
        raise raw_result

    downloaded_path = raw_result.get("local_video_path")
    api_full_response = raw_result.get("api_full_response")
    file_md5 = raw_result.get("file_md5")
    filesize = raw_result.get("filesize")
    video_mime_type = raw_result.get("video_mime_type")

    if not downloaded_path:
        raise RuntimeError("未能从结果中获取到 downloaded_path，视频生成可能失败")

    upsert_task_generation_result(
        collection=collection,
        task=task,
        local_video_path=str(downloaded_path) if downloaded_path else "",
        api_full_response=api_full_response,
        file_md5=file_md5,
        filesize=filesize,
        mime=video_mime_type,
    )
    logger.info(f"[任务:{task_id}] 任务记录已更新, 视频路径: {downloaded_path}")
    return Path(downloaded_path) if downloaded_path else Path()


async def consumer_worker(
    worker_name: str,
    scraper: GoogleFlowVideoScraperV2,
    redis_client: Any,
    task_collection: Any,
) -> None:
    """
    单个消费者工作协程的主循环。

    不断从 Redis 有序集合中弹出最高优先级的任务，执行视频生成，
    成功则移出处理中队列，失败则在重试次数未达上限时放回主队列。

    参数:
        worker_name: 工作协程名称，用于日志标识
        scraper: 视频生成爬虫实例
        redis_client: Redis 客户端
        task_collection: MongoDB 任务集合
    """
    while True:
        raw_payload, current_score = await _pop_highest_priority_task(redis_client)
        if not raw_payload:
            continue

        task: dict[str, Any] | None = None
        local_path: Path | None = None
        try:
            task = parse_queue_payload(raw_payload)
            task_id, prompt = validate_task(task)
            _convert_cos_urls_in_task(task)
            current_priority = _decode_queue_priority(current_score)
            logger.info(
                f"[{worker_name}][任务:{task_id}] 开始处理，priority={current_priority}, score={current_score}, prompt={prompt[:60]!r}"
            )

            mark_task_processing(task_collection, task)

            local_path = await handle_single_task(scraper, task_collection, task)
            _remove_processing_payload(redis_client, raw_payload)
            logger.info(f"[{worker_name}][任务:{task_id}] 处理成功，已移出 processing 队列")
        except Exception as exc:
            if task is None:
                logger.exception(f"[{worker_name}] 解析任务失败或结构错误: {exc}")
                _remove_processing_payload(redis_client, raw_payload)
                continue

            task_id = str(task.get("_id", "")).strip() or "unknown"
            if local_path and local_path.exists():
                local_path.unlink(missing_ok=True)

            retry_count = int(task.get("retry_count", 0))
            logger.exception(f"[{worker_name}][任务:{task_id}] 第 {retry_count + 1} 次处理失败: {exc}")

            if retry_count + 1 < MAX_RETRIES:
                _remove_processing_payload(redis_client, raw_payload)
                task["retry_count"] = retry_count + 1
                retry_priority = _compute_retry_priority(task["retry_count"])
                retry_score = _encode_queue_score(retry_priority)
                redis_client.zadd(TASK_CREATE_QUEUE, {dumps_queue_payload(task): retry_score})
                logger.info(
                    f"[{worker_name}][任务:{task_id}] 任务已放回主队列重试，"
                    f"retry_count={task['retry_count']}, priority={retry_priority}, score={retry_score}"
                )
            else:
                _remove_processing_payload(redis_client, raw_payload)
                error_msg = extract_error_message(exc, "采集异常")
                mark_task_failed(task_collection, task, error_msg)
                logger.info(f"[{worker_name}][任务:{task_id}] 达到最大重试次数，已标记为失败")

        cooldown = random.uniform(COOLDOWN_MIN_SEC, COOLDOWN_MAX_SEC)
        logger.debug(f"[{worker_name}] 任务间冷却 {cooldown:.1f}s")
        await asyncio.sleep(cooldown)


async def consume_forever() -> None:
    """
    Redis 任务消费者入口。

    初始化 Redis 客户端、MongoDB 集合、浏览器池爬虫实例，
    恢复遗留的处理中任务，然后启动 CONSUMER_WORKERS 个并发工作协程
    持续消费任务队列，直到进程被中断。
    """
    redis_client = create_redis_client()
    task_collection = create_task_collection()
    _recover_processing_queue(redis_client)

    headless = _env_bool("FLOW_CONSUMER_HEADLESS", True)
    viewport = _parse_viewport_env(os.getenv("FLOW_CONSUMER_VIEWPORT") or os.getenv("FLOW_BROWSER_VIEWPORT"))
    scraper = GoogleFlowVideoScraperV2(
        browser_pool_size=BROWSER_POOL_SIZE,
        max_contexts_per_browser=CONTEXTS_PER_BROWSER,
        headless=headless,
        locale=resolve_flow_locale(),
        timezone_id=resolve_flow_timezone_id(),
        extra_flags=_resolve_consumer_extra_flags(headless),
        viewport=viewport,
        task_timeout_ms=4 * 60 * 1000,
    )

    logger.info(
        "[启动] Redis 任务消费者启动，"
        f"browser_pool_size={BROWSER_POOL_SIZE}, "
        f"contexts_per_browser={CONTEXTS_PER_BROWSER}, "
        f"concurrent_workers={CONSUMER_WORKERS}"
    )
    async with scraper:
        workers = [
            asyncio.create_task(
                consumer_worker(
                    worker_name=f"consumer-{index + 1}",
                    scraper=scraper,
                    redis_client=redis_client,
                    task_collection=task_collection,
                )
            )
            for index in range(CONSUMER_WORKERS)
        ]
        await asyncio.gather(*workers)


def main() -> None:
    """
    Main entry point for the Redis task consumer.

    This function starts the async consumer that processes video
    generation tasks from a Redis queue.

    The consumer will:
    1. Connect to Redis and MongoDB
    2. Recover any orphaned processing tasks
    3. Initialize the video scraper with browser pool
    4. Start concurrent workers to process tasks
    5. Run indefinitely until interrupted

    Example:
        # Run the consumer
        python -m video_processing.consumers.redis_task_consumer
    """
    asyncio.run(consume_forever())


if __name__ == "__main__":
    main()
