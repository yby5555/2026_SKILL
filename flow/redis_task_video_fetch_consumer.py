from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any

from automation_video_v2_fetch_consumer import GoogleFlowVideoFetchScraperV2
from task_pipeline_common import (
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

logger = get_logger("RedisTaskVideoFetchConsumer")

MAX_RETRIES = DEFAULT_MAX_RETRIES
REDIS_BLOCK_TIMEOUT_SECONDS = 5
DEFAULT_TASK_PRIORITY = 10
RETRY_PRIORITY_STEP = 10
SCORE_TIME_FACTOR = 10**13
MAX_TIMESTAMP_MS = 9_999_999_999_999
BROWSER_POOL_SIZE = 2
CONTEXTS_PER_BROWSER = 2
CONSUMER_WORKERS = BROWSER_POOL_SIZE * CONTEXTS_PER_BROWSER


def upsert_task_generation_result(
    collection: Any,
    task: dict[str, Any],
    local_video_path: str,
    api_full_response: str,
) -> None:
    collection.update_one(
        {"_id": str(task["_id"])},
        {
            "$set": {
                "local_video_path": local_video_path,
                "api_full_response": api_full_response,
            },
        },
        upsert=True,
    )


def mark_task_failed(collection: Any, task: dict[str, Any], error_message: str) -> None:
    del error_message
    collection.update_one(
        {"_id": str(task["_id"])},
        {
            "$set": {
                "msg": "失败",
                "updated_at": now_local(),
            },
        },
        upsert=True,
    )


def _compute_retry_priority(retry_count: int) -> int:
    return DEFAULT_TASK_PRIORITY + max(0, retry_count) * RETRY_PRIORITY_STEP


def _encode_queue_score(priority: int, timestamp_ms: int | None = None) -> int:
    if timestamp_ms is None:
        timestamp_ms = int(time.time() * 1000)
    bounded_priority = max(0, int(priority))
    inverted_time = max(0, MAX_TIMESTAMP_MS - int(timestamp_ms))
    return bounded_priority * SCORE_TIME_FACTOR + inverted_time


def _decode_queue_priority(score: float | int) -> int:
    return int(float(score)) // SCORE_TIME_FACTOR


def _recover_processing_queue(redis_client: Any) -> int:
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
    redis_client.zrem(TASK_CREATE_PROCESSING_QUEUE, raw_payload)


def validate_task(task: dict[str, Any]) -> tuple[str, str]:
    task_id = str(task.get("_id", "")).strip()
    prompt = str(task.get("prompt", "")).strip()
    if not task_id:
        raise ValueError("缺少 _id")
    if not prompt:
        raise ValueError("缺少 prompt")
    if int(task.get("type", -1)) != 1:
        raise ValueError(f"只支持处理 type=1 的视频生成任务, 当前 type={task.get('type')}")
    return task_id, prompt


async def handle_single_task(scraper: GoogleFlowVideoFetchScraperV2, collection: Any, task: dict[str, Any]) -> Path:
    task_id, _ = validate_task(task)
    scraper_task = build_scraper_task(task)
    raw_results = await scraper.run_tasks([scraper_task])
    raw_result = raw_results[0]
    if isinstance(raw_result, Exception):
        raise raw_result

    downloaded_path = raw_result.get("local_video_path")
    api_full_response = raw_result.get("api_full_response")

    if not downloaded_path:
        raise RuntimeError("未能从结果中获取到 downloaded_path，视频生成可能失败")

    upsert_task_generation_result(
        collection=collection,
        task=task,
        local_video_path=str(downloaded_path),
        api_full_response=api_full_response,
    )
    logger.info(f"[任务:{task_id}] 任务记录已更新, 视频路径: {downloaded_path}")
    return Path(downloaded_path)


async def consumer_worker(
    worker_name: str,
    scraper: GoogleFlowVideoFetchScraperV2,
    redis_client: Any,
    task_collection: Any,
) -> None:
    while True:
        raw_payload, current_score = await _pop_highest_priority_task(redis_client)
        if not raw_payload:
            continue

        task: dict[str, Any] | None = None
        local_path: Path | None = None
        try:
            task = parse_queue_payload(raw_payload)
            task_id, prompt = validate_task(task)
            current_priority = _decode_queue_priority(current_score)
            logger.info(
                f"[{worker_name}][任务:{task_id}] 开始处理，priority={current_priority}, score={current_score}, prompt={prompt[:60]!r}"
            )

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
                mark_task_failed(task_collection, task, str(exc))
                logger.info(f"[{worker_name}][任务:{task_id}] 达到最大重试次数，已标记为失败")


async def consume_forever() -> None:
    redis_client = create_redis_client()
    task_collection = create_task_collection()
    _recover_processing_queue(redis_client)

    scraper = GoogleFlowVideoFetchScraperV2(
        browser_pool_size=BROWSER_POOL_SIZE,
        max_contexts_per_browser=CONTEXTS_PER_BROWSER,
        headless=True,
        extra_flags=["--start-maximized"],
        viewport={"width": 0, "height": 0},
        task_timeout_ms=4 * 60 * 1000,
    )

    logger.info(
        "[启动] Fetch 版 Redis 任务消费者启动，"
        f"browser_pool_size={BROWSER_POOL_SIZE}, "
        f"contexts_per_browser={CONTEXTS_PER_BROWSER}, "
        f"concurrent_workers={CONSUMER_WORKERS}"
    )
    async with scraper:
        workers = [
            asyncio.create_task(
                consumer_worker(
                    worker_name=f"fetch-consumer-{index + 1}",
                    scraper=scraper,
                    redis_client=redis_client,
                    task_collection=task_collection,
                )
            )
            for index in range(CONSUMER_WORKERS)
        ]
        await asyncio.gather(*workers)


def main() -> None:
    asyncio.run(consume_forever())


if __name__ == "__main__":
    main()
