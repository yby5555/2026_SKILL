"""flow_task_runtime 的总入口消费者。"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    ROOT_DIR = Path(__file__).resolve().parent.parent
    if str(ROOT_DIR) not in sys.path:
        sys.path.insert(0, str(ROOT_DIR))
    from flow_task_runtime.config import load_settings
    from flow_task_runtime.logging_utils import get_logger
    from flow_task_runtime.queue_manager import (
        ack_processing_payload,
        decode_queue_priority,
        parse_task_payload,
        pop_highest_priority_task,
        recover_processing_queue,
        requeue_task,
        validate_task,
    )
    from flow_task_runtime.scraper import CreditCheckedFlowScraper
    from flow_task_runtime.storage import create_account_collection, create_redis_client, create_task_collection
    from flow_task_runtime.task_repository import mark_task_failed, upsert_task_generation_result
else:
    from .config import load_settings
    from .logging_utils import get_logger
    from .queue_manager import (
        ack_processing_payload,
        decode_queue_priority,
        parse_task_payload,
        pop_highest_priority_task,
        recover_processing_queue,
        requeue_task,
        validate_task,
    )
    from .scraper import CreditCheckedFlowScraper
    from .storage import create_account_collection, create_redis_client, create_task_collection
    from .task_repository import mark_task_failed, upsert_task_generation_result


async def handle_single_task(scraper: CreditCheckedFlowScraper, task_collection: Any, task: dict[str, Any]) -> Path:
    """执行单个任务，并把成功结果写回 Mongo。

    参数:
        scraper: 当前抓取器实例。
        task_collection: Mongo 任务结果集合。
        task: 当前任务字典。

    返回:
        Path: 本地视频文件路径。
    """
    raw_results = await scraper.run_tasks([task])
    raw_result = raw_results[0]
    if isinstance(raw_result, Exception):
        raise raw_result

    upsert_task_generation_result(
        task_collection,
        task,
        local_video_path=str(raw_result["local_video_path"]),
        api_full_response=raw_result.get("api_full_response"),
    )
    return Path(raw_result["local_video_path"])


async def consumer_worker(
    worker_name: str,
    scraper: CreditCheckedFlowScraper,
    redis_client: Any,
    task_collection: Any,
    logger: Any,
    settings: Any,
) -> None:
    """不断从 Redis 中拉取并执行任务。

    参数:
        worker_name: 当前消费者协程名称。
        scraper: 当前抓取器实例。
        redis_client: Redis 客户端。
        task_collection: Mongo 任务结果集合。
        logger: 日志器对象。
        settings: 运行时配置对象。
    """
    while True:
        raw_payload, current_score = await pop_highest_priority_task(redis_client, settings.redis_block_timeout_seconds)
        if not raw_payload:
            continue

        task: dict[str, Any] | None = None
        local_path: Path | None = None
        try:
            task = parse_task_payload(raw_payload)
            task_id, prompt = validate_task(task)
            current_priority = decode_queue_priority(current_score)
            logger.info(
                f"[{worker_name}][任务:{task_id}] 开始处理，priority={current_priority}, "
                f"score={current_score}, prompt={prompt[:60]!r}"
            )

            local_path = await handle_single_task(scraper, task_collection, task)
            ack_processing_payload(redis_client, raw_payload)
            logger.info(f"[{worker_name}][任务:{task_id}] 处理成功，已移出 processing 队列")
        except Exception as exc:
            if task is None:
                logger.exception(f"[{worker_name}] 解析任务失败或结构错误: {exc}")
                ack_processing_payload(redis_client, raw_payload)
                continue

            task_id = str(task.get("_id", "")).strip() or "unknown"
            if local_path and local_path.exists():
                local_path.unlink(missing_ok=True)

            retry_count = int(task.get("retry_count", 0))
            logger.exception(f"[{worker_name}][任务:{task_id}] 第 {retry_count + 1} 次处理失败: {exc}")

            if retry_count + 1 < settings.max_retries:
                new_retry_count, retry_score = requeue_task(
                    redis_client,
                    raw_payload,
                    task,
                    default_task_priority=settings.default_task_priority,
                    retry_priority_step=settings.retry_priority_step,
                )
                logger.info(
                    f"[{worker_name}][任务:{task_id}] 已回队重试，retry_count={new_retry_count}, score={retry_score}"
                )
            else:
                ack_processing_payload(redis_client, raw_payload)
                mark_task_failed(task_collection, task, str(exc))
                logger.info(f"[{worker_name}][任务:{task_id}] 达到最大重试次数，已标记失败")


async def consume_forever() -> None:
    """初始化依赖并永久消费 Redis 队列。"""
    settings = load_settings()
    logger = get_logger("FlowTaskRuntimeConsumer", settings.log_file)
    redis_client = create_redis_client(settings)
    task_collection = create_task_collection(settings)
    account_collection = create_account_collection(settings)
    recover_processing_queue(redis_client, logger)

    scraper = CreditCheckedFlowScraper(
        settings=settings,  # 运行时配置对象
        redis_client=redis_client,  # Redis 客户端
        account_collection=account_collection,  # Mongo 账号集合
        logger=logger,  # 日志器
        browser_pool_size=settings.browser_pool_size,  # 浏览器数量（保持消费者自己的并发配置）
        max_contexts_per_browser=settings.contexts_per_browser,  # 每个浏览器的 context 数（保持消费者自己的并发配置）
        headless=settings.headless,  # 是否无头运行由 FLOW_TASK_HEADLESS 控制
        extra_flags=["--start-maximized"],  # 对齐 video_api_server.py：仅保留这个启动参数
        viewport=None,  # 对齐 video_api_server.py：不固定视口
        navigation_timeout_ms=settings.navigation_timeout_ms,  # 页面导航超时
        task_timeout_ms=settings.task_timeout_ms,  # 单任务总超时
        default_cookie_domain=".google.com",  # 对齐 video_api_server.py：缺省 cookie 域名
        ignore_https_errors=False,  # 对齐 standalone/video_api 行为：不忽略 HTTPS 错误
        add_default_launch_flags=False,  # 对齐 video_api_server.py：不额外增加 AutomationControlled / infobars 标记
        recycle_browser_after_tasks=settings.recycle_browser_after_tasks,  # 单浏览器累计执行多少任务后回收
        recycle_browser_after_failures=settings.recycle_browser_after_failures,  # 连续失败多少次后回收
    )

    logger.info(
        "[启动] 新视频任务消费者启动，"
        f"browser_pool_size={settings.browser_pool_size}, "
        f"contexts_per_browser={settings.contexts_per_browser}, "
        f"consumer_workers={settings.consumer_workers}, "
        f"headless={settings.headless}, "
        f"recycle_after_tasks={settings.recycle_browser_after_tasks}, "
        f"recycle_after_failures={settings.recycle_browser_after_failures}, "
        f"cookie_inuse_ttl_seconds={settings.cookie_inuse_ttl_seconds}"
    )

    async with scraper:
        workers = [
            asyncio.create_task(
                consumer_worker(
                    worker_name=f"runtime-consumer-{index + 1}",
                    scraper=scraper,
                    redis_client=redis_client,
                    task_collection=task_collection,
                    logger=logger,
                    settings=settings,
                )
            )
            for index in range(settings.consumer_workers)
        ]
        await asyncio.gather(*workers)


def main() -> None:
    """同步命令行入口。"""
    asyncio.run(consume_forever())


if __name__ == "__main__":
    main()
