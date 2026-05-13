from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import sys
import time
from pathlib import Path
from typing import Any

# Add parent directory to path for imports
_video_processing = Path(__file__).resolve().parent.parent.parent
if str(_video_processing) not in sys.path:
    sys.path.insert(0, str(_video_processing))

# Import COS utilities from account_mgr (not moved)
from account_mgr.cos_utils import upload_file_to_cos
from account_mgr.cos_utils import COS_VIDEO_PREFIX as _COS_VIDEO_PREFIX
from video_processing.utils.task_common import DEFAULT_UPLOAD_POLL_SECONDS, create_task_collection, get_logger

logger = get_logger("LocalVideoUploader")

POLL_SECONDS = DEFAULT_UPLOAD_POLL_SECONDS
UPLOAD_MAX_ATTEMPTS = 3
BATCH_SIZE = 5
MAX_WORKERS = 5


def now_local_naive() -> datetime:
    """
    获取当前本地时间（无时区信息）。

    返回:
        datetime: 当前本地时间作为无时区的 datetime 对象

    示例:
        current_time = now_local_naive()
        print(f"当前时间: {current_time}")
    """
    return datetime.now()


def find_pending_upload_tasks(collection: Any, limit: int = BATCH_SIZE) -> list[dict[str, Any]]:
    """
    查找准备好上传到 COS 的视频任务。

    此函数查询 MongoDB 中状态为"执行中"且具有本地视频路径需要上传到 COS 的任务。

    参数:
        collection: MongoDB 集合对象
        limit: 返回的最大任务数（默认: BATCH_SIZE = 5）

    返回:
        list[dict[str, Any]]: 准备上传的任务字典列表

    示例:
        tasks = find_pending_upload_tasks(mongo_collection, limit=10)
        print(f"找到 {len(tasks)} 个待上传任务")
    """
    cursor = collection.find(
        {
            "msg": "执行中",
            "local_video_path": {"$exists": True, "$nin": [None, ""]},
        },
        sort=[("created_at", 1)],
    )
    return list(cursor.limit(limit))


def build_cos_key(task_id: str, local_video_path: str) -> str:
    """
    构建视频文件的 COS 对象键。

    此函数为在 COS 中存储视频文件生成唯一键，使用任务 ID 和本地路径中的文件扩展名。

    参数:
        task_id: 唯一任务标识符
        local_video_path: 本地视频文件路径

    返回:
        str: COS 对象键（例如 "task123.mp4"）

    示例:
        key = build_cos_key("task123", "/path/to/video.mp4")
        print(f"COS 键: {key}")  # "task123.mp4"
    """
    suffix = Path(local_video_path).suffix or ".mp4"
    return f"{task_id}{suffix}"


def mark_task_upload_success(collection: Any, task_id: str, cos_url: str, cos_key: str) -> None:
    """
    在数据库中标记任务上传成功。

    此函数在 MongoDB 中将任务状态更新为"completed"并记录 COS URL 和密钥。

    参数:
        collection: MongoDB 集合对象
        task_id: 任务 ID
        cos_url: COS 中的文件 URL
        cos_key: COS 中的文件密钥

    示例:
        mark_task_upload_success(mongo_collection, "task123", "cos_url", "cos_key")
    """
    collection.update_one(
        {"_id": task_id},
        {
            "$set": {
                "msg": "已完成",
                "task_status": "completed",
                "cos_url": cos_url,
                "cos_key": cos_key,
                "error_msg": None,
                "updated_at": now_local_naive(),
            }
        },
    )


def mark_task_upload_failure(collection: Any, task_id: str, error_message: str) -> None:
    """
    在数据库中标记任务上传失败。

    此函数在 MongoDB 中将任务状态更新为"failed"并记录错误消息。

    参数:
        collection: MongoDB 集合对象
        task_id: 任务 ID
        error_message: 错误消息描述

    示例:
        mark_task_upload_failure(mongo_collection, "task123", "网络连接失败")
    """
    collection.update_one(
        {"_id": task_id},
        {
            "$set": {
                "msg": "失败",
                "task_status": "failed",
                "error_msg": error_message,
                "updated_at": now_local_naive(),
            }
        },
    )


def upload_once(task: dict[str, Any]) -> str:
    """
    执行单次视频上传到 COS。

    此函数将本地视频文件上传到 COS 存储服务。

    参数:
        task: 包含 "_id" 和 "local_video_path" 字段的任务字典

    返回:
        str: 上传后的 COS URL

    异常:
        FileNotFoundError: 如果本地视频文件不存在

    示例:
        cos_url = upload_once({"_id": "task123", "local_video_path": "/path/to/video.mp4"})
    """
    task_id = str(task["_id"])
    local_video_path = str(task["local_video_path"])
    path = Path(local_video_path)
    if not path.exists():
        raise FileNotFoundError(f"本地视频不存在: {local_video_path}")
    return upload_file_to_cos(path, build_cos_key(task_id, local_video_path))


def process_task(collection: Any, task: dict[str, Any]) -> None:
    """
    处理单个视频上传任务，具有重试逻辑。

    此函数尝试将视频文件上传到 COS，如果失败则重试。
    如果成功，它更新任务状态并删除本地文件。
    如果所有重试都失败，它将任务标记为失败。

    参数:
        collection: MongoDB 集合对象
        task: 包含 "_id" 和 "local_video_path" 字段的任务字典

    异常:
        FileNotFoundError: 如果本地视频文件不存在
        Exception: 对于 COS 上传失败（将重试）

    示例:
        process_task(mongo_collection, {
            "_id": "task123",
            "local_video_path": "/path/to/video.mp4"
        })
    """
    task_id = str(task["_id"])
    last_error: Exception | None = None
    for attempt in range(1, UPLOAD_MAX_ATTEMPTS + 1):
        try:
            cos_url = upload_once(task)
            raw_key = build_cos_key(task_id, str(task.get("local_video_path", "")))
            cos_key = f"{_COS_VIDEO_PREFIX.rstrip('/')}/{raw_key}"
            mark_task_upload_success(collection, task_id, cos_url, cos_key)
            local_path = Path(str(task["local_video_path"]))
            local_path.unlink(missing_ok=True)
            logger.info(f"[task:{task_id}] COS 上传完成并已删除本地文件: {cos_url}")
            return
        except Exception as exc:
            last_error = exc
            logger.exception(f"[task:{task_id}] 第 {attempt} 次上传失败: {exc}")
            if attempt < UPLOAD_MAX_ATTEMPTS:
                time.sleep(2)

    error_message = str(last_error) if last_error else "上传异常"
    if not error_message.startswith("上传异常"):
        error_message = f"上传异常: {error_message}"
    mark_task_upload_failure(collection, task_id, error_message)
    logger.info(f"[task:{task_id}] 上传失败，已标记失败")


def run_forever() -> None:
    """
    视频上传服务的主循环。

    此函数无限期运行视频上传服务：
    1. 轮询 MongoDB 查找准备上传的任务
    2. 使用线程池并发上传视频到 COS
    3. 成功时更新任务状态并删除本地文件
    4. 没有任务时休眠并重新轮询

    服务将运行直到被中断，并优雅地处理错误，
    继续处理剩余任务，即使其中一些失败。

    示例:
        # 运行上传器服务
        python -m video_processing.consumers.local_video_uploader
    """
    task_collection = create_task_collection()
    logger.info("[uploader] 本地视频上传器启动")
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="cos-upload")

    while True:
        tasks = find_pending_upload_tasks(task_collection)
        if not tasks:
            time.sleep(POLL_SECONDS)
            continue

        logger.info(f"[uploader] 本轮获取 {len(tasks)} 条待上传任务，开始并发上传")
        futures = [executor.submit(process_task, task_collection, task) for task in tasks]
        for future in as_completed(futures):
            future.result()


if __name__ == "__main__":
    run_forever()
