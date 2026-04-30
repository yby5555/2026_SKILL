from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import sys
import time
from pathlib import Path
from typing import Any

_FLOW_DIR = Path(__file__).resolve().parent
_ROOT = _FLOW_DIR.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from account_mgr.cos_utils import upload_file_to_cos
from task_pipeline_common import DEFAULT_UPLOAD_POLL_SECONDS, create_task_collection, get_logger

logger = get_logger("LocalVideoUploader")

POLL_SECONDS = DEFAULT_UPLOAD_POLL_SECONDS
UPLOAD_MAX_ATTEMPTS = 3
BATCH_SIZE = 5
MAX_WORKERS = 5


def now_local_naive() -> datetime:
    return datetime.now()


def find_pending_upload_tasks(collection: Any, limit: int = BATCH_SIZE) -> list[dict[str, Any]]:
    cursor = collection.find(
        {
            "msg": "执行中",
            "local_video_path": {"$exists": True, "$nin": [None, ""]},
        },
        sort=[("created_at", 1)],
    )
    return list(cursor.limit(limit))


def build_cos_key(task_id: str, local_video_path: str) -> str:
    suffix = Path(local_video_path).suffix or ".mp4"
    return f"{task_id}{suffix}"


def mark_task_upload_success(collection: Any, task_id: str, cos_url: str) -> None:
    collection.update_one(
        {"_id": task_id},
        {
            "$set": {
                "msg": "已完成",
                "cos_url": cos_url,
                "error_msg": None,
                "updated_at": now_local_naive(),
            }
        },
    )


def mark_task_upload_failure(collection: Any, task_id: str, error_message: str) -> None:
    collection.update_one(
        {"_id": task_id},
        {
            "$set": {
                "msg": "失败",
                "error_msg": error_message,
                "updated_at": now_local_naive(),
            }
        },
    )


def upload_once(task: dict[str, Any]) -> str:
    task_id = str(task["_id"])
    local_video_path = str(task["local_video_path"])
    path = Path(local_video_path)
    if not path.exists():
        raise FileNotFoundError(f"本地视频不存在: {local_video_path}")
    return upload_file_to_cos(path, build_cos_key(task_id, local_video_path))


def process_task(collection: Any, task: dict[str, Any]) -> None:
    task_id = str(task["_id"])
    last_error: Exception | None = None
    for attempt in range(1, UPLOAD_MAX_ATTEMPTS + 1):
        try:
            cos_url = upload_once(task)
            mark_task_upload_success(collection, task_id, cos_url)
            local_path = Path(str(task["local_video_path"]))
            local_path.unlink(missing_ok=True)
            logger.info(f"[task:{task_id}] COS 上传完成并已删除本地文件: {cos_url}")
            return
        except Exception as exc:
            last_error = exc
            logger.exception(f"[task:{task_id}] 第 {attempt} 次上传失败: {exc}")
            if attempt < UPLOAD_MAX_ATTEMPTS:
                time.sleep(2)

    error_message = str(last_error) if last_error else "上传失败"
    mark_task_upload_failure(collection, task_id, error_message)
    logger.info(f"[task:{task_id}] 上传失败，已标记失败")


def run_forever() -> None:
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
