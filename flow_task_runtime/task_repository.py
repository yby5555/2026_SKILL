"""MongoDB 任务与账号状态更新工具。"""

from __future__ import annotations

from typing import Any

from .logging_utils import now_local


def upsert_task_generation_result(
    collection: Any,
    task: dict[str, Any],
    *,
    local_video_path: str,
    api_full_response: Any,
) -> None:
    """把任务成功结果写回 MongoDB。

    注意：
        严格对齐旧脚本，只写入 `local_video_path` 和 `api_full_response`。

    参数:
        collection: Mongo 任务结果集合。
        task: 当前任务字典，至少需要包含 `_id`。
        local_video_path: 本地视频文件路径。
        api_full_response: 最终状态查询接口返回的完整响应。
    """
    collection.update_one(
        {"_id": str(task["_id"])},
        {"$set": {"local_video_path": local_video_path, "api_full_response": api_full_response}},
        upsert=True,
    )


def mark_task_failed(collection: Any, task: dict[str, Any], error_message: str) -> None:
    """把任务标记为失败。

    参数:
        collection: Mongo 任务结果集合。
        task: 当前失败任务字典。
        error_message: 失败信息；当前实现保持与旧脚本一致，不写入该字段。
    """
    del error_message
    collection.update_one(
        {"_id": str(task["_id"])},
        {"$set": {"msg": "失败", "updated_at": now_local()}},
        upsert=True,
    )


def mark_account_pending(account_collection: Any, email: str, reason: str) -> None:
    """把账号状态标记为待重新登录。

    参数:
        account_collection: Mongo 账号集合。
        email: 账号邮箱。
        reason: 标记为待登录的原因说明。
    """
    account_collection.update_one(
        {"email": email},
        {
            "$set": {
                "status": 0,
                "status_msg": reason,
                "updated_at": now_local(),
            }
        },
        upsert=False,
    )


def mark_account_abnormal(account_collection: Any, email: str, reason: str) -> None:
    """把账号状态标记为异常。

    参数:
        account_collection: Mongo 账号集合。
        email: 账号邮箱。
        reason: 标记为异常的原因说明。
    """
    account_collection.update_one(
        {"email": email},
        {
            "$set": {
                "status": 2,
                "status_msg": reason,
                "updated_at": now_local(),
            }
        },
        upsert=False,
    )
