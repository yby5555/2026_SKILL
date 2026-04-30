"""
api_task_store.py
=================
视频任务的 MongoDB CRUD。

集合：video_tasks
文档结构：
  {
    "id":               str,    # 调用方提供的唯一任务 ID
    "prompt":           str,
    "msg":              str,    # "生成中" → 完成后更新为结果描述
    "created_at":       datetime,
    "updated_at":       datetime | None,
    "status":           str | None,   # success / partial_success / failed
    "project_id":       str | None,
    "video_url":        str | None,
    "local_video_path": str | None,
    "error_code":       str | None,
    "error":            str | None,
  }
"""

import asyncio
from datetime import datetime, timezone

from pymongo import MongoClient

# api_config 已完成 sys.path 修复，此处直接导入 account_mgr 的工具
from api_config import TASK_COLLECTION
from config import MONGO_URI, DB_NAME


# ── 懒初始化单例 ──────────────────────────────────────────────────────────────
_client: MongoClient | None = None


def _get_col():
    """获取 video_tasks 集合（首次调用时建立连接）。"""
    global _client
    if _client is None:
        _client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=10000,
        )
    return _client[DB_NAME][TASK_COLLECTION]


# ── 同步操作（由 asyncio.to_thread 包装调用）────────────────────────────────

def _sync_create(task_id: str, prompt: str) -> None:
    col = _get_col()
    col.update_one(
        {"id": task_id},
        {"$setOnInsert": {
            "id":               task_id,
            "prompt":           prompt,
            "msg":              "生成中",
            "created_at":       datetime.now(timezone.utc),
            "updated_at":       None,
            "status":           None,
            "project_id":       None,
            "video_url":        None,
            "local_video_path": None,
            "error_code":       None,
            "error":            None,
        }},
        upsert=True,
    )


def _sync_update(task_id: str, fields: dict) -> None:
    col = _get_col()
    fields["updated_at"] = datetime.now(timezone.utc)
    col.update_one({"id": task_id}, {"$set": fields})


# ── 异步公开接口 ─────────────────────────────────────────────────────────────

async def create_task(task_id: str, prompt: str) -> None:
    """
    请求开始前调用：在 MongoDB 中创建任务记录。

    若 id 已存在（重复提交）则不覆盖，保持幂等。
    """
    await asyncio.to_thread(_sync_create, task_id, prompt)


async def update_task(task_id: str, result: dict) -> None:
    """
    请求完成后调用：将任务结果写回 MongoDB，并记录 updated_at。

    Args:
        task_id: 与 create_task 相同的 id
        result:  来自 _build_result 的字段字典（status/video_url/error 等）
    """
    # 根据结果生成人类可读的 msg
    status = result.get("status", "failed")
    msg_map = {
        "success":         "生成成功",
        "partial_success": "已生成（下载失败，可用 video_url 获取）",
        "failed":          "生成失败",
    }
    fields = {
        "msg":              msg_map.get(status, status),
        "status":           status,
        "project_id":       result.get("project_id"),
        "video_url":        result.get("video_url"),
        "local_video_path": result.get("local_video_path"),
        "error_code":       result.get("error_code"),
        "error":            result.get("error"),
    }
    await asyncio.to_thread(_sync_update, task_id, fields)
