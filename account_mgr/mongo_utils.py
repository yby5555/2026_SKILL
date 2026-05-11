"""
MongoDB 工具函数 - 连接管理、重试包装、账号状态更新

被 add_accounts.py 和 login_scheduler.py 共同使用。
"""

import time
from datetime import datetime, timezone

try:
    from pymongo import MongoClient, ASCENDING, ReturnDocument
    from pymongo.errors import PyMongoError
except ImportError:
    raise ImportError("缺少依赖：pip install pymongo")

from config import (
    MONGO_URI, DB_NAME, COLLECTION,
    STATUS_PROCESSING, STATUS_PENDING, STATUS_ACTIVE, STATUS_ABNORMAL,
)


def create_mongo_client() -> MongoClient:
    """创建并验证 MongoDB 连接，失败时抛出异常"""
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        socketTimeoutMS=10000,
    )
    client.admin.command("ping")
    return client


def get_collection(client: MongoClient):
    """获取目标集合，并确保 email 唯一索引存在"""
    col = client[DB_NAME][COLLECTION]
    col.create_index([("email", ASCENDING)], unique=True)
    return col


def with_mongo_retry(func, *args, max_retries: int = 3, **kwargs):
    """
    同步 MongoDB 操作的指数退避重试包装器。
    网络抖动时自动重试，超限后重新抛出最后一次异常。
    """
    last_exc = None
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except PyMongoError as e:
            last_exc = e
            time.sleep(2 ** attempt)
    raise last_exc


def reset_stale_processing(col) -> int:
    """
    启动时将上次崩溃遗留的 status=-1 账号重置为 status=0。
    Returns: 重置的账号数量
    """
    result = col.update_many(
        {"status": STATUS_PROCESSING},
        {"$set": {
            "status":     STATUS_PENDING,
            "status_msg": "进程重启后重置",
            "updated_at": datetime.now(timezone.utc),
        }},
    )
    return result.modified_count


def claim_account(col) -> dict | None:
    """
    原子认领一个待处理账号（status: 0 → -1）。
    多进程安全，保证不会重复处理同一账号。
    Returns: 账号文档 {"email", "password", "totp_key"} 或 None
    """
    return col.find_one_and_update(
        {"status": STATUS_PENDING},
        {"$set": {
            "status":     STATUS_PROCESSING,
            "updated_at": datetime.now(timezone.utc),
        }},
        projection={"_id": 0, "email": 1, "password": 1, "totp_key": 1},
        return_document=ReturnDocument.AFTER,
    )


def mark_active(col, email: str) -> None:
    """登录成功：MongoDB status → 1（Cookie 存 Redis，不存 MongoDB）"""
    with_mongo_retry(
        col.update_one,
        {"email": email},
        {"$set": {
            "status":     STATUS_ACTIVE,
            "status_msg": "",
            "updated_at": datetime.now(timezone.utc),
        }},
    )


def mark_abnormal(col, email: str, reason: str) -> None:
    """账号异常：MongoDB status → 2，记录原因"""
    with_mongo_retry(
        col.update_one,
        {"email": email},
        {"$set": {
            "status":     STATUS_ABNORMAL,
            "status_msg": reason,
            "updated_at": datetime.now(timezone.utc),
        }},
    )


def mark_pending(col, email: str, reason: str = "") -> None:
    """
    将账号从处理中 (-1) 重置回待处理 (0)。
    CancelledError 时调用，避免账号永久锁住。
    """
    with_mongo_retry(
        col.update_one,
        {"email": email},
        {"$set": {
            "status":     STATUS_PENDING,
            "status_msg": reason,
            "updated_at": datetime.now(timezone.utc),
        }},
    )


def mark_expired_to_pending(col, email: str) -> bool:
    """
    Cookie 过期 / 失效时调用：仅当 status=1（active）时才重置为 0（pending）。

    条件限制：避免覆盖正在处理中 (-1) 或已标记异常 (2) 的账号。
    Returns: True 表示成功重置，False 表示状态不符（跳过）。
    """
    result = with_mongo_retry(
        col.update_one,
        {"email": email, "status": STATUS_ACTIVE},
        {"$set": {
            "status":     STATUS_PENDING,
            "status_msg": "Cookie 失效（TTL 过期或消费者上报），触发重新登录",
            "updated_at": datetime.now(timezone.utc),
        }},
    )
    return result.modified_count > 0
