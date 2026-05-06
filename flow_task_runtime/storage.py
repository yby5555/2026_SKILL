"""flow_task_runtime 的 Redis / Mongo 存储入口。"""

from __future__ import annotations

from typing import Any

from pymongo import ASCENDING, MongoClient
from redis import Redis

from .config import RuntimeSettings


def create_redis_client(settings: RuntimeSettings) -> Redis:
    """创建并验证 Redis 客户端。

    参数:
        settings: 运行时配置对象，提供 Redis 连接参数。

    返回:
        Redis: 已完成连通性校验的 Redis 客户端。
    """
    client = Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        password=settings.redis_password,
        decode_responses=True,
        socket_timeout=10,
        socket_connect_timeout=10,
    )
    client.ping()
    return client


def create_mongo_client(settings: RuntimeSettings) -> MongoClient:
    """创建并验证 MongoDB 客户端。

    参数:
        settings: 运行时配置对象，提供 MongoDB 连接参数。

    返回:
        MongoClient: 已完成 ping 校验的 Mongo 客户端。
    """
    client = MongoClient(
        settings.mongo_uri,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        socketTimeoutMS=10000,
    )
    client.admin.command("ping")
    return client


def create_task_collection(settings: RuntimeSettings) -> Any:
    """创建并返回视频任务记录集合。

    参数:
        settings: 运行时配置对象，提供任务库名和集合名。

    返回:
        Any: 任务记录集合对象。
    """
    client = create_mongo_client(settings)
    return client[settings.task_db_name][settings.task_collection_name]


def create_account_collection(settings: RuntimeSettings) -> Any:
    """创建并返回账号集合。

    参数:
        settings: 运行时配置对象，提供账号库名和集合名。

    返回:
        Any: 账号集合对象。
    """
    client = create_mongo_client(settings)
    collection = client[settings.account_db_name][settings.account_collection_name]
    collection.create_index([("email", ASCENDING)], unique=True)
    return collection
