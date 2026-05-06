"""账号 Cookie 池操作模块。

功能说明：
1. 从 Redis 轮询获取一个可用账号及其 cookies
2. 归还账号并发槽位
3. 将异常账号从池中移除
"""

from __future__ import annotations

import json
from typing import Any

from .config import RuntimeSettings

_LUA_TRY_ACQUIRE = """
local cur = tonumber(redis.call('GET', KEYS[1]) or 0)
if cur >= tonumber(ARGV[1]) then
    return 0
end
redis.call('INCR', KEYS[1])
return 1
"""


def get_next_cookie(redis_client: Any, settings: RuntimeSettings, max_attempts: int | None = None) -> tuple[str, list] | None:
    """从 Redis cookie 池中取出一个当前可用的账号。

    参数:
        redis_client: Redis 客户端。
        settings: 运行时配置对象，提供 cookie 池 key 和并发槽位配置。
        max_attempts: 最大尝试次数；不传时默认最多轮询一整圈。

    返回:
        tuple[str, list] | None:
            成功时返回 (email, cookies)，失败时返回 None。
    """
    pool_size = redis_client.llen(settings.redis_pool_key)
    if pool_size == 0:
        return None

    script = redis_client.register_script(_LUA_TRY_ACQUIRE)

    for _ in range(max_attempts or pool_size):
        try:
            email = redis_client.lmove(settings.redis_pool_key, settings.redis_pool_key, "LEFT", "RIGHT")
        except Exception:
            email = redis_client.rpoplpush(settings.redis_pool_key, settings.redis_pool_key)

        if not email:
            break

        cookie_json = redis_client.get(settings.redis_cookie_key.format(email=email))
        if not cookie_json:
            redis_client.lrem(settings.redis_pool_key, 0, email)
            continue

        inuse_key = settings.redis_inuse_key.format(email=email)
        acquired = script(keys=[inuse_key], args=[settings.max_concurrent_per_account])
        if not acquired:
            continue

        return email, json.loads(cookie_json)

    return None


def release_cookie(redis_client: Any, settings: RuntimeSettings, email: str) -> None:
    """归还指定账号的并发槽位。

    参数:
        redis_client: Redis 客户端。
        settings: 运行时配置对象，提供 inuse key 模板。
        email: 要归还槽位的账号邮箱。
    """
    inuse_key = settings.redis_inuse_key.format(email=email)
    new_value = redis_client.decr(inuse_key)
    if new_value <= 0:
        redis_client.delete(inuse_key)


def remove_from_pool(redis_client: Any, settings: RuntimeSettings, email: str) -> None:
    """将异常账号从 cookie 池中彻底移除。

    参数:
        redis_client: Redis 客户端。
        settings: 运行时配置对象，提供相关 Redis key 模板。
        email: 要移除的异常账号邮箱。
    """
    redis_client.lrem(settings.redis_pool_key, 0, email)
    redis_client.delete(settings.redis_cookie_key.format(email=email))
    redis_client.delete(settings.redis_inuse_key.format(email=email))
