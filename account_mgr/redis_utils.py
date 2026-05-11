"""
Redis 工具函数 - 包含两套接口：

    异步（async）：供 login_scheduler.py 使用，写入 Cookie
    同步（sync）：供 cookie_reader.py 和消费者使用，Round-Robin 读取 Cookie
"""

import json

try:
    import redis.asyncio as aioredis
    import redis as sync_redis
except ImportError:
    raise ImportError("缺少依赖：pip install redis")

from config import (
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD,
    REDIS_COOKIE_KEY, REDIS_POOL_KEY, COOKIE_TTL,
    REDIS_INUSE_KEY, MAX_CONCURRENT_PER_ACCOUNT,
    REDIS_RETRY_QUEUE,
)


# ==================== 异步接口（写入方，供调度器使用） ====================

async def create_async_redis() -> aioredis.Redis:
    """创建并验证异步 Redis 连接"""
    client = aioredis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,
    )
    await client.ping()
    return client


async def push_cookie_to_redis(redis_client: aioredis.Redis, email: str, cookies: list) -> int:
    """
    将登录成功的 Cookie 推入 Redis。

    写入逻辑：
        1. SET flow:cookie:data:{email} {json}  EX {COOKIE_TTL}
        2. LREM flow:cookie:pool 0 {email}      （去重，移除旧记录）
        3. RPUSH flow:cookie:pool {email}        （加入队尾）

    Args:
        redis_client: 异步 Redis 连接
        email:        账号邮箱（作为 key 标识）
        cookies:      Cookie 列表（list of dict，可直接传给 Playwright）

    Returns:
        当前 Pool 队列长度
    """
    if not cookies:
        return 0
    data_key = REDIS_COOKIE_KEY.format(email=email)
    await redis_client.set(data_key, json.dumps(cookies, ensure_ascii=False), ex=COOKIE_TTL)
    await redis_client.lrem(REDIS_POOL_KEY, 0, email)
    await redis_client.rpush(REDIS_POOL_KEY, email)
    return await redis_client.llen(REDIS_POOL_KEY)


async def pop_retry_emails(redis_client: aioredis.Redis, timeout: float = 5.0) -> list[str]:
    """
    异步 BLPOP 重试队列，返回所有可立即取出的 email。

    先尝试非阻塞 LPOP 取一批，若空则 BLPOP 等待最多 timeout 秒。
    调度器循环中反复调用此函数来监听失效账号。
    """
    emails = []
    while True:
        email = await redis_client.lpop(REDIS_RETRY_QUEUE)
        if email:
            emails.append(email)
        else:
            break
    if not emails:
        result = await redis_client.blpop(REDIS_RETRY_QUEUE, timeout=timeout)
        if result:
            emails.append(result[1])
    return emails


# ==================== 同步接口（消费方，供外部脚本使用） ====================

_sync_client: sync_redis.Redis | None = None


def _get_sync_redis() -> sync_redis.Redis:
    """懒初始化同步 Redis 客户端（单例）"""
    global _sync_client
    if _sync_client is None:
        _sync_client = sync_redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
        )
    return _sync_client


# ── Lua 脚本：原子检查并递增并发槽位 ─────────────────────────────────────
# KEYS[1] = inuse_key
# ARGV[1] = max_concurrent（上限）
# 返回 1 = 获取成功；0 = 槽位已满
_LUA_TRY_ACQUIRE = """
local cur = tonumber(redis.call('GET', KEYS[1]) or 0)
if cur >= tonumber(ARGV[1]) then
    return 0
end
redis.call('INCR', KEYS[1])
return 1
"""
# ─────────────────────────────────────────────────────────────────────────


def get_next_cookie(max_attempts: int | None = None) -> tuple[str, list] | None:
    """
    Round-Robin 取下一个可用 Cookie，同时用 Lua 脚本原子检查并发槽位。

    机制：
        1. LMOVE pool pool LEFT RIGHT → 原子轮转（取队头，放队尾）
        2. 若 Cookie 已过期（TTL 到期）则移除该 email，继续下一个
        3. 执行 Lua 脚本原子检查 inuse 计数：
           - 计数 < MAX_CONCURRENT_PER_ACCOUNT → 递增并返回该账号
           - 计数已满 → 跳过，尝试下一个账号

    Args:
        max_attempts: 最大尝试次数（默认等于当前队列长度，最多转一圈）

    Returns:
        (email, cookies_list) 或 None（Pool 为空 / 全部过期 / 所有槽位都满）
    """
    r = _get_sync_redis()
    pool_size = r.llen(REDIS_POOL_KEY)
    if pool_size == 0:
        return None

    script = r.register_script(_LUA_TRY_ACQUIRE)

    for _ in range(max_attempts or pool_size):
        try:
            email = r.lmove(REDIS_POOL_KEY, REDIS_POOL_KEY, "LEFT", "RIGHT")
        except Exception:
            email = r.rpoplpush(REDIS_POOL_KEY, REDIS_POOL_KEY)  # 兼容 Redis < 6.2

        if not email:
            break

        # Cookie 已过期 → 清理、推入重试队列（去重）、跳过
        cookie_json = r.get(REDIS_COOKIE_KEY.format(email=email))
        if not cookie_json:
            r.lrem(REDIS_POOL_KEY, 0, email)
            if not r.lpos(REDIS_RETRY_QUEUE, email):
                r.rpush(REDIS_RETRY_QUEUE, email)
            continue

        # 原子检查并发槽位
        inuse_key = REDIS_INUSE_KEY.format(email=email)
        acquired = script(
            keys=[inuse_key],
            args=[MAX_CONCURRENT_PER_ACCOUNT],
        )
        if not acquired:
            # 该账号槽位已满，继续尝试下一个
            continue

        return email, json.loads(cookie_json)

    return None


def get_pool_status() -> dict:
    """
    查看当前 Cookie Pool 状态。

    Returns:
        {"pool_size": int, "active": [email, ...], "expired": [email, ...]}
    """
    r = _get_sync_redis()
    emails = r.lrange(REDIS_POOL_KEY, 0, -1)
    active, expired = [], []
    for email in emails:
        if r.exists(REDIS_COOKIE_KEY.format(email=email)):
            active.append(email)
        else:
            expired.append(email)
    return {"pool_size": len(emails), "active": active, "expired": expired}


def release_cookie(email: str) -> None:
    """
    归还一个账号的并发槽位（任务结束时在 finally 块调用）。

    递减 inuse 计数；若结果 <= 0 则直接删除 key，修正任何脏数据。
    """
    r = _get_sync_redis()
    inuse_key = REDIS_INUSE_KEY.format(email=email)
    new_val = r.decr(inuse_key)
    if new_val <= 0:
        r.delete(inuse_key)


def remove_from_pool(email: str) -> None:
    """手动从 Pool 中移除指定账号（账号异常时调用），同时清理 inuse 计数。"""
    r = _get_sync_redis()
    r.lrem(REDIS_POOL_KEY, 0, email)
    r.delete(REDIS_COOKIE_KEY.format(email=email))
    r.delete(REDIS_INUSE_KEY.format(email=email))  # 防止槽位泄漏


def report_cookie_invalid(email: str) -> None:
    """
    消费者调用：主动上报某个账号的 Cookie 已失效。

    做三件事：
        1. 从 Pool 移除 + 清理 data / inuse key
        2. 推入重试队列 flow:cookie:retry_queue（已去重）
    """
    r = _get_sync_redis()
    r.lrem(REDIS_POOL_KEY, 0, email)
    r.delete(REDIS_COOKIE_KEY.format(email=email))
    r.delete(REDIS_INUSE_KEY.format(email=email))
    if not r.lpos(REDIS_RETRY_QUEUE, email):
        r.rpush(REDIS_RETRY_QUEUE, email)
