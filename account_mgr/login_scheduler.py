"""
自动登录调度脚本 - 定时轮询 MongoDB，对 status=0 的账号执行 Google 登录 + Flow Cookie 获取

用法：
    python login_scheduler.py

所有参数在 config.py 中修改：
    CHECK_INTERVAL_SECONDS - 轮询间隔（秒）
    MAX_CONCURRENT         - 并发 Worker 数
    HEADLESS               - 是否无头模式
    LOGIN_TIMEOUT_SECONDS  - 单账号超时
    MAX_RETRIES            - 最大重试次数
"""

import sys
import asyncio
import json
import random
import logging
import logging.handlers
from datetime import datetime, date, timezone
from pathlib import Path

_pkg_dir       = Path(__file__).parent        # account_mgr/
_goge_login_dir = _pkg_dir.parent             # goge_login/

# 确保能找到同包模块（config, mongo_utils, redis_utils）
sys.path.insert(0, str(_pkg_dir))
# 确保能找到 get_flow_cookie.py（在 goge_login/ 目录下）
sys.path.insert(0, str(_goge_login_dir))

from config import (
    CHECK_INTERVAL_SECONDS, MAX_CONCURRENT, LOGIN_TIMEOUT_SECONDS,
    HEADLESS, MAX_RETRIES, RETRY_DELAY_SECONDS,
    MONGO_URI, DB_NAME, COLLECTION,
    REDIS_HOST, REDIS_PORT, REDIS_DB,
    LOG_MAX_BYTES, LOG_BACKUP_COUNT,
)
from mongo_utils import (
    create_mongo_client, get_collection,
    reset_stale_processing, claim_account,
    mark_active, mark_abnormal, mark_pending,
)
from redis_utils import create_async_redis, push_cookie_to_redis
from flow_login import login_and_get_flow_cookies


# ==================== 日志配置 ====================
_log_file = _pkg_dir /'log'/ "login_scheduler.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.handlers.RotatingFileHandler(
            _log_file,
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger(__name__)
# ==================================================

# 心跳文件
HEARTBEAT_FILE = _pkg_dir / "scheduler_heartbeat.txt"

# 模块级异步 Redis 客户端（在 main_async 中初始化）
_redis = None


def write_heartbeat() -> None:
    """写入心跳时间戳，供外部监控检测调度器是否存活"""
    try:
        HEARTBEAT_FILE.write_text(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"), encoding="utf-8"
        )
    except Exception:
        pass


# ==================== 登录任务 ====================

async def login_worker(col, queue: asyncio.Queue) -> None:
    """
    Worker 协程：持续从队列取账号并执行登录，直到队列为空。

    Queue + Worker 模型：避免大量账号时同时创建大量 Task 导致内存暴涨。
    """
    while True:
        try:
            account = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        email    = account["email"]
        password = account["password"]
        totp_key = account.get("totp_key", "") or ""
        last_error = ""

        # 增加每次处理账号前的随机等待，避免连续登录过快
        delay = random.uniform(5.0, 15.0)
        log.info(f"[{email}] 准备开始登录，随机等待 {delay:.1f}s 防风控...")
        await asyncio.sleep(delay)

        log.info(f"[{email}] 开始登录...")

        try:
            for attempt in range(1 + MAX_RETRIES):
                if attempt > 0:
                    log.info(f"[{email}] 第 {attempt} 次重试，等待 {RETRY_DELAY_SECONDS}s...")
                    await asyncio.sleep(RETRY_DELAY_SECONDS)

                try:
                    result = await asyncio.wait_for(
                        login_and_get_flow_cookies(
                            email=email,
                            password=password,
                            totp_key=totp_key or None,
                            headless=HEADLESS,
                        ),
                        timeout=LOGIN_TIMEOUT_SECONDS,
                    )

                    if result.get("success"):
                        all_cookies = (result.get("flow_extraction") or {}).get("all_cookies", [])
                        mark_active(col, email)
                        pool_size = await push_cookie_to_redis(_redis, email, all_cookies)
                        log.info(f"[{email}] ✓ 登录成功，Cookie 已写入 Redis，Pool 大小: {pool_size}")
                        break

                    # 登录失败（无异常）
                    google_ok = result.get("google_login") or {}
                    if google_ok.get("success") is False:
                        last_error = "Google 登录失败（账号或密码可能错误）"
                        mark_abnormal(col, email, last_error)
                        break

                    flow_ok    = result.get("flow_extraction") or {}
                    last_error = f"Flow Cookie 获取失败 (url={flow_ok.get('url', '?')})"
                    log.warning(f"[{email}] 尝试 {attempt+1} 失败: {last_error}")

                except asyncio.TimeoutError:
                    last_error = f"登录超时（>{LOGIN_TIMEOUT_SECONDS}s）"
                    log.warning(f"[{email}] 尝试 {attempt+1} 超时")

                except asyncio.CancelledError:
                    log.warning(f"[{email}] 任务被取消，重置为待处理")
                    mark_pending(col, email, "任务被取消（进程退出）")
                    raise

                except Exception as e:
                    last_error = f"{type(e).__name__}: {e}"
                    log.warning(f"[{email}] 尝试 {attempt+1} 异常: {last_error}")

            else:
                mark_abnormal(
                    col, email,
                    f"超过重试上限({MAX_RETRIES}次)，最后错误: {last_error}",
                )

        finally:
            queue.task_done()


async def run_once(col) -> int:
    """
    执行一轮：原子认领所有待处理账号，Queue + Worker 并发处理。
    Returns: 本轮认领的账号数
    """
    pending_count = col.count_documents({"status": 0})
    if pending_count == 0:
        log.info("没有待登录的账号 (status=0)")
        return 0

    log.info(f"共 {pending_count} 个待登录账号，并发 Worker: {MAX_CONCURRENT}")

    # 逐个原子认领
    queue: asyncio.Queue = asyncio.Queue()
    claimed = 0
    while True:
        account = claim_account(col)
        if account is None:
            break
        await queue.put(account)
        claimed += 1

    if claimed == 0:
        log.info("所有账号已被其他进程认领")
        return 0

    actual_workers = min(MAX_CONCURRENT, claimed)
    log.info(f"已认领 {claimed} 个账号，启动 {actual_workers} 个 Worker...")

    workers = []
    for _ in range(actual_workers):
        workers.append(asyncio.create_task(login_worker(col, queue)))
        # 错峰启动 Worker，防止并发请求同时打到 Google
        await asyncio.sleep(random.uniform(2.0, 5.0))

    await asyncio.gather(*workers, return_exceptions=True)
    return claimed


async def refresh_active_accounts(col) -> int:
    """
    早上6点普通刷新：将所有 status=1（active）的账号重置为 status=0（待处理），
    触发下一轮调度重新登录以刷新 Cookie。
    """
    result = col.update_many(
        {"status": 1},
        {"$set": {"status": 0, "last_refresh_trigger": datetime.now(timezone.utc)}},
    )
    return result.modified_count

async def refresh_non_active_accounts(col) -> int:
    """
    下午6点刷新：将所有状态码不是 1 且不是 0 的账号（即异常、失败等）重置为 0，给它们重新尝试的机会。
    """
    result = col.update_many(
        {"status": {"$nin": [0, 1]}},
        {"$set": {"status": 0, "last_refresh_trigger": datetime.now(timezone.utc)}},
    )
    return result.modified_count


async def run_daemon(col, interval: int) -> None:
    """主调度循环（永久运行直到 Ctrl+C）"""
    log.info("=" * 60)
    log.info("自动登录调度器启动")
    log.info(f"  检查间隔: {interval}s | 并发数: {MAX_CONCURRENT} | 无头: {HEADLESS}")
    log.info(f"  重试上限: {MAX_RETRIES} | 登录超时: {LOGIN_TIMEOUT_SECONDS}s")
    log.info(f"  日志文件: {_log_file}")
    log.info("=" * 60)

    # 启动时重置上次崩溃遗留的处理中账号
    count = reset_stale_processing(col)
    if count:
        log.info(f"已重置 {count} 个遗留处理中账号 (status=-1 → 0)")

    # 分别记录早班和晚班上次刷新的日期
    _last_refresh_morning: date | None = None
    _last_refresh_afternoon: date | None = None

    while True:
        now = datetime.now()
        log.info(f"--- 新一轮检查 [{now.strftime('%Y-%m-%d %H:%M:%S')}] ---")
        write_heartbeat()

        # ── 每日 06:00 早班刷新 ─────────────────────────────────────────
        # 条件：当前小时在 6 到 17 之间，且今天尚未触发过早班刷新
        if 6 <= now.hour < 18 and _last_refresh_morning != now.date():
            log.info("[早班刷新] 到达 06:00 窗口，普通执行一次：重置所有活跃账号(status=1)...")
            try:
                refreshed = await refresh_active_accounts(col)
                _last_refresh_morning = now.date()
                log.info(f"[早班刷新] 已重置 {refreshed} 个活跃账号 (status=1 → 0)，等待重新获取 Cookie")
            except Exception as e:
                log.error(f"[早班刷新] 异常: {e}", exc_info=True)
        # ────────────────────────────────────────────────────────────────

        # ── 每日 18:00 晚班刷新 ─────────────────────────────────────────
        # 条件：当前小时 >= 18，且今天尚未触发过晚班刷新
        if now.hour >= 18 and _last_refresh_afternoon != now.date():
            log.info("[晚班刷新] 到达 18:00 窗口，重试异常账号：重置所有非活跃账号(status!=1)...")
            try:
                refreshed = await refresh_non_active_accounts(col)
                _last_refresh_afternoon = now.date()
                log.info(f"[晚班刷新] 已重置 {refreshed} 个非活跃账号 (status!=1 → 0)，准备重试登录")
            except Exception as e:
                log.error(f"[晚班刷新] 异常: {e}", exc_info=True)
        # ────────────────────────────────────────────────────────────────

        try:
            await run_once(col)
        except Exception as e:
            log.error(f"本轮检查异常: {e}", exc_info=True)

        write_heartbeat()
        log.info(f"等待 {interval}s 后下次检查...")
        await asyncio.sleep(interval)


async def main_async() -> None:
    """异步主函数"""
    global _redis

    # 连接 MongoDB
    try:
        client = create_mongo_client()
    except Exception as e:
        log.error(f"无法连接 MongoDB ({MONGO_URI}): {e}")
        sys.exit(1)
    col = get_collection(client)

    # 连接 Redis
    try:
        _redis = await create_async_redis()
    except Exception as e:
        log.error(f"无法连接 Redis ({REDIS_HOST}:{REDIS_PORT}): {e}")
        client.close()
        sys.exit(1)

    try:
        await run_daemon(col, interval=CHECK_INTERVAL_SECONDS)
    finally:
        client.close()
        await _redis.aclose()
        log.info("连接已关闭，调度器退出")


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        log.info("收到 Ctrl+C，调度器退出")


if __name__ == "__main__":
    main()
