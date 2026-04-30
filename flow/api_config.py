"""
api_config.py
=============
视频生成服务配置层。所有参数优先读取环境变量，其次使用默认值。
"""
import os
import sys
from pathlib import Path

# ── sys.path 修复（供当前目录其他模块统一使用）────────────────────────────────
_FLOW_DIR    = Path(__file__).resolve().parent   # 2026_SKILL/flow/
_ROOT        = _FLOW_DIR.parent                  # 2026_SKILL/
_ACCOUNT_MGR = _ROOT / "account_mgr"

for _p in [str(_ROOT), str(_FLOW_DIR), str(_ACCOUNT_MGR)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)
# ─────────────────────────────────────────────────────────────────────────────

# 浏览器池
BROWSER_POOL_SIZE    = int(os.getenv("BROWSER_POOL_SIZE",    "2"))
CONTEXTS_PER_BROWSER = int(os.getenv("CONTEXTS_PER_BROWSER", "2"))
MAX_CONCURRENT       = BROWSER_POOL_SIZE * CONTEXTS_PER_BROWSER

# 单任务最长等待时间（4 分钟）
TASK_TIMEOUT_MS      = int(os.getenv("TASK_TIMEOUT_MS", str(4 * 60 * 1000)))

# 输出目录
VIDEO_OUTPUT_DIR     = Path(os.getenv("VIDEO_OUTPUT_DIR", str(_FLOW_DIR / "videos")))

# 同时允许的最大 HTTP 请求并发
MAX_REQUEST_CONCURRENCY = int(os.getenv("MAX_REQUEST_CONCURRENCY", str(MAX_CONCURRENT)))

# MongoDB 任务库/集合
TASK_DB_NAME = os.getenv("TASK_DB_NAME", "task_service")
TASK_COLLECTION = os.getenv("TASK_COLLECTION", "task_records")

# 服务监听
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))


class ErrorCode:
    """标准错误码常量，方便调用方做程序化识别。"""
    COOKIE_POOL_EMPTY  = "COOKIE_POOL_EMPTY"
    SCRAPER_NOT_READY  = "SCRAPER_NOT_READY"
    CONCURRENCY_LIMIT  = "CONCURRENCY_LIMIT"
    GENERATION_TIMEOUT = "GENERATION_TIMEOUT"
    DOWNLOAD_FAILED    = "DOWNLOAD_FAILED"
    PARTIAL_SUCCESS    = "PARTIAL_SUCCESS"
