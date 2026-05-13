"""
视频处理配置常量
================
此模块包含视频处理系统中使用的所有配置常量。

所有的配置参数都集中管理，便于统一调整和维护。
"""

from pathlib import Path

# ── 路径配置 ─────────────────────────────────────────────────────────
# 根目录将在运行时根据实际项目结构设置
_VIDEO_PROCESSING_ROOT = None
_ACCOUNT_MGR_ROOT = None
_FLOW_ROOT = None

def set_paths(video_processing_root: Path, account_mgr_root: Path = None, flow_root: Path = None):
    """
    设置视频处理系统的根路径。

    参数:
        video_processing_root: video_processing 目录的路径
        account_mgr_root: account_mgr 目录的路径（可选）
        flow_root: flow 目录的路径（可选）
    """
    global _VIDEO_PROCESSING_ROOT, _ACCOUNT_MGR_ROOT, _FLOW_ROOT
    _VIDEO_PROCESSING_ROOT = video_processing_root
    _ACCOUNT_MGR_ROOT = account_mgr_root or video_processing_root.parent / "account_mgr"
    _FLOW_ROOT = flow_root or video_processing_root.parent / "flow"

def get_video_processing_root() -> Path:
    """获取视频处理根目录。"""
    if _VIDEO_PROCESSING_ROOT is None:
        raise RuntimeError("视频处理根目录未设置。请先调用 set_paths()。")
    return _VIDEO_PROCESSING_ROOT

def get_account_mgr_root() -> Path:
    """获取账号管理器根目录。"""
    if _ACCOUNT_MGR_ROOT is None:
        raise RuntimeError("账号管理器根目录未设置。请先调用 set_paths()。")
    return _ACCOUNT_MGR_ROOT

def get_flow_root() -> Path:
    """获取流程根目录。"""
    if _FLOW_ROOT is None:
        raise RuntimeError("流程根目录未设置。请先调用 set_paths()。")
    return _FLOW_ROOT

# ── URL 配置 ─────────────────────────────────────────────────────────
# Google Flow 视频生成服务的首页 URL
FLOW_HOME_URL = "https://labs.google/fx/zh/tools/flow"

# ── 视频配置 ─────────────────────────────────────────────────────────
# 视频来源标签（用于素材模式）
VIDEO_SOURCE_LABEL = "素材"
# 帧图片标签（用于帧模式）
FRAME_SOURCE_LABEL = "帧"
# 默认视频宽高比（竖屏）
DEFAULT_ASPECT_RATIO = "9:16"

# ── 模型配置 ─────────────────────────────────────────────────────────
# 默认视频生成模型
DEFAULT_MODEL_LABEL = "Veo 3.1 - Lite"
# 模型映射表（数字到模型名称）
MODEL_MAP = {
    0: "Veo 3.1 - Lite",    # 轻量级模型，生成速度快
    1: "Veo 3.1 - Fast",    # 快速模型，适合快速预览
}

# ── 宽高比配置 ─────────────────────────────────────────────────────────
# 视频宽高比映射表（数字到比例字符串）
PROPORTION_MAP = {
    0: "9:16",  # 竖屏视频（适合手机观看）
    1: "16:9",  # 横屏视频（适合电脑观看）
}

# ── 浏览器配置 ─────────────────────────────────────────────────────────
# 默认浏览器池大小（同时运行的浏览器实例数量）
DEFAULT_BROWSER_POOL_SIZE = 2
# 每个浏览器的最大上下文数（并发任务数）
DEFAULT_CONTEXTS_PER_BROWSER = 2
# 默认无头模式（不显示浏览器窗口）
DEFAULT_HEADLESS_MODE = True

# ── 任务配置 ─────────────────────────────────────────────────────────
# 默认最大重试次数
DEFAULT_MAX_RETRIES = 3
# 默认轮询超时时间（毫秒）- 4分钟
DEFAULT_POLL_TIMEOUT_MS = 4 * 60 * 1000  # 4分钟
# 默认上传轮询间隔（秒）- 每10秒检查一次
DEFAULT_UPLOAD_POLL_SECONDS = 10
# 默认任务优先级（数值越小优先级越高）
DEFAULT_TASK_PRIORITY = 10
# 重试优先级步长（每次重试增加的优先级值）
RETRY_PRIORITY_STEP = 10

# ── 队列配置 ─────────────────────────────────────────────────────────
# 任务创建队列名称（Redis 有序集合）
TASK_CREATE_QUEUE = "task:create:queue"
# 任务处理中队列名称（正在处理的任务）
TASK_CREATE_PROCESSING_QUEUE = "task:create:processing"

# ── Redis 配置 ─────────────────────────────────────────────────────────
# Redis 阻塞超时时间（秒）- 从队列获取任务的超时
REDIS_BLOCK_TIMEOUT_SECONDS = 5
# 视频下载超时时间（秒）- 5分钟超时
DOWNLOAD_TIMEOUT_SECONDS = 300

# ── 评分配置 ─────────────────────────────────────────────────────────
# 队列评分时间因子（用于计算任务优先级分数）
SCORE_TIME_FACTOR = 10**13
# 最大时间戳（毫秒）- 用于时间反转算法
MAX_TIMESTAMP_MS = 9_999_999_999_999

# ── 账号配置 ─────────────────────────────────────────────────────────
# 最低 AI 点数要求（低于此值的账号将被标记为额度不足）
MIN_CREDITS_THRESHOLD = 20

# ── 人机交互配置 ─────────────────────────────────────────────────
# 最小延迟时间（秒）- 模拟人类操作的最短延迟
MIN_DELAY_SECONDS = 0.5
# 最大延迟时间（秒）- 模拟人类操作的最长延迟
MAX_DELAY_SECONDS = 2.0
# 默认鼠标移动步数（模拟人类鼠标移动的步骤数）
DEFAULT_HUMAN_DELAY_STEPS = 15

# ── 文件上传配置 ─────────────────────────────────────────────────
# 文件上传最大尝试次数
MAX_UPLOAD_ATTEMPTS = 3
# 批处理大小（每批处理的任务数）
BATCH_SIZE = 5
# 最大工作线程数（并发上传的线程数）
MAX_WORKERS = 5
# 默认文件上传超时时间（毫秒）- 90秒
DEFAULT_FILE_TIMEOUT_MS = 90000

# ── 视频生成配置 ─────────────────────────────────────────────────────────
# 默认视频生成变体数量（同时生成的视频数量）
DEFAULT_VARIANT_COUNT = 1
# 默认视频格式
DEFAULT_VIDEO_FORMAT = ".mp4"
# 默认视频质量
DEFAULT_VIDEO_QUALITY = "high"

# ── 日志配置 ─────────────────────────────────────────────────────────
# 日志格式字符串
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
# 日志日期格式
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
# 单个日志文件最大大小（字节）- 10 MB
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
# 日志文件备份数量（保留的历史日志文件数量）
LOG_BACKUP_COUNT = 5