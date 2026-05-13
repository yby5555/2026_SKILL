"""
配置常量
==========
此模块包含视频处理的所有配置常量。

可用常量:
- 通用配置（URL、路径、超时）
- 浏览器配置
- 任务配置
- 队列配置
- 账号配置
"""

# 从主常量模块导入常量
try:
    from video_processing.config.constants import (
        FLOW_HOME_URL,
        DEFAULT_MODEL_LABEL,
        MODEL_MAP,
        PROPORTION_MAP,
        DEFAULT_ASPECT_RATIO,
        MIN_CREDITS_THRESHOLD,
        DEFAULT_MAX_RETRIES,
        DEFAULT_POLL_TIMEOUT_MS,
        TASK_CREATE_QUEUE,
        TASK_CREATE_PROCESSING_QUEUE,
        SCORE_TIME_FACTOR,
        MAX_TIMESTAMP_MS,
    )

    __all__ = [
        "FLOW_HOME_URL",
        "DEFAULT_MODEL_LABEL",
        "MODEL_MAP",
        "PROPORTION_MAP",
        "DEFAULT_ASPECT_RATIO",
        "MIN_CREDITS_THRESHOLD",
        "DEFAULT_MAX_RETRIES",
        "DEFAULT_POLL_TIMEOUT_MS",
        "TASK_CREATE_QUEUE",
        "TASK_CREATE_PROCESSING_QUEUE",
        "SCORE_TIME_FACTOR",
        "MAX_TIMESTAMP_MS",
    ]
except ImportError:
    # 当常量模块尚未完全设置时
    __all__ = []

def get_config_constants():
    """获取常用配置常量。"""
    try:
        from video_processing.config.constants import (
            FLOW_HOME_URL,
            DEFAULT_MODEL_LABEL,
            DEFAULT_ASPECT_RATIO,
            MIN_CREDITS_THRESHOLD,
        )
        return {
            "FLOW_HOME_URL": FLOW_HOME_URL,
            "DEFAULT_MODEL_LABEL": DEFAULT_MODEL_LABEL,
            "DEFAULT_ASPECT_RATIO": DEFAULT_ASPECT_RATIO,
            "MIN_CREDITS_THRESHOLD": MIN_CREDITS_THRESHOLD,
        }
    except ImportError:
        return {}
