"""
工具函数和辅助模块
==================
此模块包含视频处理的工具函数和辅助程序。

可用工具:
- task_common: 常用任务处理工具
- account_utils: 账号验证和管理
"""

# 尝试导入主要函数
try:
    from video_processing.utils.task_common import (
        create_redis_client,
        create_task_collection,
        get_logger,
        now_local,
        parse_queue_payload,
        dumps_queue_payload,
        build_scraper_task,
    )
    from video_processing.utils.account_utils import check_account_status

    __all__ = [
        "create_redis_client",
        "create_task_collection",
        "get_logger",
        "now_local",
        "parse_queue_payload",
        "dumps_queue_payload",
        "build_scraper_task",
        "check_account_status",
    ]
except ImportError:
    # 当模块尚未完全设置时
    __all__ = []

def get_utility_functions():
    """获取常用工具函数。"""
    try:
        from video_processing.utils.task_common import (
            create_redis_client,
            create_task_collection,
            get_logger,
            now_local,
        )
        return {
            "create_redis_client": create_redis_client,
            "create_task_collection": create_task_collection,
            "get_logger": get_logger,
            "now_local": now_local,
        }
    except ImportError:
        return {}
