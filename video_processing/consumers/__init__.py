"""
视频处理消费者
==================
此模块包含视频处理工作流的消费者实现。

可用消费者:
- RedisTaskVideoConsumer: 基于 Redis 的视频任务消费者
- LocalVideoUploader: 本地视频到 COS 上传器
"""

# 从消费者模块导入主要函数
try:
    from video_processing.consumers.redis_task_consumer import main as redis_consumer_main
    from video_processing.consumers.local_video_uploader import run_forever as uploader_run_forever

    __all__ = [
        "redis_consumer_main",
        "uploader_run_forever",
    ]
except ImportError:
    # 当模块尚未完全设置时
    __all__ = []

def get_redis_consumer_main():
    """获取 Redis 任务消费者的主函数。"""
    try:
        from video_processing.consumers.redis_task_consumer import main
        return main
    except ImportError:
        return None

def get_uploader_run_forever():
    """获取本地视频上传器的运行函数。"""
    try:
        from video_processing.consumers.local_video_uploader import run_forever
        return run_forever
    except ImportError:
        return None
