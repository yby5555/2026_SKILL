"""
视频处理模块
==================
重构和组织的视频处理操作模块。

此模块包含:
- consumers: Redis 任务消费者和视频上传器
- scrapers: 视频爬虫和自动化工具
- utils: 公共工具和辅助函数
- config: 配置常量

示例:
    from video_processing.consumers import RedisTaskConsumer
    from video_processing.scrapers import VideoScraper
    from video_processing.utils import TaskCommon
"""