"""
视频爬虫和自动化模块
====================
此模块包含视频生成的爬虫和自动化工具。

可用模块:
- base_scraper: 基础爬虫类
- video_scraper: 视频爬虫实现
- human_interaction: 人机交互工具
- image_handler: 图片上传和处理工具
"""

# 尝试导入主要类
try:
    from video_processing.scrapers.base_scraper import BaseVideoScraper
    from video_processing.scrapers.video_scraper import GoogleFlowVideoScraperV2

    __all__ = [
        "BaseVideoScraper",
        "GoogleFlowVideoScraperV2",
    ]
except ImportError:
    # 当模块尚未完全设置时
    __all__ = []

def get_base_scraper():
    """获取 BaseVideoScraper 类。"""
    try:
        from video_processing.scrapers.base_scraper import BaseVideoScraper
        return BaseVideoScraper
    except ImportError:
        return None

def get_video_scraper():
    """获取 GoogleFlowVideoScraperV2 类。"""
    try:
        from video_processing.scrapers.video_scraper import GoogleFlowVideoScraperV2
        return GoogleFlowVideoScraperV2
    except ImportError:
        return None
