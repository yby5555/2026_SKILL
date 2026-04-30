"""
api_models.py
=============
视频生成服务 Pydantic 数据模型。
"""
from pydantic import BaseModel, Field


class VideoRequest(BaseModel):
    """
    视频生成请求体。

    调用方需提供全局唯一的 id，用于后续在数据库中追踪任务状态。
    """
    id:     str = Field(..., description="调用方提供的任务 ID（全局唯一）")
    prompt: str = Field(..., min_length=1, description="视频生成提示词")


class VideoResult(BaseModel):
    """
    单条任务结果。

    status 语义：
      success         视频已生成且本地下载成功
      partial_success 视频已生成，本地下载失败（video_url 仍可用）
      failed          生成失败
    """
    id:               str
    status:           str                # success | partial_success | failed
    error_code:       str | None = None
    project_id:       str | None = None
    video_url:        str | None = None
    local_video_path: str | None = None
    error:            str | None = None


class VideoResponse(BaseModel):
    """批量生成响应体。"""
    request_id:            str   # 本次 HTTP 请求的追踪 ID（与任务 id 不同）
    total:                 int
    success_count:         int
    partial_success_count: int
    fail_count:            int
    results:               list[VideoResult]
