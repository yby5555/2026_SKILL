"""
api_routes.py
=============
FastAPI 路由处理器。

所有路由均挂载到 `router`，由 video_api_server.py 注册到 app。
全局状态（scraper / semaphore）通过 api_state 模块共享。
"""

import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, status

from api_config import ErrorCode, MAX_REQUEST_CONCURRENCY
from api_models import VideoRequest, VideoResponse, VideoResult

import asyncio

router = APIRouter()


# ── 全局状态引用（由 video_api_server.lifespan 赋值）────────────────────────
# 使用模块级变量，避免循环导入
_scraper   = None
_semaphore: asyncio.Semaphore | None = None


def set_global_state(scraper, semaphore: asyncio.Semaphore):
    """由 lifespan 在服务启动时注入全局状态。"""
    global _scraper, _semaphore
    _scraper   = scraper
    _semaphore = semaphore


def clear_global_state():
    """由 lifespan 在服务关闭时清理。"""
    global _scraper, _semaphore
    _scraper   = None
    _semaphore = None


# ── 内部工具 ─────────────────────────────────────────────────────────────────

def _build_result(task_id: str, raw: Any) -> VideoResult:
    """将底层 scraper 返回值转换为统一 VideoResult，并确定三态 status。"""
    if isinstance(raw, Exception):
        err  = str(raw)
        code = ErrorCode.GENERATION_TIMEOUT if "timeout" in err.lower() else None
        return VideoResult(id=task_id, status="failed", error_code=code, error=err)

    local_path = raw.get("local_video_path") or ""
    video_url  = raw.get("video_url")

    if local_path:
        st, code = "success",         None
    elif video_url:
        st, code = "partial_success", ErrorCode.PARTIAL_SUCCESS
    else:
        st, code = "failed",          ErrorCode.DOWNLOAD_FAILED

    return VideoResult(
        id=task_id,
        status=st,
        error_code=code,
        project_id=raw.get("project_id"),
        video_url=video_url,
        local_video_path=local_path or None,
    )



async def _get_pool_status_async() -> dict:
    from redis_utils import get_pool_status
    return await asyncio.to_thread(get_pool_status)


# ═══════════════════════════════════════════════════════════════════════════════
# 健康检查路由（无鉴权，IP 白名单控制访问）
# ═══════════════════════════════════════════════════════════════════════════════

@router.get("/live", summary="存活探针", tags=["健康检查"])
async def liveness():
    """进程存活即返回 200，适合 Docker/K8s liveness probe。"""
    return {"status": "alive"}


@router.get("/ready", summary="就绪探针", tags=["健康检查"])
async def readiness():
    """Scraper 就绪才返回 200，适合 K8s readiness probe。"""
    if _scraper is None:
        raise HTTPException(status_code=503, detail="scraper not ready")
    return {"status": "ready"}


@router.get("/health/detail", summary="详细业务状态", tags=["健康检查"])
async def health_detail():
    """账号池聚合数量（不暴露邮箱）。"""
    from api_config import BROWSER_POOL_SIZE, CONTEXTS_PER_BROWSER, MAX_CONCURRENT
    pool           = await _get_pool_status_async()
    active_emails  = pool.get("active",  [])
    expired_emails = pool.get("expired", [])
    return {
        "scraper_ready":        _scraper is not None,
        "max_concurrent":       MAX_CONCURRENT,
        "browser_pool_size":    BROWSER_POOL_SIZE,
        "contexts_per_browser": CONTEXTS_PER_BROWSER,
        "cookie_pool": {
            "active_count":  len(active_emails),
            "expired_count": len(expired_emails),
            "total":         len(active_emails) + len(expired_emails),
        },
    }


@router.get("/pool", summary="Cookie Pool 聚合状态", tags=["运维"])
async def pool_status():
    """只返回账号数量，不暴露具体邮箱列表。"""
    pool    = await _get_pool_status_async()
    active  = pool.get("active",  [])
    expired = pool.get("expired", [])
    return {"active_count": len(active), "expired_count": len(expired),
            "total": len(active) + len(expired)}


# ═══════════════════════════════════════════════════════════════════════════════
# 视频生成路由
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/generate_video", response_model=VideoResponse,
             summary="并发生成视频", tags=["视频生成"])
async def generate_video(req: VideoRequest) -> VideoResponse:
    """
    传入任务 ID 与提示词，并发调用 Google Flow 生成视频。

    - 请求开始前将任务写入 MongoDB（msg=生成中）
    - 请求完成后（成功或失败）更新 MongoDB 结果与 updated_at
    """
    request_id = str(uuid.uuid4())

    if _scraper is None:
        raise HTTPException(
            status_code=503,
            detail={"error_code": ErrorCode.SCRAPER_NOT_READY, "request_id": request_id},
        )

    # 限流：Semaphore 满则立即 429
    assert _semaphore is not None
    if _semaphore._value == 0:
        raise HTTPException(
            status_code=429,
            detail={
                "error_code": ErrorCode.CONCURRENCY_LIMIT,
                "message":    f"服务繁忙，并发已达上限 {MAX_REQUEST_CONCURRENCY}，请稍后重试",
                "request_id": request_id,
            },
        )

    async with _semaphore:
        print(f"[API] request_id={request_id} task_id={req.id} prompt={req.prompt[:40]!r}")

        # 构建任务并执行（cookie 取用 / MongoDB 存储均由 process_task 内部负责）
        task_payload = {
            "id":              req.id,
            "prompt":          req.prompt,
            "variant_count":   1,
            "poll_timeout_ms": 4 * 60 * 1000,
        }
        raw_results: list[Any] = await _scraper.run_tasks([task_payload])

        # 构建 API 返回结果
        result = _build_result(req.id, raw_results[0])

        print(f"[API] {request_id} 完成 id={req.id} status={result.status}")

    return VideoResponse(
        request_id=request_id,
        total=1,
        success_count=         1 if result.status == "success"         else 0,
        partial_success_count= 1 if result.status == "partial_success" else 0,
        fail_count=            1 if result.status == "failed"          else 0,
        results=[result],
    )
