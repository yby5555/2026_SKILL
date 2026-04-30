"""
video_api_server.py  v3.1
==========================
FastAPI 入口：应用创建 + 生命周期管理。

启动方式：
    python video_api_server.py
    # 自定义参数用环境变量，详见 api_config.py

Swagger：http://localhost:8000/docs
"""

import asyncio
import contextlib

import uvicorn
from fastapi import FastAPI

import api_config as cfg
from api_routes import router, set_global_state, clear_global_state
from automation_video_v2_click import GoogleFlowVideoScraperV2


# ═══════════════════════════════════════════════════════════════════════════════
# 生命周期
# ═══════════════════════════════════════════════════════════════════════════════

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    print(
        f"[服务] 启动 pool={cfg.BROWSER_POOL_SIZE}×ctx={cfg.CONTEXTS_PER_BROWSER}"
        f" 最大并发={cfg.MAX_CONCURRENT}  超时={cfg.TASK_TIMEOUT_MS // 1000}s"
    )
    print(f"[服务] 视频输出目录: {cfg.VIDEO_OUTPUT_DIR}")

    scraper = GoogleFlowVideoScraperV2(
        browser_pool_size=cfg.BROWSER_POOL_SIZE,
        max_contexts_per_browser=cfg.CONTEXTS_PER_BROWSER,
        headless=True,
        extra_flags=["--start-maximized"],
        viewport={"width": 0, "height": 0},
        default_cookie_domain="labs.google",
        task_timeout_ms=cfg.TASK_TIMEOUT_MS,
    )
    semaphore = asyncio.Semaphore(cfg.MAX_REQUEST_CONCURRENCY)

    await scraper.start()
    cfg.VIDEO_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    set_global_state(scraper, semaphore)
    print("[服务] 浏览器池就绪")

    yield

    print("[服务] 正在关闭...")
    clear_global_state()
    await scraper.close()
    print("[服务] 已关闭")


# ═══════════════════════════════════════════════════════════════════════════════
# FastAPI 应用
# ═══════════════════════════════════════════════════════════════════════════════

app = FastAPI(
    title="Flow 视频生成 API",
    description=(
        "Google Labs Flow 自动化视频生成服务。\n\n"
        f"**最大并发**：{cfg.MAX_CONCURRENT}  "
        f"（{cfg.BROWSER_POOL_SIZE} 浏览器 × {cfg.CONTEXTS_PER_BROWSER} context）\n\n"
        f"**单任务超时**：{cfg.TASK_TIMEOUT_MS // 1000} 秒"
    ),
    version="3.1.0",
    lifespan=lifespan,
)

app.include_router(router)


# ═══════════════════════════════════════════════════════════════════════════════
# 入口
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    uvicorn.run(
        "video_api_server:app",
        host=cfg.HOST,
        port=cfg.PORT,
        reload=False,
        log_level="info",
    )
