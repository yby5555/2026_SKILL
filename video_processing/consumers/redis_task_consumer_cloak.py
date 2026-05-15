from __future__ import annotations

"""CloakBrowser 版 Redis 视频任务消费者。

这个文件是新增入口，不修改 `redis_task_consumer.py`：

- Redis / Mongo / 重试 / processing 队列逻辑沿用原消费者；
- 浏览器启动和 context/page 创建改用 `cloakbrowser_replacement` 里已经验证较稳定的
  `CloakBrowserRunner` 路径；
- 不复用旧消费者里的 locale / timezone / UA / viewport / extra_http_headers 等
  Playwright context 指纹处理，让 CloakBrowser 负责浏览器环境。

右键运行或命令行运行均可：

    python D:\2026_SKILL\video_processing\consumers\redis_task_consumer_cloak.py

常用环境变量：

    CLOAK_CONSUMER_BROWSER_POOL_SIZE=4
    CLOAK_CONSUMER_CONTEXTS_PER_BROWSER=1
    CLOAK_CONSUMER_PROFILE_MODE=fixed
    CLOAK_CONSUMER_PROFILE_BASE_DIR=D:\2026_SKILL\cloakbrowser_replacement\profiles
    CLOAK_CONSUMER_RANDOMIZE_BROWSER_IDENTITY=true
    CLOAK_CONSUMER_HEADLESS=true
"""

import asyncio
import json
import os
import random
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CLOAK_REPLACEMENT_DIR = ROOT / "cloakbrowser_replacement"
if str(CLOAK_REPLACEMENT_DIR) not in sys.path:
    sys.path.insert(0, str(CLOAK_REPLACEMENT_DIR))

CLOAKBROWSER_REPO = Path(os.getenv("CLOAKBROWSER_REPO", r"D:\CloakBrowser"))
if CLOAKBROWSER_REPO.exists() and str(CLOAKBROWSER_REPO) not in sys.path:
    sys.path.insert(0, str(CLOAKBROWSER_REPO))

from cloak_browser_runner import (  # noqa: E402
    CloakBrowserRunner,
    CloakBrowserRunnerConfig,
    get_cloakbrowser_status,
    safe_slug,
)
from driver_base.multi_browser_scraper_base import load_cookies  # noqa: E402
from run_one_video_task_headless import (  # noqa: E402
    build_runner_config,
    filter_flow_cookies,
)
from video_processing.consumers.redis_task_consumer import (  # noqa: E402
    COOLDOWN_MAX_SEC,
    COOLDOWN_MIN_SEC,
    MAX_RETRIES,
    _compute_retry_priority,
    _convert_cos_urls_in_task,
    _decode_queue_priority,
    _encode_queue_score,
    _pop_highest_priority_task,
    _recover_processing_queue,
    _remove_processing_payload,
    extract_error_message,
    mark_task_failed,
    mark_task_processing,
    upsert_task_generation_result,
    validate_task,
)
from video_processing.scrapers.automation_video_v2_click_consumer import (  # noqa: E402
    GoogleFlowVideoScraperV2,
)
from video_processing.utils.task_common import (  # noqa: E402
    TASK_CREATE_QUEUE,
    build_scraper_task,
    create_redis_client,
    create_task_collection,
    dumps_queue_payload,
    get_logger,
    now_local,
    parse_queue_payload,
)


logger = get_logger("RedisTaskVideoConsumerCloak")

ARTIFACT_DIR = CLOAK_REPLACEMENT_DIR / "artifacts" / "redis_consumer_cloak"
ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "y"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def _env_optional(name: str) -> str:
    raw = os.getenv(name)
    return raw.strip() if raw and raw.strip() else ""


def _get_int_env(*names: str, default: int) -> int:
    for name in names:
        raw = os.getenv(name)
        if raw is not None and raw.strip() != "":
            return int(raw)
    return default


def _get_bool_env(*names: str, default: bool) -> bool:
    for name in names:
        raw = os.getenv(name)
        if raw is not None:
            return raw.strip().lower() in {"1", "true", "yes", "on", "y"}
    return default


def _get_float_env(*names: str, default: float) -> float:
    for name in names:
        raw = os.getenv(name)
        if raw is not None and raw.strip() != "":
            return float(raw)
    return default


def _get_str_env(*names: str, default: str = "") -> str:
    for name in names:
        raw = os.getenv(name)
        if raw is not None and raw.strip() != "":
            return raw.strip()
    return default


def _parse_context_proxy_map(raw: str) -> dict[int, str]:
    """解析 `0=socks5://a:1,1=http://b:2` 形式的 context 代理映射。"""
    mapping: dict[int, str] = {}
    if not raw.strip():
        return mapping
    for part in raw.split(","):
        item = part.strip()
        if not item:
            continue
        key, sep, value = item.partition("=")
        if not sep:
            raise ValueError(f"Invalid CLOAK_CONSUMER_CONTEXT_PROXY_MAP item: {item!r}")
        mapping[int(key.strip())] = value.strip()
    return mapping


# 默认尽量贴近 run_40_task_audit_cloak.py 当前稳定设置。
BROWSER_POOL_SIZE = max(
    1,
    _get_int_env("CLOAK_CONSUMER_BROWSER_POOL_SIZE", "FLOW_CONSUMER_BROWSER_POOL_SIZE", default=4),
)
CONTEXTS_PER_BROWSER = max(
    1,
    _get_int_env("CLOAK_CONSUMER_CONTEXTS_PER_BROWSER", "FLOW_CONSUMER_CONTEXTS_PER_BROWSER", default=1),
)
CONSUMER_WORKERS = BROWSER_POOL_SIZE * CONTEXTS_PER_BROWSER

# True=后台无窗口，False=显示浏览器窗口；默认和 run_40_task_audit_cloak.py 一致。
HEADLESS = _get_bool_env("CLOAK_CONSUMER_HEADLESS", "CLOAK_VIDEO_HEADLESS", default=True)

# CloakBrowser humanize 行为层，默认开启 careful。
HUMANIZE = _get_bool_env("CLOAK_CONSUMER_HUMANIZE", "CLOAK_VIDEO_HUMANIZE", default=True)
HUMAN_PRESET = _get_str_env("CLOAK_CONSUMER_HUMAN_PRESET", "CLOAK_VIDEO_HUMAN_PRESET", default="careful")

# fixed=固定用户目录 persistent profile；incognito=临时隔离 context。
PROFILE_MODE = _get_str_env("CLOAK_CONSUMER_PROFILE_MODE", "CLOAK_VIDEO_PROFILE_MODE", default="fixed").lower()
PROFILE_DIR = _get_str_env("CLOAK_CONSUMER_PROFILE_DIR", "CLOAK_VIDEO_PROFILE_DIR", default="")
PROFILE_BASE_DIR = _get_str_env(
    "CLOAK_CONSUMER_PROFILE_BASE_DIR",
    "CLOAK_VIDEO_PROFILE_BASE_DIR",
    default=str(CLOAK_REPLACEMENT_DIR / "profiles"),
)

# worker=和 run_40_task_audit_cloak.py 一样，每个 browser/context 一个 runner；
# 随机身份开启时，每次 start/restart 会再切到新的 profile 子目录。
# account=每个任务按当前账号目录启动独立浏览器，隔离更强但速度更慢。
PROFILE_SCOPE = _get_str_env("CLOAK_CONSUMER_PROFILE_SCOPE", default="worker").lower()

# incognito 模式下可以共用一个 browser 开多个隔离 context；fixed 模式会自动禁用共享，避免抢 profile。
SHARED_BROWSER_CONTEXTS = _get_bool_env("CLOAK_CONSUMER_SHARED_BROWSER_CONTEXTS", default=True)

# 和 run_40_task_audit_cloak.py 一致：每次浏览器真正打开/断线重启时刷新 fingerprint_seed；
# fixed profile 模式下同时切换到新的 profile 子目录。
RANDOMIZE_BROWSER_IDENTITY = _get_bool_env(
    "CLOAK_CONSUMER_RANDOMIZE_BROWSER_IDENTITY",
    "CLOAK_VIDEO_RANDOMIZE_BROWSER_IDENTITY",
    default=True,
)

# 每个 context/worker 启动错峰秒数，减少同时打开/提交。
STAGGER_SECONDS = max(0.0, _get_float_env("CLOAK_CONSUMER_STAGGER_SECONDS", default=8.0))

# 单个任务整体超时，默认和稳定审计脚本一致。
TASK_TIMEOUT_MS = max(60_000, _get_int_env("CLOAK_CONSUMER_TASK_TIMEOUT_MS", "CLOAK_VIDEO_TASK_TIMEOUT_MS", default=9 * 60 * 1000))

CONTEXT_PROXY = _get_str_env("CLOAK_CONSUMER_CONTEXT_PROXY", "CLOAK_VIDEO_CONTEXT_PROXY", default="")
CONTEXT_PROXY_MAP = _parse_context_proxy_map(
    _get_str_env("CLOAK_CONSUMER_CONTEXT_PROXY_MAP", "CLOAK_VIDEO_CONTEXT_PROXY_MAP", default="")
)

RESTART_ON_DISCONNECT = _get_bool_env(
    "CLOAK_CONSUMER_RESTART_ON_DISCONNECT",
    "CLOAK_VIDEO_RESTART_ON_DISCONNECT",
    default=True,
)

# 方案 A：单个浏览器连续 403 达到阈值后，关闭浏览器并换一个新的固定用户目录。
ROTATE_PROFILE_ON_CONSECUTIVE_403 = _get_bool_env("CLOAK_CONSUMER_ROTATE_PROFILE_ON_403", default=True)
CONSECUTIVE_403_ROTATE_THRESHOLD = max(1, _get_int_env("CLOAK_CONSUMER_403_ROTATE_THRESHOLD", default=3))
ROTATE_PROFILE_COOLDOWN_SECONDS = max(0.0, _get_float_env("CLOAK_CONSUMER_403_ROTATE_COOLDOWN_SECONDS", default=30.0))


class RotatingIdentityCloakBrowserRunner(CloakBrowserRunner):
    """保持和 run_40_task_audit_cloak.py 一样的浏览器身份刷新逻辑。

    每次浏览器真正 start() 前都会刷新两类身份：
    1. fingerprint_seed：写入 CloakBrowser 的 --fingerprint 参数。
    2. persistent_profile_dir：fixed profile 模式下切到一个新的用户目录子目录。

    CloakBrowserRunner.ensure_started() 在浏览器断线时会 close() + start()，
    所以重写 start() 后，首次打开和断线重启都会自动换身份。
    """

    def __init__(
        self,
        config: CloakBrowserRunnerConfig,
        *,
        enabled: bool,
        run_id: str,
        suffix: str,
        profile_mode: str,
        profile_dir: str,
        profile_base_dir: str,
        identity_events: list[dict[str, Any]],
    ) -> None:
        super().__init__(config)
        self._enabled = enabled
        self._run_id = run_id
        self._suffix = suffix
        self._profile_mode = profile_mode
        self._profile_dir = profile_dir
        self._profile_base_dir = profile_base_dir
        self._identity_events = identity_events
        self._launch_counter = 0

    def _next_profile_base_dir(self) -> Path:
        """返回随机用户目录父目录；显式 profile_dir 和审计脚本一样被当作父目录使用。"""
        if self._profile_dir:
            return Path(self._profile_dir)
        return Path(self._profile_base_dir)

    def _apply_new_browser_identity(self) -> None:
        """生成并应用本次浏览器启动使用的随机指纹和用户目录。"""
        self._launch_counter += 1
        fingerprint_seed = random.randint(1, 0x7FFFFFFF)
        launch_token = safe_slug(
            f"{self._run_id}-{self._suffix}-l{self._launch_counter}-{int(time.time() * 1000)}-{fingerprint_seed}"
        )

        self.config.fingerprint_seed = fingerprint_seed
        profile_dir = ""
        if self._profile_mode == "fixed":
            profile_path = self._next_profile_base_dir() / launch_token
            self.config.use_persistent_context = True
            self.config.persistent_profile_dir = profile_path
            profile_dir = str(profile_path)

        event = {
            "run_id": self._run_id,
            "suffix": self._suffix,
            "launch_counter": self._launch_counter,
            "fingerprint_seed": fingerprint_seed,
            "profile_mode": self._profile_mode,
            "persistent_profile_dir": profile_dir,
            "created_at": now_local().isoformat(),
        }
        self._identity_events.append(event)
        message = (
            "[browser_identity] "
            f"suffix={self._suffix} launch={self._launch_counter} "
            f"fingerprint_seed={fingerprint_seed} profile_dir={profile_dir or '<incognito>'}"
        )
        logger.info(message)
        try:
            print(message, flush=True)
        except Exception:
            pass

    async def start(self) -> None:
        """首次打开/断线重启前刷新身份；已启动时不重复刷新。"""
        if self.browser is not None or self.persistent_context is not None:
            return
        if self._enabled:
            self._apply_new_browser_identity()
        await super().start()


@dataclass(slots=True)
class CloakConsumerWorkerContext:
    worker_id: int
    context_id: int = 0
    browser_task_capacity: int = 1
    active_tasks: int = 1
    tasks_since_recycle: int = 0
    consecutive_failures: int = 0


@dataclass(slots=True)
class BrowserHealthState:
    suffix: str
    consecutive_403: int = 0
    profile_rotation: int = 0


def _task_success_from_path(path_value: str) -> bool:
    if not path_value:
        return False
    path = Path(path_value)
    return path.exists() and path.is_file() and path.stat().st_size > 0


def _resolve_context_proxy(context_id: int) -> str:
    return CONTEXT_PROXY_MAP.get(context_id) or CONTEXT_PROXY


def _is_recaptcha_403_error(exc: Exception) -> bool:
    text = str(exc)
    return (
        "PUBLIC_ERROR_UNUSUAL_ACTIVITY" in text
        or "reCAPTCHA evaluation failed" in text
        or '"code": 403' in text
        or "'code': 403" in text
    )


def _rotated_profile_dir(suffix: str, rotation: int) -> Path:
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    rotated_name = f"{safe_slug(suffix)}-r{rotation}-{timestamp}"
    if PROFILE_DIR:
        base = Path(PROFILE_DIR)
        return base.parent / f"{safe_slug(base.name)}-{rotated_name}"
    return Path(PROFILE_BASE_DIR) / rotated_name


async def _rotate_runner_profile_after_403(
    runner: CloakBrowserRunner | None,
    health: BrowserHealthState | None,
    worker_name: str,
) -> None:
    if runner is None or health is None:
        return
    if not ROTATE_PROFILE_ON_CONSECUTIVE_403:
        return
    if PROFILE_MODE != "fixed" or PROFILE_SCOPE != "worker":
        return

    health.profile_rotation += 1
    logger.warning(
        f"[{worker_name}] 连续 403 达到 {CONSECUTIVE_403_ROTATE_THRESHOLD} 次，"
        "关闭浏览器并按 audit 脚本模式重新打开"
    )
    await runner.close()

    # 默认由 RotatingIdentityCloakBrowserRunner.start() 生成新 profile + 新 fingerprint；
    # 如果显式关闭随机身份，则保留旧的 403 轮换兜底逻辑。
    if not RANDOMIZE_BROWSER_IDENTITY:
        new_profile_dir = _rotated_profile_dir(health.suffix, health.profile_rotation)
        runner.config.persistent_profile_dir = new_profile_dir
        runner.config.fingerprint_seed = random.randint(1, 0x7FFFFFFF)

    health.consecutive_403 = 0
    if ROTATE_PROFILE_COOLDOWN_SECONDS > 0:
        logger.info(f"[{worker_name}] 403 换目录后冷却 {ROTATE_PROFILE_COOLDOWN_SECONDS:.1f}s")
        await asyncio.sleep(ROTATE_PROFILE_COOLDOWN_SECONDS)
    await runner.start()
    logger.info(
        f"[{worker_name}] 403 后浏览器已重新启动: {runner.config.persistent_profile_dir}, "
        f"fingerprint_seed={runner.config.fingerprint_seed}"
    )


def _apply_consumer_runner_overrides(
    config: CloakBrowserRunnerConfig,
    *,
    suffix: str,
    task_identity: str = "",
) -> None:
    """把消费者参数覆盖到 run_one_video_task_headless 的基础 CloakBrowser 配置上。"""
    config.headless = HEADLESS
    config.humanize = HUMANIZE
    config.human_preset = HUMAN_PRESET
    config.restart_on_disconnect = RESTART_ON_DISCONNECT

    if PROFILE_MODE == "incognito":
        config.use_persistent_context = False
        config.persistent_profile_dir = None
        return

    if PROFILE_MODE != "fixed":
        raise ValueError(f"不支持的 CLOAK_CONSUMER_PROFILE_MODE: {PROFILE_MODE!r}")

    config.use_persistent_context = True
    if PROFILE_SCOPE == "account" and task_identity:
        # 强隔离模式：一个账号一个目录。为了按账号目录启动，需要每个任务单独 runner。
        account_label = safe_slug(task_identity)[:48]
        config.persistent_profile_dir = Path(PROFILE_BASE_DIR) / f"{account_label}-{safe_slug(suffix)}"
    elif PROFILE_DIR:
        # 明确指定固定目录时，使用该目录；多 worker 时建议不要共用同一个目录。
        config.persistent_profile_dir = Path(PROFILE_DIR)
    else:
        # start 前的基础目录；随机身份开启后会被 RotatingIdentityCloakBrowserRunner
        # 覆盖成 run_id/suffix/launch/fingerprint 组成的新子目录。
        config.persistent_profile_dir = Path(PROFILE_BASE_DIR) / suffix


def _make_runner_config(
    *,
    run_identity: str,
    suffix: str,
    task_identity: str = "",
) -> CloakBrowserRunnerConfig:
    config = build_runner_config({"_id": run_identity, "email": task_identity or run_identity}, suffix=suffix)
    _apply_consumer_runner_overrides(config, suffix=suffix, task_identity=task_identity)
    return config


def _redacted_runner_config(config: CloakBrowserRunnerConfig) -> dict[str, Any]:
    return {
        "headless": config.headless,
        "humanize": config.humanize,
        "human_preset": config.human_preset,
        "geoip": config.geoip,
        "backend": config.backend,
        "timezone": config.timezone,
        "locale": config.locale,
        "fingerprint_seed": config.fingerprint_seed,
        "persistent_profile_dir": str(config.persistent_profile_dir or ""),
        "use_persistent_context": config.use_persistent_context,
        "inject_cookies_into_persistent_profile": config.inject_cookies_into_persistent_profile,
        "extra_args": list(config.extra_args),
        "proxy_configured": bool(config.default_proxy),
        "context_proxy_configured": bool(config.context_proxy),
        "restart_on_disconnect": config.restart_on_disconnect,
    }


async def run_one_with_cloak(
    runner: CloakBrowserRunner,
    scraper: GoogleFlowVideoScraperV2,
    collection: Any,
    task: dict[str, Any],
    *,
    worker_id: int,
    context_id: int,
    context_proxy: str = "",
) -> Path:
    """用 CloakBrowserRunner 执行一个 Redis 任务，并更新 Mongo 成功结果。"""
    task_id, _ = validate_task(task)
    scraper_task = build_scraper_task(task)
    normalized_task = scraper.normalize_task(scraper_task)

    raw_cookies = load_cookies(normalized_task.get("cookies"), default_domain=".google.com")
    filtered_cookies, dropped_cookies = filter_flow_cookies(raw_cookies)
    normalized_task["cookies"] = filtered_cookies
    if context_proxy:
        normalized_task["context_proxy"] = context_proxy

    artifact_prefix = ARTIFACT_DIR / f"{safe_slug(task_id)}-{int(time.time())}.failure"
    worker = CloakConsumerWorkerContext(worker_id=worker_id, context_id=context_id)

    async def handler(page: Any, task_data: dict[str, Any]) -> dict[str, Any]:
        try:
            result = await scraper.process_task(page, task_data, worker)
            return {"ok": True, "result": result}
        except Exception as exc:
            screenshot = f"{artifact_prefix}.png"
            html = f"{artifact_prefix}.html"
            try:
                await page.screenshot(path=screenshot, full_page=True)
            except Exception:
                screenshot = ""
            try:
                Path(html).write_text(await page.content(), encoding="utf-8")
            except Exception:
                html = ""
            return {
                "ok": False,
                "error_type": type(exc).__name__,
                "error": str(exc),
                "error_msg": str(exc) or "unknown_error",
                "traceback": traceback.format_exc(),
                "failure_screenshot": screenshot,
                "failure_html": html,
                "cookie_count_raw": len(raw_cookies),
                "cookie_count": len(filtered_cookies),
                "cookie_dropped_count": len(dropped_cookies),
            }

    execution = await runner.run_task(normalized_task, handler)
    if not isinstance(execution, dict) or not execution.get("ok"):
        raise RuntimeError((execution or {}).get("error_msg") or (execution or {}).get("error") or "cloak task failed")

    result = execution.get("result") or {}
    local_video_path = str(result.get("local_video_path") or "")
    if not _task_success_from_path(local_video_path):
        raise RuntimeError("CloakBrowser 任务未返回有效视频文件")

    upsert_task_generation_result(
        collection=collection,
        task=task,
        local_video_path=local_video_path,
        api_full_response=result.get("api_full_response"),
        file_md5=result.get("file_md5"),
        filesize=result.get("filesize"),
        mime=result.get("video_mime_type"),
    )
    logger.info(
        f"[B{worker_id}:C{context_id}][任务:{task_id}] 任务记录已更新, "
        f"视频路径: {local_video_path}, cookies={len(filtered_cookies)}/{len(raw_cookies)}"
    )
    return Path(local_video_path)


async def _run_one_account_scoped(
    scraper: GoogleFlowVideoScraperV2,
    collection: Any,
    task: dict[str, Any],
    *,
    consumer_run_id: str,
    identity_events: list[dict[str, Any]],
    worker_id: int,
    context_id: int,
    context_proxy: str,
) -> Path:
    """账号目录隔离模式：每个任务按账号启动独立 persistent runner。"""
    # 先 normalize 一次只为了拿到 email 作为 profile identity；真正执行时复用同一份 task。
    # 注意：这个模式更慢，但可以避免多个账号混入同一个固定用户目录。
    identity_task = scraper.normalize_task(build_scraper_task(task))
    task_with_identity = dict(task)
    task_with_identity["email"] = identity_task.get("email")
    task_with_identity["cookies"] = identity_task.get("cookies")
    task_identity = str(task_with_identity.get("email") or task_with_identity.get("_id") or "account")
    suffix = f"b{worker_id}-c{context_id}"
    runner_config = _make_runner_config(
        run_identity=consumer_run_id,
        suffix=suffix,
        task_identity=task_identity,
    )
    if context_proxy:
        runner_config.context_proxy = context_proxy

    async with RotatingIdentityCloakBrowserRunner(
        runner_config,
        enabled=RANDOMIZE_BROWSER_IDENTITY,
        run_id=consumer_run_id,
        suffix=suffix,
        profile_mode=PROFILE_MODE,
        profile_dir=PROFILE_DIR,
        profile_base_dir=PROFILE_BASE_DIR,
        identity_events=identity_events,
    ) as runner:
        return await run_one_with_cloak(
            runner,
            scraper,
            collection,
            task_with_identity,
            worker_id=worker_id,
            context_id=context_id,
            context_proxy=context_proxy,
        )


async def consumer_context_loop(
    *,
    consumer_run_id: str,
    identity_events: list[dict[str, Any]],
    worker_name: str,
    worker_id: int,
    context_id: int,
    runner: CloakBrowserRunner | None,
    health: BrowserHealthState | None,
    scraper: GoogleFlowVideoScraperV2,
    redis_client: Any,
    task_collection: Any,
) -> None:
    if STAGGER_SECONDS > 0:
        await asyncio.sleep((worker_id - 1 + context_id) * STAGGER_SECONDS)

    context_proxy = _resolve_context_proxy(context_id)
    while True:
        raw_payload, current_score = await _pop_highest_priority_task(redis_client)
        if not raw_payload:
            continue

        task: dict[str, Any] | None = None
        local_path: Path | None = None
        try:
            task = parse_queue_payload(raw_payload)
            task_id, prompt = validate_task(task)
            _convert_cos_urls_in_task(task)
            current_priority = _decode_queue_priority(current_score)
            logger.info(
                f"[{worker_name}][任务:{task_id}] CloakBrowser 开始处理，"
                f"priority={current_priority}, score={current_score}, prompt={prompt[:60]!r}"
            )

            mark_task_processing(task_collection, task)

            if PROFILE_MODE == "fixed" and PROFILE_SCOPE == "account":
                local_path = await _run_one_account_scoped(
                    scraper,
                    task_collection,
                    task,
                    consumer_run_id=consumer_run_id,
                    identity_events=identity_events,
                    worker_id=worker_id,
                    context_id=context_id,
                    context_proxy=context_proxy,
                )
            else:
                if runner is None:
                    raise RuntimeError("runner is required outside account-scoped profile mode")
                local_path = await run_one_with_cloak(
                    runner,
                    scraper,
                    task_collection,
                    task,
                    worker_id=worker_id,
                    context_id=context_id,
                    context_proxy=context_proxy,
                )

            _remove_processing_payload(redis_client, raw_payload)
            if health is not None:
                health.consecutive_403 = 0
            logger.info(f"[{worker_name}][任务:{task_id}] 处理成功，已移出 processing 队列")
        except Exception as exc:
            if task is None:
                logger.exception(f"[{worker_name}] 解析任务失败或结构错误: {exc}")
                _remove_processing_payload(redis_client, raw_payload)
                continue

            task_id = str(task.get("_id", "")).strip() or "unknown"
            if local_path and local_path.exists():
                local_path.unlink(missing_ok=True)

            retry_count = int(task.get("retry_count", 0))
            logger.exception(f"[{worker_name}][任务:{task_id}] 第 {retry_count + 1} 次处理失败: {exc}")
            is_403 = _is_recaptcha_403_error(exc)
            if health is not None:
                if is_403:
                    health.consecutive_403 += 1
                    logger.info(
                        f"[{worker_name}][任务:{task_id}] 当前浏览器连续 403 次数: "
                        f"{health.consecutive_403}/{CONSECUTIVE_403_ROTATE_THRESHOLD}"
                    )
                else:
                    health.consecutive_403 = 0

            if retry_count + 1 < MAX_RETRIES:
                _remove_processing_payload(redis_client, raw_payload)
                task["retry_count"] = retry_count + 1
                retry_priority = _compute_retry_priority(task["retry_count"])
                retry_score = _encode_queue_score(retry_priority)
                redis_client.zadd(TASK_CREATE_QUEUE, {dumps_queue_payload(task): retry_score})
                logger.info(
                    f"[{worker_name}][任务:{task_id}] 任务已放回主队列重试，"
                    f"retry_count={task['retry_count']}, priority={retry_priority}, score={retry_score}"
                )
            else:
                _remove_processing_payload(redis_client, raw_payload)
                error_msg = extract_error_message(exc, "采集异常")
                mark_task_failed(task_collection, task, error_msg)
                logger.info(f"[{worker_name}][任务:{task_id}] 达到最大重试次数，已标记为失败: {error_msg}")

            if (
                health is not None
                and is_403
                and health.consecutive_403 >= CONSECUTIVE_403_ROTATE_THRESHOLD
            ):
                await _rotate_runner_profile_after_403(runner, health, worker_name)

        cooldown = random.uniform(COOLDOWN_MIN_SEC, COOLDOWN_MAX_SEC)
        logger.debug(f"[{worker_name}] 任务间冷却 {cooldown:.1f}s")
        await asyncio.sleep(cooldown)


async def consume_forever() -> None:
    redis_client = create_redis_client()
    task_collection = create_task_collection()
    recovered = _recover_processing_queue(redis_client)
    consumer_run_id = time.strftime("%Y%m%d-%H%M%S")
    browser_identity_events: list[dict[str, Any]] = []

    scraper = GoogleFlowVideoScraperV2(
        browser_pool_size=BROWSER_POOL_SIZE,
        max_contexts_per_browser=CONTEXTS_PER_BROWSER,
        headless=HEADLESS,
        task_timeout_ms=TASK_TIMEOUT_MS,
        recycle_browser_after_failures=1,
    )

    logger.info(
        "[启动] CloakBrowser Redis 任务消费者启动，"
        f"browser_pool_size={BROWSER_POOL_SIZE}, "
        f"contexts_per_browser={CONTEXTS_PER_BROWSER}, "
        f"concurrent_workers={CONSUMER_WORKERS}, "
        f"consumer_run_id={consumer_run_id}, "
        f"profile_mode={PROFILE_MODE}, profile_scope={PROFILE_SCOPE}, "
        f"shared_browser_contexts={SHARED_BROWSER_CONTEXTS and PROFILE_MODE != 'fixed'}, "
        f"headless={HEADLESS}, humanize={HUMANIZE}, human_preset={HUMAN_PRESET}, "
        f"randomize_browser_identity={RANDOMIZE_BROWSER_IDENTITY}, "
        f"rotate_profile_on_403={ROTATE_PROFILE_ON_CONSECUTIVE_403}, "
        f"consecutive_403_threshold={CONSECUTIVE_403_ROTATE_THRESHOLD}, "
        f"recovered_processing={recovered}, cloakbrowser={json.dumps(get_cloakbrowser_status(), ensure_ascii=False)}"
    )

    shared_browser_contexts = SHARED_BROWSER_CONTEXTS and PROFILE_MODE != "fixed"
    account_scoped = PROFILE_MODE == "fixed" and PROFILE_SCOPE == "account"

    def worker_suffix(browser_id: int, *, context_id: int | None = None) -> str:
        """生成 browser/context 后缀，和 run_40_task_audit_cloak.py 保持一致。"""
        return f"b{browser_id}" if context_id is None else f"b{browser_id}-c{context_id}"

    def make_runner(browser_id: int, *, context_id: int | None = None) -> RotatingIdentityCloakBrowserRunner:
        """创建 audit 模式 runner：每次 start/restart 随机指纹，fixed 模式随机 profile 子目录。"""
        suffix = worker_suffix(browser_id, context_id=context_id)
        runner_label = f"B{browser_id}" if context_id is None else f"B{browser_id}:C{context_id}"
        runner_config = _make_runner_config(run_identity=consumer_run_id, suffix=suffix)
        logger.info(
            f"[{runner_label}] "
            f"runner_config_before_start={json.dumps(_redacted_runner_config(runner_config), ensure_ascii=False)}"
        )
        return RotatingIdentityCloakBrowserRunner(
            runner_config,
            enabled=RANDOMIZE_BROWSER_IDENTITY,
            run_id=consumer_run_id,
            suffix=suffix,
            profile_mode=PROFILE_MODE,
            profile_dir=PROFILE_DIR,
            profile_base_dir=PROFILE_BASE_DIR,
            identity_events=browser_identity_events,
        )

    async def browser_loop(browser_id: int) -> None:
        async with make_runner(browser_id) as runner:
            context_tasks = [
                asyncio.create_task(
                    consumer_context_loop(
                        consumer_run_id=consumer_run_id,
                        identity_events=browser_identity_events,
                        worker_name=f"cloak-b{browser_id}-c{context_index}",
                        worker_id=browser_id,
                        context_id=context_index,
                        runner=runner,
                        health=BrowserHealthState(suffix=f"b{browser_id}-c{context_index}"),
                        scraper=scraper,
                        redis_client=redis_client,
                        task_collection=task_collection,
                    )
                )
                for context_index in range(CONTEXTS_PER_BROWSER)
            ]
            await asyncio.gather(*context_tasks)

    async def standalone_context_loop(browser_id: int, context_id: int) -> None:
        suffix = worker_suffix(browser_id, context_id=context_id)
        if account_scoped:
            await consumer_context_loop(
                consumer_run_id=consumer_run_id,
                identity_events=browser_identity_events,
                worker_name=f"cloak-b{browser_id}-c{context_id}",
                worker_id=browser_id,
                context_id=context_id,
                runner=None,
                health=None,
                scraper=scraper,
                redis_client=redis_client,
                task_collection=task_collection,
            )
            return

        async with make_runner(browser_id, context_id=context_id) as runner:
            health = BrowserHealthState(suffix=suffix)
            await consumer_context_loop(
                consumer_run_id=consumer_run_id,
                identity_events=browser_identity_events,
                worker_name=f"cloak-b{browser_id}-c{context_id}",
                worker_id=browser_id,
                context_id=context_id,
                runner=runner,
                health=health,
                scraper=scraper,
                redis_client=redis_client,
                task_collection=task_collection,
            )

    if shared_browser_contexts:
        workers = [
            asyncio.create_task(browser_loop(browser_index + 1))
            for browser_index in range(BROWSER_POOL_SIZE)
        ]
    else:
        workers = [
            asyncio.create_task(standalone_context_loop(browser_index + 1, context_index))
            for browser_index in range(BROWSER_POOL_SIZE)
            for context_index in range(CONTEXTS_PER_BROWSER)
        ]
    await asyncio.gather(*workers)


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    logger.info(f"[启动] 当前时间: {now_local().isoformat()}")
    asyncio.run(consume_forever())


if __name__ == "__main__":
    main()
