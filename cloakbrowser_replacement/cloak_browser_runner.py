"""Isolated CloakBrowser-backed automation runner prototype.

This file intentionally lives outside the existing driver_base implementation so the
current production code remains untouched while testing whether CloakBrowser can act
as the browser-launch replacement beneath the existing MultiBrowserScraperBase shape.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable

# Use the local GitHub checkout without requiring global installation.
DEFAULT_CLOAKBROWSER_REPO = Path(os.getenv("CLOAKBROWSER_REPO", r"D:\CloakBrowser"))
if DEFAULT_CLOAKBROWSER_REPO.exists() and str(DEFAULT_CLOAKBROWSER_REPO) not in sys.path:
    sys.path.insert(0, str(DEFAULT_CLOAKBROWSER_REPO))

from cloakbrowser import binary_info, launch_async, launch_persistent_context_async  # type: ignore  # noqa: E402

ContextHook = Callable[[Any, dict[str, Any]], Awaitable[Any]]
PageHook = Callable[[Any, dict[str, Any]], Awaitable[None]]
TaskHandler = Callable[[Any, dict[str, Any]], Awaitable[Any]]


@dataclass(slots=True)
class CloakBrowserRunnerConfig:
    """Launch-only config; CloakBrowser owns browser fingerprint defaults."""

    headless: bool = False
    humanize: bool = True
    human_preset: str = "careful"
    stealth_args: bool = True
    default_proxy: str | dict[str, Any] | None = None
    geoip: bool = False
    backend: str | None = None
    timezone: str | None = None
    locale: str | None = None
    fingerprint_seed: int | str | None = None
    extra_args: tuple[str, ...] = ()
    persistent_profile_dir: str | Path | None = None
    inject_cookies_into_persistent_profile: bool = True
    use_persistent_context: bool = False


def safe_slug(value: str, default: str = "profile") -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "-", value).strip(".-_") or default


def stable_fingerprint_seed(identity: str | None, *, fallback: str = "cloak-flow") -> int:
    """Return a deterministic 31-bit fingerprint seed for a returning browser identity."""
    source = (identity or fallback).strip() or fallback
    digest = hashlib.sha256(source.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) & 0x7FFFFFFF


def profile_dir_for_identity(base_dir: str | Path, identity: str | None, *, suffix: str = "") -> Path:
    seed = stable_fingerprint_seed(identity)
    label = safe_slug(identity or "default")[:48]
    suffix_part = f"-{safe_slug(suffix)}" if suffix else ""
    return Path(base_dir) / f"{label}-{seed}{suffix_part}"


class CloakBrowserRunner:
    """Small isolated CloakBrowser replacement for the launch/session part.

    It deliberately does not import or modify driver_base. The intended production
    migration point would be BrowserWorker.ensure_started(), replacing
    AsyncStealthySession startup with the launch_async() call shown here while
    retaining the existing scheduler and task hooks.
    """

    def __init__(self, config: CloakBrowserRunnerConfig | None = None) -> None:
        self.config = config or CloakBrowserRunnerConfig()
        self.browser: Any | None = None
        self.persistent_context: Any | None = None

    def _launch_args(self) -> list[str]:
        args = list(self.config.extra_args)
        if self.config.fingerprint_seed is not None:
            args.append(f"--fingerprint={self.config.fingerprint_seed}")
        return args

    async def __aenter__(self) -> "CloakBrowserRunner":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def start(self) -> None:
        if self.browser is not None or self.persistent_context is not None:
            return

        launch_kwargs = {
            "headless": self.config.headless,
            "proxy": self.config.default_proxy,
            "args": self._launch_args(),
            "humanize": self.config.humanize,
            "human_preset": self.config.human_preset,  # type: ignore[arg-type]
            "stealth_args": self.config.stealth_args,
            "geoip": self.config.geoip,
            "backend": self.config.backend,
            "timezone": self.config.timezone,
            "locale": self.config.locale,
        }

        if self.config.use_persistent_context or self.config.persistent_profile_dir:
            if not self.config.persistent_profile_dir:
                raise ValueError("persistent_profile_dir is required when use_persistent_context=True")
            profile_dir = Path(self.config.persistent_profile_dir)
            profile_dir.mkdir(parents=True, exist_ok=True)
            self.persistent_context = await launch_persistent_context_async(
                profile_dir,
                viewport=None,
                **launch_kwargs,
            )
            return

        self.browser = await launch_async(**launch_kwargs)

    async def close(self) -> None:
        if self.persistent_context is not None:
            try:
                await self.persistent_context.close()
            finally:
                self.persistent_context = None
        if self.browser is not None:
            try:
                await self.browser.close()
            finally:
                self.browser = None

    async def run_task(
        self,
        task_data: dict[str, Any],
        handler: TaskHandler,
        *,
        initialize_context: ContextHook | None = None,
        initialize_page: PageHook | None = None,
    ) -> Any:
        await self.start()
        if self.browser is None and self.persistent_context is None:
            raise RuntimeError("CloakBrowserRunner failed to start browser")

        # Keep context creation deliberately minimal. Do not copy the old
        # redis_task_consumer/driver_base browser fingerprint handling here
        # (locale/timezone/UA/headers/viewport/screen). CloakBrowser should own
        # those signals. The only context-level data this probe injects is the
        # account cookie payload needed to stay logged in.
        cookies_payload = task_data.get("cookies")
        owns_context = False
        if self.persistent_context is not None:
            context = self.persistent_context
            if cookies_payload and self.config.inject_cookies_into_persistent_profile:
                await context.add_cookies(cookies_payload)
        else:
            owns_context = True
            if cookies_payload:
                context = await self.browser.new_context(storage_state={"cookies": cookies_payload})
            else:
                context = await self.browser.new_context()
        page = None
        try:
            if initialize_context:
                context = await initialize_context(context, task_data)
            page = await context.new_page()
            if initialize_page:
                await initialize_page(page, task_data)
            return await handler(page, task_data)
        finally:
            if page is not None:
                try:
                    await page.close()
                except Exception:
                    pass
            if owns_context:
                await context.close()


def get_cloakbrowser_status() -> dict[str, Any]:
    """Expose binary status for verification scripts."""
    return binary_info()


async def smoke_open(url: str) -> str:
    """Tiny manual smoke helper."""
    async with CloakBrowserRunner() as runner:
        async def handler(page: Any, task: dict[str, Any]) -> str:
            await page.goto(task["url"], wait_until="domcontentloaded")
            return await page.title()

        return await runner.run_task({"url": url}, handler)


if __name__ == "__main__":
    print(asyncio.run(smoke_open("https://example.com")))
