"""Isolated CloakBrowser-backed automation runner prototype.

This file intentionally lives outside the existing driver_base implementation so the
current production code remains untouched while testing whether CloakBrowser can act
as the browser-launch replacement beneath the existing MultiBrowserScraperBase shape.
"""

from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable

# Use the local GitHub checkout without requiring global installation.
DEFAULT_CLOAKBROWSER_REPO = Path(os.getenv("CLOAKBROWSER_REPO", r"D:\CloakBrowser"))
if DEFAULT_CLOAKBROWSER_REPO.exists() and str(DEFAULT_CLOAKBROWSER_REPO) not in sys.path:
    sys.path.insert(0, str(DEFAULT_CLOAKBROWSER_REPO))

from cloakbrowser import binary_info, launch_async  # type: ignore  # noqa: E402

ContextHook = Callable[[Any, dict[str, Any]], Awaitable[Any]]
PageHook = Callable[[Any, dict[str, Any]], Awaitable[None]]
TaskHandler = Callable[[Any, dict[str, Any]], Awaitable[Any]]


@dataclass(slots=True)
class CloakBrowserRunnerConfig:
    """Launch-only config; CloakBrowser owns browser fingerprint defaults."""

    headless: bool = False
    humanize: bool = True
    human_preset: str = "default"
    stealth_args: bool = True
    default_proxy: str | dict[str, Any] | None = None


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

    async def __aenter__(self) -> "CloakBrowserRunner":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def start(self) -> None:
        if self.browser is not None:
            return
        self.browser = await launch_async(
            headless=self.config.headless,
            proxy=self.config.default_proxy,
            humanize=self.config.humanize,
            human_preset=self.config.human_preset,  # type: ignore[arg-type]
            stealth_args=self.config.stealth_args,
        )

    async def close(self) -> None:
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
        if self.browser is None:
            raise RuntimeError("CloakBrowserRunner failed to start browser")

        # Keep context creation deliberately minimal. Do not copy the old
        # redis_task_consumer/driver_base browser fingerprint handling here
        # (locale/timezone/UA/headers/viewport/screen). CloakBrowser should own
        # those signals. The only context-level data this probe injects is the
        # account cookie payload needed to stay logged in.
        cookies_payload = task_data.get("cookies")
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
