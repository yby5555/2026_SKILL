from __future__ import annotations

import unittest
from unittest.mock import patch

from driver_base.browser_worker import BrowserWorker


class FakeBrowser:
    def __init__(self, *, connected: bool = True, version: str = "123.0") -> None:
        self._connected = connected
        self.version = version

    def is_connected(self) -> bool:
        return self._connected


class FakeSession:
    def __init__(
        self,
        *,
        browser: FakeBrowser | None = None,
        close_raises: bool = False,
    ) -> None:
        self.browser = browser
        self.context = None
        self._context_options: dict[str, str] = {}
        self._config = object()
        self.close_raises = close_raises
        self.start_calls = 0
        self.close_calls = 0

    async def start(self) -> None:
        self.start_calls += 1

    async def close(self) -> None:
        self.close_calls += 1
        if self.close_raises:
            raise RuntimeError("session already dead")


async def _create_cookies_payload(task_data):
    del task_data
    return []


async def _initialize_context(context, task_data, worker):
    del task_data, worker
    return context


async def _initialize_page(page, task_data, worker):
    del page, task_data, worker
    return None


async def _process_task(page, task_data, worker):
    del page, task_data, worker
    return {"ok": True}


class BrowserWorkerHealthTests(unittest.IsolatedAsyncioTestCase):
    def _make_worker(self) -> BrowserWorker:
        return BrowserWorker(
            worker_id=1,
            max_contexts=1,
            navigation_timeout_ms=1000,
            launch_options_factory=lambda worker: {},
            context_options_factory=lambda worker, proxy=None: {},
            create_cookies_payload=_create_cookies_payload,
            initialize_context=_initialize_context,
            initialize_page=_initialize_page,
            process_task=_process_task,
            recycle_after_tasks=None,
            recycle_after_failures=None,
        )

    async def test_ensure_started_restarts_when_browser_reference_is_stale(self):
        worker = self._make_worker()
        stale_browser = FakeBrowser(connected=False)
        stale_session = FakeSession(browser=stale_browser, close_raises=True)
        fresh_session = FakeSession(browser=FakeBrowser(connected=True, version="124.0"))

        worker._browser = stale_browser
        worker._session = stale_session

        with patch("driver_base.browser_worker.AsyncStealthySession", return_value=fresh_session) as session_cls:
            await worker.ensure_started()

        self.assertEqual(stale_session.close_calls, 1)
        self.assertIs(worker._session, fresh_session)
        self.assertIs(worker._browser, fresh_session.browser)
        self.assertEqual(fresh_session.start_calls, 1)
        session_cls.assert_called_once()

    async def test_ensure_started_keeps_healthy_browser(self):
        worker = self._make_worker()
        healthy_browser = FakeBrowser(connected=True, version="124.0")
        healthy_session = FakeSession(browser=healthy_browser)

        worker._browser = healthy_browser
        worker._session = healthy_session

        with patch("driver_base.browser_worker.AsyncStealthySession") as session_cls:
            await worker.ensure_started()

        self.assertIs(worker._session, healthy_session)
        self.assertIs(worker._browser, healthy_browser)
        session_cls.assert_not_called()


if __name__ == "__main__":
    unittest.main()
