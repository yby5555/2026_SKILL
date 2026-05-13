"""`MultiBrowserScraperBase` 的回归测试。"""

from __future__ import annotations

import asyncio
import importlib
import unittest

from driver_base import FailureCategory, MultiBrowserScraperBase


class ClassificationScraper(MultiBrowserScraperBase):
    async def process_task(self, page, task_data, worker):
        """提供一个最小可运行的任务实现供测试使用。

        参数:
            page:
                测试任务收到的页面对象。
            task_data:
                测试任务数据。
            worker:
                当前 worker 的运行时快照。

        返回:
            dict[str, bool]:
                固定返回成功结果，便于聚焦基类行为测试。
        """
        del page, task_data, worker
        return {"ok": True}


class DummyWorker:
    def __init__(self, *, worker_id: int = 0, max_contexts: int = 1, delay_seconds: float = 0.0) -> None:
        """构造用于测试的简化 worker 假对象。

        参数:
            worker_id:
                worker 编号。
            max_contexts:
                允许的最大并发 context 数。
            delay_seconds:
                执行任务时额外等待的秒数，用于模拟超时场景。

        返回:
            None:
                只初始化测试状态。
        """
        self.worker_id = worker_id
        self.max_contexts = max_contexts
        self.delay_seconds = delay_seconds
        self.consecutive_failures = 0
        self.tasks_since_recycle = 0
        self.request_recycle = False
        self.failure_categories: list[FailureCategory] = []
        self.recycle_calls: list[int] = []
        self._recycle_gate: asyncio.Event | None = None

    async def run_task(self, *, task_data, proxy, worker_context):
        """模拟执行任务。

        参数:
            task_data:
                测试任务数据。
            proxy:
                当前测试代理配置。
            worker_context:
                当前 worker 运行时快照。

        返回:
            dict[str, bool]:
                固定的成功结果。
        """
        del task_data, proxy, worker_context
        if self.delay_seconds > 0:
            await asyncio.sleep(self.delay_seconds)
        return {"ok": True}

    async def recycle_if_needed(self, *, active_tasks: int) -> None:
        """记录回收调用，并在需要时等待测试同步信号。

        参数:
            active_tasks:
                调用回收时的活跃任务数。

        返回:
            None:
                仅用于测试回收过程。
        """
        self.recycle_calls.append(active_tasks)
        if self._recycle_gate is not None:
            await self._recycle_gate.wait()

    def record_success(self) -> None:
        """模拟记录任务成功。

        参数:
            无。

        返回:
            None:
                仅更新测试用计数器。
        """
        self.tasks_since_recycle += 1
        self.consecutive_failures = 0

    def record_failure(self, category: FailureCategory) -> None:
        """模拟记录任务失败。

        参数:
            category:
                当前失败的分类结果。

        返回:
            None:
                仅更新测试用失败状态。
        """
        self.failure_categories.append(category)
        self.tasks_since_recycle += 1
        self.consecutive_failures += 1
        if category is FailureCategory.WORKER_ERROR:
            self.request_recycle = True


class MultiBrowserScraperBaseTests(unittest.IsolatedAsyncioTestCase):
    def test_import_driver_base_from_repo_root(self):
        """验证仓库根目录可以直接导入 `driver_base`。

        参数:
            无。

        返回:
            None:
                通过断言验证导入结果。
        """
        module = importlib.import_module("driver_base")
        self.assertIs(MultiBrowserScraperBase, module.MultiBrowserScraperBase)

    def test_context_options_use_consistent_fingerprint_headers(self):
        """验证 context 选项包含真实化 UA/语言/时区配置。"""
        scraper = ClassificationScraper(
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36"
            ),
        )

        options = scraper.build_context_options(DummyWorker(), None)

        self.assertEqual(options["locale"], "zh-CN")
        self.assertEqual(options["timezone_id"], "Asia/Shanghai")
        self.assertIn("Chrome/145", options["user_agent"])
        self.assertIn('v="145"', options["extra_http_headers"]["Sec-Ch-Ua"])
        self.assertEqual(options["extra_http_headers"]["Sec-Ch-Ua-Platform"], '"Windows"')

    async def test_classify_failure_maps_core_categories(self):
        """验证失败分类逻辑能覆盖核心异常场景。

        参数:
            无。

        返回:
            None:
                通过断言校验分类结果。
        """
        scraper = ClassificationScraper(task_timeout_ms=100)

        self.assertEqual(
            await scraper.classify_failure(RuntimeError("worker crashed"), {}, worker=None),
            FailureCategory.WORKER_ERROR,
        )
        self.assertEqual(
            await scraper.classify_failure(Exception("403 forbidden"), {}, worker=None),
            FailureCategory.BAN_ERROR,
        )
        self.assertEqual(
            await scraper.classify_failure(Exception("proxy connection reset"), {}, worker=None),
            FailureCategory.PROXY_ERROR,
        )
        self.assertEqual(
            await scraper.classify_failure(ValueError("bad payload"), {}, worker=None),
            FailureCategory.TASK_ERROR,
        )
        self.assertEqual(
            await scraper.classify_failure(asyncio.TimeoutError(), {}, worker=None),
            FailureCategory.WORKER_ERROR,
        )

    async def test_release_worker_blocks_reacquire_while_recycling(self):
        """验证 worker 回收期间不会被重新分配。

        参数:
            无。

        返回:
            None:
                通过断言校验回收中的 worker 不可重新获取。
        """
        scraper = ClassificationScraper(task_timeout_ms=100)
        worker = DummyWorker()
        worker.request_recycle = True
        worker._recycle_gate = asyncio.Event()

        scraper._workers = [worker]
        scraper._worker_active_counts = {worker.worker_id: 1}
        scraper._started = True
        scraper._closed = False

        release_task = asyncio.create_task(scraper._release_worker(worker))
        await asyncio.sleep(0.02)

        acquire_task = asyncio.create_task(scraper._acquire_worker())
        await asyncio.sleep(0.02)

        self.assertFalse(acquire_task.done(), "worker should stay unavailable while recycle is in progress")
        self.assertEqual(scraper._worker_active_counts[worker.worker_id], 0)
        self.assertEqual(worker.recycle_calls, [0])

        worker._recycle_gate.set()
        await release_task
        reacquired_worker = await asyncio.wait_for(acquire_task, timeout=0.2)

        self.assertIs(reacquired_worker, worker)
        self.assertEqual(scraper._worker_active_counts[worker.worker_id], 1)

    async def test_task_timeout_records_worker_error(self):
        """验证任务总超时会被记录为 worker 错误。

        参数:
            无。

        返回:
            None:
                通过断言校验失败分类和回收标记。
        """
        scraper = ClassificationScraper(task_timeout_ms=10)
        worker = DummyWorker(delay_seconds=0.05)

        scraper._workers = [worker]
        scraper._worker_active_counts = {worker.worker_id: 0}
        scraper._started = True
        scraper._closed = False

        with self.assertRaises(asyncio.TimeoutError):
            await scraper._run_single_task({"id": 1})

        self.assertEqual(worker.failure_categories, [FailureCategory.WORKER_ERROR])
        self.assertTrue(worker.request_recycle)
        self.assertEqual(scraper._worker_active_counts[worker.worker_id], 0)


if __name__ == "__main__":
    unittest.main()
