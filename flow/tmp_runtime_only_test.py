import asyncio
from account_mgr.redis_utils import release_cookie
from flow.automation_video_v2_fetch_consumer import GoogleFlowVideoFetchScraperV2

class RuntimeOnlyScraper(GoogleFlowVideoFetchScraperV2):
    async def process_task(self, page, task_data, worker):
        try:
            access_token, recaptcha_token, credits = await self._prepare_runtime_with_account_rotation(page, task_data, worker)
            return {
                'email': task_data.get('email'),
                'has_access_token': bool(access_token),
                'has_recaptcha_token': bool(recaptcha_token),
                'credits_keys': sorted(list((credits or {}).keys()))[:10],
            }
        finally:
            final_email = task_data.get('email')
            if final_email:
                try:
                    release_cookie(final_email)
                except Exception:
                    pass

async def main():
    scraper = RuntimeOnlyScraper(
        browser_pool_size=1,
        max_contexts_per_browser=1,
        headless=True,
        extra_flags=['--start-maximized'],
        viewport={'width': 0, 'height': 0},
        task_timeout_ms=120000,
    )
    async with scraper:
        result = await scraper.run_tasks([{'_id': 'runtime-only-test', 'prompt': 'test', 'type': 1}])
        print(result)

asyncio.run(main())
