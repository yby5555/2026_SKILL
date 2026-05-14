"""Demo: one browser, two contexts, different proxies.

Open two browser windows (contexts) from the same CloakBrowser instance,
each routed through a different proxy fetched from 1024proxy API.
Visit an IP-checking page and a foreign site to confirm the proxies work.
"""

import asyncio

import requests

from cloak_browser_runner import CloakBrowserRunner, CloakBrowserRunnerConfig

CHECK_URL = "https://api.ipify.org?format=json"
FOREIGN_TESTS = [
    ("https://httpbin.org/ip", 15000),
    ("https://www.bing.com", 15000),
    ("https://www.google.com", 15000),
]


def get_proxy_list():
    url = "https://white.1024proxy.com/white/api?region=US&num=5&time=10&format=1&type=json"
    res = requests.get(url)
    if res.status_code != 200:
        raise RuntimeError(f"Proxy API returned status {res.status_code}: {res.text[:200]}")

    try:
        data = res.json()
        if isinstance(data, list) and len(data) >= 2:
            return [f"http://{item['host']}:{item['port']}" for item in data]
        raise RuntimeError(f"Need at least 2 proxies, got {len(data)}. Response: {res.text[:200]}")
    except Exception:
        raise RuntimeError(
            f"Failed to parse proxy API response. You may need to add this machine's "
            f"IP to the 1024proxy whitelist.\n"
            f"Response ({res.status_code}): {res.text[:200]}"
        )


async def open_page_in_context(browser, proxy: str, label: str):
    """Create a context with the given proxy, open pages, and verify connectivity."""
    context = await browser.new_context(proxy={"server": proxy})
    page = await context.new_page()
    try:
        print(f"[{label}] Testing proxy {proxy}")

        print(f"[{label}] Step 1 - Check exit IP via {CHECK_URL}")
        resp = await page.goto(CHECK_URL, wait_until="domcontentloaded", timeout=15000)
        print(f"[{label}]   status={resp.status if resp else '?'}")
        body = await page.inner_text("body")
        print(f"[{label}]   Exit IP: {body}")

        for site_url, timeout_ms in FOREIGN_TESTS:
            print(f"[{label}] Step - Test {site_url}")
            try:
                resp = await page.goto(site_url, wait_until="commit", timeout=timeout_ms)
                if resp:
                    print(f"[{label}]   status={resp.status} url={resp.url}")
                    title = await page.title()
                    print(f"[{label}]   title: {title}")
            except Exception as e:
                print(f"[{label}]   FAILED: {type(e).__name__}: {e}")

        await asyncio.sleep(300)
    except Exception as e:
        print(f"[{label}] ERROR: {e}")
    finally:
        await page.close()
        await context.close()


async def main():
    config = CloakBrowserRunnerConfig(
        headless=False,
        humanize=False,
        stealth_args=False,
    )
    runner = CloakBrowserRunner(config)
    await runner.start()
    browser = runner.browser

    try:
        proxies = get_proxy_list()
        proxy_a = proxies[0]
        proxy_b = proxies[1]
        print(f"Window-A proxy: {proxy_a}")
        print(f"Window-B proxy: {proxy_b}")

        await asyncio.gather(
            open_page_in_context(browser, proxy_a, "Window-A"),
            open_page_in_context(browser, proxy_b, "Window-B"),
        )
    finally:
        await runner.close()


if __name__ == "__main__":
    asyncio.run(main())
