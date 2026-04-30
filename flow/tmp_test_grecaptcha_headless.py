import asyncio
from playwright.async_api import async_playwright
SITE_KEY = '6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV'
URL = 'https://labs.google/fx/zh/tools/flow'
SRC = f'https://www.google.com/recaptcha/enterprise.js?render={SITE_KEY}'

async def test():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--start-maximized'])
        context = await browser.new_context(viewport=None)
        page = await context.new_page()
        await page.goto(URL, wait_until='domcontentloaded')
        try:
            await page.add_script_tag(url=SRC)
        except Exception as e:
            print('add_script_tag_error', repr(e))
        await page.wait_for_timeout(5000)
        state = await page.evaluate("""() => ({
            href: location.href,
            hasGrecaptcha: !!window.grecaptcha,
            hasEnterprise: !!window.grecaptcha?.enterprise,
            ua: navigator.userAgent,
            webdriver: navigator.webdriver,
        })""")
        print(state)
        await browser.close()

asyncio.run(test())
