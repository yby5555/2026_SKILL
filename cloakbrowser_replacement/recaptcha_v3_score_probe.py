"""Headed CloakBrowser probe for Google's reCAPTCHA v3 demo score page.

Run from repo root:
    python cloakbrowser_replacement\recaptcha_v3_score_probe.py

Environment overrides:
    CLOAKBROWSER_REPO=D:\CloakBrowser
    RECAPTCHA_PROBE_URL=https://recaptcha-demo.appspot.com/recaptcha-v3-request-scores.php
    RECAPTCHA_PROBE_KEEP_OPEN_SECONDS=3
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

from cloak_browser_runner import CloakBrowserRunner, CloakBrowserRunnerConfig, get_cloakbrowser_status

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

TARGET_URL = os.getenv(
    "RECAPTCHA_PROBE_URL",
    "https://recaptcha-demo.appspot.com/recaptcha-v3-request-scores.php",
)
KEEP_OPEN_SECONDS = float(os.getenv("RECAPTCHA_PROBE_KEEP_OPEN_SECONDS", "3"))
OUTPUT_DIR = Path(__file__).resolve().parent / "artifacts"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _find_score(value: Any) -> float | None:
    """Recursively find a score-like field in backend JSON."""
    if isinstance(value, dict):
        for key, nested in value.items():
            if str(key).lower() == "score" and isinstance(nested, (int, float, str)):
                try:
                    score = float(nested)
                    if 0 <= score <= 1:
                        return score
                except Exception:
                    pass
            found = _find_score(nested)
            if found is not None:
                return found
    elif isinstance(value, list):
        for item in value:
            found = _find_score(item)
            if found is not None:
                return found
    return None


async def _extract_score(page: Any, backend_response: Any | None = None) -> dict[str, Any]:
    """Try several page-shape tolerant ways to extract a v3 score."""
    body_text = await page.locator("body").inner_text(timeout=10_000)
    score_match = re.search(r"(?:score|Score)\D*([01](?:\.\d+)?)", body_text)

    structured_texts = await page.locator("textarea, pre, code, #scores, #score, .scores, .score").evaluate_all(
        """els => els.map(el => (el.value || el.innerText || el.textContent || '').trim()).filter(Boolean)"""
    )
    for text in structured_texts:
        score_match = score_match or re.search(r"(?:score|Score)\D*([01](?:\.\d+)?)", text)

    backend_score = _find_score(backend_response) if backend_response is not None else None
    return {
        "score": backend_score if backend_score is not None else (float(score_match.group(1)) if score_match else None),
        "backend_response": backend_response,
        "body_text_excerpt": body_text[:2000],
        "structured_texts": structured_texts[:10],
    }


async def _click_likely_score_button(page: Any) -> list[str]:
    attempts: list[str] = []
    selectors = [
        'button:has-text("Request scores")',
        'button:has-text("request scores")',
        'input[type="submit"]',
        'button[type="submit"]',
        'button',
        'input[type="button"]',
    ]
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            if await locator.count() and await locator.is_visible(timeout=1500):
                attempts.append(selector)
                await locator.click(timeout=10_000)
                await asyncio.sleep(8)
                break
        except Exception as exc:
            attempts.append(f"{selector} failed: {exc}")
    return attempts


async def run_probe() -> dict[str, Any]:
    config = CloakBrowserRunnerConfig(
        headless=False,
        # locale=os.getenv("FLOW_BROWSER_LOCALE", "en-US"),
        # timezone_id=os.getenv("FLOW_BROWSER_TIMEZONE_ID", "UTC"),
        # viewport={"width": 1366, "height": 900},
        # extra_flags=["--start-maximized"],
        humanize=True,
    )

    async with CloakBrowserRunner(config) as runner:
        async def handler(page: Any, task: dict[str, Any]) -> dict[str, Any]:
            started = time.time()
            verify_future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
            network_log: list[str] = []

            def on_response(response: Any) -> None:
                url = getattr(response, "url", "")
                if "recaptcha-v3-verify.php" in url:
                    network_log.append(f"{getattr(response.request, 'method', '?')} {url} [{getattr(response, 'status', '?')}]")
                    if not verify_future.done():
                        verify_future.set_result(response)

            page.on("response", on_response)
            await page.goto(task["url"], wait_until="domcontentloaded", timeout=90_000)
            try:
                await page.wait_for_load_state("networkidle", timeout=30_000)
            except Exception:
                pass
            await asyncio.sleep(15)  # let reCAPTCHA v3 collect behavior before requesting score
            click_attempts = await _click_likely_score_button(page)

            backend_response = None
            try:
                response = await asyncio.wait_for(verify_future, timeout=75)
                try:
                    backend_response = await response.json()
                except Exception:
                    backend_response = await response.text()
            except asyncio.TimeoutError:
                backend_response = {"error": "timed out waiting for recaptcha-v3-verify.php", "network_log": network_log}

            try:
                await page.wait_for_function(
                    """() => {
                        const el = document.querySelector('.response');
                        return el && !el.closest('.hidden') && !el.textContent.includes('from-backend');
                    }""",
                    timeout=10_000,
                )
            except Exception:
                pass

            extracted = await _extract_score(page, backend_response)
            screenshot_path = OUTPUT_DIR / f"recaptcha_v3_score_{int(started)}.png"
            html_path = OUTPUT_DIR / f"recaptcha_v3_score_{int(started)}.html"
            await page.screenshot(path=str(screenshot_path), full_page=True)
            html_path.write_text(await page.content(), encoding="utf-8")
            if KEEP_OPEN_SECONDS > 0:
                await asyncio.sleep(KEEP_OPEN_SECONDS)
            return {
                "url": page.url,
                "title": await page.title(),
                "click_attempts": click_attempts,
                "network_log": network_log,
                "screenshot": str(screenshot_path),
                "html": str(html_path),
                **extracted,
            }

        result = await runner.run_task({"url": TARGET_URL}, handler)

    result["cloakbrowser"] = get_cloakbrowser_status()
    result_path = OUTPUT_DIR / "last_recaptcha_v3_score.json"
    result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return result


if __name__ == "__main__":
    asyncio.run(run_probe())
