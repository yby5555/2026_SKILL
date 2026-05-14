# CloakBrowser replacement prototype

This directory is an isolated experiment. It does **not** modify the existing
`driver_base`, `video_processing`, or Redis consumer code.

## Purpose

Prototype the browser-launch replacement path for the current automation stack:

```text
current:  BrowserWorker -> scrapling AsyncStealthySession -> Playwright Browser
probe:    CloakBrowserRunner -> cloakbrowser.launch_async -> Playwright Browser
```

The existing `MultiBrowserScraperBase` should be kept as the task scheduler. In a
production migration, the replacement point is `driver_base/browser_worker`, not
`video_processing/consumers/redis_task_consumer.py`.

## Files

- `cloak_browser_runner.py` - isolated CloakBrowser-backed runner that mirrors the
  current launch/context/page/task-hook shape.
- `recaptcha_v3_score_probe.py` - headed browser probe for
  `https://recaptcha-demo.appspot.com/recaptcha-v3-request-scores.php`.
- `artifacts/` - generated screenshots, HTML, and JSON score evidence.

## Run

```powershell
python cloakbrowser_replacement\recaptcha_v3_score_probe.py
```

The script imports CloakBrowser from the local checkout at `D:\CloakBrowser` by
default. Override with `CLOAKBROWSER_REPO` if needed.
