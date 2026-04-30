import asyncio
import random
import re
import sys
from pathlib import Path
from time import monotonic
from typing import Any
import httpx

# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from driver_base import MultiBrowserScraperBase

FLOW_HOME_URL = "https://labs.google/fx/zh/tools/flow"
VIDEO_SOURCE_LABEL = "素材"
VIDEO_ASPECT_RATIO_LABEL = "9:16"
VIDEO_MODEL_LABEL = "Veo 3.1 - Lite"

async def human_delay(min_sec: float = 0.5, max_sec: float = 2.0):
    """模拟人类随机暂停"""
    await asyncio.sleep(random.uniform(min_sec, max_sec))

async def human_type(page, locator_str: str, text: str):
    """模拟人类逐字符输入，带随机延迟"""
    for char in text:
        if char in "\r\n":
            char = " "
        await page.locator(locator_str).type(char, delay=random.randint(30, 150))
        if random.random() < 0.05:
            await human_delay(0.2, 0.6)

def normalize_prompt_text(text: str) -> str:
    """将多行 prompt 压成单行，避免换行被页面当成 Enter 提交。"""
    text = re.sub(r"\r\n?|\n", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

async def get_textbox_content(textbox_loc) -> str:
    """兼容 input 和 contenteditable，读取输入框当前文本。"""
    content = await textbox_loc.evaluate("""(el) => {
        const read = (value) => typeof value === "string" ? value : "";
        return {
            value: read(el.value),
            innerText: read(el.innerText),
            textContent: read(el.textContent),
        };
    }""")

    for candidate in (content.get("value", ""), content.get("innerText", ""), content.get("textContent", "")):
        normalized = normalize_prompt_text(candidate)
        if normalized:
            return normalized
    return ""

async def fill_textbox_with_validation(page, textbox_loc, text: str, worker_id: Any, max_attempts: int = 2) -> None:
    """按聚焦、全选、一次性写入、校验的流程填写 prompt。"""
    for attempt in range(1, max_attempts + 1):
        box = await textbox_loc.bounding_box()
        if box:
            await page.mouse.move(box["x"] + box["width"] / 3, box["y"] + box["height"] / 2, steps=15)

        # 重复点击能让编辑器类输入框更稳定地拿到焦点
        await human_click(page, textbox_loc)
        await human_delay(0.2, 0.4)
        await human_click(page, textbox_loc)
        await human_delay(0.5, 1.0)

        await page.keyboard.press("Control+A")
        await human_delay(0.2, 0.6)
        await page.keyboard.insert_text(text)
        await human_delay(0.6, 1.2)

        current_text = await get_textbox_content(textbox_loc)
        if current_text == text:
            return

        print(
            f"[Worker {worker_id}] 第 {attempt} 次写入校验失败，"
            f"当前文本: {current_text!r}，目标文本: {text!r}"
        )
        await human_delay(0.5, 0.9)

    raise RuntimeError("输入框写入后校验失败，未继续点击创建")

async def human_mouse_move(page):
    """模拟随机鼠标移动"""
    viewport = page.viewport_size
    if not viewport:
        return
    width, height = viewport['width'], viewport['height']
    for _ in range(random.randint(2, 5)):
        x = random.randint(100, max(120, width - 100))
        y = random.randint(100, max(120, height - 100))
        await page.mouse.move(x, y, steps=random.randint(5, 15))
        await asyncio.sleep(random.uniform(0.1, 0.3))

async def human_scroll(page):
    """模拟随机鼠标滚动"""
    await page.mouse.wheel(0, random.randint(100, 400))
    await human_delay(0.2, 0.6)
    if random.random() < 0.3:
        await page.mouse.wheel(0, -random.randint(50, 200))
        await human_delay(0.1, 0.3)

async def human_click(page, locator_or_element, timeout=5000):
    """模拟人类鼠标移动并点击"""
    try:
        if hasattr(locator_or_element, 'bounding_box'):
            box = await locator_or_element.bounding_box()
        else:
            box = await locator_or_element.bounding_box()
            
        if not box:
            await locator_or_element.click(timeout=timeout)
            return
            
        target_x = box['x'] + box['width'] * random.uniform(0.3, 0.7)
        target_y = box['y'] + box['height'] * random.uniform(0.3, 0.7)
        
        # 移动过去
        await page.mouse.move(
            box['x'] + box['width'] * random.uniform(0.1, 0.9), 
            box['y'] + box['height'] * random.uniform(0.1, 0.9), 
            steps=random.randint(5, 15)
        )
        await human_delay(0.1, 0.3)
        await page.mouse.move(target_x, target_y, steps=random.randint(5, 10))
        await human_delay(0.1, 0.2)
        await page.mouse.down()
        await human_delay(0.05, 0.15)
        await page.mouse.up()
    except Exception:
        # 兜底直接 click
        await locator_or_element.click(timeout=timeout)

async def get_creation_mode_button(page):
    """定位真正的创建模式按钮，而不是顶部工具栏或模型选择器。"""
    submit_btn = page.locator("button").filter(
        has_text=re.compile(r"arrow_forward", re.IGNORECASE)
    ).filter(has_text="创建").first
    submit_box = None
    if await submit_btn.count() > 0:
        try:
            submit_box = await submit_btn.bounding_box()
        except Exception:
            submit_box = None

    all_menu_btns = page.locator('button[aria-haspopup="menu"]')
    count = await all_menu_btns.count()
    best_btn = None
    best_score = None

    for idx in range(count):
        btn = all_menu_btns.nth(idx)
        try:
            if not await btn.is_visible(timeout=200):
                continue

            current_box = await btn.bounding_box()
            if not current_box:
                continue

            text = (await btn.inner_text()).strip()
            lowered = text.lower()

            # 排除顶部工具栏、更多菜单等明显无关按钮
            if any(flag in lowered for flag in ["more_vert", "settings", "帮助", "help", "filter", "search"]):
                continue

            # 模式按钮通常在底部输入区，且紧挨着右侧提交按钮左边
            score = 0.0
            if submit_box:
                horizontal_gap = submit_box["x"] - (current_box["x"] + current_box["width"])
                vertical_gap = abs(current_box["y"] - submit_box["y"])
                if horizontal_gap < -20 or horizontal_gap > 260:
                    continue
                score += max(0.0, 260 - horizontal_gap)
                score += max(0.0, 120 - vertical_gap)
            else:
                score += current_box["x"]

            if re.search(r"x[1-4]", lowered):
                score += 80
            if any(flag in lowered for flag in ["video", "视频", "image", "图片", "nano banana", "veo"]):
                score += 40

            if best_score is None or score > best_score:
                best_btn = btn
                best_score = score
        except Exception:
            continue

    return best_btn

async def get_creation_settings_menu(page):
    """返回当前可见的底部配置菜单，而不是模型下拉。"""
    menus = page.locator('[role="menu"]')
    count = await menus.count()
    for idx in range(count):
        menu = menus.nth(idx)
        try:
            if not await menu.is_visible(timeout=200):
                continue
            if await menu.locator('[role="tablist"]').count() > 0:
                return menu
        except Exception:
            continue
    return None

async def open_creation_settings_menu(page, worker_id: Any):
    """打开底部配置菜单并返回菜单 locator。"""
    target_btn = await get_creation_mode_button(page)
    if not target_btn:
        raise RuntimeError("找不到底部配置按钮")

    if await target_btn.get_attribute("aria-expanded") != "true":
        print(f"[Worker {worker_id}] 正在打开底部配置菜单")
        await human_click(page, target_btn)
        await human_delay(0.8, 1.5)

    menu = await get_creation_settings_menu(page)
    if menu:
        return target_btn, menu

    print(f"[Worker {worker_id}] 菜单未出现，重试打开")
    await human_click(page, target_btn)
    await human_delay(0.8, 1.2)

    menu = await get_creation_settings_menu(page)
    if not menu:
        raise RuntimeError("底部配置菜单未成功展开")
    return target_btn, menu

async def select_tab_in_group(page, menu, group_index: int, label_pattern, worker_id: Any, description: str) -> None:
    """在指定 tab 组里选择目标选项。"""
    tablist = menu.locator('[role="tablist"]').nth(group_index)
    target_tab = tablist.locator('[role="tab"]').filter(has_text=label_pattern).first
    if await target_tab.count() == 0:
        raise RuntimeError(f"第 {group_index + 1} 组未找到 {description}")

    if await target_tab.get_attribute("aria-selected") == "true":
        print(f"[Worker {worker_id}] {description} 已经选中")
        return

    print(f"[Worker {worker_id}] 正在选择 {description}")
    await human_click(page, target_tab)
    await human_delay(0.6, 1.1)

async def ensure_video_model(page, menu, worker_id: Any, target_model: str) -> None:
    """确保视频模型为目标值。"""
    model_btn = menu.locator('button[aria-haspopup="menu"]').filter(
        has_text=re.compile(r"Veo 3\.1", re.IGNORECASE)
    ).first
    if await model_btn.count() == 0:
        raise RuntimeError("未找到视频模型下拉按钮")

    current_model = normalize_prompt_text(await model_btn.inner_text())
    if target_model.lower() in current_model.lower():
        print(f"[Worker {worker_id}] 视频模型已经是 {target_model}")
        return

    print(f"[Worker {worker_id}] 正在切换视频模型到 {target_model}")
    await human_click(page, model_btn)
    await human_delay(0.5, 0.9)

    target_option = page.locator('[role="menuitem"]').filter(has_text=target_model).first
    if await target_option.count() == 0:
        raise RuntimeError(f"模型菜单中未找到 {target_model}")

    await human_click(page, target_option)
    await human_delay(0.6, 1.0)

async def ensure_video_mode(page, worker_id: Any, variant_count: int) -> None:
    """确保底部配置为视频、素材、9:16、xN、Veo 3.1 - Lite。"""
    print(f"[Worker {worker_id}] 正在设置视频配置 (素材, {VIDEO_ASPECT_RATIO_LABEL}, x{variant_count}, {VIDEO_MODEL_LABEL})...")

    try:
        await asyncio.sleep(2)

        target_btn, menu = await open_creation_settings_menu(page, worker_id)

        # 第 1 组：图片 / 视频
        await select_tab_in_group(
            page,
            menu,
            0,
            re.compile(r"video|videocam|视频", re.IGNORECASE),
            worker_id,
            "视频模式",
        )

        # 切换模式后菜单会重绘，重新获取一遍
        target_btn, menu = await open_creation_settings_menu(page, worker_id)

        # 第 2 组：帧 / 素材
        await select_tab_in_group(
            page,
            menu,
            1,
            re.compile(rf"{re.escape(VIDEO_SOURCE_LABEL)}|chrome_extension", re.IGNORECASE),
            worker_id,
            f"来源 {VIDEO_SOURCE_LABEL}",
        )

        # 第 3 组：比例
        await select_tab_in_group(
            page,
            menu,
            2,
            re.compile(rf"{re.escape(VIDEO_ASPECT_RATIO_LABEL)}|crop_9_16", re.IGNORECASE),
            worker_id,
            f"比例 {VIDEO_ASPECT_RATIO_LABEL}",
        )

        # 第 4 组：变体数量
        target_variant = f"x{variant_count}"
        await select_tab_in_group(
            page,
            menu,
            3,
            re.compile(rf"^{re.escape(target_variant)}$", re.IGNORECASE),
            worker_id,
            f"生成数量 {target_variant}",
        )

        # 模型选择
        await ensure_video_model(page, menu, worker_id, VIDEO_MODEL_LABEL)

        # 关闭菜单
        if await menu.is_visible(timeout=500):
            await page.keyboard.press("Escape")
            await human_delay(0.3, 0.6)

        # 最终校验
        target_btn, menu = await open_creation_settings_menu(page, worker_id)
        selected_video_tab = menu.locator('[role="tablist"]').nth(0).locator('[role="tab"][aria-selected="true"]').first
        selected_source_tab = menu.locator('[role="tablist"]').nth(1).locator('[role="tab"][aria-selected="true"]').first
        selected_ratio_tab = menu.locator('[role="tablist"]').nth(2).locator('[role="tab"][aria-selected="true"]').first
        selected_variant_tab = menu.locator('[role="tablist"]').nth(3).locator('[role="tab"][aria-selected="true"]').first
        model_btn = menu.locator('button[aria-haspopup="menu"]').filter(
            has_text=re.compile(r"Veo 3\.1", re.IGNORECASE)
        ).first

        selected_video_text = normalize_prompt_text(await selected_video_tab.inner_text())
        selected_source_text = normalize_prompt_text(await selected_source_tab.inner_text())
        selected_ratio_text = normalize_prompt_text(await selected_ratio_tab.inner_text())
        selected_variant_text = normalize_prompt_text(await selected_variant_tab.inner_text())
        selected_model_text = normalize_prompt_text(await model_btn.inner_text())

        if "视频" not in selected_video_text and "videocam" not in selected_video_text.lower():
            raise RuntimeError(f"最终校验失败，模式不是视频: {selected_video_text}")
        if VIDEO_SOURCE_LABEL not in selected_source_text and "chrome_extension" not in selected_source_text.lower():
            raise RuntimeError(f"最终校验失败，来源不是 {VIDEO_SOURCE_LABEL}: {selected_source_text}")
        if VIDEO_ASPECT_RATIO_LABEL not in selected_ratio_text and "crop_9_16" not in selected_ratio_text.lower():
            raise RuntimeError(f"最终校验失败，比例不是 {VIDEO_ASPECT_RATIO_LABEL}: {selected_ratio_text}")
        if selected_variant_text.lower() != target_variant.lower():
            raise RuntimeError(f"最终校验失败，生成数量不是 {target_variant}: {selected_variant_text}")
        if VIDEO_MODEL_LABEL.lower() not in selected_model_text.lower():
            raise RuntimeError(f"最终校验失败，模型不是 {VIDEO_MODEL_LABEL}: {selected_model_text}")

        await page.keyboard.press("Escape")
        await human_delay(0.3, 0.6)
        await human_delay(0.8, 1.5)

    except Exception as e:
        print(f"[Worker {worker_id}] 设置模式错误: {e}")
        try:
            await page.keyboard.press("Escape")
        except Exception:
            pass

class GoogleFlowVideoScraperV2(MultiBrowserScraperBase):
    """Google Flow视频生成爬虫"""

    async def _wait_for_video_status(
        self,
        page,
        media_names: set[str],
        worker_id: Any,
        timeout_ms: int,
    ) -> dict[str, Any]:
        """等待页面自然发出的状态轮询请求返回成功。"""
        deadline = monotonic() + timeout_ms / 1000
        last_statuses: dict[str, str] = {}

        while monotonic() < deadline:
            remaining_ms = max(1000, int((deadline - monotonic()) * 1000))
            async with page.expect_response(
                lambda r: (
                    "batchCheckAsyncVideoGenerationStatus" in r.url and
                    r.request.method == "POST"
                ),
                timeout=remaining_ms,
            ) as response_info:
                pass

            response = await response_info.value
            payload = await response.json()
            if "result" in payload and "data" in payload["result"]:
                media_items = payload["result"]["data"].get("media", [])
            else:
                media_items = payload.get("media", [])

            tracked_items = [item for item in media_items if item.get("name") in media_names]
            if not tracked_items:
                # 顺便加点随机鼠标动作，防假死判定
                if random.random() < 0.2:
                    await human_mouse_move(page)
                continue

            last_statuses = {
                item["name"]: item.get("mediaMetadata", {})
                .get("mediaStatus", {})
                .get("mediaGenerationStatus", "UNKNOWN")
                for item in tracked_items
            }
            print(f"[Worker {worker_id}] 当前视频状态: {last_statuses}")

            if any(status == "MEDIA_GENERATION_STATUS_FAILED" for status in last_statuses.values()):
                raise RuntimeError(f"视频生成失败: {last_statuses}")

            if media_names.issubset(last_statuses.keys()) and all(
                status == "MEDIA_GENERATION_STATUS_SUCCESSFUL" for status in last_statuses.values()
            ):
                return {"media": tracked_items}
            
            # 等待过程中加入拟人化动作
            if random.random() < 0.3:
                await human_mouse_move(page)
                await human_scroll(page)

        raise TimeoutError(f"等待视频生成超时，最后一次状态: {last_statuses}")

    async def _wait_for_video_completion(self, page, media_name: str, worker_id: Any, timeout_ms: int) -> str:
        """等待视频生成完成并返回签名URL"""
        deadline = monotonic() + timeout_ms / 1000
        redirect_url = f"https://labs.google/fx/api/trpc/media.getMediaUrlRedirect?name={media_name}"

        print(f"[Worker {worker_id}] 等待视频 {media_name[:8]}... 生成完成")

        # 从页面获取user agent
        user_agent = await page.evaluate("() => navigator.userAgent")

        while monotonic() < deadline:
            try:
                # 尝试获取视频URL - 如果返回307带location，说明视频准备好了
                async with httpx.AsyncClient(timeout=10.0, follow_redirects=False) as client:
                    cookies = await page.context.cookies()
                    cookie_dict = {c["name"]: c["value"] for c in cookies}

                    response = await client.get(
                        redirect_url,
                        cookies=cookie_dict,
                        headers={
                            "User-Agent": user_agent,
                            "Referer": "https://labs.google/"
                        }
                    )

                    # 307状态码带Location头表示视频已就绪
                    if response.status_code == 307 and response.headers.get("location"):
                        video_url = response.headers["location"]
                        print(f"[Worker {worker_id}] 视频已就绪! URL: {video_url[:80]}...")
                        return video_url

                    # 其他状态码可能表示还在处理中
                    print(f"[Worker {worker_id}] 视频状态: {response.status_code}, 继续等待...")

            except Exception as e:
                print(f"[Worker {worker_id}] 轮询错误: {e}")

            # 下次轮询前等待
            await asyncio.sleep(10)

        raise TimeoutError(f"视频生成超时 ({timeout_ms/1000}秒后)")

    async def _capture_video_urls(self, page, media_name: str, worker_id: Any) -> tuple[str, str | None]:
        """获取视频重定向URL和CDN URL"""
        redirect_url = f"https://labs.google/fx/api/trpc/media.getMediaUrlRedirect?name={media_name}"
        try:
            user_agent = await page.evaluate("() => navigator.userAgent")
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=False) as client:
                cookies = await page.context.cookies()
                cookie_dict = {c["name"]: c["value"] for c in cookies}
                response = await client.get(
                    redirect_url,
                    cookies=cookie_dict,
                    headers={
                        "User-Agent": user_agent,
                        "Referer": "https://labs.google/",
                        "Accept": "*/*",
                    },
                )
                if response.status_code == 307:
                    return redirect_url, response.headers.get("location")
        except Exception as e:
            print(f"[Worker {worker_id}] 获取视频URL失败: {e}")
        return redirect_url, None

    async def _download_video(self, page, video_url: str, output_path: Path, worker_id: Any, cookies: list[dict]) -> bool:
        """下载视频"""
        print(f"[Worker {worker_id}] 模拟观看视频停留...")
        await human_delay(4.0, 8.0) # 假装人在看生成的视频，防下载过快被封
        await human_mouse_move(page)
        await human_scroll(page)
        
        try:
            user_agent = await page.evaluate("() => navigator.userAgent")
            cookie_dict = {c["name"]: c["value"] for c in cookies}
            headers = {
                "User-Agent": user_agent,
                "Referer": "https://labs.google/",
                "Accept": "video/webm,video/ogg,video/*;q=0.9,application/ogg;q=0.7,audio/*;q=0.6,*/*;q=0.5",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Sec-Fetch-Dest": "video",
                "Sec-Fetch-Mode": "no-cors",
                "Sec-Fetch-Site": "cross-site",
            }
            async with httpx.AsyncClient(timeout=120.0) as client:
                async with client.stream("GET", video_url, cookies=cookie_dict, headers=headers) as resp:
                    resp.raise_for_status()
                    with open(output_path, "wb") as f:
                        async for chunk in resp.aiter_bytes(): f.write(chunk)
            print(f"[Worker {worker_id}] 视频已下载: {output_path}")
            return True
        except Exception as e:
            print(f"[Worker {worker_id}] 下载失败: {e}")
            return False

    async def _prepare_video_project(self, page, worker, prompt: str, variant_count: int) -> None:
        """按图片脚本的节奏进入项目页，并补上视频模式和提示词。"""
        # 检查并关闭底部 Cookie 弹窗
        try:
            cookie_btn = page.locator('.glue-cookie-notification-bar button:has-text("Got it"), .glue-cookie-notification-bar button:has-text("同意"), .glue-cookie-notification-bar button:has-text("Accept"), .glue-cookie-notification-bar button').first
            if await cookie_btn.is_visible(timeout=2000):
                print(f"[Worker {worker.worker_id}] 发现 Cookie 弹窗，正在关闭...")
                await human_click(page, cookie_btn)
                await human_delay(1.0, 2.0)
        except Exception:
            pass

        # 点击“新建项目”
        print(f"[Worker {worker.worker_id}] 点击新建项目...")
        try:
            new_btn = page.locator('button:has-text("新建项目")')
            if await new_btn.is_visible(timeout=2000):
                await human_click(page, new_btn)
                await human_delay(1.0, 3.0)
        except Exception:
            pass

        # 确保视频模式和变体数量正确
        await ensure_video_mode(page, worker.worker_id, variant_count)

        # 等待输入框并输入提示词
        input_prompt = normalize_prompt_text(prompt)
        print(f"[Worker {worker.worker_id}] 等待输入框并输入提示词: {input_prompt[:10]}")
        await page.wait_for_selector('[role="textbox"]', state="visible", timeout=15000)
        await human_delay(1.5, 3.0)

        textbox_loc = page.locator('[role="textbox"]')
        await fill_textbox_with_validation(page, textbox_loc, input_prompt, worker.worker_id)
        await human_delay(1.0, 2.5)

    async def _submit_video_generation_humanized(self, page, worker_id: Any) -> tuple[str, dict[str, Any]]:
        """提交生成请求；如果首个响应没抓到，则等待页面自己的状态轮询。"""
        print(f"[Worker {worker_id}] 提交视频生成请求...")
        submit_btn = page.locator("button").filter(has_text=re.compile(r"arrow_forward", re.IGNORECASE)).filter(has_text="创建").first
        if not await submit_btn.is_visible(timeout=1500):
            submit_btn = page.locator('button:has-text("创建")').last

        if not await submit_btn.is_visible(timeout=5000):
            raise RuntimeError("找不到真实可点击的创建提交按钮")

        try:
            async with page.expect_response(
                lambda r: "batchAsyncGenerateVideoText" in r.url and r.request.method == "POST",
                timeout=60000
            ) as response_info:
                await human_click(page, submit_btn)
                await human_delay(0.5, 1.0)
                await human_mouse_move(page)

            response = await response_info.value
            return "submit", await response.json()
        except Exception as submit_exc:
            print(f"[Worker {worker_id}] 未捕获到首个生成响应: {submit_exc}，改为等待页面状态轮询...")

        async with page.expect_response(
            lambda r: "batchCheckAsyncVideoGenerationStatus" in r.url and r.request.method == "POST",
            timeout=180000
        ) as status_info:
            await human_delay(1.0, 2.0)
            await human_mouse_move(page)

        response = await status_info.value
        return "status", await response.json()

        raise RuntimeError("未捕获到首个生成响应，且未等到页面状态轮询")

    async def process_task(self, page, task_data: dict[str, Any], worker) -> dict[str, Any]:
        prompt = task_data.get(
            "prompt",
            (
                "Realistic medical education video in a clean clinical examination room. "
                "A middle-aged American male doctor with short curly hair wears a white lab coat "
                "over green surgical scrubs and blue medical gloves. He stands beside an "
                "examination bed and explains the anatomical location of the prostate to a male "
                "patient, pointing to the lower abdominal area and an anatomical chart for "
                "reference. The patient lies on his side in a standard medical examination "
                "posture, appropriately draped in blue examination garments for a professional "
                "clinical demonstration. Medium shot, stable camera, slight forward push during "
                "the explanation, bright hospital lighting, blue privacy curtains in the "
                "background, highly realistic medical documentary style, non-sexual, educational, "
                "professional."
            ),
        )
        variant_count = int(task_data.get("variant_count", 1))
        poll_timeout_ms = int(task_data.get("poll_timeout_ms", 8 * 60 * 1000))

        # 1. 访问 Google Flow 首页
        print(f"[Worker {worker.worker_id}] 正在访问 Flow 首页...")
        await page.goto(FLOW_HOME_URL, wait_until="domcontentloaded")
        await human_delay(2.0, 4.0)
        await human_mouse_move(page)

        await self._prepare_video_project(page, worker, prompt, variant_count)

        media_name = None
        project_id = None

        response_kind, response_payload = await self._submit_video_generation_humanized(page, worker.worker_id)

        try:
            print(f"[Worker {worker.worker_id}] 收到{response_kind}响应")

            if "result" in response_payload and "data" in response_payload["result"]:
                media_items = response_payload["result"]["data"].get("media", [])
            else:
                media_items = response_payload.get("media", [])

            if media_items:
                primary_media_item = media_items[0] if response_kind == "project_recovery" else media_items[0]
                media_name = primary_media_item.get("name")
                project_id = primary_media_item.get("projectId")
                print(f"[Worker {worker.worker_id}] Media name: {media_name}, Project ID: {project_id}")
            else:
                print(f"[Worker {worker.worker_id}] 响应结构: {list(response_payload.keys())}")
        except Exception as e:
            print(f"[Worker {worker.worker_id}] 捕获media name失败: {e}")

        if not media_name:
            raise RuntimeError("无法从视频生成响应中获取media name")

        await human_mouse_move(page)
        await asyncio.sleep(2)

        # 5. 等待视频生成完成
        print(f"[Worker {worker.worker_id}] 等待视频生成 (可能需要4-8分钟)...")
        final_status_payload = await self._wait_for_video_status(
            page=page,
            media_names={media_name},
            worker_id=worker.worker_id,
            timeout_ms=poll_timeout_ms,
        )
        final_media_items = final_status_payload.get("media", [])
        final_media_by_name = {item["name"]: item for item in final_media_items if item.get("name")}
        primary_media = final_media_by_name.get(media_name, {"name": media_name, "projectId": project_id})
        project_id = primary_media.get("projectId") or project_id

        _, video_url = await self._capture_video_urls(page, primary_media["name"], worker.worker_id)
        if not video_url:
            raise RuntimeError("视频状态已成功，但未能获取最终视频下载地址")

        # 6. 下载视频
        local_path = None
        if video_url:
            output_dir = Path("d:/kuanghu-poc/flow/videos")
            output_dir.mkdir(parents=True, exist_ok=True)
            safe_prompt = re.sub(r'[<>:"/\\|?*]', '_', prompt[:30])
            local_path = output_dir / f"{safe_prompt}_{primary_media['name'][:8]}.mp4"
            cookies = await page.context.cookies()
            if await self._download_video(page, video_url, local_path, worker.worker_id, cookies):
                print(f"[Worker {worker.worker_id}] 视频已保存: {local_path}")
            else:
                local_path = None
                print(f"[Worker {worker.worker_id}] 下载视频失败, URL是: {video_url}")

        screenshot_path = Path(f"d:/kuanghu-poc/flow/v2_video_result_{worker.worker_id}.png")
        await page.screenshot(path=screenshot_path)

        return {
            "status": "success",
            "prompt": prompt,
            "project_id": project_id,
            "video_url": video_url,
            "local_video_path": str(local_path) if local_path else None,
            "screenshot": str(screenshot_path)
        }

async def main():
    cookies_path = Path(r"D:\kuanghu-poc\flow\flow_cookie\EnnalscrAvey@gmail.com.txt")
    if not cookies_path.exists():
        return print("Cookie文件不存在!")

    scraper = GoogleFlowVideoScraperV2(
        browser_pool_size=1,
        headless=False,
        extra_flags=["--start-maximized"],
        default_cookies=cookies_path,
        default_cookie_domain="labs.google",
        task_timeout_ms=12 * 60 * 1000
    )

    medical_prompt = """
Realistic medical education video in a clean clinical examination room.

Subject:
A 50-year-old American male doctor with short curly hair wears a white lab coat over green surgical scrubs and blue medical gloves. He looks professional, clean, and calm, with a natural smile.

Scene:
A hospital consultation room with blue medical privacy curtains, bright and clean lighting, and a strong clinical atmosphere. An anatomical chart stands beside the bed.

Action:
The doctor stands beside the examination bed and explains the anatomical location of the prostate to the patient. He points to the lower abdominal area and to the anatomical chart for reference. A 45-55-year-old male patient lies on his side facing the doctor in a standard medical examination posture, appropriately draped in loose blue examination garments for a professional clinical demonstration.

Camera:
Medium shot including the doctor upper body and part of the patient position for anatomical explanation. Stable camera with a slight forward push while the doctor is speaking.

Style:
Highly realistic, medical educational tone, natural skin tone, visible glove texture, soft indoor lighting, detailed curtain folds, no cartoon style, no exaggerated filters, non-sexual, professional.
""".strip()

    tasks = [{"prompt": medical_prompt, "variant_count": 1}]
    async with scraper:
        results = await scraper.run_tasks(tasks)
        for res in results:
            if isinstance(res, Exception):
                print(f"任务失败: {res}")
            else:
                print(f"任务成功: {res.get('local_video_path')}")

if __name__ == "__main__":
    asyncio.run(main())
