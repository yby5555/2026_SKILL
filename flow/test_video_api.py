"""
test_video_api.py
=================
Flow 视频生成 API 集成测试。

前提条件：
  1. video_api_server.py 已启动（python video_api_server.py）
  2. Redis 有活跃 Cookie（login_scheduler.py 已运行）
  3. MongoDB 可连接

用法：
  # 运行所有测试
  python test_video_api.py

  # 跳过实际生成（只测接口可用性）
  python test_video_api.py --skip-generate
"""

import argparse
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

BASE_URL = "http://localhost:8000"


# ═══════════════════════════════════════════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════════════════════════════════════════

class Colors:
    GREEN  = "\033[92m"
    RED    = "\033[91m"
    YELLOW = "\033[93m"
    RESET  = "\033[0m"


def ok(msg):  print(f"  {Colors.GREEN}✓{Colors.RESET} {msg}")
def fail(msg): print(f"  {Colors.RED}✗{Colors.RESET} {msg}"); sys.exit(1)
def warn(msg): print(f"  {Colors.YELLOW}~{Colors.RESET} {msg}")
def section(title): print(f"\n{'─'*50}\n  {title}\n{'─'*50}")


def get(path, **kwargs):
    return requests.get(f"{BASE_URL}{path}", timeout=10, **kwargs)


def post(path, json=None, **kwargs):
    return requests.post(f"{BASE_URL}{path}", json=json, timeout=300, **kwargs)


# ═══════════════════════════════════════════════════════════════════════════════
# 测试用例
# ═══════════════════════════════════════════════════════════════════════════════

def test_live():
    section("GET /live — 存活探针")
    r = get("/live")
    assert r.status_code == 200, f"期望 200，实际 {r.status_code}"
    assert r.json().get("status") == "alive"
    ok(f"status=alive  (HTTP {r.status_code})")


def test_ready():
    section("GET /ready — 就绪探针")
    r = get("/ready")
    assert r.status_code in (200, 503), f"期望 200/503，实际 {r.status_code}"
    if r.status_code == 200:
        ok("Scraper 已就绪")
    else:
        warn("Scraper 未就绪（服务可能还在初始化）")


def test_health_detail():
    section("GET /health/detail — 详细业务状态")
    r = get("/health/detail")
    assert r.status_code == 200, f"期望 200，实际 {r.status_code}"
    data = r.json()

    assert "scraper_ready"    in data
    assert "max_concurrent"   in data
    assert "cookie_pool"      in data

    pool = data["cookie_pool"]
    assert "active_count"  in pool
    assert "expired_count" in pool
    assert "total"         in pool

    # 确认不包含邮箱列表
    assert "active" not in data
    assert "active" not in pool

    ok(f"scraper_ready={data['scraper_ready']}  "
       f"active={pool['active_count']}  expired={pool['expired_count']}")

    if pool["active_count"] == 0:
        warn("Cookie Pool 没有活跃账号，generate_video 测试将失败")


def test_pool():
    section("GET /pool — Cookie Pool 聚合状态")
    r = get("/pool")
    assert r.status_code == 200, f"期望 200，实际 {r.status_code}"
    data = r.json()

    assert "active_count"  in data
    assert "expired_count" in data
    assert "total"         in data

    # 确认不包含邮箱列表（安全要求）
    for key in ("active", "expired"):
        assert key not in data, f"响应中不应出现 '{key}' 邮箱列表"

    ok(f"active={data['active_count']}  expired={data['expired_count']}  total={data['total']}")


def test_generate_video_bad_request():
    section("POST /generate_video — 参数校验（缺少必填字段）")

    # 缺少 id
    r = post("/generate_video", json={"prompt": "test"})
    assert r.status_code == 422, f"缺少 id 时期望 422，实际 {r.status_code}"
    ok("缺少 id → 422 Unprocessable Entity")

    # 缺少 prompt
    r = post("/generate_video", json={"id": "test-001"})
    assert r.status_code == 422, f"缺少 prompt 时期望 422，实际 {r.status_code}"
    ok("缺少 prompt → 422 Unprocessable Entity")

    # prompt 为空字符串
    r = post("/generate_video", json={"id": "test-002", "prompt": ""})
    assert r.status_code == 422, f"prompt 为空时期望 422，实际 {r.status_code}"
    ok("prompt='' → 422 Unprocessable Entity")


def test_generate_video_success():
    section("POST /generate_video — 批量并发生成测试（20 个词，6 并发）")

    # 准备 20 个测试词
    base_prompts = [
        "生成猪追狗的视频",
        "一个机器人在赛博朋克城市里跳舞",
        "一只穿着宇航服的猫在火星漫步",
        "一辆老爷车在夕阳下的海岸线疾驰",
        "一条巨龙在天空中盘旋",
        "深海中发光的水母群",
        "森林里的小精灵在采集露水",
        "古代武士在雪中练剑",
        # "一艘帆船在暴风雨中航行",
        # "一只鹰在悬崖边展翅高飞",
        # "现代都市里飞行的汽车",
        # "一个老爷爷在湖边钓鱼",
        # "小女孩在花海中追逐蝴蝶",
        # "夜晚霓虹灯下的咖啡馆",
        # "两只老虎在草原上奔跑",
        # "一束光穿透黑暗的洞穴",
        # "巨大的瀑布从云端落下",
        # "一群企鹅在冰面上滑行",
        # "太空站里的宇航员在维修设备",
        # "一个魔法师在施展绚丽的法术"
    ]

    tasks = []
    for i, prompt in enumerate(base_prompts):
        tasks.append({
            "id": f"test-batch-{i+1:02d}-{uuid.uuid4().hex[:6]}",
            "prompt": prompt
        })

    print(f"  准备提交 {len(tasks)} 个生成任务，最大并发: 6...")

    def submit_task(payload):
        task_id = payload["id"]
        prompt = payload["prompt"]
        t0 = time.time()
        print(f"  [Task {task_id}] 开始提交: {prompt[:15]}...")
        
        try:
            r = post("/generate_video", json=payload)
            elapsed = time.time() - t0
            
            if r.status_code != 200:
                return {"id": task_id, "status": "http_error", "code": r.status_code, "msg": r.text, "elapsed": elapsed}
            
            data = r.json()
            result = data.get("results", [{}])[0]
            
            return {
                "id": task_id,
                "status": result.get("status"),
                "video_url": result.get("video_url"),
                "local_video_path": result.get("local_video_path"),
                "error": result.get("error"),
                "elapsed": elapsed
            }
        except Exception as e:
            elapsed = time.time() - t0
            return {"id": task_id, "status": "exception", "error": str(e), "elapsed": elapsed}

    # 使用 6 并发执行
    results = []
    success_count = 0
    failed_count = 0

    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_task = {executor.submit(submit_task, task): task for task in tasks}
        for future in as_completed(future_to_task):
            res = future.result()
            results.append(res)
            
            status = res.get("status")
            elapsed = res.get("elapsed", 0)
            task_id = res.get("id")
            
            if status == "success":
                ok(f"[Task {task_id}] 成功 ({elapsed:.1f}s) - {res.get('video_url')}")
                success_count += 1
            elif status == "partial_success":
                warn(f"[Task {task_id}] 部分成功 ({elapsed:.1f}s) - {res.get('video_url')}")
                success_count += 1
            else:
                fail_msg = f"[Task {task_id}] 失败 ({elapsed:.1f}s) - {res.get('error') or res.get('msg')}"
                print(f"  {Colors.RED}✗{Colors.RESET} {fail_msg}")
                failed_count += 1

    print(f"\n  批量测试完成: 成功 {success_count}, 失败 {failed_count}")
    assert failed_count == 0, f"有 {failed_count} 个任务生成失败"


def test_idempotency():
    section("POST /generate_video — 幂等性（相同 id 重复提交）")

    task_id = f"{uuid.uuid4().hex[:8]}"
    payload = {"id": task_id, "prompt": "生成一个狗被猪追的视频"}

    # 第一次（实际执行）
    r1 = post("/generate_video", json=payload)
    assert r1.status_code == 200, f"第一次提交失败: {r1.status_code}"
    ok(f"第一次提交成功  status={r1.json()['results'][0]['status']}")

    # 第二次（相同 id）—— MongoDB 不会覆盖旧记录
    r2 = post("/generate_video", json=payload)
    assert r2.status_code == 200, f"第二次提交失败: {r2.status_code}"
    ok("第二次提交相同 id 无报错（幂等）")


# ═══════════════════════════════════════════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Flow 视频 API 测试")
    parser.add_argument("--skip-generate", action="store_true",
                        help="跳过实际视频生成测试（只测接口可用性）")
    parser.add_argument("--url", default="http://localhost:8000",
                        help="API 服务地址（默认 http://localhost:8000）")
    args = parser.parse_args()

    global BASE_URL
    BASE_URL = args.url.rstrip("/")

    print(f"\n[START] 开始测试  目标: {BASE_URL}")

    # 先检查服务是否可达
    try:
        requests.get(f"{BASE_URL}/live", timeout=5)
    except requests.exceptions.ConnectionError:
        print(f"\n{Colors.RED}✗ 无法连接 {BASE_URL}，请先启动 video_api_server.py{Colors.RESET}")
        sys.exit(1)

    passed = 0
    failed = 0

    test_cases = []

    if not args.skip_generate:
        test_cases.append(test_generate_video_success)
        # 幂等性测试也需要生成，耗时较长，默认跳过
        # test_cases.append(test_idempotency)

    for tc in test_cases:
        try:
            tc()
            passed += 1
        except AssertionError as e:
            print(f"  {Colors.RED}✗ 断言失败: {e}{Colors.RESET}")
            failed += 1
        except Exception as e:
            print(f"  {Colors.RED}✗ 异常: {e}{Colors.RESET}")
            failed += 1

    print(f"\n{'═'*50}")
    print(f"  结果: {Colors.GREEN}{passed} 通过{Colors.RESET}  "
          f"{Colors.RED if failed else ''}{failed} 失败{Colors.RESET}")
    print(f"{'═'*50}\n")

    if args.skip_generate:
        print(f"  {Colors.YELLOW}提示：使用 --skip-generate，已跳过视频生成测试{Colors.RESET}\n")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
