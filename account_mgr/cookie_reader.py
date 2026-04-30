"""
Cookie 消费工具 - 从 Redis 中以 Round-Robin 方式获取可用的 Flow Cookie

用法：
    from cookie_reader import get_next_cookie, get_pool_status

    # 获取下一个可用 Cookie
    result = get_next_cookie()
    if result:
        email, cookies = result
        # cookies 可直接传给 Playwright: await context.add_cookies(cookies)

    # 查看 Pool 状态
    status = get_pool_status()
    print(status)  # {"pool_size": 3, "active": [...], "expired": [...]}
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

# 直接从 redis_utils 导出，保持接口一致
from redis_utils import get_next_cookie, get_pool_status, remove_from_pool  # noqa: F401


if __name__ == "__main__":
    # 直接运行时打印当前 Pool 状态
    print("=" * 50)
    print("Cookie Pool 状态")
    print("=" * 50)
    try:
        status = get_pool_status()
        print(f"队列总数 : {status['pool_size']}")
        print(f"有效账号 : {len(status['active'])} 个")
        for e in status["active"]:
            print(f"  ✓ {e}")
        print(f"过期账号 : {len(status['expired'])} 个")
        for e in status["expired"]:
            print(f"  ✗ {e}")
    except Exception as ex:
        print(f"连接 Redis 失败: {ex}")
