#!/usr/bin/env python3
"""临时脚本：清空队列并立即运行审计测试"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

def main():
    # 步骤1: 清空Redis队列
    print("正在清空Redis队列...")
    subprocess.run(
        [sys.executable, "-c",
         "from video_processing.utils.task_common import create_redis_client; "
         "r = create_redis_client(); "
         "r.delete('task_create_queue', 'task_create_processing_queue'); "
         "print('队列已清空')"],
        cwd=str(ROOT),
        check=True
    )

    # 步骤2: 立即运行审计脚本
    print("启动审计测试...")
    result = subprocess.run(
        [sys.executable, "video_processing/consumers/run_40_task_audit.py",
         "--timeout-seconds", "14400",
         "--poll-interval-seconds", "20",
         "--timezone-id", "Asia/Shanghai",
         "--locale", "zh-CN"],
        cwd=str(ROOT)
    )
    return result.returncode

if __name__ == "__main__":
    raise SystemExit(main())