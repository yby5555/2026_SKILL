#!/usr/bin/env python3
"""监控Redis队列状态，找出任务来源"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import time
from video_processing.utils.task_common import create_redis_client

def main():
    r = create_redis_client()
    print("开始监控Redis队列（按Ctrl+C停止）...")
    print("时间                    队列任务数  处理中任务数")

    try:
        for i in range(10):  # 监控10次，每次间隔3秒
            queue_count = r.zcard("task_create_queue")
            processing_count = r.zcard("task_create_processing_queue")
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}  {queue_count:10d}  {processing_count:12d}")

            if queue_count > 0:
                # 获取队列中的前5个任务看看
                tasks = r.zrange("task_create_queue", 0, 4, withscores=True)
                print(f"  队列中的任务样例:")
                for task, score in tasks:
                    print(f"    - {task[:100]}... (score: {score})")

            if i < 9:  # 不是最后一次
                time.sleep(3)
    except KeyboardInterrupt:
        print("\n监控已停止")

if __name__ == "__main__":
    main()