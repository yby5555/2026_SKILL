#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base64
import mimetypes
import time

import requests

API_HOST = "http://172.16.101.32:9898/internal_api"
TIMEOUT = 15


def file_to_base64(file_path):
    """本地图片转 data:image/...;base64,... 格式"""
    with open(file_path, "rb") as f:
        raw = f.read()
    mime, _ = mimetypes.guess_type(file_path)
    if not mime:
        mime = "image/jpeg"
    b64 = base64.b64encode(raw).decode("utf-8")
    return f"data:{mime};base64,{b64}"


# ============================================================
#  创建任务
# ============================================================
def create_task(prompt, image=None, image_type=None,
                task_type=1, gen_type=None, model_type=None,
                priority=None, proportion=None, video_time=None):
    """
    参数:
        prompt:      提示词（必填）
        image:       图片列表（可选），如 ["base64_1", "base64_2"]
        image_type:  图片类型，'url' 或 'base64'（可选）
        task_type:   任务类型，0=图片, 1=视频（默认 1）
        gen_type:    生成视频类型，0=帧, 1=素材（可选）
        model_type:  生成视频模型，0=lite, 1=fast（可选）
        priority:    优先级，正整数（可选）
        proportion:  宽高比，0=9:16, 1=16:9（可选，默认 0 即 9:16）
        video_time:  视频时长，4/6/8 秒（可选，默认 8）
    """
    url = API_HOST.rstrip("/") + "/daas/veo/task/create"

    body = {"prompt": prompt, "type": task_type}
    if image is not None:
        body["image"] = image
    if image_type is not None:
        body["image_type"] = image_type
    if gen_type is not None:
        body["gen_type"] = gen_type
    if model_type is not None:
        body["model_type"] = model_type
    if priority is not None:
        body["priority"] = priority
    if proportion is not None:
        body["proportion"] = proportion
    if video_time is not None:
        body["video_time"] = video_time

    resp = requests.post(url, json=body, timeout=TIMEOUT)
    return resp.json()


# ============================================================
#  查询任务状态
# ============================================================
def query_status(task_id):
    """
    参数:
        task_id: 任务 ID（必填）
    """
    url = API_HOST.rstrip("/") + "/daas/veo/task/status"
    resp = requests.get(url, params={"task_id": task_id}, timeout=TIMEOUT)
    return resp.json()


# ============================================================
#  完整流程：创建 → 轮询 → 获取结果
# ============================================================
def main():
    image1 = file_to_base64(r"D:\2026_SKILL\flow\生成一张赛博朋克的孙悟空_2K_202605091728.jpeg")
    image2 = file_to_base64(r"D:\2026_SKILL\flow\生成一张赛博朋克的猪八戒_2K_202605091728.jpeg")
    """
    参数:
        prompt:      提示词（必填）
        image:       图片列表（可选），如 ["base64_1", "base64_2"]
        image_type:  图片类型，'url' 或 'base64'（可选）
        task_type:   任务类型，0=图片, 1=视频（默认 1）
        gen_type:    生成视频类型，0=帧, 1=素材（可选）
        model_type:  生成视频模型，0=lite, 1=fast（可选）
        priority:    优先级，正整数（可选）
        proportion:  宽高比，0=9:16, 1=16:9（可选，默认 0 即 9:16）
        video_time:  视频时长，4/6/8 秒（可选，默认 8）
    """
    result = create_task(
        prompt="生成一个猫追老鼠的视频",
        # image=[image1,image2],
        # image_type="base64",
        task_type=1,
        gen_type=0,
        model_type=0,
        priority=10,
        proportion=0,
        video_time=8,
    )

    if result.get("code") != 200:
        print("创建失败:", result)
        return

    task_id = result["data"]["task_id"]
    print(f"任务已创建，task_id = {task_id}")

    for i in range(1, 11):
        time.sleep(5)
        status = query_status(task_id)

        if status.get("code") != 200:
            print(f"查询失败: {status}")
            continue

        data = status["data"]
        task_status = data.get("task_status", "")

        if task_status == "completed":
            print(f"任务完成！COS 地址: {data.get('cos_url')}")
            print(f"COS Key: {data.get('cos_key')}")
            print(f"文件大小: {data.get('filesize')} KB")
            print(f"视频时长: {data.get('video_time')} 秒")
            break
        elif task_status == "failed":
            print(f"任务失败，错误信息: {data.get('error_msg')}")
            break
        else:
            print(f"第 {i} 次查询，当前状态: {data.get('msg')} ({task_status})")


if __name__ == "__main__":
    main()