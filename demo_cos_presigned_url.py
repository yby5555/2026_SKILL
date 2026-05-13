from __future__ import annotations

from urllib.parse import urlparse, unquote

from account_mgr.cos_utils import get_presigned_url


DEFAULT_SUFFIX = ".mp4"
DEFAULT_EXPIRE_SECONDS = 24 * 60 * 60


def build_object_key(task_id: str, suffix: str = DEFAULT_SUFFIX) -> str:
    task_id = str(task_id).strip()
    if not task_id:
        raise ValueError("task_id 不能为空")
    return f"video_processing/videos/{task_id}{suffix}"


def normalize_object_key(value: str) -> str:
    raw = str(value).strip()
    if not raw:
        raise ValueError("输入不能为空")

    if raw.startswith(("http://", "https://")):
        parsed = urlparse(raw)
        path = unquote(parsed.path.lstrip("/"))
        if not path:
            raise ValueError(f"无法从链接中提取对象 key: {raw}")
        return path

    if "/" in raw:
        return raw.lstrip("/")

    return build_object_key(raw)


def create_preview_url(value: str, expire_seconds: int = DEFAULT_EXPIRE_SECONDS) -> str:
    object_key = normalize_object_key(value)
    signed_url = get_presigned_url(object_key, expire_time=expire_seconds)
    if not signed_url:
        raise RuntimeError(f"生成预签名链接失败: {object_key}")
    return signed_url


def main() -> None:
    # 三种输入方式都支持：
    # 1. task_id: "065e7d24-fd62-4bff-901c-fc37102628a2"
    # 2. object key: "video_processing/videos/065e7d24-fd62-4bff-901c-fc37102628a2.mp4"
    # 3. 现有 cos_url: "https://xxx/video_processing/videos/065e7d24-fd62-4bff-901c-fc37102628a2.mp4"
    sample_value = "65ae7be9-f737-461f-9e8b-0296a22486b4"
    'https://1392049403.cos.ap-guangzhou.myqcloud.com/video_processing/videos/72a1e4eb-019a-40f4-b438-16c25134fa2f.mp4'
# 'https://1392049403.cos.ap-guangzhou.myqcloud.com/video_processing/videos/fe3e401f-c899-4cc3-a1d9-607085090d79.mp4'
# 'https://1392049403.cos.ap-guangzhou.myqcloud.com/video_processing/videos/f2e32c0c-7458-409a-a169-136ddd526a86.mp4'
    preview_url = create_preview_url(sample_value)
    print("输入值:", sample_value)
    print("对象 key:", normalize_object_key(sample_value))
    print("预签名链接:")
    print(preview_url)


if __name__ == "__main__":
    main()
