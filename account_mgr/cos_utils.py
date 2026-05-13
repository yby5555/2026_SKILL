"""
腾讯云 COS 上传工具

依赖：cos-python-sdk-v5
    pip install cos-python-sdk-v5
"""

import re
import time
from pathlib import Path

from qcloud_cos import CosConfig, CosS3Client, CosClientError, CosServiceError

import logging
import logging.handlers
import sys
from pathlib import Path

_log_dir = Path(__file__).resolve().parent.parent / "video_processing" / "log"
_log_dir.mkdir(parents=True, exist_ok=True)
_log_file = _log_dir / "automation_video.log"

logger = logging.getLogger("COS")
logger.setLevel(logging.INFO)
# 防止重复添加 Handler 导致日志重复
if not logger.handlers:
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    # 只写入文件，不在终端打印
    fh = logging.handlers.RotatingFileHandler(_log_file, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    # 移除向上传播，避免被 root logger 捕获后再次打印
    logger.propagate = False


from account_mgr.config import (
    COS_SECRET_ID,
    COS_SECRET_KEY,
    COS_REGION,
    COS_BUCKET,
    COS_APPID,
    COS_VIDEO_PREFIX,
    COS_CUSTOM_DOMAIN,
)

# 延迟初始化，避免模块加载时就校验凭证
_client: CosS3Client | None = None


def _get_client() -> CosS3Client:
    global _client
    if _client is None:
        config = CosConfig(
            Region=COS_REGION,
            SecretId=COS_SECRET_ID,
            SecretKey=COS_SECRET_KEY,
            Scheme="https",
            Timeout=300  # 增加 COS 上传/下载的基础超时时间，默认为 60s，这里改为 600s (10分钟)
        )
        _client = CosS3Client(config)
    return _client


def upload_file_to_cos(local_path: str | Path, cos_key: str | None = None) -> str:
    """
    上传本地文件到 COS，返回可访问的 URL。

    - 使用高级分块上传接口（upload_file），自动断点续传
    - 失败自动重试最多 10 次

    Args:
        local_path: 本地文件路径
        cos_key:    COS 对象键（不含前缀），留空则使用文件名

    Returns:
        上传后的 COS 访问 URL

    Raises:
        RuntimeError: 10 次重试全部失败时抛出
    """
    local_path = Path(local_path)
    if cos_key is None:
        cos_key = local_path.name

    full_key = f"{COS_VIDEO_PREFIX.rstrip('/')}/{cos_key}"
    full_bucket = f"{COS_BUCKET}-{COS_APPID}"  # 动态拼接，实时读取 config
    client = _get_client()

    start_time = time.time()
    last_error = None

    for attempt in range(10):
        try:
            client.upload_file(
                Bucket=full_bucket,
                Key=full_key,
                LocalFilePath=str(local_path),
            )
            elapsed = int(time.time() - start_time)
            logger.info(f"[COS] 上传成功，耗时 {elapsed}s，Key: {full_key}")
            break  # 成功则跳出重试循环
        except CosClientError as e:
            last_error = e
            logger.info(f"[COS] 第 {attempt + 1} 次重试，CosClientError: {e}")
        except CosServiceError as e:
            last_error = e
            logger.info(f"[COS] 第 {attempt + 1} 次重试，CosServiceError: {e}")
        except Exception as e:
            last_error = e
            logger.info(f"[COS] 第 {attempt + 1} 次重试，未知错误: {e}")
    else:
        raise RuntimeError(f"COS 上传失败，已重试 10 次，最后一次错误: {last_error}")

    # 拼接访问 URL
    if COS_CUSTOM_DOMAIN:
        base = COS_CUSTOM_DOMAIN.rstrip("/")
    else:
        base = f"https://{COS_BUCKET}.cos.{COS_REGION}.myqcloud.com"

    return f"{base}/{full_key}"

def upload_bytes_to_cos(file_bytes: bytes, cos_key: str) -> str:
    """
    直接将内存中的字节流上传到 COS，返回可访问的 URL。
    
    Args:
        file_bytes: 视频的字节流数据
        cos_key:    COS 对象键（含扩展名，如 'abc.mp4'）
        
    Returns:
        上传后的 COS 访问 URL
    """
    full_key = f"{COS_VIDEO_PREFIX.rstrip('/')}/{cos_key}"
    full_bucket = f"{COS_BUCKET}-{COS_APPID}"
    client = _get_client()

    start_time = time.time()
    last_error = None

    for attempt in range(3):  # 内存直传重试3次即可
        try:
            client.put_object(
                Bucket=full_bucket,
                Body=file_bytes,
                Key=full_key,
            )
            elapsed = int(time.time() - start_time)
            logger.info(f"[COS] 内存直传成功，耗时 {elapsed}s，Key: {full_key}")
            break
        except Exception as e:
            last_error = e
            logger.info(f"[COS] 内存直传第 {attempt + 1} 次重试，错误: {e}")
            time.sleep(2)  # 失败后短暂等待再重试
    else:
        raise RuntimeError(f"COS 内存直传失败，已重试 3 次，最后一次错误: {last_error}")

    if COS_CUSTOM_DOMAIN:
        base = COS_CUSTOM_DOMAIN.rstrip("/")
    else:
        base = f"https://{COS_BUCKET}.cos.{COS_REGION}.myqcloud.com"

    return f"{base}/{full_key}"


def get_presigned_url(cos_key: str, expire_time: int = 3600) -> str | None:
    """
    生成 COS 对象的预签名下载 URL（带时效，私有 Bucket 场景下使用）。

    Args:
        cos_key:     COS 对象的完整 Key（含路径，如 video_processing/videos/xxx.mp4）
        expire_time: 链接有效期，单位秒，默认 3600（1 小时）

    Returns:
        预签名 URL 字符串，失败时返回 None
    """
    full_bucket = f"{COS_BUCKET}-{COS_APPID}"
    client = _get_client()
    try:
        url = client.get_presigned_url(
            Method="GET",
            Bucket=full_bucket,
            Key=cos_key,
            Expired=expire_time,
        )
        return url
    except Exception as e:
        logger.info(f"[COS] 生成预签名链接失败: {e}")
        return None


_COS_URL_PATTERN = re.compile(r"^https://([^.]+-\d+)\.cos\.([^.]+)\.myqcloud\.com/(.+?)(\?|$)")


def is_cos_url(url: str) -> bool:
    return bool(_COS_URL_PATTERN.match(url))


def download_cos_image(url: str) -> bytes:
    m = _COS_URL_PATTERN.match(url)
    if not m:
        raise ValueError(f"不是合法的 COS URL: {url[:80]}...")

    bucket, region, key, _ = m.groups()
    client = _get_client()

    for attempt in range(3):
        try:
            response = client.get_object(Bucket=bucket, Key=key)
            data = response["Body"].get_raw_stream().read()
            logger.info(f"[COS] 下载成功: {key} ({len(data)} bytes)")
            return data
        except Exception as exc:
            logger.warning(f"[COS] 下载第 {attempt + 1} 次失败: {key}, {exc}")
            if attempt < 2:
                time.sleep(1)
    raise RuntimeError(f"COS 下载失败，已重试 3 次: {key}")
