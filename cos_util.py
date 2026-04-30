# path: app/utils/common/tos_util.py

import time

# noinspection PyPackageRequirements
from qcloud_cos import CosConfig, CosS3Client, CosClientError, CosServiceError

# from app.utils.common.daily_logger_manager import LoggerManager
# from app.utils.common.env_util import EnvUtil
# from app.utils.common.file_util import create_path_auto, check_file_exist


def upload_to_cos(file_path, cos_key) -> bool:
    logger = LoggerManager(logger_name='COS')
    # 从环境变量获取 AK 和 SK 信息。
    secret_id = EnvUtil.get_env('COS_SECRET_ID')
    secret_key = EnvUtil.get_env('COS_SECRET_KEY')
    cos_cdn_domain = EnvUtil.get_env('COS_CDN_DOMAIN')
    region = EnvUtil.get_env('COS_REGION')
    bucket_name = f"{EnvUtil.get_env('COS_BUCKET')}-{EnvUtil.get_env('COS_APPID')}"
    token = None
    scheme = 'https'
    start_time = time.time()

    # logger.write_to_log(f'开始上传文件 {file_path} 到 COS, bucket_name: {bucket_name}, cos_key: {cos_key}')
    # return True

    # 创建 CosS3Client 对象，对桶和对象的操作都通过 CosS3Client 实现
    config = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key, Token=token, Scheme=scheme)
    client = CosS3Client(config)

    if not check_file_exist(file_path):
        logger.write_to_error_log(f'需要上传的文件不存在, file_path: {file_path}')
        return False

    try:
        # 使用高级接口断点续传，失败重试时不会上传已成功的分块(这里重试10次)
        for i in range(0, 10):
            try:
                response = client.upload_file(
                    Bucket=bucket_name,
                    Key=cos_key,
                    LocalFilePath=file_path)
                break
            except CosClientError as e:
                logger.write_to_error_log(f'上传到COS 发生错误第{i}次, CosClientError: {e}')
            except CosServiceError as e:
                logger.write_to_error_log(f'上传到COS 发生错误第{i}次, CosServiceError: {e}')
            except Exception as e:
                logger.write_to_error_log(f'上传到COS 发生错误第{i}次, unknown error: {e}')
        end_time = time.time()
        logger.write_to_log(f'COS 上传用时 {int(end_time - start_time)}s')
        return True
    except Exception as e:
        logger.write_to_error_log('fail with unknown error: {}'.format(e))
        return False


async def async_upload_to_cos(file_path, cos_key, forch_upload=False) -> bool:
    logger = LoggerManager(logger_name='COS')

    if not check_file_exist(file_path):
        logger.write_to_error_log(f'需要上传的文件不存在, file_path: {file_path}')
        return False
    # 从环境变量获取 AK 和 SK 信息。
    secret_id = EnvUtil.get_env('COS_SECRET_ID')
    secret_key = EnvUtil.get_env('COS_SECRET_KEY')
    cos_cdn_domain = EnvUtil.get_env('COS_CDN_DOMAIN')
    region = EnvUtil.get_env('COS_REGION')
    bucket_name = f"{EnvUtil.get_env('COS_BUCKET')}-{EnvUtil.get_env('COS_APPID')}"
    token = None
    scheme = 'https'
    start_time = time.time()

    # 创建 CosS3Client 对象，对桶和对象的操作都通过 CosS3Client 实现
    config = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key, Token=token, Scheme=scheme)
    client = CosS3Client(config)
    try:
        # 使用高级接口断点续传，失败重试时不会上传已成功的分块(这里重试10次)
        for i in range(0, 10):
            try:
                response = client.upload_file(
                    Bucket=bucket_name,
                    Key=cos_key,
                    LocalFilePath=file_path)
                break
            except CosClientError as e:
                logger.write_to_error_log(f'上传到COS 发生错误第{i}次, CosClientError: {e}')
            except CosServiceError as e:
                logger.write_to_error_log(f'上传到COS 发生错误第{i}次, CosServiceError: {e}')
            except Exception as e:
                logger.write_to_error_log(f'上传到COS 发生错误第{i}次, unknown error: {e}')
        end_time = time.time()
        logger.write_to_log(f'COS 上传用时 {int(end_time - start_time)}s')
        return True
    except Exception as e:
        logger.write_to_error_log('fail with unknown error: {}'.format(e))
        return False


def download_from_cos(file_path, cos_key):
    logger = LoggerManager(logger_name='COS')
    create_path_auto(file_path)
    # 从环境变量获取 AK 和 SK 信息。
    secret_id = EnvUtil.get_env('COS_SECRET_ID')
    secret_key = EnvUtil.get_env('COS_SECRET_KEY')
    cos_cdn_domain = EnvUtil.get_env('COS_CDN_DOMAIN')
    region = EnvUtil.get_env('COS_REGION')
    bucket_name = f"{EnvUtil.get_env('COS_BUCKET')}-{EnvUtil.get_env('COS_APPID')}"
    token = None
    scheme = 'https'

    try:
        start_time = time.time()
        # 创建 CosS3Client 对象，对桶和对象的操作都通过 CosS3Client 实现
        config = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key, Token=token, Scheme=scheme)
        client = CosS3Client(config)

        # 使用高级接口断点续传，失败重试时不会下载已成功的分块(这里重试10次)
        for i in range(0, 10):
            try:
                response = client.download_file(
                    Bucket=bucket_name,
                    Key=cos_key,
                    DestFilePath=file_path)
                break
            except CosClientError as e:
                logger.write_to_error_log(f'COS 下载 发生错误第{i}次, CosClientError: {e}')
            except CosServiceError as e:
                logger.write_to_error_log(f'COS 下载 发生错误第{i}次, CosServiceError: {e}')
            except Exception as e:
                logger.write_to_error_log(f'COS 下载 发生错误第{i}次, unknown error: {e}')

        end_time = time.time()
        logger.write_to_log(f'COS 下载用时 {int(end_time - start_time)}s')
        return True
    except Exception as e:
        logger.write_to_error_log('fail with unknown error: {}'.format(e))
        return False


def create_pre_signed_url_cos(object_key, expire_time=3600):
    logger = LoggerManager(logger_name='COS')
    # 从环境变量获取 AK 和 SK 信息。
    secret_id = EnvUtil.get_env('COS_SECRET_ID')
    secret_key = EnvUtil.get_env('COS_SECRET_KEY')
    cos_cdn_domain = EnvUtil.get_env('COS_CDN_DOMAIN')
    region = EnvUtil.get_env('COS_REGION')
    bucket_name = f"{EnvUtil.get_env('COS_BUCKET')}-{EnvUtil.get_env('COS_APPID')}"
    token = None
    scheme = 'https'

    try:
        # 创建 TosClientV2 对象，对桶和对象的操作都通过 TosClientV2 实现
        config = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key, Token=token, Scheme=scheme)
        client = CosS3Client(config)
        # 生成下载 URL，未限制请求头部和请求参数
        url = client.get_presigned_url(
            Method='GET',
            Bucket=bucket_name,
            Key=object_key,
            Expired=expire_time  # 120秒后过期，过期时间请根据自身场景定义
        )
        return url
    except Exception as e:
        logger.write_to_error_log('COS 生成预签名链接失败: {}'.format(e))
        return False
