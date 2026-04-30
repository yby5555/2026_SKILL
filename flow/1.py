"""
OpenAI GPT Image API 图片生成
支持模型: gpt-image-2, gpt-image-1
文档: https://platform.openai.com/docs/guides/image-generation
需要环境变量: OPENAI_API_KEY
"""

import base64
from pathlib import Path
import httpx
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

class _StealthTransport(httpx.HTTPTransport):
    """移除 OpenAI SDK 的 X-Stainless-* 特征头，避免被 Cloudflare WAF 拦截。"""
    _STRIP = {
        "x-stainless-lang",
        "x-stainless-package-version",
        "x-stainless-os",
        "x-stainless-arch",
        "x-stainless-runtime",
        "x-stainless-runtime-version",
    }

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        headers = httpx.Headers(
            {k: v for k, v in request.headers.items() if k.lower() not in self._STRIP}
        )
        new_request = httpx.Request(
            method=request.method,
            url=request.url,
            headers=headers,
            content=request.content,
            extensions=request.extensions,
        )
        new_request.headers["user-agent"] = "curl/8.7.1"
        return super().handle_request(new_request)


_http_client = httpx.Client(transport=_StealthTransport(), follow_redirects=True)


def generate_image(
    prompt: str,
    output_path: str = "output.png",
    model: str = "gpt-image-2",
    size: str = "auto",
    quality: str = "auto",
    output_format: str = "png",
    output_compression: int = 100,
    background: str = "auto",
    n: int = 1,
) -> list[str]:
    """
    使用 GPT Image 模型生成图片。

    参数:
        prompt: 图片描述，最长 32000 字符
        output_path: 输出路径
        model: gpt-image-2 / gpt-image-1
        size: 1024x1024 / 1536x1024 / 1024x1536 / auto
        quality: low / medium / high / auto
        output_format: png / jpeg / webp
        output_compression: 压缩级别 0-100（仅 jpeg/webp）
        background: transparent / opaque / auto
        n: 生成数量 1-10

    返回:
        保存的文件路径列表
    """
    client = OpenAI(http_client=_http_client)

    result = client.images.generate(
        model=model,
        prompt=prompt,
        n=n,
        size=size,
        quality=quality,
        output_format=output_format,
        output_compression=output_compression,
        background=background,
    )

    saved = []
    ext = output_format if output_format != "jpeg" else "jpg"
    stem = Path(output_path).stem
    out_dir = Path(output_path).parent
    out_dir.mkdir(parents=True, exist_ok=True)

    for i, img in enumerate(result.data):
        path = out_dir / f"{stem}_{i}.{ext}" if n > 1 else out_dir / f"{stem}.{ext}"
        path.write_bytes(base64.b64decode(img.b64_json))
        saved.append(str(path))

    if result.usage:
        print(f"Tokens - 总: {result.usage.total_tokens}, "
              f"输入: {result.usage.input_tokens}, 输出: {result.usage.output_tokens}")

    return saved


def edit_image(
    prompt: str,
    image_paths: list[str],
    output_path: str = "edited.png",
    model: str = "gpt-image-2",
    quality: str = "auto",
    output_format: str = "png",
    mask_path: str | None = None,
    input_fidelity: str = "low",
) -> str:
    """
    使用参考图片编辑/生成新图片（最多 16 张输入图片）。

    参数:
        prompt: 编辑描述
        image_paths: 输入图片路径列表
        output_path: 输出路径
        model: 模型名称
        quality: low / medium / high / auto
        output_format: png / jpeg / webp
        mask_path: 可选 mask 路径（用于 inpainting）
        input_fidelity: high 可更好保留输入图片细节

    返回:
        保存的文件路径
    """
    client = OpenAI(http_client=_http_client)

    images = [open(p, "rb") for p in image_paths]
    mask = open(mask_path, "rb") if mask_path else None

    try:
        kwargs = dict(
            model=model,
            image=images,
            prompt=prompt,
            quality=quality,
            output_format=output_format,
            input_fidelity=input_fidelity,
        )
        if mask:
            kwargs["mask"] = mask

        result = client.images.edit(**kwargs)

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_bytes(base64.b64decode(result.data[0].b64_json))
        return output_path
    finally:
        for f in images:
            f.close()
        if mask:
            mask.close()


def generate_via_responses_api(
    prompt: str,
    output_path: str = "output.png",
    model: str = "gpt-5.4",
    image_model_config: dict | None = None,
) -> str:
    """
    通过 Responses API 生成图片（支持多轮对话编辑）。

    参数:
        prompt: 图片描述
        output_path: 输出路径
        model: 主模型（如 gpt-4.1, gpt-5）
        image_model_config: 图片生成工具配置，如 {"quality": "high", "background": "transparent"}

    返回:
        保存的文件路径
    """
    client = OpenAI(http_client=_http_client)

    tool = {"type": "image_generation"}
    if image_model_config:
        tool.update(image_model_config)

    response = client.responses.create(
        model=model,
        input=prompt,
        tools=[tool],
    )

    image_data = [
        output.result
        for output in response.output
        if output.type == "image_generation_call"
    ]

    if not image_data:
        raise ValueError("未生成图片")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_bytes(base64.b64decode(image_data[0]))
    return output_path


if __name__ == "__main__":
    # 方式一：最简调用（参考官方示例）
    client = OpenAI(http_client=_http_client)

    prompt = """
    A children's book drawing of a veterinarian using a stethoscope to
    listen to the heartbeat of a baby otter.
    """

    result = client.images.generate(
        model="gpt-image-2",
        prompt=prompt,
    )

    image_bytes = base64.b64decode(result.data[0].b64_json)
    with open("otter.png", "wb") as f:
        f.write(image_bytes)
    print("生成完成: otter.png")

    # 方式二：使用封装函数（支持更多参数）
    # paths = generate_image(
    #     prompt="一只可爱的海獭宝宝仰面漂浮在平静的蓝色水面上，水彩画风格",
    #     output_path="output.png",
    #     model="gpt-image-2",
    #     quality="high",
    # )
    # print(f"生成完成: {paths}")

    # 方式三：Responses API 生成（支持多轮编辑）
    # path = generate_via_responses_api(
    #     prompt="画一只灰色虎斑猫抱着一只戴着橙色围巾的水獭",
    #     output_path="cat_otter.png",
    #     image_model_config={"quality": "high", "background": "transparent"},
    # )
    # print(f"Responses API 生成完成: {path}")