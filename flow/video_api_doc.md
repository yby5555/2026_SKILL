# Flow 视频生成 API 接口文档

本接口基于 FastAPI 构建，提供 Google Labs Flow 自动化视频生成服务，包含后台浏览器池的并发调用及账号池的自动流转。

---

## 1. 核心接口：并发生成视频

**接口路径**: `POST /generate_video`
**功能说明**: 传入任务 ID 与提示词，并发调用 Google Flow 生成视频。该接口为**同步阻塞**，最长等待时间受配置 `TASK_TIMEOUT_MS` (默认 4 分钟) 控制。

### 1.1 请求参数 (Request Body)
格式: `application/json`

| 字段名 | 类型 | 必填 | 描述 |
| --- | --- | --- | --- |
| `id` | string | 是 | 调用方提供的任务 ID，需全局唯一。用于后续在数据库中追踪任务状态。 |
| `prompt` | string | 是 | 视频生成提示词，最小长度必须大于 0。 |

**请求示例**:
```json
{
  "id": "task_20260427_0001",
  "prompt": "一只可爱的小猫在草地上玩耍，电影质感，4k"
}
```

### 1.2 响应参数 (Response Body)
格式: `application/json`

| 字段名 | 类型 | 描述 |
| --- | --- | --- |
| `request_id` | string | 本次 HTTP 请求的后端追踪 ID（内部使用，与入参的业务 `id` 不同）。 |
| `total` | integer | 任务总数（目前批量处理固定返回 1）。 |
| `success_count` | integer | 成功生成的视频数量。 |
| `partial_success_count` | integer | 部分成功的数量。 |
| `fail_count` | integer | 失败的数量。 |
| `results` | array[Object] | 任务结果列表，包含具体的任务状态详情。 |

**`results` 列表元素详情 (`VideoResult`)**:

| 字段名 | 类型 | 描述 |
| --- | --- | --- |
| `id` | string | 任务业务 ID（与入参 `id` 对应）。 |
| `status` | string | 任务结果状态。可选值：<br> - `success`: 视频已生成且本地下载成功。<br> - `partial_success`: 视频已生成，但下载到本地失败（可使用远程 `video_url`）。<br> - `failed`: 视频生成彻底失败。 |
| `error_code` | string \| null | 业务错误码（仅异常时存在，详见 1.4）。 |
| `project_id` | string \| null | Google Flow 返回的项目 ID。 |
| `video_url` | string \| null | 生成成功的视频远程 URL。 |
| `local_video_path`| string \| null | 成功下载到本地的视频路径。 |
| `error` | string \| null | 详细的错误报错信息。 |

**成功响应示例**:
```json
{
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "total": 1,
  "success_count": 1,
  "partial_success_count": 0,
  "fail_count": 0,
  "results": [
    {
      "id": "task_20260427_0001",
      "status": "success",
      "error_code": null,
      "project_id": "project_abc123",
      "video_url": "https://labs.google/xxx.mp4",
      "local_video_path": "/path/to/videos/project_abc123.mp4",
      "error": null
    }
  ]
}
```

### 1.3 HTTP 状态码

| HTTP 状态码 | 含义 | 说明 |
| --- | --- | --- |
| **200 OK** | 处理完成 | 接口成功处理。注意：只要服务运行正常，即使视频**生成失败**（如账号封禁、提示词违规等），HTTP 状态码依然是 200。请通过 `results[0].status` 判断实际结果。 |
| **422 Unprocessable Entity** | 参数校验错误 | 传入的 Body 缺少必填字段或格式不对。 |
| **429 Too Many Requests** | 并发限制拦截 | 服务并发已达上限 (`MAX_REQUEST_CONCURRENCY`)，立刻被限流拒绝。 |
| **503 Service Unavailable** | 引擎未就绪 | 后台浏览器自动化服务 (Scraper) 尚未初始化完毕，无法处理请求。 |

### 1.4 业务错误码 (`error_code` 字典)

当由于环境、并发、底层异常导致问题时，将返回或在 `error_code` 字段中体现以下标准错误码：

| 错误码常量 | 出现场景与含义 |
| --- | --- |
| `SCRAPER_NOT_READY` | 浏览器引擎或整个运行池未能启动完成，无法接收任务。 |
| `CONCURRENCY_LIMIT` | 并发限制。当前请求并发数已满，触发 HTTP 429 时附带。 |
| `GENERATION_TIMEOUT` | 视频生成超时。在自动化页面上等待 Google 生成的时间超出了配置时间（默认4分钟）。 |
| `DOWNLOAD_FAILED` | 视频生成可能已完成，但无法获取远程链接也未能下载成功，被判定为完全失败 `failed`。 |
| `PARTIAL_SUCCESS` | 视频生成成功，也拿到了 `video_url`，但是下载到本地 `local_video_path` 的过程发生错误，此时状态被标记为 `partial_success`。 |
| `COOKIE_POOL_EMPTY` | Cookie 池已被耗尽或无可用的未封禁账号。 |

---

## 2. 运维与健康检查接口

这类接口通常为 GET 请求，用于监控、探测和状态查看，无需传递 JSON Body。

### 2.1 存活探针
**接口**: `GET /live`
**功能**: 探测 FastAPI 服务进程是否处于存活状态。
**出参**:
```json
{
  "status": "alive"
}
```
**HTTP 状态码**: `200 OK`

### 2.2 就绪探针
**接口**: `GET /ready`
**功能**: 探测底层 Browser Scraper 是否已完全初始化。
**出参**: 
- `200 OK` (就绪): `{"status": "ready"}`
- `503 Service Unavailable` (未就绪): `{"detail": "scraper not ready"}`

### 2.3 核心池监控
**接口**: `GET /health/detail`
**功能**: 暴露系统内部并发配置与 Redis Cookie 账号池的详细存量数据（出于安全考量，不会泄露邮箱原文）。
**出参示例**:
```json
{
  "scraper_ready": true,
  "max_concurrent": 6,
  "browser_pool_size": 2,
  "contexts_per_browser": 3,
  "cookie_pool": {
    "active_count": 10,
    "expired_count": 2,
    "total": 12
  }
}
```

### 2.4 极简账号池监控
**接口**: `GET /pool`
**功能**: 仅返回 Redis Cookie 池存量。
**出参示例**:
```json
{
  "active_count": 10,
  "expired_count": 2,
  "total": 12
}
```
