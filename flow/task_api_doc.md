# 任务接口开发文档

> 版本：v1.0.0  
> 创建时间：2026-04-28  
> 作者：后端开发团队  

---

## 目录

- [通用说明](#通用说明)
- [状态码说明](#状态码说明)
- [接口一：创建任务](#接口一创建任务)
- [接口二：查询任务状态](#接口二查询任务状态)

---

## 通用说明

### 请求规范

| 项目 | 说明 |
|------|------|
| 请求协议 | HTTPS |
| 请求格式 | `application/json` |
| 响应格式 | `application/json` |
| 字符编码 | UTF-8 |

### 通用响应结构

所有接口统一返回以下结构：

```json
{
  "code": 200,
  "message": "success",
  "data": { }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `code` | Integer | 业务状态码，见[状态码说明](#状态码说明) |
| `message` | String | 状态描述信息 |
| `data` | Object / null | 业务数据，失败时为 `null` |

---

## 状态码说明

### HTTP 状态码

| HTTP 状态码 | 说明 |
|-------------|------|
| `200` | 请求成功，业务处理正常 |
| `400` | 请求参数错误（缺少必填项、格式非法等） |
| `401` | 未授权，鉴权失败 |
| `403` | 权限不足，禁止访问 |
| `404` | 资源不存在 |
| `413` | 请求体过大（如上传图片超限） |
| `429` | 请求频率超限（触发限流） |
| `500` | 服务器内部错误 |
| `503` | 服务暂时不可用（如 Redis/数据库连接异常） |

### 业务状态码（code 字段）

| 业务码 | 说明 |
|--------|------|
| `200` | 操作成功 |
| `10001` | 参数校验失败（缺少必填参数） |
| `10002` | 参数类型错误（type 值非法，必须为 0 或 1） |
| `10003` | 图片格式不支持（非 Base64 或 URL 格式） |
| `10004` | 图片大小超出限制 |
| `20001` | 任务创建失败（数据库写入异常） |
| `20002` | 任务推送到队列失败（Redis 异常） |
| `30001` | 任务 ID 不存在 |
| `30002` | 任务查询失败（数据库读取异常） |
| `50000` | 服务器未知错误 |

### 任务 msg 状态枚举

| msg 值 | 说明 |
|--------|------|
| `执行中` | 任务已入队，正在等待或正在生成 |
| `已完成` | 任务执行成功，结果已上传 |
| `失败` | 任务执行过程中发生错误 |

---

## 接口一：创建任务

### 基本信息

| 项目 | 说明 |
|------|------|
| 接口名称 | 创建任务 |
| 请求方法 | `POST` |
| 接口路径 | `/api/v1/task/create` |
| 功能描述 | 接收用户提交的生成参数，将任务信息写入数据库，并将任务推送至 Redis 消息队列异步执行，返回唯一任务 ID |

---

### 请求参数

#### Header 参数

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| `Content-Type` | String | 是 | 固定值：`application/json` |

#### Body 参数（JSON）

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| `prompt` | String | **是** | 提示词，用于描述生成内容，长度限制 1~2000 字符 |
| `type` | Integer | **是** | 生成类型：`0` = 生成图片，`1` = 生成视频 |
| `image` | String | 否 | 参考图片（Base64 编码或图片 URL），若传入则基于此图片进行生成 |

#### 请求示例

```json
{
  "prompt": "一只在草原上奔跑的白色马，电影感镜头",
  "type": 1,
  "image": "https://example.com/reference.jpg"
}
```

> **注意**：`image` 字段为可选。若不需要参考图片，可省略此字段或传 `null`。

---

### 数据库写入字段说明

任务创建时，系统将向数据库写入如下字段：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `_id` | String (UUID) | 系统生成的唯一任务 ID（即返回给用户的任务 ID） |
| `prompt` | String | 用户传入的提示词 |
| `type` | Integer | 生成类型：`0` 图片 / `1` 视频 |
| `image` | String / null | 参考图片，无则为 `null` |
| `msg` | String | 任务状态，初始值为 `执行中` |
| `created_at` | DateTime | 任务创建时间（UTC+8） |

#### Redis 队列推送格式

写入数据库后，系统将以下格式推送到 Redis 队列：

```json
{
  "_id": "550e8400-e29b-41d4-a716-446655440000",
  "prompt": "一只在草原上奔跑的白色马，电影感镜头",
  "type": 1,
  "image": "https://example.com/reference.jpg"
}
```

---

### 响应参数

#### 成功响应（HTTP 200）

| 字段 | 类型 | 说明 |
|------|------|------|
| `code` | Integer | 业务状态码，`200` 表示成功 |
| `message` | String | `"success"` |
| `data.task_id` | String | 唯一任务 ID（UUID 格式），用于后续查询任务状态 |

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "task_id": "550e8400-e29b-41d4-a716-446655440000"
  }
}
```

#### 失败响应示例

**缺少必填参数（prompt 未传）**

```json
{
  "code": 10001,
  "message": "参数校验失败：prompt 为必填项",
  "data": null
}
```

**type 值非法**

```json
{
  "code": 10002,
  "message": "参数错误：type 必须为 0（图片）或 1（视频）",
  "data": null
}
```

**Redis 推送失败**

```json
{
  "code": 20002,
  "message": "任务队列推送失败，请稍后重试",
  "data": null
}
```

---

### 业务逻辑流程

```
用户请求
   │
   ▼
参数校验（prompt 必填 / type 合法 / image 格式）
   │ 失败 → 返回 10001 / 10002 / 10003
   │
   ▼
生成 UUID 作为 _id（即 task_id）
   │
   ▼
写入数据库
  ├── _id = UUID
  ├── prompt / type / image
  ├── msg = "执行中"
  └── created_at = 当前时间
   │ 失败 → 返回 20001
   │
   ▼
推送到 Redis 队列（含 _id / prompt / type / image）
   │ 失败 → 返回 20002
   │
   ▼
返回 task_id 给用户（code: 200）
```

---

## 接口二：查询任务状态

### 基本信息

| 项目 | 说明 |
|------|------|
| 接口名称 | 查询任务状态 |
| 请求方法 | `GET` |
| 接口路径 | `/api/v1/task/status` |
| 功能描述 | 根据任务 ID 查询任务当前状态及生成结果（视频链接、COS 链接等） |

---

### 请求参数

#### Query 参数

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| `task_id` | String | **是** | 创建任务时返回的任务 ID（UUID 格式） |

#### 请求示例

```
GET /api/v1/task/status?task_id=550e8400-e29b-41d4-a716-446655440000
```

---

### 响应参数

#### data 字段说明

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `task_id` | String | 任务唯一 ID |
| `msg` | String | 任务状态：`执行中` / `已完成` / `失败` |
| `type` | Integer | 生成类型：`0` 图片 / `1` 视频 |
| `video_url` | String / null | 视频临时访问链接（仅 type=1 且已完成时有值） |
| `cos_url` | String / null | 腾讯云 COS 持久化链接（已完成时有值） |
| `created_at` | String | 任务创建时间（格式：`yyyy-MM-dd HH:mm:ss`） |
| `updated_at` | String / null | 任务最后更新时间（格式：`yyyy-MM-dd HH:mm:ss`） |
| `error_msg` | String / null | 失败原因描述，仅 msg=`失败` 时有值，其余为 `null` |

---

#### 成功响应：任务执行中（HTTP 200）

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "task_id": "550e8400-e29b-41d4-a716-446655440000",
    "msg": "执行中",
    "type": 1,
    "video_url": null,
    "cos_url": null,
    "created_at": "2026-04-28 10:00:00",
    "updated_at": null,
    "error_msg": null
  }
}
```

#### 成功响应：任务已完成（视频生成，HTTP 200）

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "task_id": "550e8400-e29b-41d4-a716-446655440000",
    "msg": "已完成",
    "type": 1,
    "video_url": "https://storage.googleapis.com/xxx/video_tmp.mp4",
    "cos_url": "https://your-bucket.cos.ap-guangzhou.myqcloud.com/videos/550e8400.mp4",
    "created_at": "2026-04-28 10:00:00",
    "updated_at": "2026-04-28 10:03:25",
    "error_msg": null
  }
}
```

#### 成功响应：任务已完成（图片生成，HTTP 200）

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "task_id": "550e8400-e29b-41d4-a716-446655440000",
    "msg": "已完成",
    "type": 0,
    "video_url": null,
    "cos_url": "https://your-bucket.cos.ap-guangzhou.myqcloud.com/images/550e8400.png",
    "created_at": "2026-04-28 10:00:00",
    "updated_at": "2026-04-28 10:01:10",
    "error_msg": null
  }
}
```

#### 失败响应：任务执行失败（HTTP 200）

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "task_id": "550e8400-e29b-41d4-a716-446655440000",
    "msg": "失败",
    "type": 1,
    "video_url": null,
    "cos_url": null,
    "created_at": "2026-04-28 10:00:00",
    "updated_at": "2026-04-28 10:05:00",
    "error_msg": "视频生成超时，请重新提交任务"
  }
}
```

#### 失败响应：task_id 不存在（HTTP 200）

```json
{
  "code": 30001,
  "message": "任务 ID 不存在",
  "data": null
}
```

---

### 业务逻辑流程

```
用户请求（携带 task_id）
   │
   ▼
参数校验（task_id 不为空）
   │ 失败 → 返回 10001
   │
   ▼
查询数据库（根据 _id = task_id）
   │ 数据库异常 → 返回 30002
   │ 记录不存在 → 返回 30001
   │
   ▼
返回任务字段：
  msg / type / video_url / cos_url / created_at / updated_at / error_msg
   │
   ▼
响应给用户（code: 200）
```

---

## 附录：接口汇总

| 接口名称 | 方法 | 路径 | 功能 |
|----------|------|------|------|
| 创建任务 | `POST` | `/api/v1/task/create` | 提交生成任务，返回 task_id |
| 查询任务状态 | `GET` | `/api/v1/task/status` | 根据 task_id 查询任务状态及结果 |
