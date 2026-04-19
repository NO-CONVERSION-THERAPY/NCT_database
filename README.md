# NCT API SQL

基于 `Cloudflare Workers + D1 + R2 + Hono + Vite + React` 的全栈应用，用于：

- 接收下游推送数据并写入未加密原始表
- 根据原始表生成部分列加密的发布表，并维护版本号
- 记录下游同步时回传的 URL 地址与同步状态
- 对下游提供同步 API，并在版本落后时回推最新数据
- 定时把 D1 三张表打包到 R2，并以邮件附件形式发出
- 提供 liquid glass 风格的管理台，用于查询、管理、分析、调试

生产环境默认通过 Cloudflare Workers 绑定的自定义域名对外提供服务，例如：

- 根路径公开 JSON：`https://api.example.com/`
- 管理台首页：`https://api.example.com/Console`
- API 基地址：`https://api.example.com/api/*`

## 架构

### D1 三表

1. `raw_records`
原始未加密数据，保存完整 JSON。
`ingest` 时会按顶层字段自动为本表补充 `payload_*` 动态列，方便直接查询。

2. `secure_records`
按 `encryptFields` 或 `DEFAULT_ENCRYPT_FIELDS` 拆分字段。
非敏感字段落在 `public_json`，敏感字段用 `ENCRYPTION_KEY` 做 `AES-GCM` 加密，版本号在本表递增维护。
同时会自动补充：

- 非敏感字段的 `public_*` 动态列
- 敏感字段的 `encrypted_*` 动态列

3. `downstream_clients`
记录下游同步时携带的 `callback_url`、版本号、同步时间、响应状态和错误信息。

### 关键接口

- `GET /`
直接返回公开 JSON 数据：

```json
{
  "avg_age": 17,
  "last_synced": 123,
  "statistics": [
    { "province": "河南", "count": 12 },
    { "province": "湖北", "count": 66 }
  ],
  "data": [
    {
      "name": "学校名称",
      "addr": "学校地址",
      "province": "省份",
      "prov": "区、县",
      "else": "其他补充内容",
      "lat": 36.62728,
      "lng": 118.58882,
      "experience": "经历描述",
      "HMaster": "负责人/校长姓名",
      "scandal": "已知丑闻",
      "contact": "学校联系方式",
      "inputType": "受害者本人"
    }
  ]
}
```

其中：

- `data` 来自 D1 的 `raw_records`
- `statistics` 是对 `province` 的聚合统计
- `avg_age` 是对 `age` 的平均值，按四舍五入返回整数
- `last_synced` 是当前版本号，也就是 `secure_records.version` 的最大值

- `POST /api/ingest`
下游把数据推送到这里。Worker 会先按 ingest 顶层字段自动扩列并写 `raw_records`，再按加密规则更新 `secure_records`。

- `POST /api/sync`
下游带上当前版本号和回调 URL 请求同步。
如果下游版本落后，Worker 会把 `secure_records` 的全量或增量内容回推到 `callbackUrl`，同时把 URL 和记要写到第三张表。

- `GET /api/public/secure-records`
按版本对外公布表 2 数据，可选 `mode=full|delta` 和 `currentVersion`。

- `POST /api/admin/export-now`
手动触发导出。

### 定时任务

`wrangler.toml` 默认配置了 UTC Cron：

```toml
[triggers]
crons = ["0 18 * * *"]
```

这表示每天 `18:00 UTC` 触发一次。按 `Asia/Shanghai` 来看，相当于次日 `02:00`。

## 本地开发

### 1. 安装依赖

```bash
npm install
```

### 2. 创建 Cloudflare 资源

```bash
npx wrangler d1 create nct-api-sql
npx wrangler r2 bucket create nct-api-sql-exports
npx wrangler r2 bucket create nct-api-sql-exports-preview
```

把创建出来的 `database_id` 和 bucket 名称填回 [wrangler.toml](/home/medicago/projects/nct-api-sql/wrangler.toml)。

### 3. 准备本地密钥和令牌

```bash
cp .dev.vars.example .dev.vars
openssl rand -base64 32
```

把生成的 base64 值写入 `ENCRYPTION_KEY`。如果你希望管理台、写入接口和同步接口分别鉴权，可以分别设置：

- `ADMIN_TOKEN`
- `INGEST_TOKEN`
- `SYNC_TOKEN`

管理台支持分别填写这三个 token；如果 `INGEST_TOKEN` 或 `SYNC_TOKEN` 留空，前端会回退使用 `ADMIN_TOKEN`。

### 4. 执行 D1 migration

```bash
npm run db:migrate
```

如果你直接运行 `npm run dev`，这一步现在会自动执行。
项目会先在本地创建或更新一个调试用 D1 数据库，然后再启动 Vite 和 Wrangler。
本地持久化目录固定为 `.wrangler/state`。

### 5. 启动开发环境

```bash
npm run dev
```

默认会同时启动：

- Vite 前端 Console：`http://127.0.0.1:5173/Console`
- Wrangler 本地 Worker：`http://127.0.0.1:8787`

本地开发时可以这样理解：

- `http://127.0.0.1:5173/Console` 用来看管理台
- `http://127.0.0.1:8787/` 用来看 Worker 返回的公开 JSON
- `http://127.0.0.1:8787/api/*` 用来直接调试 API

其中 `npm run dev` 会先自动执行：

```bash
node scripts/prepare-local-d1.mjs
```

它会调用：

```bash
npx wrangler d1 migrations apply DB --local --persist-to .wrangler/state
```

也就是说，只要你执行一次 `npm run dev`，本地调试 D1 库就会被自动建立好。

注意：Cloudflare Workers 在生产环境不是传统“监听自定义端口”的模式，因此这里用的是 HTTP API 入口 `POST /api/ingest` 来承接下游写入。

### 动态扩列规则

`ingest` 接收到新的顶层字段时，会自动对 D1 执行 `ALTER TABLE ... ADD COLUMN`。
为了避免和系统列冲突，动态列名会做安全规整，并带上短哈希后缀，例如：

- `payload_city_x1y2z3`
- `public_score_a8k2m1`
- `encrypted_phone_q9w8e7`

规则说明：

- 只针对 payload 的顶层字段自动扩列
- 标量会直接转成字符串写入动态列
- 对象或数组会序列化成 JSON 字符串写入动态列
- 原始 JSON 列仍然保留，作为完整数据兜底

## 部署

```bash
npm run build
npx wrangler deploy
```

远端 D1 migration：

```bash
npm run db:migrate:remote
```

### 自定义域名

部署到 Workers 后，线上服务建议直接绑定你自己的域名，而不是依赖默认的 `*.workers.dev` 地址。
本文档后续涉及的生产环境示例统一以 `https://api.example.com` 作为占位域名。

常见做法是：

1. 在 Cloudflare 中接入你的站点域名，例如 `example.com`
2. 部署 Worker
3. 将 Worker 绑定到一个自定义子域名，例如 `api.example.com`
4. 让管理台走根路径 `/`，让 API 继续走 `/api/*`
实际实现中建议改为：

4. 让公开 JSON 走根路径 `/`
5. 让管理台走 `/Console`
6. 让 API 继续走 `/api/*`

如果你准备使用 `wrangler.toml` 管理线上路由，需要把当前配置补充为你自己的自定义域名路由，并在正式环境关闭默认 `workers.dev` 暴露。

### 生产访问约定

假设你的自定义域名是 `https://api.example.com`，则：

- 公开 JSON：`https://api.example.com/`
- 管理台入口：`https://api.example.com/Console`
- 健康检查：`https://api.example.com/api/health`
- 数据写入：`https://api.example.com/api/ingest`
- 下游同步：`https://api.example.com/api/sync`
- 公布数据：`https://api.example.com/api/public/secure-records`
- 管理导出：`https://api.example.com/api/admin/export-now`

## 邮件导出

当前实现默认用 `Resend` 发附件邮件，需要在 `.dev.vars` 或 Cloudflare secret 中设置：

- `RESEND_API_KEY`
- `EXPORT_EMAIL_TO`
- `EXPORT_EMAIL_FROM`

导出流程会：

1. 全量查询 D1 三张表
2. 生成 JSON + CSV 文件
3. 打成 zip
4. 上传到 R2
5. 把 zip 作为附件发往目标邮箱

## 示例请求

### 写入原始数据

本地开发示例：

```bash
curl -X POST http://127.0.0.1:8787/api/ingest \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_INGEST_TOKEN" \
  -d '{
    "records": [
      {
        "recordKey": "patient-1001",
        "source": "hospital-a",
        "encryptFields": ["name", "phone", "email"],
        "payload": {
          "id": "patient-1001",
          "name": "Zhang San",
          "phone": "13800000000",
          "email": "demo@example.com",
          "city": "Shanghai",
          "score": 91
        }
      }
    ]
  }'
```

生产环境自定义域名示例：

```bash
curl -X POST https://api.example.com/api/ingest \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_INGEST_TOKEN" \
  -d '{
    "records": [
      {
        "recordKey": "patient-1001",
        "source": "hospital-a",
        "encryptFields": ["name", "phone", "email"],
        "payload": {
          "id": "patient-1001",
          "name": "Zhang San",
          "phone": "13800000000",
          "email": "demo@example.com",
          "city": "Shanghai",
          "score": 91
        }
      }
    ]
  }'
```

### 下游请求同步

本地开发示例：

```bash
curl -X POST http://127.0.0.1:8787/api/sync \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_SYNC_TOKEN" \
  -d '{
    "clientName": "analytics-service",
    "callbackUrl": "https://example.com/sync/callback",
    "currentVersion": 3,
    "mode": "full"
  }'
```

生产环境自定义域名示例：

```bash
curl -X POST https://api.example.com/api/sync \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_SYNC_TOKEN" \
  -d '{
    "clientName": "analytics-service",
    "callbackUrl": "https://consumer.example.com/sync/callback",
    "currentVersion": 3,
    "mode": "full"
  }'
```

## 验证状态

已经在本地执行通过：

- `npm run check`
- `npm run build`
