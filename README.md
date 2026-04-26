# NCT API SQL

基于 `Cloudflare Workers + D1 + R2 + Hono + Vite + React` 的全栈应用，用于：

- 接收下游推送数据并写入未加密原始表
- 根据原始表生成部分列加密的发布表，并维护版本号
- 记录 `NCT_backend` 定时上报的域名、版本号和上报次数
- 向已登记的 `NCT_backend` 推送公开 secure records
- 按子库版本从新到旧主动回拉 `nct_databack` 文件，并回灌到主库的 `secure_records` 与 `raw_records`
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
作为第三张表，统一记录：

- `NCT_backend` 上报的 `serviceUrl`、`databackVersion`、`reportCount` 和原始 payload
- 主库最近一次成功推送到该子库的版本号与时间
- 主库最近一次成功回拉该子库的版本号、时间和状态

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
已废弃。母库现在通过已登记子库的 `serviceUrl` 主动推送公开 secure records。

- `POST /api/sub/report`
只接收带 `serviceWatermark: "nct-api-sql-sub:v1"` 的 `NCT_backend` 上报。
收到后会把 `service`、`serviceUrl`、`databackVersion`、`reportCount`、`reportedAt` 存入第三张表。
同一子库的重复上报会按 `SUB_REPORT_MIN_INTERVAL_MS` 做限频，过快会返回 `429`。

- `POST /api/admin/push-now`
手动触发一次“主库 -> 已登记子库”的公开 secure records 推送。

- `POST /api/admin/pull-now`
手动触发一次“主库 <- 已登记子库”的灾备回拉。
主库会按第三表中记录的子库版本，从新到旧调用子库的 `GET /api/export/nct_databack`，接收 JSON 附件文件并导入回主库。

- `GET /api/public/secure-records`
按版本对外公布表 2 数据，可选 `mode=full|delta` 和 `currentVersion`。

- `POST /api/admin/export-now`
手动触发导出。仅在 Cloudflare 账号已启用 R2 且配置 `EXPORT_BUCKET` 绑定后可用。

### 定时任务

当前默认部署不配置导出 Cron，因为 R2 导出需要账号先启用 R2 并添加 `EXPORT_BUCKET` 绑定。启用后可再在 `wrangler.toml` 加回导出定时任务，例如每天 `18:00 UTC`。

子库侧会定时上报自身状态并回传待同步表单记录；母库在收到上报、导入新数据或手动 push 时，会向已登记子库推送公开 secure records。

## 本地开发

### 1. 安装依赖

```bash
npm install
```

### 2. 准备本地 D1

本地开发不需要创建线上 D1。`npm run dev` 前置脚本会准备本地 D1 并执行本地 migrations；线上 D1 由 Cloudflare Workers 部署命令自动创建。

### 3. 准备本地密钥和令牌

```bash
cp .env.example .dev.vars
openssl rand -base64 32
```

[`./.env.example`](./.env.example) 已按修改必要性排序列出当前项目的全部环境变量。
本地 Wrangler 仍然读取 `.dev.vars`，线上部署则把同名键写入 Cloudflare Variables / Secrets。
这些运行变量不会写入 `wrangler.toml`；部署脚本使用 `wrangler deploy --keep-vars`，避免 Git 中的配置覆盖 Dashboard 里的生产变量。

#### 必填环境变量

完整列表见 [`./.env.example`](./.env.example)。这里先标出真正需要优先确认的几项：

- 绝对必填：`ENCRYPTION_KEY`
- 平台绑定必填但不写进 `.env`：`DB`、`ASSETS`，在 [`wrangler.toml`](./wrangler.toml) 中绑定 D1 和静态资产；`EXPORT_BUCKET` 仅在账号启用 R2 导出时再绑定
- 管理台密码不再通过环境变量配置；部署后首次打开 `/Console` 设置
- 按功能必填：`RESEND_API_KEY`、`EXPORT_EMAIL_TO`、`EXPORT_EMAIL_FROM` 仅在你要启用邮件导出时需要

把生成的 base64 值写入 `ENCRYPTION_KEY`。服务间调用统一使用子库 `serviceUrl` 派生的 30 秒 HMAC Bearer token：

- 子库首次成功 `POST /api/sub/report` 即完成登记，母库保存 `sha256(serviceUrl)` 用于后续校验
- 子库上报、表单回传、母库推送 secure records、母库灾备回拉都按相同 30 秒窗口派生 token
- `GET /api/public/secure-records` 返回公开 payload，不再包 signed envelope；记录里的 `encryptedData` 仍然是母库 t2 字段密文
- 母库不再要求子库回传数据时做额外字段加密；子库本地普通 JSON 回传后，由母库按自身 `ENCRYPTION_KEY` 重新生成 t2

下面这些变量不是鉴权凭据，而是 mother/sub 同步调优项：

- `SUB_REPORT_MIN_INTERVAL_MS`
- `SUB_PULL_BATCH_SIZE`
- `SUB_PULL_MAX_ATTEMPTS`
- `SUB_PULL_RECORD_LIMIT`
- `SUB_PULL_RETRY_DELAY_MS`
- `SUB_PULL_TIMEOUT_MS`

管理台首次打开时会把你设置的管理员密码哈希写入 D1，之后登录会得到短期 session token。
`/api/ingest` 只接受已登录管理台 session，不再接受外部 `INGEST_TOKEN` Bearer 写入；母子库之间的数据同步走 `/api/sub/*`、`/api/push/secure-records` 和 `/api/export/nct_databack`。
新链路中不再有单独 bootstrap。双方直接以子库 `serviceUrl` 作为 verification seed，按 `NCT-MOTHER-AUTH-HMAC-SHA256-T30-V1` 每 30 秒派生短期 Bearer token，并用相邻时间窗口复算验证。无法验证的 report / form-records 请求会收到伪成功响应，但母库不落库、不触发推送。母库推送 `POST /api/push/secure-records` 和灾备回拉 `GET /api/export/nct_databack` 也带同样的 Bearer token；`nct_databack` 导出文件明文传输，不再使用 proof 字段。
`SUB_REPORT_MIN_INTERVAL_MS` 用于限制主库接收子库上报的最小时间间隔。
`SUB_PULL_BATCH_SIZE` 表示每轮最多处理多少个已登记子库。
`SUB_PULL_RECORD_LIMIT` 表示每次从单个子库拉取多少条 `nct_databack` 记录。
`SUB_PULL_MAX_ATTEMPTS` 表示单次子库导出请求最多连续尝试多少次，默认 5。
`SUB_PULL_RETRY_DELAY_MS` 表示失败后再次请求同一子库导出文件前等待多久，默认 60000 毫秒。
`SUB_PULL_TIMEOUT_MS` 表示主库请求子库导出文件时的超时时间。

生成推荐密钥的最小命令：

```bash
openssl rand -base64 32
```

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

- Vite 前端 Console：`http://127.0.0.1:5174/Console`
- Wrangler 本地 Worker：`http://127.0.0.1:8787`

本地开发时可以这样理解：

- `http://127.0.0.1:5174/Console` 用来看管理台
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

注意：Cloudflare Workers 在生产环境不是传统“监听自定义端口”的模式；`POST /api/ingest` 只作为管理台登录态下的手动写入入口，不再承接外部 token 写入。

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

## Cloudflare Workers 部署

仅推荐使用 Cloudflare Dashboard 的 Workers Builds 网页部署。本项目的 Worker 项目名使用目录名的 Workers 兼容形式：`nct-database`。

网页部署会读取 [`wrangler.toml`](./wrangler.toml)。部署命令里的 `npm run cf:ensure` 会自动创建 D1 数据库 `nct-database`、把真实 `database_id` 写入当前构建环境中的 `wrangler.toml`，并执行远端 D1 migrations；不需要再手动创建 D1 或手动填写 `database_id`。`wrangler.toml` 不包含 `[vars]`，生产变量和密钥以 Cloudflare Dashboard 为准。R2 导出需要账号启用 R2 后再额外添加 `EXPORT_BUCKET` 绑定。

### Workers Builds 填写

| Cloudflare 页面字段 | 填写值 |
| --- | --- |
| Project name | `nct-database` |
| Production branch | 你的生产分支，例如 `main` |
| Path / Root directory | 在本仓库部署填 `NCT_database`；如果本项目单独成库填 `/` |
| Build command | `npm run check` |
| Deploy command | `npm run deploy` |
| Non-production branch deploy command | `npm run deploy:preview` |

### 网页端步骤

1. 进入 Cloudflare Dashboard -> `Workers & Pages` -> `Create` -> `Import a repository`。
2. 选择 Git 仓库后，按上表填写 `Project name`、`Path`、`Build command`、`Deploy command` 和 `Non-production branch deploy command`。
3. 在 `Settings` -> `Variables and Secrets` 配置生产变量：
   - Variables：`APP_NAME`、`DEFAULT_ENCRYPT_FIELDS`、`EXPORT_EMAIL_TO`、`EXPORT_EMAIL_FROM`、`SUB_AUTH_MAX_FAILURES`、`SUB_REPORT_MIN_INTERVAL_MS`、`SUB_PULL_BATCH_SIZE`、`SUB_PULL_MAX_ATTEMPTS`、`SUB_PULL_RECORD_LIMIT`、`SUB_PULL_RETRY_DELAY_MS`、`SUB_PULL_TIMEOUT_MS`
   - Secrets：`ENCRYPTION_KEY`、`RESEND_API_KEY`
4. 在 `Settings` -> `Domains & Routes` -> `Add` -> `Custom Domain` 绑定 `api.example.com`。
5. 推送生产分支触发部署。首次部署时会自动创建 D1、执行 migrations、构建管理台静态资产，然后发布 Worker。

建议生产变量：

```text
APP_NAME=NCT API SQL
DEFAULT_ENCRYPT_FIELDS=name,phone,email,idCard
EXPORT_EMAIL_TO=ops@example.com
EXPORT_EMAIL_FROM=NCT API SQL <exports@example.com>
SUB_AUTH_MAX_FAILURES=5
SUB_REPORT_MIN_INTERVAL_MS=5000
SUB_PULL_BATCH_SIZE=10
SUB_PULL_MAX_ATTEMPTS=5
SUB_PULL_RECORD_LIMIT=100
SUB_PULL_RETRY_DELAY_MS=60000
SUB_PULL_TIMEOUT_MS=10000
```

`ENCRYPTION_KEY` 必须是 base64 编码的 32 字节随机值，可在本地生成后粘贴到 Dashboard Secret：

```bash
openssl rand -base64 32
```

部署后首次打开 `https://api.example.com/Console` 设置管理员密码，再检查：

```text
https://api.example.com/
https://api.example.com/api/health
```

### 生产访问约定

假设你的自定义域名是 `https://api.example.com`，则：

- 公开 JSON：`https://api.example.com/`
- 管理台入口：`https://api.example.com/Console`
- 健康检查：`https://api.example.com/api/health`
- 数据写入：`https://api.example.com/api/ingest`
- 灾备回拉：`https://api.example.com/api/admin/pull-now`
- 子库上报：`https://api.example.com/api/sub/report`
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

`/api/ingest` 现在只给管理台登录态使用，外部 Bearer token 写入已关闭。本地开发示例：

```bash
curl -X POST http://127.0.0.1:8787/api/ingest \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_CONSOLE_SESSION_TOKEN" \
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
  -H "Authorization: Bearer YOUR_CONSOLE_SESSION_TOKEN" \
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

### 手动触发灾备回拉

```bash
curl -X POST https://api.example.com/api/admin/pull-now \
  -H "Authorization: Bearer YOUR_CONSOLE_SESSION_TOKEN"
```

### 子库上报

`/api/sub/report` 只接受带 `serviceWatermark: "nct-api-sql-sub:v1"` 的 `NCT_backend` 上报，其他 `service` 会被拒绝。
母库识别 `serviceWatermark` 后，会用 `serviceUrl` 派生的 30 秒 HMAC Bearer token 验证请求；首次验证成功会登记该子库。

```bash
curl -X POST https://api.example.com/api/sub/report \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer SERVICE_URL_DERIVED_30S_HMAC" \
  -d '{
    "service": "NCT API SQL Sub",
    "serviceWatermark": "nct-api-sql-sub:v1",
    "serviceUrl": "https://sub.example.com",
    "databackVersion": 12,
    "reportCount": 7,
    "reportedAt": "2026-04-20T13:30:00.000Z"
  }'
```

## 验证状态

已经在本地执行通过：

- `npm run test`
- `npm run check`
- `npm run build`
