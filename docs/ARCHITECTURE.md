# Reader 架构

## 服务拓扑

| 服务 | 职责 |
|---|---|
| `web` | Next.js 页面与同源 API 代理 |
| `api` | FastAPI 业务 API、鉴权与健康检查 |
| `worker-fetch` | RSS 抓取、正文提取与图片预取 |
| `worker-llm` | Embedding 等异步模型任务 |
| `postgres` | PostgreSQL + pgvector，保存事实与派生投影 |
| `redis` | RQ 队列、短期批量操作清单与调度状态 |
| `schema-gate` | 启动前只读确认数据库 revision 等于代码 head |

## 核心数据层

- `Source`：订阅对象及抓取、隐私、外发许可。
- `SourceEntryIdentity`：来源发布对象跨 revision 的稳定身份。
- `RawEntry`：每次采集到的不可变来源证据。
- `Document` / `ContentItem`：当前可阅读投影与派生状态。
- `Cluster`：可重建的相似内容分组。
- `Event` / `EventRevision` / `EventEvidenceVersion`：稳定事件、固定版本和可定位证据。
- `EvidenceSnapshot` / `SynthesisVersion`：一次生成实际消费的证据集合与不可变合成稿。
- `InteractionEvent`：用户动作事实；`UserState` 是其当前投影。
- `GenerationRequest` / `GenerationAttempt` / `GenerationResult` / `GenerationApplication`：生成请求、执行、不可变结果和结果应用的分离生命周期。
- `LLMTask`：仅保留历史生成记录兼容和翻译缓存；新生成任务不再写入。

## 数据不变量

1. `RawEntry`、Event Revision、Evidence Snapshot 与 Interaction Event 只追加，不原位改写历史事实。
2. Source Entry 身份与每次采集 revision 分离；相同 payload 幂等，变化 payload 追加 revision。
3. Cluster 可重建，Event 身份不可由当前 Cluster ID 替代。
4. 合成稿只能引用其 Evidence Snapshot 中的证据；非法或迟到结果不得推进当前指针。
5. 阅读、收藏、稍后阅读和显式反馈只由用户动作改变，抓取、聚类和模型生成不得代写。
6. 私密来源只能使用本地模型；未分类来源或未明确允许外发的公开来源不得发送给远端 API。
7. 生成结果先保存为不可变 Result，再单独应用；应用失败只重放同一 Result，不再次调用模型。
8. 历史 Alembic migration 冻结；契约变化只新增 revision。

## 内容管道

1. Fetch worker 获取 Feed，使用 ETag、Last-Modified 和 payload hash 跳过无变化响应。
2. 每个条目解析为稳定 Source Entry Identity 与不可变 RawEntry revision。
3. 启用全文抓取的来源通过安全公共网络入口获取网页，按手工 selector、公共规则、Trafilatura、RSS 顺序选择正文。
4. 同一规范化正文树同时生成 `reading_html` 与 `content_text`；前者用于阅读，后者用于搜索、Embedding、翻译和生成。
5. 正文图片统一经过安全代理和持久缓存；每次重定向都重新校验目标地址和大小限制。
6. 新内容进入 Embedding、Cluster 和 Event projection；失败只影响当前来源或派生阶段，不删除已经提交的证据。

## 生成与模型

- 本地模型使用 LM Studio 风格接口；远端模型使用 OpenAI-compatible HTTP API。
- 翻译、摘要、Event 合成和报告都由明确的 provider 设置选择，不静默 fallback。
- 远端调用在创建 payload 前解析完整来源集合并执行隐私与 allowlist 门禁。
- Event 合成固定 Evidence Snapshot，输出使用严格 JSON schema，并验证每个引用。
- Generation lifecycle 记录 provider、模型、prompt/schema 版本、输入 fingerprint、token 用量、状态与结果应用。
- 任务读取 API 不调用模型；只有明确的生成动作会创建 Attempt。

## API 与安全边界

- 除 `/health` 外，API 路由要求 `X-Reader-API-Token`；Web 同源代理负责注入。
- 浏览器写操作按 `READER_DEPLOY_URL` 校验 Origin/Referer。
- 外部抓取逐跳拒绝 loopback、私网、本地链路地址、带凭据 URL、超量响应和主动 SVG 内容。
- 模型密钥只保存在数据库写入型设置或私有环境文件，读取 API 只返回“是否已配置”。
- `/about` 独立检查 DB、Redis、LLM 与 Embedding；单项失败不遮蔽其它结果。

## Schema 与部署

当前 Alembic head 为 `0072_reading_body_contract`。API 和 worker 启动只检查 schema head，不自动迁移；数据库升级应作为独立维护步骤执行。仓库根目录的 Compose 文件提供通用自托管入口。
