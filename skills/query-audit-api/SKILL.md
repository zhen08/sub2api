---
name: query-audit-api
description: 通过 Sub2API 的 Claude Relay 兼容 audit-query 只读 HTTP API 查询、分页、读取调用详情与 artifact，并导出 gzip NDJSON 以分析审计数据。Use when Codex needs to investigate AI request/response history, user or API-key usage, failures, token or cost anomalies, request IDs, audit artifacts, or mentions Sub2API audit query, audit-query, 审计查询, or 审计导出.
---

# Query Audit API

通过只读 API 检索 Sub2API 及其继承的 Claude Relay 历史审计数据，并用最小必要范围回答问题。优先使用随 Skill 提供的标准库 CLI，避免重复拼接 curl、泄露 Token 或错误处理分页。

## 配置

要求调用方提供：

```bash
export AUDIT_QUERY_API_BASE_URL='https://aiproxy.example.com/audit-query'
export AUDIT_QUERY_API_TOKEN='<raw-bearer-token>'
```

把 Base URL 指向外部 `/audit-query` 根路径，不要包含 `/v1/audit`。只从 `AUDIT_QUERY_API_TOKEN` 读取原始 Token；不要打印 Token、把 Token 写入命令参数、提交到仓库或误用服务端保存的 SHA-256 值。

把 `<skill-root>` 替换为本 Skill 的实际目录，然后运行：

```bash
python3 <skill-root>/scripts/audit_query.py ready
```

若环境变量未配置，明确指出缺少的配置；不要猜测地址或凭据。

## 查询工作流

1. 明确问题中的时间范围、身份、模型、协议、状态或 request ID。未给时间时先查最近 24 小时，并在结论中说明这一默认范围。
2. 在连接状态未知时运行 `ready`。`health` 只证明进程存活，`ready` 还检查配置和数据库模式。
3. 先用 `list` 检索元数据。使用尽可能窄的精确过滤条件和合理的 `--limit`。
4. 检查 `hasMore`、`nextCursor` 和 `truncatedByClient`。只有问题需要全量统计时才使用 `--all`，并设置合适的 `--max-records`。
5. 获得 request ID 后用 `call` 查询调用详情及 artifact 描述符。仅在问题确实需要请求或响应正文时，再用 `artifact` 读取单个对象。
6. 对跨多条调用的正文分析使用 `export`。显式提供 `--from` 和 `--to`，限定 artifact 类型和记录上限，并优先输出到临时 NDJSON 文件，避免把大量敏感正文塞进对话上下文。
7. 汇总结果时报告查询范围、过滤器、返回数量、分页或截断状态，以及任何 `artifactErrors`。把 `summary.complete=false` 或缺少 summary 视为不完整结果。

## 常用命令

查询列表：

```bash
python3 <skill-root>/scripts/audit_query.py list \
  --from '2026-07-15T00:00:00Z' \
  --to '2026-07-16T00:00:00Z' \
  --user-username alice \
  --status error \
  --limit 50
```

自动分页但限制最多读取 1000 条：

```bash
python3 <skill-root>/scripts/audit_query.py list --all --max-records 1000 \
  --model claude-sonnet-4-5
```

查询调用详情或 artifact：

```bash
python3 <skill-root>/scripts/audit_query.py call '<request-id>'
python3 <skill-root>/scripts/audit_query.py artifact '<artifact-id>' \
  --output /tmp/audit-artifact.json
```

导出解压后的 NDJSON：

```bash
python3 <skill-root>/scripts/audit_query.py export \
  --from '2026-07-15T00:00:00Z' \
  --to '2026-07-16T00:00:00Z' \
  --user-username alice \
  --artifact-kind client_request \
  --artifact-kind response \
  --limit 10000 \
  --output /tmp/audit-export.ndjson
```

## 查询边界

- 只执行 API 已支持的精确匹配过滤；不要声称支持正文关键词搜索、模糊搜索、任意 SQL 或数据写入。
- 默认单次时间范围上限为 7 天、单页上限为 200；服务端配置可能调整这些值。更长周期按不重叠时间窗分段查询，并避免重复计数边界记录。
- `statusCode` 只接受 100 到 599。artifact 类型只使用 `client_request`、`upstream_request`、`response`。
- 用户名等身份信息来自 PostgreSQL 审计快照；不要把 S3 metadata 当作完整身份来源。
- 把原始请求和响应视为敏感数据。仅提取回答问题所需的最小片段，并避免在最终回复中复述密钥、完整 prompt 或个人数据。
- 若收到 401，先确认使用的是原始 Token 而非 SHA-256。若收到 400，检查 UTC 时间、范围、精确字段和 cursor。若 artifact 返回 404、422 或 502，保留元数据并把正文标记为不可用，不要假定整条调用不存在。

需要确认端点、字段、响应结构或错误语义时，读取 [references/api.md](references/api.md)。
