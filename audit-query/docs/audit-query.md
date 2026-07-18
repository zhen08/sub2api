# Sub2API AI Audit 查询节点

查询节点是与 Sub2API 主进程隔离的只读 HTTP 服务。Sub2API 继续监听 `8080`，查询节点只绑定宿主机 `127.0.0.1:3100`，Caddy 或 Nginx 通过同一 HTTPS 域名下的 `/audit-query/` 目录转发请求。查询接口和历史 Claude Relay 审计数据保持兼容。

## 1. 准备只读凭据

生成一个长度至少为 32 字符的随机 Token，只把原文交给调用方，服务端保存 SHA-256：

```bash
printf '%s' 'replace-with-a-long-random-token' | shasum -a 256
```

为查询节点创建独立 PostgreSQL 账号。以下 SQL 中的数据库、写入角色和密码需要替换为实际值：

```sql
CREATE ROLE ai_audit_reader LOGIN PASSWORD 'replace-me';
GRANT CONNECT ON DATABASE ai_audit TO ai_audit_reader;
GRANT USAGE ON SCHEMA public TO ai_audit_reader;
GRANT SELECT ON audit_calls, audit_artifacts TO ai_audit_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ai_audit_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE ai_audit_writer IN SCHEMA public
  GRANT SELECT ON TABLES TO ai_audit_reader;
```

S3/MinIO 服务账号只需要读取审计前缀，不需要 `ListBucket` 或写权限：

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::ai-call-audit/ai-call-audit/*"]
    }
  ]
}
```

## 2. 应用数据库迁移

迁移为 artifact 增加 `sequence`，并为已有月分区并发创建查询索引。检查模式不修改数据库：

```bash
cd audit-query
AUDIT_QUERY_MIGRATION_POSTGRES_URL='postgresql://writer:secret@db/ai_audit' \
  npm run migrate:check

AUDIT_QUERY_MIGRATION_POSTGRES_URL='postgresql://writer:secret@db/ai_audit' \
  npm run migrate:apply
```

应在查询节点上线前执行迁移。该 Node 命令只负责历史查询结构和已有分区的索引；应用运行时的审计 worker 不执行 DDL。请另使用具有 DDL 权限的迁移身份定期运行：

```bash
CALL_AUDIT_POSTGRES_URL='postgresql://migration:secret@db/ai_audit' \
  /app/call-audit-migrate --partitions-only
```

该命令通过 advisory lock 幂等确保当前月和下月分区及其索引存在，建议每日执行一次。

## 3. 启动节点

复制 `audit-query/.env.example` 为本地 `.env` 并配置只读凭据：

```dotenv
AUDIT_QUERY_TOKEN_SHA256=<sha256-hex>
AUDIT_QUERY_POSTGRES_URL=postgresql://ai_audit_reader:secret@db/ai_audit
AUDIT_QUERY_S3_ENDPOINT=https://s3.example.com
AUDIT_QUERY_S3_BUCKET=ai-call-audit
AUDIT_QUERY_S3_ACCESS_KEY=<read-only-access-key>
AUDIT_QUERY_S3_SECRET_KEY=<read-only-secret-key>
AUDIT_QUERY_S3_REGION=us-east-1
AUDIT_QUERY_OBJECT_KEY_PREFIX=ai-call-audit
```

查询节点使用独立镜像和 Compose 文件。标准 `deploy/docker-compose.yml` 默认创建 `deploy_sub2api-network`；若部署使用了其他 Compose project name，通过 `docker network ls` 确认实际网络名并设置 `SUB2API_NETWORK`：

```bash
cd audit-query
docker compose --env-file .env -f compose.yml up -d --build audit-query
curl http://127.0.0.1:3100/readyz
```

将 [`config/nginx-audit-query.conf.example`](../config/nginx-audit-query.conf.example) 放入现有 HTTPS `server` 块。配置中的 `proxy_pass` 末尾 `/` 会把外部 `/audit-query/` 前缀移除。Compose 默认只把 `3100` 暴露在宿主机回环地址，不应改为内网或公网地址。

## 4. API

所有 `/v1/audit/*` 请求使用原始 Token，而不是 SHA-256 值：

```bash
curl -H 'Authorization: Bearer <raw-token>' \
  'https://aiproxy.example.com/audit-query/v1/audit/calls?from=2026-07-09T00:00:00Z&to=2026-07-10T00:00:00Z&userUsername=alice&limit=50'
```

列表查询支持以下精确匹配过滤器：

- `requestId`、`userId`、`userUsername`
- `apiKeyId`、`apiKeyName`
- `protocol`、`model`、`status`、`statusCode`、`captureStatus`
- `from`、`to`、`cursor`、`limit`

未提供时间时默认查询最近 24 小时；单次范围最多 7 天，单页最多 200 条。详情和 artifact 接口分别为：

```text
GET /audit-query/v1/audit/calls/:requestId
GET /audit-query/v1/audit/artifacts/:artifactId
```

流式导出要求显式提供 `from` 和 `to`：

```bash
curl --compressed \
  -H 'Authorization: Bearer <raw-token>' \
  -H 'Content-Type: application/json' \
  -X POST 'https://aiproxy.example.com/audit-query/v1/audit/exports/stream' \
  -d '{
    "from":"2026-07-09T00:00:00Z",
    "to":"2026-07-10T00:00:00Z",
    "limit":10000,
    "filters":{"userUsername":"alice"},
    "artifactKinds":["client_request","response","upstream_request"]
  }'
```

响应为 gzip 压缩的 `application/x-ndjson`，依次包含：

1. `header`：查询范围、过滤器、artifact 类型和上限。
2. `record`：调用元数据、成功读取的 artifact，以及当前记录的 `artifactErrors`。
3. `summary`：导出数量、artifact 失败数、是否截断和继续查询用的 `nextCursor`。

单个 S3 对象缺失、校验失败或损坏时，导出会记录错误并继续。详情接口则返回对应的 `404`、`422` 或 `502`。查询节点不会执行任意 SQL、正文关键词搜索或任何数据写入。
