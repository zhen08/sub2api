# Audit Query API reference

## Authentication and base path

Use the original Bearer token on every `/v1/audit/*` request:

```text
Authorization: Bearer <raw-token>
```

Do not send the SHA-256 value stored by the server. `GET /healthz` and `GET /readyz` are unauthenticated. When deployed behind the documented reverse proxy, the public base path is `/audit-query`, while the internal service receives paths without that prefix.

## Endpoints

### `GET /healthz`

Return process liveness:

```json
{"status":"ok","service":"audit-query"}
```

### `GET /readyz`

Return HTTP 200 with `status: ready` when configuration and the PostgreSQL schema are usable. Return HTTP 503 with `status: not_ready` otherwise.

### `GET /v1/audit/calls`

Accept exact-match query parameters:

- Identity: `requestId`, `userId`, `userUsername`, `apiKeyId`, `apiKeyName`
- Request: `protocol`, `model`
- Outcome: `status`, `statusCode`, `captureStatus`
- Window and pagination: `from`, `to`, `cursor`, `limit`

If `from` and `to` are absent, query the most recent 24 hours by default. The default maximum range is 7 days. The documented single-page maximum is 200.

Return:

```json
{
  "success": true,
  "data": {
    "calls": [],
    "hasMore": false,
    "nextCursor": null
  }
}
```

Treat `cursor` as opaque. Keep all other filters and the time range unchanged when requesting the next page.

Each call can contain:

- IDs and timestamps: `requestId`, `createdAt`, `requestStartedAt`, `updatedAt`, `retentionUntil`
- Routing: `endpoint`, `method`, `protocol`, `model`, `stream`
- Identity: `apiKeyId`, `apiKeyName`, `userId`, `userUsername`, `accountId`, `accountType`
- Outcome: `status`, `statusCode`, `captureStatus`, `error`, `meta`
- Usage: `inputTokens`, `outputTokens`, `cacheReadTokens`, `cacheCreateTokens`, `totalTokens`
- Cost: `cost`, `realCost`

Token and cost values are serialized as strings. Convert them deliberately before arithmetic and do not silently treat missing values as measured zero.

### `GET /v1/audit/calls/:requestId`

Return one retained call plus its artifact descriptors:

```json
{
  "success": true,
  "data": {
    "call": {},
    "artifacts": []
  }
}
```

An artifact descriptor can contain `id`, `requestId`, `kind`, `sequence`, `bucket`, `objectKey`, `bytes`, `sha256`, `contentType`, and `createdAt`.

### `GET /v1/audit/artifacts/:artifactId`

Verify, decompress, and parse the stored S3/MinIO artifact. Return:

```json
{
  "success": true,
  "data": {
    "artifact": {},
    "payload": {}
  }
}
```

Use the numeric artifact `id`, not `objectKey` or `requestId`.

### `POST /v1/audit/exports/stream`

Require top-level `from` and `to`. Accept:

```json
{
  "from": "2026-07-15T00:00:00Z",
  "to": "2026-07-16T00:00:00Z",
  "cursor": null,
  "limit": 10000,
  "filters": {"userUsername": "alice", "status": "error"},
  "artifactKinds": ["client_request", "response"]
}
```

`filters` accepts the exact-match fields supported by the list endpoint. `artifactKinds` accepts `client_request`, `upstream_request`, and `response`; the default is `client_request` plus `response`.

Return gzip-compressed `application/x-ndjson` records in this order:

1. `header`: schema version, start time, range, filters, artifact kinds, and limit.
2. `record`: one `call`, zero or more successfully read `artifacts`, and zero or more `artifactErrors`.
3. `summary`: `complete`, `exportedRecords`, `artifactFailures`, `truncated`, `nextCursor`, and finish time.

One missing or corrupt artifact does not abort the export. Inspect every record's `artifactErrors` and the final summary. A stream-level failure can end with `complete: false` and an error code such as `export_timeout`, `export_aborted`, or `export_failed`.

## Error envelope

Errors returned before streaming use:

```json
{
  "success": false,
  "error": {
    "code": "invalid_query",
    "message": "...",
    "requestId": "...",
    "details": {}
  }
}
```

Common statuses include:

- 400: invalid date, range, cursor, status code, artifact kind, request ID, artifact ID, or JSON body
- 401: missing or invalid raw Bearer token
- 404: route, retained call, or artifact not found
- 413: export request body exceeds 64 KiB
- 422: artifact integrity or content validation failed
- 429: concurrent export limit reached
- 502: object storage read failed
- 503: readiness failed

Capture the HTTP status, API error code, API request ID, and the `X-Request-Id` response header when troubleshooting. Never include the Bearer token in logs or reports.
