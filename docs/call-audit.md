# Full call audit migration

Sub2API's call audit is independent from Prompt Audit. It archives the authenticated client request, every final outbound HTTP request (including retries/failover), and the client-visible JSON/SSE response. Smooth weighted scheduling is not part of this migration.

## Storage compatibility

The writer keeps the existing `audit_calls` and `audit_artifacts` schema and the historical S3 key layout:

```text
<prefix>/dt=YYYY-MM-DD/api_key=<id>/request_id=<request-id>/<kind>.json.gz
<prefix>/dt=YYYY-MM-DD/api_key=<id>/request_id=<request-id>/<kind>-<sequence>.json.gz
```

S3 metadata remains `request-id` and `artifact-kind`. Point `CALL_AUDIT_POSTGRES_URL` and the S3 settings at the existing audit database/bucket; no historical copy is required.

Each application replica owns `/app/data/audit-spool` and consumes only its own manifests. The directory must be a persistent volume. Redis is not involved. Inference remains available if PostgreSQL/S3 is down; manifests retry locally and move to `dead-letter/` after five attempts. Retryable dead letters then receive a separate, bounded slow-retry budget; permanent failures remain visible for operator intervention.

The S3 writer identity only needs `s3:PutObject` for the configured prefix. It does not need `GetObject` or `ListBucket`; the query sidecar uses a separate read-only identity. The PostgreSQL writer keeps logical artifacts unique by `(request_id, kind, sequence)`, preserving the oldest externally visible artifact ID across retries.

PostgreSQL DDL is separated from the inference writer as well. The application
writer needs only the DML privileges required for `audit_calls` and
`audit_artifacts`; it never creates tables or indexes. Run
`call-audit-migrate` with a dedicated migration identity that can create and
alter the audit schema. Runtime readiness checks only read PostgreSQL catalogs
and never perform an implicit migration.

## Initial migration

1. Deploy the read-only `audit-query/` sidecar against the existing PostgreSQL/S3 and compare historical list/detail/export results with the old node.
   In the bundled deployment run `docker compose --profile audit-query up -d`;
   `deploy/Caddyfile` publishes it at `/audit-query/` while the container port
   remains bound to host loopback only. Keep `flush_interval -1` for streaming
   gzip NDJSON exports.
2. Apply the initial schema migration and create the current and next month
   partitions without enabling capture:

   ```bash
   CALL_AUDIT_POSTGRES_URL='postgresql://migration:secret@db/audit' \
     /app/call-audit-migrate
   ```

   Subsequent scheduled partition maintenance must use the same DDL-capable
   identity and the narrow partitions-only mode. It takes a PostgreSQL advisory
   lock and idempotently creates only the current and next month partitions and
   their indexes:

   ```bash
   CALL_AUDIT_POSTGRES_URL='postgresql://migration:secret@db/audit' \
     /app/call-audit-migrate --partitions-only
   ```

   Run this periodically (for example daily) so the next partition is ready
   before a UTC month boundary. The application runtime does not replace this
   scheduled migration.
3. Configure writer-only PostgreSQL/S3 credentials, mount `/app/data`, and deploy with `CALL_AUDIT_ENABLED=false`.
4. Verify the deployed writer identity with the truly read-only check, which
   validates connectivity, the base schema, current/next partitions, and
   required indexes without running DDL:

   ```bash
   CALL_AUDIT_POSTGRES_URL='postgresql://writer:secret@db/audit' \
     /app/call-audit-migrate --check
   ```

5. Enable capture for a dedicated API key/account pool. Check the coarse public `/health/call-audit` result and the authenticated `/api/v1/admin/call-audit/health` counters, the spool backlog, S3 object metadata/SHA-256, and audit-query results before moving general traffic. PostgreSQL/schema readiness is probed asynchronously and periodically; a failed probe makes the dedicated Call Audit health degraded without taking inference offline, and a later successful probe or worker write restores readiness.
6. Import accounts with `source_priority_mode=weight` and `refresh_oauth=false`. All imported accounts receive priority 50; the former CRS value is retained only as `extra.crs_priority`.
7. Stop the old CRS inference and OAuth refresh processes before assigning the same accounts to Sub2API.

## Cutover gates

Do not switch the public ingress until production access logs show that active
traffic uses supported authentication and routes. CCR, Bedrock, Azure, Droid,
query-string API keys, and any other account platform not exercised in staging
must be at zero; otherwise the cutover is blocked. Compare route, model,
authentication-header, account-platform, error-envelope, and SSE samples.

CRS account sync does not migrate or overwrite Sub2API users, groups, API keys,
or sticky-session state. Restore only API keys whose plaintext is available;
rotate hash-only keys. Sticky sessions may bind again after cutover. Keep the
gray account pool isolated and use `source_priority_mode=weight` plus
`refresh_oauth=false` until CRS inference and OAuth refresh are stopped.

Before broad traffic, record a no-audit baseline and an enabled-audit load run.
The release gate is audit synchronous overhead p95 <= 5 ms, throughput and
first-token regression <= 5%, and a completed call queryable within 60 seconds.
Also fault PostgreSQL, S3, and the local worker and verify inference remains
available, `capture_failures`/backlog/oldest age/disk waterline/dead-letter
change as expected, and the persistent spool drains after recovery.

Legacy client prefixes are direct aliases: `/api/v1`, `/claude/v1`, `/openai/v1`, `/antigravity/api/v1`, and `/gemini-cli/api/v1`. They reuse the canonical handlers and create one audit call per inbound request.

## Dead-letter recovery and alerts

With capture and the local worker enabled, dead-letter recovery is enabled by
default. The first slow retry is due 15 minutes after failure, then the delay
doubles up to six hours. A bundle receives at most 12 slow attempts, with at most
two due bundles processed per worker pass. Normal ready/retry work retains its
own batch budget. The retry count and failure timestamps are persisted in the
manifest, so a process restart does not renew the retry allowance.

Slow retries share the sequential audit worker. Each attempt's storage work is
limited to 30 seconds (or the shorter claim timeout), with separate bounded
failed-status synchronization. A default historical batch can therefore delay
the next audit batch by about a minute when storage times out. Inference remains
nonblocking; use the pending-age alert to detect delayed audit persistence.

Recovery writes only the existing audit PostgreSQL/S3 records. It retains the
original request ID, timestamps, identity, usage and artifact bytes; it does
not reissue inference or apply business billing. Missing, malformed or unsafe
local artifacts and permanent processing errors remain dead letters. Existing
manifests without the new retry fields are eligible only for recognized
retryable failure codes, after their local bundle has been validated. Keep a
private backup before any manual repair; successful processing removes the
original local spool bundle after durable storage completes.

The worker owns failed-status reconciliation and slow retries in one execution
sequence. A separate maintenance scan cannot overwrite a successfully stored
call as failed. A dead letter being recovered still makes audit readiness
degraded until its durable commit finishes. Exhausted and permanent records
remain unhealthy until repaired; neither the recovery worker nor alerts delete
them to clear the health status.

Configuration is under `call_audit.worker.dead_letter_retry`:

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `CALL_AUDIT_DEAD_LETTER_RETRY_ENABLED` | `true` | Enable slow recovery when the worker is enabled |
| `CALL_AUDIT_DEAD_LETTER_MAX_RETRIES` | `12` | Maximum slow attempts per bundle, 1–100 |
| `CALL_AUDIT_DEAD_LETTER_BATCH_SIZE` | `2` | Maximum slow attempts per pass, 1–100 |
| `CALL_AUDIT_DEAD_LETTER_INITIAL_DELAY_SECONDS` | `900` | Delay before the first slow attempt |
| `CALL_AUDIT_DEAD_LETTER_MAX_DELAY_SECONDS` | `21600` | Maximum delay; must be at least the initial delay and at most seven days |

The authenticated `/api/v1/admin/call-audit/health` snapshot includes pending,
recovering, retryable, permanent and exhausted counts, the oldest unresolved
dead-letter age, and slow-attempt/recovery counters. Counters are process-local;
per-bundle retry limits and ages survive restarts. An old failed counter does
not prevent readiness from recovering after the underlying problem is fixed.

[VM101 monitoring integration](../deploy/call-audit-monitoring/README.md)
exports this snapshot through the existing node_exporter textfile collector
and provides Prometheus rules for persistent dead letters, exhausted/permanent
records, aging backlog, capture failures and a missing or failed collector.
The collector accepts a valid HTTP 503 health snapshot as a successful sample
with degraded readiness. It never exports admin credentials, request IDs,
usernames or artifact contents. Install the collector **and** load its rules;
application retry behavior alone does not deliver notifications.

The CRS self-service reads `/api/v1/key-info`, `/api/v1/me`, and
`/api/v1/organizations/:org_id/usage` are not contract-compatible with
Sub2API and are not aliased by this release. Production access-log checks must
also show these paths at zero; otherwise the cutover is blocked until explicit
adapters and contract tests are added. `/api/v1/usage` is supported through a
single dual-auth route for both legacy API keys and Sub2API management JWTs.

Legacy OpenAI `/v1/completions` requests are converted to Chat Completions exactly once after audit capture, so `client_request` retains the original `prompt` request while `upstream_request` contains the mapped request. Image, video, WebSocket, asynchronous, and batch endpoints remain intentionally outside the first audit scope.

## Retention

Automatic deletion is intentionally disabled for the first rollout. Preview the historical impact first; this mode performs only readiness/catalog reads and the retention aggregate query:

```bash
CALL_AUDIT_POSTGRES_URL='postgresql://writer:secret@db/audit' \
  /app/call-audit-migrate --retention-dry-run --retention-days 180
```

`call_audit.retention_cleanup_enabled` is reserved for the explicitly approved cleanup rollout and defaults to `false`; this release rejects `true` rather than silently deleting data or pretending cleanup is active.

## Rollback

Switch the proxy back to CRS and restore the freshest OAuth credentials before restarting its refresh loop. Do not delete audit rows or objects created by Sub2API; both query nodes can read them.
