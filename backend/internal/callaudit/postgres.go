package callaudit

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/lib/pq"
)

const schemaAdvisoryLockID int64 = 0x43414c4c41554449 // "CALLAUDI"

// StoredArtifact is the durable object metadata written to audit_artifacts.
// Its wire names intentionally match the legacy Node writer.
type StoredArtifact struct {
	Kind        ArtifactKind
	Sequence    int
	Bucket      string
	ObjectKey   string
	Bytes       int64
	SHA256      string
	ContentType string
	CreatedAt   time.Time
}

// PostgreSQLStore writes the historical audit_calls/audit_artifacts contract.
// It deliberately uses an independent pool so audit credentials can be
// restricted separately from the main Sub2API database.
type PostgreSQLStore struct {
	db *sql.DB
}

func OpenPostgreSQLStore(postgresURL string, maxOpen, maxIdle int) (*PostgreSQLStore, error) {
	if strings.TrimSpace(postgresURL) == "" {
		return nil, fmt.Errorf("audit postgres URL is required")
	}
	db, err := sql.Open("postgres", postgresURL)
	if err != nil {
		return nil, fmt.Errorf("open audit postgres: %w", err)
	}
	if maxOpen <= 0 {
		maxOpen = 5
	}
	if maxIdle < 0 {
		maxIdle = 0
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(30 * time.Minute)
	return &PostgreSQLStore{db: db}, nil
}

func (s *PostgreSQLStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *PostgreSQLStore) Ping(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("audit postgres is not configured")
	}
	return s.db.PingContext(ctx)
}

// EnsureSchema performs the initial legacy-schema migration and pre-creates
// the current and next month partitions. Only the dedicated migration command
// should call it; the writer runtime deliberately has no DDL responsibility.
func (s *PostgreSQLStore) EnsureSchema(ctx context.Context, reference time.Time) error {
	return s.ensureSchemaAndPartitions(ctx, reference, true)
}

// EnsurePartitions pre-creates the current and next month partitions without
// re-running the global base migration. It is the implementation behind the
// dedicated migration command's --partitions-only mode, not a writer hot path.
func (s *PostgreSQLStore) EnsurePartitions(ctx context.Context, reference time.Time) error {
	return s.ensureSchemaAndPartitions(ctx, reference, false)
}

func (s *PostgreSQLStore) ensureSchemaAndPartitions(ctx context.Context, reference time.Time, migrateBase bool) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("audit postgres is not configured")
	}
	if reference.IsZero() {
		reference = time.Now()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin audit schema transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, schemaAdvisoryLockID); err != nil {
		return fmt.Errorf("lock audit schema: %w", err)
	}
	if migrateBase {
		if _, err := tx.ExecContext(ctx, auditBaseSchemaSQL); err != nil {
			return fmt.Errorf("create audit schema: %w", err)
		}
	}
	for _, month := range []time.Time{reference.UTC(), reference.UTC().AddDate(0, 1, 0)} {
		if err := ensureMonthlyPartition(ctx, tx, month); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit audit schema: %w", err)
	}
	return nil
}

func ensureMonthlyPartition(ctx context.Context, tx *sql.Tx, reference time.Time) error {
	start := time.Date(reference.Year(), reference.Month(), 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	partition := auditPartitionName(start)
	statements := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s PARTITION OF audit_calls FOR VALUES FROM ('%s') TO ('%s')`, partition, start.Format(time.RFC3339), end.Format(time.RFC3339)),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_query_order_idx ON %s (created_at DESC, request_id DESC)`, partition, partition),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_request_id_idx ON %s (request_id)`, partition, partition),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_user_query_idx ON %s (user_id, created_at DESC, request_id DESC) WHERE user_id IS NOT NULL`, partition, partition),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_api_key_query_idx ON %s (api_key_id, created_at DESC, request_id DESC) WHERE api_key_id IS NOT NULL`, partition, partition),
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("ensure audit partition %s: %w", partition, err)
		}
	}
	return nil
}

type readinessColumn struct {
	table  string
	column string
}

type readinessIndex struct {
	name   string
	table  string
	unique bool
}

type readinessRequirements struct {
	columns    []readinessColumn
	partitions []string
	indexes    []readinessIndex
}

func auditReadinessRequirements(reference time.Time) readinessRequirements {
	if reference.IsZero() {
		reference = time.Now()
	}
	columns := make([]readinessColumn, 0, len(auditCallsReadinessColumns)+len(auditArtifactsReadinessColumns))
	for _, column := range auditCallsReadinessColumns {
		columns = append(columns, readinessColumn{table: "audit_calls", column: column})
	}
	for _, column := range auditArtifactsReadinessColumns {
		columns = append(columns, readinessColumn{table: "audit_artifacts", column: column})
	}

	partitions := make([]string, 0, 2)
	indexes := []readinessIndex{
		{name: "audit_calls_pkey", table: "audit_calls", unique: true},
		{name: "audit_artifacts_pkey", table: "audit_artifacts", unique: true},
		{name: "audit_artifacts_request_id_id_idx", table: "audit_artifacts"},
		{name: "audit_artifacts_request_kind_sequence_uidx", table: "audit_artifacts", unique: true},
	}
	for _, month := range []time.Time{reference.UTC(), reference.UTC().AddDate(0, 1, 0)} {
		partition := auditPartitionName(month)
		partitions = append(partitions, partition)
		indexes = append(indexes,
			readinessIndex{name: partition + "_query_order_idx", table: partition},
			readinessIndex{name: partition + "_request_id_idx", table: partition},
			readinessIndex{name: partition + "_user_query_idx", table: partition},
			readinessIndex{name: partition + "_api_key_query_idx", table: partition},
		)
	}
	return readinessRequirements{columns: columns, partitions: partitions, indexes: indexes}
}

func auditPartitionName(reference time.Time) string {
	utc := reference.UTC()
	return fmt.Sprintf("audit_calls_%04d_%02d", utc.Year(), int(utc.Month()))
}

// CheckReadiness performs only catalog reads and a connectivity check. It
// verifies the base schema, all columns used by the writer/query contract, the
// current and next month partitions, and the indexes required for idempotent
// upserts and query pagination. It never creates or alters database objects.
func (s *PostgreSQLStore) CheckReadiness(ctx context.Context, reference time.Time) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("audit postgres is not configured")
	}
	if reference.IsZero() {
		reference = time.Now()
	}
	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping audit postgres: %w", err)
	}

	var callsPartitioned, artifactsTable bool
	if err := s.db.QueryRowContext(ctx, auditReadinessRelationsSQL).Scan(&callsPartitioned, &artifactsTable); err != nil {
		return fmt.Errorf("check audit relations: %w", err)
	}
	issues := make([]string, 0, 4)
	if !callsPartitioned {
		issues = append(issues, "audit_calls is missing or is not partitioned")
	}
	if !artifactsTable {
		issues = append(issues, "audit_artifacts is missing or is not a table")
	}
	if len(issues) > 0 {
		return fmt.Errorf("audit postgres is not ready: %s", strings.Join(issues, "; "))
	}

	requirements := auditReadinessRequirements(reference)
	tables := make([]string, 0, len(requirements.columns))
	columns := make([]string, 0, len(requirements.columns))
	for _, requirement := range requirements.columns {
		tables = append(tables, requirement.table)
		columns = append(columns, requirement.column)
	}
	missingColumns, err := s.readinessNames(ctx, auditReadinessColumnsSQL, pq.Array(tables), pq.Array(columns))
	if err != nil {
		return fmt.Errorf("check audit columns: %w", err)
	}
	if len(missingColumns) > 0 {
		issues = append(issues, "missing columns: "+strings.Join(missingColumns, ", "))
	}

	missingPartitions, err := s.readinessNames(ctx, auditReadinessPartitionsSQL, pq.Array(requirements.partitions))
	if err != nil {
		return fmt.Errorf("check audit partitions: %w", err)
	}
	if len(missingPartitions) > 0 {
		issues = append(issues, "missing partitions: "+strings.Join(missingPartitions, ", "))
	}

	indexNames := make([]string, 0, len(requirements.indexes))
	indexTables := make([]string, 0, len(requirements.indexes))
	indexUnique := make([]bool, 0, len(requirements.indexes))
	for _, requirement := range requirements.indexes {
		indexNames = append(indexNames, requirement.name)
		indexTables = append(indexTables, requirement.table)
		indexUnique = append(indexUnique, requirement.unique)
	}
	missingIndexes, err := s.readinessNames(ctx, auditReadinessIndexesSQL, pq.Array(indexNames), pq.Array(indexTables), pq.Array(indexUnique))
	if err != nil {
		return fmt.Errorf("check audit indexes: %w", err)
	}
	if len(missingIndexes) > 0 {
		issues = append(issues, "missing or invalid indexes: "+strings.Join(missingIndexes, ", "))
	}
	if len(issues) > 0 {
		return fmt.Errorf("audit postgres is not ready: %s", strings.Join(issues, "; "))
	}
	return nil
}

func (s *PostgreSQLStore) readinessNames(ctx context.Context, query string, args ...any) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

// UpsertCall persists metadata before object upload as well as after success,
// eliminating the historical usage-update-before-call-insert race.
func (s *PostgreSQLStore) UpsertCall(ctx context.Context, manifest Manifest, captureStatus CaptureStatus, captureErr error) error {
	meta, err := json.Marshal(manifest.Meta)
	if err != nil {
		return fmt.Errorf("marshal audit metadata: %w", err)
	}
	statusCode := any(nil)
	if manifest.StatusCode != nil {
		statusCode = *manifest.StatusCode
	}
	errorText := any(nil)
	if captureErr != nil {
		errorText = captureErr.Error()
	}
	usage := manifest.Usage
	_, err = s.db.ExecContext(ctx, auditUpsertCallSQL,
		manifest.RequestID, manifest.CreatedAt, nullableTime(manifest.RequestStartedAt), nullableString(manifest.Endpoint), nullableString(manifest.Method), nullableString(string(manifest.Protocol)),
		nullableString(manifest.APIKeyID), nullableString(manifest.APIKeyName), nullableString(manifest.UserID), nullableString(manifest.UserUsername),
		nullableString(usage.AccountID), nullableString(usage.AccountType), nullableString(usage.Model), nullableString(string(manifest.Status)), statusCode, manifest.Stream,
		usage.InputTokens, usage.OutputTokens, usage.CacheReadTokens, usage.CacheCreateTokens, usage.TotalTokens,
		decimalOrZero(usage.Cost), decimalOrZero(usage.RealCost), manifest.RetentionUntil, string(captureStatus), errorText, meta,
	)
	if err != nil {
		return fmt.Errorf("upsert audit call: %w", err)
	}
	return nil
}

// StoreCompleted atomically marks the call stored and replaces its artifact
// metadata. S3 keys are deterministic, so a retry before this transaction is
// idempotent.
func (s *PostgreSQLStore) StoreCompleted(ctx context.Context, manifest Manifest, artifacts []StoredArtifact) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin audit store transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	meta, err := json.Marshal(manifest.Meta)
	if err != nil {
		return fmt.Errorf("marshal audit metadata: %w", err)
	}
	statusCode := any(nil)
	if manifest.StatusCode != nil {
		statusCode = *manifest.StatusCode
	}
	u := manifest.Usage
	if _, err := tx.ExecContext(ctx, auditUpsertCallSQL,
		manifest.RequestID, manifest.CreatedAt, nullableTime(manifest.RequestStartedAt), nullableString(manifest.Endpoint), nullableString(manifest.Method), nullableString(string(manifest.Protocol)),
		nullableString(manifest.APIKeyID), nullableString(manifest.APIKeyName), nullableString(manifest.UserID), nullableString(manifest.UserUsername),
		nullableString(u.AccountID), nullableString(u.AccountType), nullableString(u.Model), nullableString(string(manifest.Status)), statusCode, manifest.Stream,
		u.InputTokens, u.OutputTokens, u.CacheReadTokens, u.CacheCreateTokens, u.TotalTokens,
		decimalOrZero(u.Cost), decimalOrZero(u.RealCost), manifest.RetentionUntil, string(CaptureStored), nil, meta,
	); err != nil {
		return fmt.Errorf("store audit call: %w", err)
	}
	for _, artifact := range artifacts {
		createdAt := artifact.CreatedAt
		if createdAt.IsZero() {
			createdAt = time.Now().UTC()
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO audit_artifacts
			(call_id, request_id, kind, sequence, bucket, object_key, bytes, sha256, content_type, created_at)
			VALUES ($1,$1,$2,$3,$4,$5,$6,$7,$8,$9)
			ON CONFLICT (request_id, kind, sequence) DO UPDATE SET
			  call_id=EXCLUDED.call_id,
			  bucket=EXCLUDED.bucket,
			  object_key=EXCLUDED.object_key,
			  bytes=EXCLUDED.bytes,
			  sha256=EXCLUDED.sha256,
			  content_type=EXCLUDED.content_type`,
			manifest.RequestID, string(artifact.Kind), artifact.Sequence, artifact.Bucket, artifact.ObjectKey,
			artifact.Bytes, artifact.SHA256, artifact.ContentType, createdAt,
		); err != nil {
			return fmt.Errorf("insert audit artifact metadata: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit audit store transaction: %w", err)
	}
	return nil
}

func (s *PostgreSQLStore) MarkFailed(ctx context.Context, requestID string, captureErr error) error {
	if s == nil || s.db == nil || strings.TrimSpace(requestID) == "" {
		return nil
	}
	message := "audit capture failed"
	if captureErr != nil {
		message = captureErr.Error()
	}
	_, err := s.db.ExecContext(ctx, `UPDATE audit_calls SET capture_status=$2, error=$3, updated_at=now() WHERE request_id=$1`, requestID, string(CaptureFailed), message)
	return err
}

type RetentionPreview struct {
	Calls     int64     `json:"calls"`
	Artifacts int64     `json:"artifacts"`
	Bytes     int64     `json:"bytes"`
	Oldest    time.Time `json:"oldest,omitempty"`
	Newest    time.Time `json:"newest,omitempty"`
}

func (s *PostgreSQLStore) PreviewRetention(ctx context.Context, cutoff time.Time) (RetentionPreview, error) {
	var preview RetentionPreview
	var oldest, newest sql.NullTime
	err := s.db.QueryRowContext(ctx, `
		SELECT count(DISTINCT c.request_id), count(a.id), COALESCE(sum(a.bytes),0), min(c.created_at), max(c.created_at)
		FROM audit_calls c LEFT JOIN audit_artifacts a ON a.request_id=c.request_id
		WHERE c.created_at <= $1`, cutoff).Scan(&preview.Calls, &preview.Artifacts, &preview.Bytes, &oldest, &newest)
	if err != nil {
		return RetentionPreview{}, err
	}
	if oldest.Valid {
		preview.Oldest = oldest.Time
	}
	if newest.Valid {
		preview.Newest = newest.Time
	}
	return preview, nil
}

func nullableString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func decimalOrZero(value string) string {
	if strings.TrimSpace(value) == "" {
		return "0"
	}
	return value
}

var auditCallsReadinessColumns = []string{
	"request_id", "created_at", "request_started_at", "endpoint", "method", "protocol",
	"api_key_id", "api_key_name", "user_id", "user_username", "account_id", "account_type",
	"model", "status", "status_code", "stream", "input_tokens", "output_tokens",
	"cache_read_tokens", "cache_create_tokens", "total_tokens", "cost", "real_cost",
	"retention_until", "capture_status", "error", "meta", "updated_at",
}

var auditArtifactsReadinessColumns = []string{
	"id", "call_id", "request_id", "kind", "sequence", "bucket", "object_key", "bytes",
	"sha256", "content_type", "created_at",
}

const auditReadinessRelationsSQL = `
SELECT
  COALESCE((SELECT relation.relkind = 'p'
            FROM pg_class relation
            WHERE relation.oid = to_regclass('audit_calls')), FALSE),
  COALESCE((SELECT relation.relkind = 'r'
            FROM pg_class relation
            WHERE relation.oid = to_regclass('audit_artifacts')), FALSE)`

const auditReadinessColumnsSQL = `
WITH required(table_name, column_name) AS (
  SELECT * FROM unnest($1::text[], $2::text[])
)
SELECT required.table_name || '.' || required.column_name
FROM required
WHERE NOT EXISTS (
  SELECT 1
  FROM pg_attribute attribute
  WHERE attribute.attrelid = to_regclass(required.table_name)
    AND attribute.attname = required.column_name
    AND attribute.attnum > 0
    AND NOT attribute.attisdropped
)
ORDER BY 1`

const auditReadinessPartitionsSQL = `
WITH required(partition_name) AS (
  SELECT unnest($1::text[])
)
SELECT required.partition_name
FROM required
WHERE NOT EXISTS (
  SELECT 1
  FROM pg_inherits inheritance
  WHERE inheritance.inhparent = to_regclass('audit_calls')
    AND inheritance.inhrelid = to_regclass(required.partition_name)
)
ORDER BY 1`

const auditReadinessIndexesSQL = `
WITH required(index_name, table_name, must_be_unique) AS (
  SELECT * FROM unnest($1::text[], $2::text[], $3::boolean[])
)
SELECT required.index_name
FROM required
WHERE NOT EXISTS (
  SELECT 1
  FROM pg_class index_relation
  JOIN pg_index index_state ON index_state.indexrelid = index_relation.oid
  WHERE index_relation.oid = to_regclass(required.index_name)
    AND index_state.indrelid = to_regclass(required.table_name)
    AND index_state.indisvalid
    AND index_state.indisready
    AND (NOT required.must_be_unique OR index_state.indisunique)
)
ORDER BY 1`

var auditBaseSchemaSQL = `
CREATE TABLE IF NOT EXISTS audit_calls (
  request_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  request_started_at TIMESTAMPTZ NULL,
  endpoint TEXT NULL,
  method TEXT NULL,
  protocol TEXT NULL,
  api_key_id TEXT NULL,
  api_key_name TEXT NULL,
  user_id TEXT NULL,
  user_username TEXT NULL,
  account_id TEXT NULL,
  account_type TEXT NULL,
  model TEXT NULL,
  status TEXT NULL,
  status_code INTEGER NULL,
  stream BOOLEAN NOT NULL DEFAULT FALSE,
  input_tokens BIGINT NOT NULL DEFAULT 0,
  output_tokens BIGINT NOT NULL DEFAULT 0,
  cache_read_tokens BIGINT NOT NULL DEFAULT 0,
  cache_create_tokens BIGINT NOT NULL DEFAULT 0,
  total_tokens BIGINT NOT NULL DEFAULT 0,
  cost NUMERIC(20,10) NOT NULL DEFAULT 0,
  real_cost NUMERIC(20,10) NOT NULL DEFAULT 0,
  retention_until TIMESTAMPTZ NOT NULL,
  capture_status TEXT NOT NULL,
  error TEXT NULL,
  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (request_id, created_at)
) PARTITION BY RANGE (created_at);
ALTER TABLE IF EXISTS audit_calls ADD COLUMN IF NOT EXISTS user_username TEXT NULL;
CREATE TABLE IF NOT EXISTS audit_artifacts (
  id BIGSERIAL PRIMARY KEY,
  call_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  sequence INTEGER NOT NULL DEFAULT 0,
  bucket TEXT NOT NULL,
  object_key TEXT NOT NULL,
  bytes BIGINT NOT NULL,
  sha256 TEXT NOT NULL,
  content_type TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE IF EXISTS audit_artifacts ADD COLUMN IF NOT EXISTS sequence INTEGER NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS audit_artifacts_request_id_id_idx ON audit_artifacts (request_id,id);
-- Retain the oldest externally visible artifact id if the legacy delete/insert
-- writer left duplicate logical artifacts after a crash or concurrent retry.
DELETE FROM audit_artifacts newer
USING audit_artifacts older
WHERE newer.request_id=older.request_id
  AND newer.kind=older.kind
  AND newer.sequence=older.sequence
  AND newer.id>older.id;
CREATE UNIQUE INDEX IF NOT EXISTS audit_artifacts_request_kind_sequence_uidx
ON audit_artifacts (request_id,kind,sequence);`

var auditUpsertCallSQL = `
INSERT INTO audit_calls (
 request_id,created_at,request_started_at,endpoint,method,protocol,
 api_key_id,api_key_name,user_id,user_username,account_id,account_type,model,
 status,status_code,stream,input_tokens,output_tokens,cache_read_tokens,
 cache_create_tokens,total_tokens,cost,real_cost,retention_until,capture_status,error,meta,updated_at
) VALUES (
 $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27::jsonb,now()
)
ON CONFLICT (request_id,created_at) DO UPDATE SET
 request_started_at=EXCLUDED.request_started_at, endpoint=EXCLUDED.endpoint, method=EXCLUDED.method,
 protocol=EXCLUDED.protocol,
 api_key_id=COALESCE(NULLIF(audit_calls.api_key_id,''),EXCLUDED.api_key_id),
 api_key_name=COALESCE(NULLIF(audit_calls.api_key_name,''),EXCLUDED.api_key_name),
 user_id=COALESCE(NULLIF(audit_calls.user_id,''),EXCLUDED.user_id),
 user_username=COALESCE(NULLIF(audit_calls.user_username,''),EXCLUDED.user_username),
 account_id=COALESCE(NULLIF(audit_calls.account_id,''),EXCLUDED.account_id),
 account_type=COALESCE(NULLIF(audit_calls.account_type,''),EXCLUDED.account_type),
 model=COALESCE(EXCLUDED.model,audit_calls.model), status=EXCLUDED.status,
 status_code=EXCLUDED.status_code, stream=EXCLUDED.stream,
 input_tokens=EXCLUDED.input_tokens, output_tokens=EXCLUDED.output_tokens,
 cache_read_tokens=EXCLUDED.cache_read_tokens, cache_create_tokens=EXCLUDED.cache_create_tokens,
 total_tokens=EXCLUDED.total_tokens, cost=EXCLUDED.cost, real_cost=EXCLUDED.real_cost,
 retention_until=EXCLUDED.retention_until, capture_status=EXCLUDED.capture_status,
 error=EXCLUDED.error, meta=audit_calls.meta || EXCLUDED.meta, updated_at=now()`
