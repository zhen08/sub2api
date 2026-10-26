package callaudit

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

func TestDurableProcessorPostgreSQLMinIOIntegration(t *testing.T) {
	if os.Getenv("RUN_CALL_AUDIT_INTEGRATION") != "true" {
		t.Skip("set RUN_CALL_AUDIT_INTEGRATION=true to run PostgreSQL/MinIO integration")
	}
	postgresURL := os.Getenv("CALL_AUDIT_POSTGRES_URL")
	migrationPostgresURL := os.Getenv("CALL_AUDIT_MIGRATION_POSTGRES_URL")
	endpoint := os.Getenv("CALL_AUDIT_S3_ENDPOINT")
	bucket := os.Getenv("CALL_AUDIT_S3_BUCKET")
	accessKey := os.Getenv("CALL_AUDIT_S3_ACCESS_KEY")
	secretKey := os.Getenv("CALL_AUDIT_S3_SECRET_KEY")
	if postgresURL == "" || endpoint == "" || bucket == "" || accessKey == "" || secretKey == "" {
		t.Fatal("CALL_AUDIT_POSTGRES_URL and CALL_AUDIT_S3_* integration settings are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	spool, err := NewSpool(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenPostgreSQLStore(postgresURL, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	migrationStore := store
	if migrationPostgresURL != "" && migrationPostgresURL != postgresURL {
		migrationStore, err = OpenPostgreSQLStore(migrationPostgresURL, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = migrationStore.Close() })
	}
	if err := migrationStore.EnsureSchema(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.CheckReadiness(ctx, time.Now()); err != nil {
		t.Fatalf("initial schema readiness: %v", err)
	}
	objects, err := NewObjectStore(ctx, spool, ObjectStoreConfig{
		Endpoint:        endpoint,
		Region:          "us-east-1",
		Bucket:          bucket,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		Prefix:          LegacyObjectPrefix,
		ForcePathStyle:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := objects.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create integration bucket: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = objects.client.DeleteBucket(cleanupCtx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})
	processor, err := NewDurableProcessor(store, objects)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	requestID := "sub2api-audit-it-" + uuid.NewString()
	boundaryRequestID := "sub2api-audit-boundary-it-" + uuid.NewString()
	s3FailureRequestID := "sub2api-audit-s3-failure-it-" + uuid.NewString()
	artifacts := make([]ArtifactRef, 0, 4)
	for index, kind := range []ArtifactKind{ArtifactClientRequest, ArtifactUpstreamRequest, ArtifactUpstreamRequest, ArtifactResponse} {
		sequence := 0
		if index == 2 {
			sequence = 1
		}
		artifact, err := spool.WriteArtifact(now, requestID, kind, sequence, map[string]any{
			"kind": kind,
			"body": map[string]any{"index": index},
		})
		if err != nil {
			t.Fatal(err)
		}
		artifacts = append(artifacts, artifact)
	}
	manifest := Manifest{
		Version:          1,
		State:            ManifestReady,
		RequestID:        requestID,
		CreatedAt:        now,
		RequestStartedAt: now,
		RetentionUntil:   now.AddDate(0, 0, 180),
		Endpoint:         "/v1/messages",
		Method:           "POST",
		Protocol:         ProtocolAnthropic,
		APIKeyID:         "integration-key",
		APIKeyName:       "Integration Key",
		UserID:           "integration-user",
		UserUsername:     "integration-user-name",
		Status:           CallStatusOK,
		StatusCode:       integrationIntPointer(httpStatusOK),
		CaptureStatus:    CapturePending,
		Usage: Usage{
			AccountID:   "42",
			AccountType: "claude-oauth",
			Model:       "claude-integration",
			InputTokens: 3,
			TotalTokens: 3,
			Cost:        "0.0100000000",
			RealCost:    "0.0050000000",
		},
		Artifacts: artifacts,
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		rows, _ := store.db.QueryContext(cleanupCtx, `SELECT bucket, object_key FROM audit_artifacts WHERE request_id IN ($1, $2)`, requestID, s3FailureRequestID)
		if rows != nil {
			for rows.Next() {
				var objectBucket, objectKey string
				if rows.Scan(&objectBucket, &objectKey) == nil {
					_ = objects.Delete(cleanupCtx, objectBucket, objectKey)
				}
			}
			_ = rows.Close()
		}
		_, _ = store.db.ExecContext(cleanupCtx, `DELETE FROM audit_artifacts WHERE request_id=$1`, requestID)
		_, _ = store.db.ExecContext(cleanupCtx, `DELETE FROM audit_calls WHERE request_id=$1`, requestID)
		_, _ = store.db.ExecContext(cleanupCtx, `DELETE FROM audit_calls WHERE request_id=$1`, boundaryRequestID)
		_, _ = store.db.ExecContext(cleanupCtx, `DELETE FROM audit_artifacts WHERE request_id=$1`, s3FailureRequestID)
		_, _ = store.db.ExecContext(cleanupCtx, `DELETE FROM audit_calls WHERE request_id=$1`, s3FailureRequestID)
	})

	if err := processor.Process(ctx, manifest); err != nil {
		t.Fatal(err)
	}
	var firstArtifactID int64
	if err := store.db.QueryRowContext(ctx, `SELECT min(id) FROM audit_artifacts WHERE request_id=$1`, requestID).Scan(&firstArtifactID); err != nil {
		t.Fatal(err)
	}
	// Reprocessing simulates a crash after the durable commit but before spool
	// cleanup. Logical artifact IDs must remain stable.
	if err := processor.Process(ctx, manifest); err != nil {
		t.Fatal(err)
	}
	var artifactCount int
	var secondArtifactID int64
	if err := store.db.QueryRowContext(ctx, `SELECT count(*), min(id) FROM audit_artifacts WHERE request_id=$1`, requestID).Scan(&artifactCount, &secondArtifactID); err != nil {
		t.Fatal(err)
	}
	if artifactCount != len(artifacts) || firstArtifactID != secondArtifactID {
		t.Fatalf("artifact count/id after retry = %d/%d, want %d/%d", artifactCount, secondArtifactID, len(artifacts), firstArtifactID)
	}
	// A crash after StoreCompleted but before spool cleanup leaves an old claim.
	// Its later failed-status reconciliation must never demote a stored call.
	if err := store.UpsertCall(ctx, manifest, CaptureFailed, errors.New("stale claim after durable commit")); err != nil {
		t.Fatal(err)
	}
	var committedStatus string
	var committedError bool
	if err := store.db.QueryRowContext(ctx, `SELECT capture_status, error IS NOT NULL FROM audit_calls WHERE request_id=$1`, requestID).
		Scan(&committedStatus, &committedError); err != nil || committedStatus != string(CaptureStored) || committedError {
		t.Fatalf("stale claim demoted stored call: status=%q error=%v err=%v", committedStatus, committedError, err)
	}

	// Later usage/dead-letter upserts must not replace identity values already
	// snapshotted by the original call. Empty historical fields may still be
	// backfilled, which is why the SQL uses NULLIF(..., '') before COALESCE.
	identityUpdate := manifest
	identityUpdate.APIKeyID = "replacement-key"
	identityUpdate.APIKeyName = "Replacement Key"
	identityUpdate.UserID = "replacement-user"
	identityUpdate.UserUsername = "replacement-user-name"
	identityUpdate.Usage.AccountID = "replacement-account"
	identityUpdate.Usage.AccountType = "replacement-account-type"
	if err := store.UpsertCall(ctx, identityUpdate, CaptureStored, nil); err != nil {
		t.Fatal(err)
	}
	var storedAPIKeyID, storedAPIKeyName, storedUserID, storedUsername, storedAccountID, storedAccountType string
	if err := store.db.QueryRowContext(ctx, `
		SELECT api_key_id, api_key_name, user_id, user_username, account_id, account_type
		FROM audit_calls WHERE request_id=$1`, requestID).Scan(
		&storedAPIKeyID, &storedAPIKeyName, &storedUserID, &storedUsername, &storedAccountID, &storedAccountType,
	); err != nil {
		t.Fatal(err)
	}
	if storedAPIKeyID != manifest.APIKeyID || storedAPIKeyName != manifest.APIKeyName ||
		storedUserID != manifest.UserID || storedUsername != manifest.UserUsername ||
		storedAccountID != manifest.Usage.AccountID || storedAccountType != manifest.Usage.AccountType {
		t.Fatalf("identity snapshot was overwritten: key=%q/%q user=%q/%q account=%q/%q",
			storedAPIKeyID, storedAPIKeyName, storedUserID, storedUsername, storedAccountID, storedAccountType)
	}

	// Write across a year/month boundary to prove partition maintenance is not
	// tied to process startup or the current wall-clock month.
	boundaryCreatedAt := time.Date(now.Year()+1, time.January, 1, 0, 0, 1, 0, time.UTC)
	boundaryManifest := manifest
	boundaryManifest.RequestID = boundaryRequestID
	boundaryManifest.CreatedAt = boundaryCreatedAt
	boundaryManifest.RequestStartedAt = boundaryCreatedAt
	boundaryManifest.RetentionUntil = boundaryCreatedAt.AddDate(0, 0, 180)
	boundaryManifest.Artifacts = nil
	// Partition DDL belongs to the dedicated migration command, never the
	// writer. Repeating it proves the --partitions-only operation is idempotent.
	if err := migrationStore.EnsurePartitions(ctx, boundaryCreatedAt); err != nil {
		t.Fatalf("ensure cross-month partitions: %v", err)
	}
	if err := migrationStore.EnsurePartitions(ctx, boundaryCreatedAt); err != nil {
		t.Fatalf("repeat cross-month partition ensure: %v", err)
	}
	if err := store.CheckReadiness(ctx, boundaryCreatedAt); err != nil {
		t.Fatalf("cross-month readiness: %v", err)
	}
	if err := processor.Process(ctx, boundaryManifest); err != nil {
		t.Fatalf("cross-month process: %v", err)
	}
	var boundaryStored bool
	if err := store.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM audit_calls WHERE request_id=$1)`, boundaryRequestID).Scan(&boundaryStored); err != nil || !boundaryStored {
		t.Fatalf("cross-month call was not stored: exists=%v err=%v", boundaryStored, err)
	}

	// A real S3 connection failure is retryable and leaves PostgreSQL metadata
	// in retrying state; inference is decoupled because only the local worker
	// invokes this processor.
	failureArtifact, err := spool.WriteArtifact(now, s3FailureRequestID, ArtifactResponse, 0, map[string]any{"body": "retry"})
	if err != nil {
		t.Fatal(err)
	}
	failureManifest := manifest
	failureManifest.RequestID = s3FailureRequestID
	failureManifest.Artifacts = []ArtifactRef{failureArtifact}
	badObjects, err := NewObjectStore(ctx, spool, ObjectStoreConfig{
		Endpoint:        "http://127.0.0.1:1",
		Region:          "us-east-1",
		Bucket:          bucket,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		Prefix:          LegacyObjectPrefix,
		ForcePathStyle:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	failureProcessor, err := NewDurableProcessor(store, badObjects)
	if err != nil {
		t.Fatal(err)
	}
	processErr := failureProcessor.Process(ctx, failureManifest)
	var classified *ProcessError
	if !errors.As(processErr, &classified) || !classified.Retryable || classified.Code != "s3_upload_failed" {
		t.Fatalf("S3 failure = %#v", processErr)
	}
	var failureStatus string
	if err := store.db.QueryRowContext(ctx, `SELECT capture_status FROM audit_calls WHERE request_id=$1`, s3FailureRequestID).Scan(&failureStatus); err != nil || failureStatus != string(CaptureRetrying) {
		t.Fatalf("S3 failure status=%q err=%v", failureStatus, err)
	}

	// The durable queue must recover the same call after fast attempts exhaust,
	// including a process restart during the slow retry delay. Real PostgreSQL
	// and S3 writes prove recovery does not only clear the local health marker.
	if _, err := spool.CommitManifest(failureManifest); err != nil {
		t.Fatal(err)
	}
	recoveryNow := now
	options := testWorkerOptions(&recoveryNow)
	options.MaxAttempts = 1
	options.DeadLetterRetry = DeadLetterRetryOptions{
		Enabled: true, MaxRetries: 3, BatchSize: 1,
		InitialDelay: 5 * time.Minute, MaxDelay: 20 * time.Minute,
	}
	options.SyncDeadLetter = func(syncCtx context.Context, m Manifest, cause error) error {
		return store.UpsertCall(syncCtx, m, CaptureFailed, cause)
	}
	options.OnDeadLetter = func(m Manifest, cause error) {
		if err := store.UpsertCall(ctx, m, CaptureFailed, cause); err != nil {
			t.Fatalf("persist dead-letter status: %v", err)
		}
	}
	storageUnavailable := true
	processorCalls := 0
	recoveringProcessor := ProcessorFunc(func(processCtx context.Context, m Manifest) error {
		processorCalls++
		if storageUnavailable {
			return failureProcessor.Process(processCtx, m)
		}
		return processor.Process(processCtx, m)
	})
	worker, err := NewWorker(spool, recoveringProcessor, options)
	if err != nil {
		t.Fatal(err)
	}
	if stats, err := worker.ProcessOnce(ctx); err != nil || stats.DeadLettered != 1 {
		t.Fatalf("failed worker pass = %+v, %v", stats, err)
	}
	if err := store.db.QueryRowContext(ctx, `SELECT capture_status FROM audit_calls WHERE request_id=$1`, s3FailureRequestID).Scan(&failureStatus); err != nil || failureStatus != string(CaptureFailed) {
		t.Fatalf("dead-letter status=%q err=%v", failureStatus, err)
	}
	storageUnavailable = false
	worker, err = NewWorker(spool, recoveringProcessor, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.ProcessOnce(ctx); err != nil || processorCalls != 1 {
		t.Fatalf("restart ignored persisted slow retry deadline: calls=%d err=%v", processorCalls, err)
	}
	recoveryNow = recoveryNow.Add(5 * time.Minute)
	if stats, err := worker.ProcessOnce(ctx); err != nil || stats.Succeeded != 1 || processorCalls != 2 {
		t.Fatalf("automatic recovery = %+v, calls=%d, err=%v", stats, processorCalls, err)
	}
	if _, err := worker.ProcessOnce(ctx); err != nil {
		t.Fatal(err)
	}
	var recoveredCost string
	var recoveredTokens int64
	var recoveredError bool
	if err := store.db.QueryRowContext(ctx, `SELECT capture_status, cost::text, total_tokens, error IS NOT NULL FROM audit_calls WHERE request_id=$1`, s3FailureRequestID).
		Scan(&failureStatus, &recoveredCost, &recoveredTokens, &recoveredError); err != nil || failureStatus != string(CaptureStored) || recoveredError {
		t.Fatalf("recovered call status=%q error=%v err=%v", failureStatus, recoveredError, err)
	}
	if recoveredCost != failureManifest.Usage.Cost || recoveredTokens != failureManifest.Usage.TotalTokens {
		t.Fatalf("historical usage changed: cost=%q tokens=%d", recoveredCost, recoveredTokens)
	}
	if err := store.db.QueryRowContext(ctx, `SELECT count(*) FROM audit_artifacts WHERE request_id=$1`, s3FailureRequestID).Scan(&artifactCount); err != nil || artifactCount != 1 {
		t.Fatalf("recovered logical artifacts=%d err=%v", artifactCount, err)
	}
	if dead, err := spool.listManifestPaths(spoolDeadDir); err != nil || len(dead) != 0 {
		t.Fatalf("dead letters remain after durable recovery: %v, %v", dead, err)
	}

	var objectKey, storedSHA string
	if err := store.db.QueryRowContext(ctx, `
		SELECT object_key, sha256 FROM audit_artifacts
		WHERE request_id=$1 AND kind='response' AND sequence=0`, requestID).Scan(&objectKey, &storedSHA); err != nil {
		t.Fatal(err)
	}
	object, err := objects.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(objectKey)})
	if err != nil {
		t.Fatal(err)
	}
	compressed, err := io.ReadAll(object.Body)
	_ = object.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(compressed)
	if hex.EncodeToString(digest[:]) != storedSHA || object.Metadata[S3MetadataRequestID] != requestID || object.Metadata[S3MetadataArtifactKind] != string(ArtifactResponse) {
		t.Fatalf("stored object metadata/checksum mismatch: sha=%s metadata=%v", storedSHA, object.Metadata)
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || !json.Valid(decoded) {
		t.Fatalf("stored artifact is not valid gzip JSON: %v", err)
	}

	t.Run("FIFOConcurrentDurableQueue", func(t *testing.T) {
		testFIFOConcurrentDurableQueue(t, ctx, store, objects, manifest)
	})
}

func testFIFOConcurrentDurableQueue(t *testing.T, ctx context.Context, store *PostgreSQLStore, objects *ObjectStore, template Manifest) {
	t.Helper()
	queueSpool, err := NewSpool(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	// Reuse the isolated test database and S3 bucket, but give this worker its
	// own spool so complete cleanup can be checked independently of direct writes.
	queueObjects := &ObjectStore{client: objects.client, spool: queueSpool, cfg: objects.cfg}
	processor, err := NewDurableProcessor(store, queueObjects)
	if err != nil {
		t.Fatal(err)
	}
	const bundleCount = 8
	manifests := make([]Manifest, 0, bundleCount)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, manifest := range manifests {
			// Deterministic keys also clean up uploads left by a failed DB commit.
			for _, artifact := range manifest.Artifacts {
				key, err := BuildLegacyObjectKey(objects.cfg.Prefix, manifest.CreatedAt, manifest.APIKeyID, manifest.RequestID, artifact.Kind, artifact.Sequence)
				if err == nil {
					_ = objects.Delete(cleanupCtx, objects.cfg.Bucket, key)
				}
			}
			_, _ = store.db.ExecContext(cleanupCtx, `DELETE FROM audit_artifacts WHERE request_id=$1`, manifest.RequestID)
			_, _ = store.db.ExecContext(cleanupCtx, `DELETE FROM audit_calls WHERE request_id=$1`, manifest.RequestID)
		}
	})
	now := time.Now().UTC()
	suffix := uuid.NewString()
	for index := range bundleCount {
		manifest := template
		// Reverse lexical order so old UUID/name ordering selects the wrong batch.
		manifest.RequestID = fmt.Sprintf("sub2api-audit-fifo-it-%02d-%s", bundleCount-index, suffix)
		manifest.Artifacts = nil
		for artifactIndex, kind := range []ArtifactKind{ArtifactClientRequest, ArtifactUpstreamRequest, ArtifactUpstreamRequest, ArtifactResponse} {
			sequence := 0
			if artifactIndex == 2 {
				sequence = 1
			}
			artifact, err := queueSpool.WriteArtifact(manifest.CreatedAt, manifest.RequestID, kind, sequence, map[string]int{"bundle": index, "artifact": artifactIndex})
			if err != nil {
				t.Fatal(err)
			}
			manifest.Artifacts = append(manifest.Artifacts, artifact)
		}
		manifests = append(manifests, manifest)
		path, err := queueSpool.CommitManifest(manifest)
		if err != nil {
			t.Fatal(err)
		}
		queued, err := queueSpool.LoadManifest(path)
		if err != nil {
			t.Fatal(err)
		}
		queued.EnqueuedAt = timePointer(now.Add(time.Duration(index-bundleCount) * time.Minute))
		writeTestManifest(t, filepath.Join(queueSpool.Root(), path), queued)
	}

	var active, peak, started atomic.Int32
	firstPairReady := make(chan struct{})
	concurrentProcessor := ProcessorFunc(func(processCtx context.Context, manifest Manifest) error {
		count := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); count > previous && !peak.CompareAndSwap(previous, count); previous = peak.Load() {
		}
		// Force both initial slots to be occupied before real PostgreSQL/S3 work.
		// This fails if concurrency silently regresses to a sequential worker.
		if ordinal := started.Add(1); ordinal <= 2 {
			if ordinal == 2 {
				close(firstPairReady)
			}
			select {
			case <-firstPairReady:
			case <-processCtx.Done():
				return processCtx.Err()
			}
		}
		return processor.Process(processCtx, manifest)
	})
	options := testWorkerOptions(&now)
	options.Concurrency = 2
	options.BatchSize = 3
	worker, err := NewWorker(queueSpool, concurrentProcessor, options)
	if err != nil {
		t.Fatal(err)
	}
	for completed := 0; completed < bundleCount; {
		want := min(options.BatchSize, bundleCount-completed)
		stats, err := worker.ProcessOnce(ctx)
		if err != nil || stats.Claimed != want || stats.Succeeded != want || stats.Retried != 0 || stats.DeadLettered != 0 {
			t.Fatalf("concurrent durable pass = %+v, err=%v, want %d successes", stats, err, want)
		}
		completed += want
		for index, manifest := range manifests {
			var stored bool
			if err := store.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM audit_calls WHERE request_id=$1 AND capture_status='stored')`, manifest.RequestID).Scan(&stored); err != nil {
				t.Fatal(err)
			}
			if stored != (index < completed) {
				t.Fatalf("FIFO batch completed %d bundles but bundle %d stored=%v", completed, index, stored)
			}
		}
	}
	if peak.Load() != 2 || started.Load() != bundleCount || active.Load() != 0 {
		t.Fatalf("storage execution peak=%d started=%d active=%d", peak.Load(), started.Load(), active.Load())
	}
	for index, manifest := range manifests {
		var total, unique int
		if err := store.db.QueryRowContext(ctx, `SELECT count(*), count(DISTINCT (kind, sequence)) FROM audit_artifacts WHERE request_id=$1`, manifest.RequestID).Scan(&total, &unique); err != nil {
			t.Fatal(err)
		}
		if total != len(manifest.Artifacts) || unique != total {
			t.Fatalf("bundle %d artifacts total=%d unique=%d, want %d", index, total, unique, len(manifest.Artifacts))
		}
		for artifactIndex, artifact := range manifest.Artifacts {
			var key, storedSHA string
			var storedBytes int64
			if err := store.db.QueryRowContext(ctx, `SELECT object_key, sha256, bytes FROM audit_artifacts WHERE request_id=$1 AND kind=$2 AND sequence=$3`, manifest.RequestID, string(artifact.Kind), artifact.Sequence).Scan(&key, &storedSHA, &storedBytes); err != nil {
				t.Fatal(err)
			}
			object, err := objects.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(objects.cfg.Bucket), Key: aws.String(key)})
			if err != nil {
				t.Fatal(err)
			}
			compressed, readErr := io.ReadAll(object.Body)
			_ = object.Body.Close()
			if readErr != nil {
				t.Fatal(readErr)
			}
			digest := sha256.Sum256(compressed)
			if storedSHA != hex.EncodeToString(digest[:]) || storedBytes != int64(len(compressed)) || object.Metadata[S3MetadataRequestID] != manifest.RequestID || object.Metadata[S3MetadataArtifactKind] != string(artifact.Kind) {
				t.Fatalf("bundle %d artifact %d durable metadata mismatch", index, artifactIndex)
			}
			reader, err := gzip.NewReader(bytes.NewReader(compressed))
			if err != nil {
				t.Fatal(err)
			}
			var payload map[string]int
			decodeErr := json.NewDecoder(reader).Decode(&payload)
			_ = reader.Close()
			if decodeErr != nil || payload["bundle"] != index || payload["artifact"] != artifactIndex {
				t.Fatalf("bundle %d artifact %d payload=%v err=%v", index, artifactIndex, payload, decodeErr)
			}
		}
	}
	for _, directory := range []string{spoolReadyDir, spoolRetryDir, spoolProcessingDir, spoolCompletedDir, spoolDeadDir} {
		paths, err := queueSpool.listManifestPaths(directory)
		if err != nil || len(paths) != 0 {
			t.Fatalf("queue directory %s retained manifests=%v err=%v", directory, paths, err)
		}
	}
	if err := filepath.WalkDir(filepath.Join(queueSpool.Root(), spoolArtifactsDir), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			return fmt.Errorf("completed queue retained artifact %s", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

const httpStatusOK = 200

func integrationIntPointer(value int) *int { return &value }
