package callaudit

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	spool, err := NewSpool(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenPostgreSQLStore(postgresURL, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	migrationStore := store
	if migrationPostgresURL != "" && migrationPostgresURL != postgresURL {
		migrationStore, err = OpenPostgreSQLStore(migrationPostgresURL, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer migrationStore.Close()
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
		rows, _ := store.db.QueryContext(cleanupCtx, `SELECT bucket, object_key FROM audit_artifacts WHERE request_id=$1`, requestID)
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
}

const httpStatusOK = 200

func integrationIntPointer(value int) *int { return &value }
