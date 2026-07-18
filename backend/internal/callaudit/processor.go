package callaudit

import (
	"context"
	"fmt"
)

// DurableProcessor implements the local worker's S3-then-PostgreSQL commit.
// A retry may overwrite deterministic S3 keys, while PostgreSQL replacement is
// transactional, making process restart and claim recovery idempotent.
type DurableProcessor struct {
	store   *PostgreSQLStore
	objects *ObjectStore
}

func NewDurableProcessor(store *PostgreSQLStore, objects *ObjectStore) (*DurableProcessor, error) {
	if store == nil || objects == nil {
		return nil, fmt.Errorf("audit database and object storage are required")
	}
	return &DurableProcessor{store: store, objects: objects}, nil
}

func (p *DurableProcessor) Process(ctx context.Context, manifest Manifest) error {
	status := manifest.CaptureStatus
	if status == "" {
		status = CapturePending
	}
	if err := p.store.UpsertCall(ctx, manifest, status, nil); err != nil {
		return Retryable("postgres_upsert_failed", err)
	}

	uploaded := make([]StoredArtifact, 0, len(manifest.Artifacts))
	for _, artifact := range manifest.Artifacts {
		stored, err := p.objects.Upload(ctx, manifest, artifact)
		if err != nil {
			_ = p.store.UpsertCall(ctx, manifest, CaptureRetrying, err)
			return Retryable("s3_upload_failed", err)
		}
		uploaded = append(uploaded, stored)
	}
	if err := p.store.StoreCompleted(ctx, manifest, uploaded); err != nil {
		_ = p.store.UpsertCall(ctx, manifest, CaptureRetrying, err)
		return Retryable("postgres_commit_failed", err)
	}
	return nil
}
