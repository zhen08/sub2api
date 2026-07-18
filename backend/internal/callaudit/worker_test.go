package callaudit

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testWorkerOptions(now *time.Time) WorkerOptions {
	return WorkerOptions{
		MaxAttempts:       2,
		BatchSize:         10,
		PollInterval:      time.Second,
		RetryInitialDelay: time.Second,
		RetryMaxDelay:     4 * time.Second,
		ClaimTimeout:      time.Minute,
		Now:               func() time.Time { return *now },
	}
}

func commitWorkerBundle(t *testing.T, spool *Spool, now time.Time, requestID string) ArtifactRef {
	t.Helper()
	artifact, err := spool.WriteArtifact(now, requestID, ArtifactResponse, 0, map[string]any{"ok": true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spool.CommitManifest(testManifest(requestID, now, artifact)); err != nil {
		t.Fatal(err)
	}
	return artifact
}

func TestWorkerRetriesThenCompletesAndCleansBundle(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	now := time.Date(2026, time.July, 18, 9, 0, 0, 0, time.UTC)
	artifact := commitWorkerBundle(t, spool, now, "req-retry")
	calls := 0
	failures := 0
	successRequestID := ""
	options := testWorkerOptions(&now)
	options.OnError = func(error) { failures++ }
	options.OnSuccess = func(manifest Manifest) { successRequestID = manifest.RequestID }
	worker, err := NewWorker(spool, ProcessorFunc(func(context.Context, Manifest) error {
		calls++
		if calls == 1 {
			return Retryable("s3_unavailable", errors.New("temporary"))
		}
		return nil
	}), options)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := worker.ProcessOnce(context.Background())
	if err != nil || stats.Retried != 1 || stats.Claimed != 1 {
		t.Fatalf("first ProcessOnce = %+v, %v", stats, err)
	}
	stats, err = worker.ProcessOnce(context.Background())
	if err != nil || stats.Claimed != 0 {
		t.Fatalf("early ProcessOnce = %+v, %v", stats, err)
	}
	now = now.Add(time.Second)
	stats, err = worker.ProcessOnce(context.Background())
	if err != nil || stats.Succeeded != 1 || calls != 2 {
		t.Fatalf("final ProcessOnce = %+v, %v; calls=%d", stats, err, calls)
	}
	if successRequestID != "req-retry" {
		t.Fatalf("success callback request = %q", successRequestID)
	}
	if failures != 1 {
		t.Fatalf("failure callbacks = %d, want 1", failures)
	}
	if _, err := os.Stat(filepath.Join(spool.Root(), filepath.FromSlash(artifact.SpoolPath))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("artifact was not cleaned: %v", err)
	}
	for _, directory := range []string{spoolReadyDir, spoolRetryDir, spoolProcessingDir, spoolCompletedDir} {
		paths, err := spool.listManifestPaths(directory)
		if err != nil || len(paths) != 0 {
			t.Fatalf("%s manifests = %#v, %v", directory, paths, err)
		}
	}
}

func TestWorkerPermanentlyDeadLetters(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	now := time.Date(2026, time.July, 18, 10, 0, 0, 0, time.UTC)
	commitWorkerBundle(t, spool, now, "req-dead")
	options := testWorkerOptions(&now)
	callbackRequestID := ""
	options.OnDeadLetter = func(manifest Manifest, _ error) { callbackRequestID = manifest.RequestID }
	worker, err := NewWorker(spool, ProcessorFunc(func(context.Context, Manifest) error {
		return Permanent("invalid_manifest", errors.New("invalid"))
	}), options)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := worker.ProcessOnce(context.Background())
	if err != nil || stats.DeadLettered != 1 {
		t.Fatalf("ProcessOnce = %+v, %v", stats, err)
	}
	paths, err := spool.listManifestPaths(spoolDeadDir)
	if err != nil || len(paths) != 1 {
		t.Fatalf("dead letters = %#v, %v", paths, err)
	}
	manifest, err := spool.LoadManifest(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	if manifest.State != ManifestDeadLetter || manifest.CaptureStatus != CaptureFailed || manifest.LastErrorCode != "invalid_manifest" || manifest.Attempts != 1 {
		t.Fatalf("dead manifest = %+v", manifest)
	}
	if callbackRequestID != "req-dead" {
		t.Fatalf("dead-letter callback request = %q", callbackRequestID)
	}
}

func TestWorkerRecoversStaleClaim(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	now := time.Date(2026, time.July, 18, 11, 0, 0, 0, time.UTC)
	commitWorkerBundle(t, spool, now, "req-stale")
	ready, err := spool.listManifestPaths(spoolReadyDir)
	if err != nil || len(ready) != 1 {
		t.Fatalf("ready manifests = %#v, %v", ready, err)
	}
	processing := filepath.Join(spool.Root(), spoolProcessingDir, filepath.Base(ready[0]))
	if err := os.Rename(ready[0], processing); err != nil {
		t.Fatal(err)
	}
	old := now.Add(-2 * time.Minute)
	if err := os.Chtimes(processing, old, old); err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(spool, ProcessorFunc(func(context.Context, Manifest) error { return nil }), testWorkerOptions(&now))
	if err != nil {
		t.Fatal(err)
	}
	stats, err := worker.ProcessOnce(context.Background())
	if err != nil || stats.Recovered != 1 {
		t.Fatalf("ProcessOnce = %+v, %v", stats, err)
	}
	retry, err := spool.listManifestPaths(spoolRetryDir)
	if err != nil || len(retry) != 1 {
		t.Fatalf("retry manifests = %#v, %v", retry, err)
	}
}
