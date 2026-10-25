package callaudit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

func slowRetryOptions(now *time.Time) WorkerOptions {
	options := testWorkerOptions(now)
	options.MaxAttempts = 1
	options.DeadLetterRetry = DeadLetterRetryOptions{Enabled: true, MaxRetries: 2, BatchSize: 1,
		InitialDelay: 10 * time.Second, MaxDelay: 20 * time.Second}
	return options
}

func writeTestManifest(t *testing.T, path string, manifest Manifest) {
	t.Helper()
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := atomicWriteFile(path, raw, 0o600, false); err != nil {
		t.Fatal(err)
	}
}

func legacyDeadLetter(t *testing.T, spool *Spool, now time.Time, id, code string) (Manifest, string) {
	t.Helper()
	commitWorkerBundle(t, spool, now, id)
	ready := filepath.Join(spool.Root(), spoolReadyDir, id+"-event.json")
	manifest, err := spool.LoadManifest(ready)
	if err != nil {
		t.Fatal(err)
	}
	manifest.State = ManifestDeadLetter
	manifest.CaptureStatus = CaptureFailed
	manifest.Attempts = 5
	manifest.LastErrorCode = code
	manifest.FailedAt = timePointer(now.Add(-time.Hour))
	dead := filepath.Join(spool.Root(), spoolDeadDir, id+"-event.json")
	manifest.EventSpoolPath = filepath.ToSlash(filepath.Join(spoolDeadDir, filepath.Base(dead)))
	writeTestManifest(t, ready, manifest)
	if err := renameDurable(ready, dead); err != nil {
		t.Fatal(err)
	}
	return manifest, dead
}

func TestDeadLetterBackoffAndBudgetPersistAcrossWorkers(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	now := time.Now().UTC().Truncate(time.Second)
	firstFailure := now
	commitWorkerBundle(t, spool, now, "bounded")
	calls := 0
	processor := ProcessorFunc(func(context.Context, Manifest) error {
		calls++
		return Retryable("s3_upload_failed", errors.New("unavailable"))
	})
	step := func() {
		t.Helper()
		worker, err := NewWorker(spool, processor, slowRetryOptions(&now))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := worker.ProcessOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	load := func() Manifest {
		t.Helper()
		m, err := spool.LoadManifest(filepath.Join(spoolDeadDir, "bounded-event.json"))
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	step()
	if m := load(); m.DeadLetterRetries != 0 || m.FirstFailedAt == nil || !m.FirstFailedAt.Equal(firstFailure) || !m.NextAttemptAt.Equal(now.Add(10*time.Second)) {
		t.Fatalf("initial dead-letter state: %+v", m)
	}
	now = now.Add(9 * time.Second)
	step()
	if calls != 1 {
		t.Fatalf("retried before durable deadline: %d", calls)
	}
	now = now.Add(time.Second)
	step()
	if m := load(); m.DeadLetterRetries != 1 || !m.FirstFailedAt.Equal(firstFailure) || !m.NextAttemptAt.Equal(now.Add(20*time.Second)) {
		t.Fatalf("first slow retry state: %+v", m)
	}
	now = now.Add(19 * time.Second)
	step()
	if calls != 2 {
		t.Fatalf("restart bypassed backoff: %d", calls)
	}
	now = now.Add(time.Second)
	step()
	if m := load(); m.DeadLetterRetries != 2 || m.NextAttemptAt != nil {
		t.Fatalf("exhausted state: %+v", m)
	}
	now = now.Add(24 * time.Hour)
	step()
	if calls != 3 {
		t.Fatalf("restart reset retry budget: %d", calls)
	}
	snapshot := spool.deadLetterSnapshot(now, slowRetryOptions(&now).DeadLetterRetry)
	if snapshot.Exhausted != 1 || snapshot.Retryable != 0 || snapshot.OldestSeconds < 86400 {
		t.Fatalf("snapshot: %+v", snapshot)
	}
}

func TestDeadLetterLegacyRecoveryPreservesBusinessFields(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	now := time.Now().UTC()
	want, _ := legacyDeadLetter(t, spool, now, "legacy", "postgres_commit_failed")
	want.APIKeyID = "existing-key"
	want.Usage.Cost = "12.34"
	want.Meta = map[string]any{"preserve": "historical"}
	writeTestManifest(t, filepath.Join(spool.Root(), spoolDeadDir, "legacy-event.json"), want)
	calls := 0
	worker, err := NewWorker(spool, ProcessorFunc(func(_ context.Context, got Manifest) error {
		calls++
		if got.RequestID != want.RequestID || !got.CreatedAt.Equal(want.CreatedAt) || !got.RetentionUntil.Equal(want.RetentionUntil) ||
			got.APIKeyID != want.APIKeyID || !reflect.DeepEqual(got.Usage, want.Usage) || !reflect.DeepEqual(got.Meta, want.Meta) ||
			!reflect.DeepEqual(got.Artifacts, want.Artifacts) || got.DeadLetterRetries != 1 || got.CaptureStatus != CaptureRetrying {
			t.Fatalf("business fields or recovery state changed: %+v", got)
		}
		return nil
	}), slowRetryOptions(&now))
	if err != nil {
		t.Fatal(err)
	}
	stats, err := worker.ProcessOnce(context.Background())
	if err != nil || stats.Succeeded != 1 || calls != 1 {
		t.Fatalf("recovery: %+v %v calls=%d", stats, err, calls)
	}
	if paths, _ := spool.listManifestPaths(spoolDeadDir); len(paths) != 0 {
		t.Fatalf("dead letters remain: %v", paths)
	}
	if _, err := os.Stat(filepath.Join(spool.Root(), want.Artifacts[0].SpoolPath)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("artifact not cleaned: %v", err)
	}
}

func TestDeadLetterPermanentAndDamagedInputsAreNotReplayed(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"unknown_legacy", "explicit_permanent", "missing_artifact", "invalid_json", "corrupt_manifest"} {
		t.Run(kind, func(t *testing.T) {
			spool := newTestSpool(t)
			now := time.Now().UTC()
			manifest, path := legacyDeadLetter(t, spool, now, "exclude", "s3_upload_failed")
			switch kind {
			case "unknown_legacy":
				manifest.LastErrorCode = "processing_failed"
				writeTestManifest(t, path, manifest)
			case "explicit_permanent":
				retryable := false
				manifest.LastFailureRetryable = &retryable
				writeTestManifest(t, path, manifest)
			case "missing_artifact":
				if err := os.Remove(filepath.Join(spool.Root(), manifest.Artifacts[0].SpoolPath)); err != nil {
					t.Fatal(err)
				}
			case "invalid_json":
				manifest.Artifacts[0].Bytes = 0
				writeTestManifest(t, path, manifest)
				if err := os.WriteFile(filepath.Join(spool.Root(), manifest.Artifacts[0].SpoolPath), []byte("{broken"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "corrupt_manifest":
				if err := os.WriteFile(path, []byte("{broken"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			calls := 0
			worker, err := NewWorker(spool, ProcessorFunc(func(context.Context, Manifest) error { calls++; return nil }), slowRetryOptions(&now))
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if _, err := worker.ProcessOnce(context.Background()); err != nil {
					t.Fatal(err)
				}
				now = now.Add(24 * time.Hour)
			}
			if calls != 0 {
				t.Fatalf("permanent input reached processor %d times", calls)
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("quarantined input lost: %v", err)
			}
			if s := spool.deadLetterSnapshot(now, worker.options.DeadLetterRetry); s.Permanent != 1 {
				t.Fatalf("snapshot %+v", s)
			}
		})
	}
}

func TestDeadLetterInFlightKeepsRuntimeDegradedAndReconcileIsReadOnly(t *testing.T) {
	spool := newTestSpool(t)
	now := time.Now().UTC()
	legacyDeadLetter(t, spool, now, "recovering", "s3_upload_failed")
	runtime := &Runtime{cfg: config.CallAuditConfig{Enabled: true}, spool: spool}
	runtime.setSchemaReadiness(true, nil)
	runtime.setStorageReadiness(true, nil)
	started, release := make(chan struct{}), make(chan struct{})
	options := slowRetryOptions(&now)
	options.OnDeadLetterState = runtime.setDeadLetterSnapshot
	options.OnSuccess = func(Manifest) { runtime.setStorageReadiness(true, nil) }
	var failedWrites atomic.Int32
	options.SyncDeadLetter = func(context.Context, Manifest, error) error { failedWrites.Add(1); return nil }
	worker, err := NewWorker(spool, ProcessorFunc(func(context.Context, Manifest) error { close(started); <-release; return nil }), options)
	if err != nil {
		t.Fatal(err)
	}
	runtime.worker = worker
	done := make(chan error, 1)
	go func() { _, err := worker.ProcessOnce(context.Background()); done <- err }()
	<-started
	for range 10 {
		runtime.reconcileDeadLetters()
		runtime.setStorageReadiness(true, nil)
	}
	snapshot := runtime.Snapshot()
	if runtime.Ready() || snapshot.Ready || snapshot.DeadLetterRecovering != 1 || snapshot.DeadLetterFiles != 0 || failedWrites.Load() != 0 {
		t.Fatalf("in-flight recovery incorrectly ready or reconciled: %+v writes=%d", snapshot, failedWrites.Load())
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for range 10 {
		runtime.reconcileDeadLetters()
	}
	if !runtime.Ready() || failedWrites.Load() != 0 {
		t.Fatalf("completed recovery changed by reconciliation: ready=%v writes=%d", runtime.Ready(), failedWrites.Load())
	}
}

func TestWorkersSharingSpoolDoNotRepeatAClaim(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	now := time.Now().UTC()
	legacyDeadLetter(t, spool, now, "shared", "postgres_upsert_failed")
	var calls atomic.Int32
	processor := ProcessorFunc(func(context.Context, Manifest) error { calls.Add(1); return nil })
	var wg sync.WaitGroup
	for range 2 {
		worker, err := NewWorker(spool, processor, slowRetryOptions(&now))
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := worker.ProcessOnce(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("shared manifest processed %d times", calls.Load())
	}
}

func TestDeadLetterRetryCrashDoesNotResetBudget(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	now := time.Now().UTC()
	manifest, path := legacyDeadLetter(t, spool, now, "crash", "s3_upload_failed")
	manifest.DeadLetterRetries = 1
	writeTestManifest(t, path, manifest)
	options := slowRetryOptions(&now)
	worker, err := NewWorker(spool, ProcessorFunc(func(context.Context, Manifest) error { panic("simulated process crash") }), options)
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("expected simulated crash")
			}
		}()
		_, _ = worker.ProcessOnce(context.Background())
	}()
	processing := filepath.Join(spool.Root(), spoolProcessingDir, "crash-event.json")
	claimed, err := spool.LoadManifest(processing)
	if err != nil || claimed.DeadLetterRetries != 2 {
		t.Fatalf("budget not durable before processing: %+v %v", claimed, err)
	}
	old := now.Add(-2 * options.ClaimTimeout)
	if err := os.Chtimes(processing, old, old); err != nil {
		t.Fatal(err)
	}
	calls := 0
	restarted, err := NewWorker(spool, ProcessorFunc(func(context.Context, Manifest) error { calls++; return nil }), options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("restart exceeded budget: %d", calls)
	}
	if s := spool.deadLetterSnapshot(now, options.DeadLetterRetry); s.Exhausted != 1 {
		t.Fatalf("crash snapshot: %+v", s)
	}
}

func TestDeadLetterBatchDoesNotStarveNewRequests(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	now := time.Now().UTC()
	legacyDeadLetter(t, spool, now, "historical", "s3_upload_failed")
	for _, id := range []string{"a", "b", "c"} {
		commitWorkerBundle(t, spool, now, id)
	}
	options := slowRetryOptions(&now)
	options.BatchSize = 2
	var order []string
	worker, err := NewWorker(spool, ProcessorFunc(func(_ context.Context, m Manifest) error { order = append(order, m.RequestID); return nil }), options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"a", "b", "historical"}) {
		t.Fatalf("work order = %v", order)
	}
}

func TestFirstDeadLetterClaimCrashRemainsUnreadyBeforeCounterWrite(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	now := time.Now().UTC()
	_, dead := legacyDeadLetter(t, spool, now, "first-claim", "s3_upload_failed")
	processing := filepath.Join(spool.Root(), spoolProcessingDir, filepath.Base(dead))
	// Simulate a crash immediately after the atomic claim, before writing the
	// processing state or incremented slow-attempt counter.
	if err := renameDurable(dead, processing); err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{cfg: config.CallAuditConfig{Enabled: true}, spool: spool}
	runtime.setSchemaReadiness(true, nil)
	runtime.setStorageReadiness(true, nil)
	runtime.setDeadLetterReadiness(true, nil)
	options := slowRetryOptions(&now)
	options.OnDeadLetterState = runtime.setDeadLetterSnapshot
	calls := 0
	worker, err := NewWorker(spool, ProcessorFunc(func(context.Context, Manifest) error { calls++; return nil }), options)
	if err != nil {
		t.Fatal(err)
	}
	runtime.worker = worker
	if _, err := worker.ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := runtime.Snapshot()
	if calls != 0 || runtime.Ready() || snapshot.DeadLetterRecovering != 1 || snapshot.DeadLetterFiles != 0 {
		t.Fatalf("first claim crash was hidden: calls=%d snapshot=%+v", calls, snapshot)
	}
}

func TestStoredLastAttemptClaimOnlyCleansUpAfterCrash(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	now := time.Now().UTC()
	manifest, dead := legacyDeadLetter(t, spool, now, "stored-crash", "s3_upload_failed")
	manifest.DeadLetterRetries = 2
	manifest.FirstFailedAt = manifest.FailedAt
	manifest.State = ManifestProcessing
	manifest.CaptureStatus = CaptureStored
	processing := filepath.Join(spool.Root(), spoolProcessingDir, filepath.Base(dead))
	writeTestManifest(t, dead, manifest)
	if err := renameDurable(dead, processing); err != nil {
		t.Fatal(err)
	}
	old := now.Add(-2 * time.Minute)
	if err := os.Chtimes(processing, old, old); err != nil {
		t.Fatal(err)
	}
	calls, successes := 0, 0
	options := slowRetryOptions(&now)
	options.OnSuccess = func(Manifest) { successes++ }
	worker, err := NewWorker(spool, ProcessorFunc(func(context.Context, Manifest) error { calls++; return nil }), options)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := worker.ProcessOnce(context.Background())
	if err != nil || calls != 0 || successes != 1 || stats.Recovered != 1 {
		t.Fatalf("stored recovery: %+v %v calls=%d success=%d", stats, err, calls, successes)
	}
	if s := spool.deadLetterSnapshot(now, options.DeadLetterRetry); s.unresolved() != 0 {
		t.Fatalf("stored recovery still unresolved: %+v", s)
	}
	if _, err := os.Stat(filepath.Join(spool.Root(), manifest.Artifacts[0].SpoolPath)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stored artifact not cleaned: %v", err)
	}
}

func TestQueueTransitionsPreserveBothBundlesOnTargetCollision(t *testing.T) {
	t.Parallel()
	for _, targetDir := range []string{spoolDeadDir, spoolRetryDir, spoolCompletedDir} {
		t.Run(targetDir, func(t *testing.T) {
			spool := newTestSpool(t)
			now := time.Now().UTC()
			artifact := commitWorkerBundle(t, spool, now, "collision")
			ready := filepath.Join(spool.Root(), spoolReadyDir, "collision-event.json")
			processing := filepath.Join(spool.Root(), spoolProcessingDir, filepath.Base(ready))
			manifest, err := spool.LoadManifest(ready)
			if err != nil {
				t.Fatal(err)
			}
			if err := renameDurable(ready, processing); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(spool.Root(), targetDir, filepath.Base(ready))
			original := []byte(`{"retain":"different durable bundle"}`)
			if err := os.WriteFile(target, original, 0o600); err != nil {
				t.Fatal(err)
			}
			worker, err := NewWorker(spool, ProcessorFunc(func(context.Context, Manifest) error { return nil }), testWorkerOptions(&now))
			if err != nil {
				t.Fatal(err)
			}
			switch targetDir {
			case spoolCompletedDir:
				err = worker.complete(processing, manifest)
			case spoolDeadDir:
				_, _, err = worker.transitionFailure(processing, manifest, Permanent("invalid_manifest", nil), now)
			case spoolRetryDir:
				_, _, err = worker.transitionFailure(processing, manifest, Retryable("s3_upload_failed", nil), now)
			}
			if !errors.Is(err, os.ErrExist) {
				t.Fatalf("target collision was not rejected: %v", err)
			}
			got, err := os.ReadFile(target)
			if err != nil || string(got) != string(original) {
				t.Fatalf("original target overwritten: %q %v", got, err)
			}
			if _, err := os.Stat(processing); err != nil {
				t.Fatalf("source bundle lost: %v", err)
			}
			if _, err := os.Stat(filepath.Join(spool.Root(), artifact.SpoolPath)); err != nil {
				t.Fatalf("source artifact lost: %v", err)
			}
		})
	}
}

func TestDeadLetterStatusSynchronizationDoesNotStarveTail(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	now := time.Now().UTC()
	for i := range 7 {
		legacyDeadLetter(t, spool, now, fmt.Sprintf("permanent-%d", i), "invalid_manifest")
	}
	options := slowRetryOptions(&now)
	options.DeadLetterRetry.BatchSize = 2
	options.DeadLetterRetry.InitialDelay = time.Minute
	options.DeadLetterRetry.MaxDelay = time.Minute
	seen := make(map[string]int)
	options.SyncDeadLetter = func(_ context.Context, m Manifest, _ error) error { seen[m.RequestID]++; return nil }
	worker, err := NewWorker(spool, ProcessorFunc(func(context.Context, Manifest) error { t.Fatal("permanent record processed"); return nil }), options)
	if err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if _, err := worker.ProcessOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		now = now.Add(30 * time.Second)
	}
	if len(seen) != 7 {
		t.Fatalf("tail records starved: %v", seen)
	}
}
