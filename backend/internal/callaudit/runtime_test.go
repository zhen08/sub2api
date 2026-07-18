package callaudit

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

func TestRuntimeStorageReadinessDoesNotBlockCapture(t *testing.T) {
	runtime := &Runtime{
		cfg: config.CallAuditConfig{
			Enabled:                  true,
			RetentionDays:            180,
			DiskHighWatermarkPercent: 99,
		},
		spool: newTestSpool(t),
	}
	runtime.setSchemaReadiness(true, nil)
	runtime.setStorageReadiness(true, nil)
	runtime.setDeadLetterReadiness(true, nil)
	runtime.setStorageReadiness(false, Retryable(
		"s3_unavailable;password=super-secret",
		errors.New("postgres://audit:super-secret@database.example/audit"),
	))

	if runtime.Ready() {
		t.Fatal("runtime reported ready while storage was degraded")
	}
	snapshot := runtime.Snapshot()
	if !snapshot.SchemaReady || snapshot.StorageReady || !snapshot.DeadLetterReady || snapshot.ReadinessError != "audit worker failure: unknown" {
		t.Fatalf("storage snapshot = %+v", snapshot)
	}
	for _, secret := range []string{"postgres://", "super-secret", "password="} {
		if strings.Contains(snapshot.ReadinessError, secret) {
			t.Fatalf("storage readiness leaked %q: %q", secret, snapshot.ReadinessError)
		}
	}

	session, err := runtime.StartSession(ScopeInput{
		RequestID: "storage-degraded",
		Endpoint:  "/v1/messages",
		Method:    "POST",
	})
	if err != nil || session == nil {
		t.Fatalf("StartSession while storage degraded = %#v, %v", session, err)
	}
	session.Release()

	// A worker success only recovers worker/object storage. It must not hide a
	// schema failure such as a missing next-month partition.
	runtime.setSchemaReadiness(false, errors.New("audit postgres is not ready: missing partitions: audit_calls_2026_08"))
	runtime.setStorageReadiness(true, nil)
	if runtime.Ready() {
		t.Fatal("worker recovery hid schema degradation")
	}
	runtime.setSchemaReadiness(true, nil)
	if !runtime.Ready() {
		t.Fatal("successful storage operation did not restore readiness")
	}

	// A successful PostgreSQL catalog check cannot hide an ongoing S3/worker
	// failure either.
	runtime.setStorageReadiness(false, Retryable("s3_upload_failed", errors.New("temporary")))
	runtime.setSchemaReadiness(false, errors.New("ping audit postgres: temporary"))
	runtime.setSchemaReadiness(true, nil)
	if runtime.Ready() {
		t.Fatal("schema recovery hid worker/object-storage degradation")
	}
	runtime.setStorageReadiness(true, nil)
	if !runtime.Ready() {
		t.Fatal("storage recovery did not restore readiness after schema recovery")
	}

	// Likewise, a successful later bundle cannot hide an unresolved dead letter.
	runtime.setDeadLetterReadiness(false, errDeadLettersPresent)
	runtime.setStorageReadiness(false, Retryable("s3_upload_failed", errors.New("temporary")))
	runtime.setStorageReadiness(true, nil)
	if runtime.Ready() {
		t.Fatal("worker recovery hid unresolved dead letters")
	}
	runtime.setDeadLetterReadiness(true, nil)
	snapshot = runtime.Snapshot()
	if !snapshot.SchemaReady || !snapshot.StorageReady || !snapshot.DeadLetterReady || snapshot.ReadinessError != "" || !snapshot.Ready {
		t.Fatalf("recovered storage snapshot = %+v", snapshot)
	}
}

func TestSafeStorageReadinessErrorRedactsDriverDetails(t *testing.T) {
	got := safeStorageReadinessError(errors.New(
		"ping audit postgres: connect postgres://audit:super-secret@database.example/audit",
	))
	if got != "ping audit postgres failed" {
		t.Fatalf("safe readiness error = %q", got)
	}
	if strings.Contains(got, "super-secret") {
		t.Fatalf("safe readiness error leaked credentials: %q", got)
	}

	missingSchema := "audit postgres is not ready: missing partitions: audit_calls_2026_08"
	if got := safeStorageReadinessError(errors.New(missingSchema)); got != missingSchema {
		t.Fatalf("schema diagnostic = %q, want %q", got, missingSchema)
	}
}

func TestRuntimeRunFinalizeIsAsyncAndShutdownDrainsIt(t *testing.T) {
	runtime := NewRuntime(&config.Config{})
	started := make(chan struct{})
	release := make(chan struct{})
	runtime.RunFinalize(func() {
		close(started)
		<-release
	})

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("finalizer did not start asynchronously")
	}

	shutdownDone := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { shutdownDone <- runtime.Shutdown(ctx) }()
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned before finalizer drained: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeRunFinalizeEnforcesExecutionLimit(t *testing.T) {
	runtime := NewRuntime(&config.Config{})
	const totalFinalizers = maxConcurrentFinalizers + 32

	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	var callers sync.WaitGroup
	defer func() {
		releaseAll()
		callers.Wait()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = runtime.Shutdown(ctx)
	}()

	entered := make(chan struct{}, totalFinalizers)
	var active atomic.Int64
	var maximum atomic.Int64
	var executed atomic.Int64
	for index := 0; index < totalFinalizers; index++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			runtime.RunFinalize(func() {
				current := active.Add(1)
				for {
					observed := maximum.Load()
					if current <= observed || maximum.CompareAndSwap(observed, current) {
						break
					}
				}
				executed.Add(1)
				entered <- struct{}{}
				<-release
				active.Add(-1)
			})
		}()
	}

	for index := 0; index < maxConcurrentFinalizers; index++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d finalizers entered", index)
		}
	}
	select {
	case <-entered:
		t.Fatalf("more than %d finalizers executed concurrently", maxConcurrentFinalizers)
	case <-time.After(50 * time.Millisecond):
	}
	if got := maximum.Load(); got != maxConcurrentFinalizers {
		t.Fatalf("maximum concurrent finalizers = %d, want %d", got, maxConcurrentFinalizers)
	}
	if runtime.Snapshot().FinalizeSaturated == 0 {
		t.Fatal("saturation was not observable")
	}

	releaseAll()
	callers.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if got := executed.Load(); got != totalFinalizers {
		t.Fatalf("executed finalizers = %d, want %d", got, totalFinalizers)
	}
}

func TestRuntimeShutdownWaitsForLateFinalizerOfAcceptedSession(t *testing.T) {
	done := make(chan struct{})
	maintenanceDone := make(chan struct{})
	close(done)
	close(maintenanceDone)
	runtime := &Runtime{
		cfg: config.CallAuditConfig{
			Enabled:                  true,
			RetentionDays:            180,
			DiskHighWatermarkPercent: 99,
		},
		spool:           newTestSpool(t),
		done:            done,
		maintenanceDone: maintenanceDone,
	}
	session, err := runtime.StartSession(ScopeInput{
		RequestID: "late-finalizer",
		Endpoint:  "/v1/messages",
		Method:    "POST",
	})
	if err != nil {
		t.Fatal(err)
	}

	shutdownDone := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { shutdownDone <- runtime.Shutdown(ctx) }()
	deadline := time.Now().Add(time.Second)
	for {
		runtime.finalizeMu.Lock()
		closing := runtime.closing
		runtime.finalizeMu.Unlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("runtime did not begin shutdown")
		}
		time.Sleep(time.Millisecond)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	go runtime.RunFinalize(func() {
		defer session.Release()
		close(started)
		<-release
	})
	<-started
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned before late finalizer: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
}
