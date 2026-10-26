package callaudit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// queueFixture deliberately makes request time and file names unsuitable for
// scheduling. Only the persisted queue time (or legacy mtime) identifies age.
func queueFixture(t *testing.T, spool *Spool, id string, created time.Time, enqueued *time.Time, mtime time.Time, retryAt *time.Time) string {
	t.Helper()
	commitWorkerBundle(t, spool, created, id)
	path := filepath.Join(spool.Root(), spoolReadyDir, id+"-event.json")
	manifest, err := spool.LoadManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	manifest.EnqueuedAt = enqueued
	if retryAt != nil {
		manifest.State = ManifestRetry
		manifest.CaptureStatus = CaptureRetrying
		manifest.NextAttemptAt = retryAt
		manifest.Attempts = 1
		manifest.EventSpoolPath = filepath.ToSlash(filepath.Join(spoolRetryDir, filepath.Base(path)))
	}
	writeTestManifest(t, path, manifest)
	if retryAt != nil {
		target := filepath.Join(spool.Root(), spoolRetryDir, filepath.Base(path))
		if err := renameDurable(path, target); err != nil {
			t.Fatal(err)
		}
		path = target
	}
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	return path
}

type queuePassResult struct {
	stats WorkerStats
	err   error
}

func queueStartPass(worker *Worker, ctx context.Context) <-chan queuePassResult {
	done := make(chan queuePassResult, 1)
	go func() {
		stats, err := worker.ProcessOnce(ctx)
		done <- queuePassResult{stats: stats, err: err}
	}()
	return done
}

func queueReceive[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("audit worker did not make expected progress")
		var zero T
		return zero
	}
}

func TestQueueCommitPersistsActualEnqueueTime(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	requestTime := time.Now().UTC().Add(-time.Hour)
	artifact, err := spool.WriteArtifact(requestTime, "long-request", ArtifactResponse, 0, map[string]any{"ok": true})
	if err != nil {
		t.Fatal(err)
	}
	manifest := testManifest("long-request", requestTime, artifact)
	manifest.EnqueuedAt = timePointer(requestTime)
	before := time.Now().UTC()
	path, err := spool.CommitManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	after := time.Now().UTC()
	stored, err := spool.LoadManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if stored.EnqueuedAt == nil || stored.EnqueuedAt.Before(before) || stored.EnqueuedAt.After(after) {
		t.Fatalf("enqueue time must be the durable queue admission time, got %v outside [%v, %v]", stored.EnqueuedAt, before, after)
	}
	if !stored.CreatedAt.Equal(requestTime) {
		t.Fatal("queue admission changed the original request time")
	}
}

func TestQueueFIFOCombinesReadyAndDueRetriesWithLegacyFallback(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	now := time.Now().UTC().Truncate(time.Second)
	legacyTime := now.Add(-50 * time.Minute)
	queueFixture(t, spool, "z-legacy-oldest", now.Add(-time.Hour), nil, legacyTime, nil)
	queueFixture(t, spool, "y-due-retry", now.Add(-2*time.Hour), timePointer(now.Add(-40*time.Minute)), now, timePointer(now))
	queueFixture(t, spool, "n-ready-older", now.Add(-3*time.Hour), timePointer(now.Add(-30*time.Minute)), now.Add(time.Second), nil)
	queueFixture(t, spool, "b-ready-newer", now.Add(-4*time.Hour), timePointer(now.Add(-20*time.Minute)), now.Add(-time.Hour), nil)
	queueFixture(t, spool, "a-retry-not-due", now.Add(-5*time.Hour), timePointer(now.Add(-time.Hour)), now.Add(-time.Hour), timePointer(now.Add(time.Hour)))
	options := testWorkerOptions(&now)
	options.Concurrency = 1
	options.BatchSize = 2
	var order []string
	worker, err := NewWorker(spool, ProcessorFunc(func(_ context.Context, m Manifest) error {
		order = append(order, m.RequestID)
		if m.RequestID == "z-legacy-oldest" && (m.EnqueuedAt == nil || !m.EnqueuedAt.Equal(legacyTime)) {
			t.Errorf("legacy queue age was not retained before claim rewrite: %v", m.EnqueuedAt)
		}
		return nil
	}), options)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		stats, err := worker.ProcessOnce(context.Background())
		if err != nil || stats.Claimed != 2 || stats.Succeeded != 2 {
			t.Fatalf("FIFO pass = %+v, %v", stats, err)
		}
	}
	want := []string{"z-legacy-oldest", "y-due-retry", "n-ready-older", "b-ready-newer"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("FIFO order = %v, want %v", order, want)
	}
	stats, err := worker.ProcessOnce(context.Background())
	if err != nil || stats.Claimed != 0 {
		t.Fatalf("future retry was claimed: %+v, %v", stats, err)
	}
	if paths, _ := spool.listManifestPaths(spoolRetryDir); len(paths) != 1 {
		t.Fatalf("future retry was not retained: %v", paths)
	}
}

func TestQueueLegacyAgeSurvivesRetryAndWorkerRestart(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	now := time.Now().UTC().Truncate(time.Second)
	oldest := now.Add(-10 * time.Minute)
	queueFixture(t, spool, "z-retry-oldest", now, nil, oldest, nil)
	options := testWorkerOptions(&now)
	options.Concurrency = 1
	worker, err := NewWorker(spool, ProcessorFunc(func(context.Context, Manifest) error {
		return Retryable("s3_upload_failed", errors.New("temporary"))
	}), options)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := worker.ProcessOnce(context.Background())
	if err != nil || stats.Retried != 1 {
		t.Fatalf("initial retry = %+v, %v", stats, err)
	}
	retryPath := filepath.Join(spool.Root(), spoolRetryDir, "z-retry-oldest-event.json")
	persisted, err := spool.LoadManifest(retryPath)
	if err != nil || persisted.EnqueuedAt == nil || !persisted.EnqueuedAt.Equal(oldest) {
		t.Fatalf("retry lost original queue age: %v, %v", persisted.EnqueuedAt, err)
	}
	queueFixture(t, spool, "a-newer", now.Add(-time.Hour), timePointer(now.Add(-time.Minute)), now, nil)
	now = now.Add(time.Second)
	var order []string
	restarted, err := NewWorker(spool, ProcessorFunc(func(_ context.Context, m Manifest) error {
		order = append(order, m.RequestID)
		return nil
	}), options)
	if err != nil {
		t.Fatal(err)
	}
	stats, err = restarted.ProcessOnce(context.Background())
	if err != nil || stats.Succeeded != 2 || !reflect.DeepEqual(order, []string{"z-retry-oldest", "a-newer"}) {
		t.Fatalf("restart order = %v, stats=%+v, err=%v", order, stats, err)
	}
}

func TestQueueBoundedConcurrencyRefillsBeforeSlowestFinishes(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	now := time.Now().UTC()
	ids := []string{"z-slow-oldest", "y-second", "x-third", "w-fourth", "v-fifth"}
	releases := make(map[string]chan struct{}, len(ids))
	for i, id := range ids {
		queueFixture(t, spool, id, now, timePointer(now.Add(time.Duration(i)*time.Second-time.Minute)), now, nil)
		releases[id] = make(chan struct{})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan string, len(ids))
	var active, peak atomic.Int32
	options := testWorkerOptions(&now)
	options.Concurrency = 2
	var successes []string // Callbacks must stay serialized even while storage runs concurrently.
	options.OnSuccess = func(m Manifest) { successes = append(successes, m.RequestID) }
	worker, err := NewWorker(spool, ProcessorFunc(func(ctx context.Context, m Manifest) error {
		count := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); count > previous && !peak.CompareAndSwap(previous, count); previous = peak.Load() {
		}
		started <- m.RequestID
		select {
		case <-releases[m.RequestID]:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}), options)
	if err != nil {
		t.Fatal(err)
	}
	done := queueStartPass(worker, ctx)
	first := []string{queueReceive(t, started), queueReceive(t, started)}
	sort.Strings(first)
	wantFirst := append([]string(nil), ids[:2]...)
	sort.Strings(wantFirst)
	if !reflect.DeepEqual(first, wantFirst) {
		t.Fatalf("initial claims = %v, want oldest two %v", first, wantFirst)
	}
	// The oldest item remains blocked while the other slot drains the rest.
	for i := 1; i < len(ids)-1; i++ {
		close(releases[ids[i]])
		if got := queueReceive(t, started); got != ids[i+1] {
			t.Fatalf("refilled slot with %s, want %s", got, ids[i+1])
		}
	}
	close(releases[ids[len(ids)-1]])
	select {
	case result := <-done:
		t.Fatalf("pass returned while oldest storage operation was still active: %+v", result)
	default:
	}
	close(releases[ids[0]])
	result := queueReceive(t, done)
	if result.err != nil || result.stats.Claimed != len(ids) || result.stats.Succeeded != len(ids) || peak.Load() != 2 || len(successes) != len(ids) {
		t.Fatalf("concurrent pass = %+v, peak=%d, successes=%v", result, peak.Load(), successes)
	}
}

func TestQueueCancellationWaitsForClaimsAndDoesNotDispatchMore(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	now := time.Now().UTC()
	for i := range 6 {
		queueFixture(t, spool, fmt.Sprintf("request-%d", i), now, timePointer(now.Add(time.Duration(i)*time.Second-time.Minute)), now, nil)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, sawCancellation := make(chan string, 6), make(chan string, 6)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	options := testWorkerOptions(&now)
	options.Concurrency = 2
	worker, err := NewWorker(spool, ProcessorFunc(func(ctx context.Context, m Manifest) error {
		started <- m.RequestID
		<-ctx.Done()
		sawCancellation <- m.RequestID
		<-release
		return ctx.Err()
	}), options)
	if err != nil {
		t.Fatal(err)
	}
	done := queueStartPass(worker, ctx)
	queueReceive(t, started)
	queueReceive(t, started)
	cancel()
	queueReceive(t, sawCancellation)
	queueReceive(t, sawCancellation)
	select {
	case result := <-done:
		t.Fatalf("cancellation released spool ownership with storage still running: %+v", result)
	default:
	}
	releaseOnce.Do(func() { close(release) })
	result := queueReceive(t, done)
	if !errors.Is(result.err, context.Canceled) || result.stats.Claimed != 2 || result.stats.Retried != 2 {
		t.Fatalf("canceled pass = %+v", result)
	}
	if len(started) != 0 {
		t.Fatalf("claimed %d extra requests after cancellation", len(started))
	}
	for dir, want := range map[string]int{spoolReadyDir: 4, spoolRetryDir: 2, spoolProcessingDir: 0} {
		paths, err := spool.listManifestPaths(dir)
		if err != nil || len(paths) != want {
			t.Fatalf("%s after cancellation = %v, %v; want %d", dir, paths, err, want)
		}
	}
	// A later instance can resume all durable work without recapturing it.
	now = now.Add(time.Second)
	restarted, err := NewWorker(spool, ProcessorFunc(func(context.Context, Manifest) error { return nil }), options)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := restarted.ProcessOnce(context.Background())
	if err != nil || stats.Claimed != 6 || stats.Succeeded != 6 {
		t.Fatalf("restart after cancellation = %+v, %v", stats, err)
	}
}

func TestQueueConcurrentPassCannotReclaimAnActiveStorageOperation(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	now := time.Now().UTC()
	for i, id := range []string{"first", "second"} {
		queueFixture(t, spool, id, now, timePointer(now.Add(time.Duration(i)*time.Second-time.Minute)), now, nil)
	}
	firstStarted, release := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	options := testWorkerOptions(&now)
	options.BatchSize, options.Concurrency = 1, 2
	first, err := NewWorker(spool, ProcessorFunc(func(ctx context.Context, m Manifest) error {
		if m.RequestID != "first" {
			return fmt.Errorf("first worker claimed %s", m.RequestID)
		}
		close(firstStarted)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}), options)
	if err != nil {
		t.Fatal(err)
	}
	firstDone := queueStartPass(first, ctx)
	queueReceive(t, firstStarted)
	future := now.Add(10 * options.ClaimTimeout)
	secondOptions := testWorkerOptions(&future)
	secondOptions.Concurrency = 2
	var secondCalls []string
	second, err := NewWorker(spool, ProcessorFunc(func(_ context.Context, m Manifest) error {
		secondCalls = append(secondCalls, m.RequestID)
		return nil
	}), secondOptions)
	if err != nil {
		t.Fatal(err)
	}
	secondDone := queueStartPass(second, ctx)
	// The second pass sees a clock beyond the first claim's recovery deadline,
	// but must wait for spool ownership instead of recovering an active claim.
	select {
	case result := <-secondDone:
		t.Fatalf("second pass entered active spool: %+v", result)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	firstResult, secondResult := queueReceive(t, firstDone), queueReceive(t, secondDone)
	if firstResult.err != nil || firstResult.stats.Succeeded != 1 || secondResult.err != nil || secondResult.stats.Succeeded != 1 || secondResult.stats.Recovered != 0 || !reflect.DeepEqual(secondCalls, []string{"second"}) {
		t.Fatalf("shared spool: first=%+v second=%+v second calls=%v", firstResult, secondResult, secondCalls)
	}
}

func TestQueueOrdinaryAttemptHasDeadlineAndRetainsRetry(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	now := time.Now().UTC()
	artifact := commitWorkerBundle(t, spool, now, "storage-timeout")
	options := testWorkerOptions(&now)
	options.Concurrency = 2
	options.ClaimTimeout = 25 * time.Millisecond
	worker, err := NewWorker(spool, ProcessorFunc(func(ctx context.Context, _ Manifest) error {
		if _, ok := ctx.Deadline(); !ok {
			return Permanent("missing_deadline", errors.New("ordinary storage attempt has no deadline"))
		}
		<-ctx.Done()
		return ctx.Err()
	}), options)
	if err != nil {
		t.Fatal(err)
	}
	result := queueReceive(t, queueStartPass(worker, context.Background()))
	if result.err != nil || result.stats.Retried != 1 || result.stats.DeadLettered != 0 {
		t.Fatalf("storage timeout = %+v", result)
	}
	if paths, _ := spool.listManifestPaths(spoolRetryDir); len(paths) != 1 {
		t.Fatalf("timed out work not retained: %v", paths)
	}
	if _, err := os.Stat(filepath.Join(spool.Root(), artifact.SpoolPath)); err != nil {
		t.Fatalf("timeout removed source artifact: %v", err)
	}
}

func TestQueueConcurrentPermanentFailuresKeepCallbacksAndDeadLettersConsistent(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	now := time.Now().UTC()
	for i := range 8 {
		commitWorkerBundle(t, spool, now, fmt.Sprintf("invalid-%d", i))
	}
	options := testWorkerOptions(&now)
	options.Concurrency = 2
	dead := map[string]int{}
	failed := 0
	// These callback-owned values intentionally have no mutex: the coordinator
	// must serialize callbacks; go test -race checks that ownership contract.
	options.OnDeadLetter = func(m Manifest, _ error) { dead[m.RequestID]++ }
	options.OnError = func(error) { failed++ }
	worker, err := NewWorker(spool, ProcessorFunc(func(context.Context, Manifest) error {
		return Permanent("invalid_manifest", errors.New("permanent fixture failure"))
	}), options)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := worker.ProcessOnce(context.Background())
	if err != nil || stats.Claimed != 8 || stats.DeadLettered != 8 || len(dead) != 8 || failed != 8 {
		t.Fatalf("permanent failures: %+v, %v, callbacks=%v errors=%d", stats, err, dead, failed)
	}
	for id, count := range dead {
		if count != 1 {
			t.Fatalf("dead-letter callback for %s fired %d times", id, count)
		}
	}
	if paths, _ := spool.listManifestPaths(spoolDeadDir); len(paths) != 8 {
		t.Fatalf("permanent sources not retained: %v", paths)
	}
	stats, err = worker.ProcessOnce(context.Background())
	if err != nil || stats.Claimed != 0 || failed != 8 {
		t.Fatalf("permanent failures were automatically retried: %+v, %v, errors=%d", stats, err, failed)
	}
}

func TestQueueConcurrencyBounds(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{-1, 17} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			now := time.Now().UTC()
			options := testWorkerOptions(&now)
			options.Concurrency = concurrency
			if _, err := NewWorker(newTestSpool(t), ProcessorFunc(func(context.Context, Manifest) error { return nil }), options); err == nil {
				t.Fatalf("accepted unsupported concurrency %d", concurrency)
			}
		})
	}
}

func TestQueuePendingAgeSurvivesClaimAndRetry(t *testing.T) {
	t.Parallel()
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			spool := newTestSpool(t)
			now := time.Now().UTC()
			old := now.Add(-3 * time.Minute)
			queuedAt, mtime := timePointer(old), now
			if legacy {
				queuedAt, mtime = nil, old
			}
			queueFixture(t, spool, "aged", now.Add(-time.Hour), queuedAt, mtime, nil)
			runtime := &Runtime{spool: spool}
			assertAge := func(phase string) {
				t.Helper()
				var snapshot RuntimeSnapshot
				runtime.populateSpoolSnapshot(&snapshot)
				if snapshot.Backlog != 1 || snapshot.OldestPendingSeconds < 180 {
					t.Fatalf("%s reset pending age: backlog=%d age=%d", phase, snapshot.Backlog, snapshot.OldestPendingSeconds)
				}
			}
			assertAge("ready")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started, release := make(chan struct{}), make(chan struct{})
			options := testWorkerOptions(&now)
			options.Concurrency = 2
			worker, err := NewWorker(spool, ProcessorFunc(func(ctx context.Context, _ Manifest) error {
				close(started)
				select {
				case <-release:
					return Retryable("s3_upload_failed", errors.New("temporary"))
				case <-ctx.Done():
					return ctx.Err()
				}
			}), options)
			if err != nil {
				t.Fatal(err)
			}
			done := queueStartPass(worker, ctx)
			queueReceive(t, started)
			assertAge("processing")
			close(release)
			result := queueReceive(t, done)
			if result.err != nil || result.stats.Retried != 1 {
				t.Fatalf("retry = %+v", result)
			}
			assertAge("retry")
		})
	}
}
