package callaudit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"golang.org/x/sys/unix"
)

type RuntimeSnapshot struct {
	Enabled              bool   `json:"enabled"`
	Ready                bool   `json:"ready"`
	SchemaReady          bool   `json:"schema_ready"`
	StorageReady         bool   `json:"storage_ready"`
	DeadLetterReady      bool   `json:"dead_letter_ready"`
	ReadinessError       string `json:"readiness_error,omitempty"`
	Captured             uint64 `json:"captured"`
	CaptureFailures      uint64 `json:"capture_failures"`
	WorkerFailures       uint64 `json:"worker_failures"`
	DeadLettered         uint64 `json:"dead_lettered"`
	RawCaptureDisabled   uint64 `json:"raw_capture_disabled"`
	ActiveSessions       int64  `json:"active_sessions"`
	FinalizeSaturated    uint64 `json:"finalize_saturated"`
	Initialization       string `json:"initialization_error,omitempty"`
	Backlog              int    `json:"backlog"`
	DeadLetterFiles      int    `json:"dead_letter_files"`
	OldestPendingSeconds int64  `json:"oldest_pending_seconds"`
	DiskUsedPercent      int    `json:"disk_used_percent"`
	DiskAvailableBytes   uint64 `json:"disk_available_bytes"`
}

const (
	maxConcurrentFinalizers  = 256
	storageReadinessInterval = time.Minute
)

var (
	errSchemaReadinessPending     = errors.New("audit schema readiness check pending")
	errDeadLetterReadinessPending = errors.New("audit dead-letter scan pending")
	errDeadLettersPresent         = errors.New("audit dead letters require attention")
)

// Runtime owns one instance's spool and local worker. It never relies on Redis,
// so a manifest is only consumed by the instance that can read its artifacts.
type Runtime struct {
	cfg     config.CallAuditConfig
	spool   *Spool
	store   *PostgreSQLStore
	objects *ObjectStore
	worker  *Worker

	ctx             context.Context
	cancel          context.CancelFunc
	done            chan struct{}
	maintenanceDone chan struct{}

	initErr             error
	captured            atomic.Uint64
	captureFailures     atomic.Uint64
	workerFailures      atomic.Uint64
	deadLettered        atomic.Uint64
	rawCaptureDisabled  atomic.Uint64
	activeSessions      atomic.Int64
	finalizeSaturated   atomic.Uint64
	schemaReady         atomic.Bool
	storageReady        atomic.Bool
	deadLetterReady     atomic.Bool
	readinessMu         sync.RWMutex
	schemaReadinessErr  string
	storageReadinessErr string
	deadLetterErr       string
	closeOnce           sync.Once
	finalizeMu          sync.Mutex
	finalizeWG          sync.WaitGroup
	finalizeSlots       chan struct{}
	sessionWG           sync.WaitGroup
	closing             bool
}

// NewRuntime intentionally returns a degraded Runtime instead of failing the
// application. The configured failure policy is nonblocking: inference keeps
// serving while health/metrics and logs expose audit degradation.
func NewRuntime(cfg *config.Config) *Runtime {
	runtime := &Runtime{
		done:            make(chan struct{}),
		maintenanceDone: make(chan struct{}),
		finalizeSlots:   make(chan struct{}, maxConcurrentFinalizers),
	}
	if cfg == nil {
		runtime.initErr = errors.New("call audit config is unavailable")
		close(runtime.done)
		close(runtime.maintenanceDone)
		return runtime
	}
	runtime.cfg = cfg.CallAudit
	if !runtime.cfg.Enabled {
		close(runtime.done)
		close(runtime.maintenanceDone)
		return runtime
	}

	spool, err := NewSpool(runtime.cfg.SpoolDir, runtime.cfg.MaxArtifactBytes)
	if err != nil {
		runtime.failInitialization(err)
		return runtime
	}
	if _, err := spool.CleanupOrphanTemps(); err != nil {
		runtime.failInitialization(fmt.Errorf("clean audit spool temporary files: %w", err))
		return runtime
	}
	store, err := OpenPostgreSQLStore(runtime.cfg.PostgresURL, 5, 2)
	if err != nil {
		runtime.failInitialization(err)
		return runtime
	}
	objects, err := NewObjectStore(context.Background(), spool, ObjectStoreConfig{
		Endpoint:        runtime.cfg.S3.Endpoint,
		Region:          runtime.cfg.S3.Region,
		Bucket:          runtime.cfg.S3.Bucket,
		AccessKeyID:     runtime.cfg.S3.AccessKey,
		SecretAccessKey: runtime.cfg.S3.SecretKey,
		Prefix:          runtime.cfg.ObjectKeyPrefix,
		ForcePathStyle:  runtime.cfg.S3.ForcePathStyle,
	})
	if err != nil {
		_ = store.Close()
		runtime.failInitialization(err)
		return runtime
	}
	processor, err := NewDurableProcessor(store, objects)
	if err != nil {
		_ = store.Close()
		runtime.failInitialization(err)
		return runtime
	}
	runtime.spool, runtime.store, runtime.objects = spool, store, objects
	runtime.ctx, runtime.cancel = context.WithCancel(context.Background())
	runtime.setSchemaReadiness(false, errSchemaReadinessPending)
	// A worker/object-store failure has not been observed yet. Schema readiness
	// remains the startup gate until the read-only catalog check completes.
	runtime.setStorageReadiness(true, nil)
	// The initial maintenance pass must prove that the durable dead-letter
	// directory is empty before this independent gate can become ready.
	runtime.setDeadLetterReadiness(false, errDeadLetterReadinessPending)

	if runtime.cfg.Worker.Enabled {
		worker, workerErr := NewWorker(spool, processor, WorkerOptions{
			MaxAttempts:       runtime.cfg.Worker.MaxAttempts,
			BatchSize:         runtime.cfg.Worker.BatchSize,
			PollInterval:      time.Duration(runtime.cfg.Worker.PollIntervalMS) * time.Millisecond,
			RetryInitialDelay: time.Duration(runtime.cfg.Worker.RetryInitialDelayMS) * time.Millisecond,
			RetryMaxDelay:     time.Duration(runtime.cfg.Worker.RetryMaxDelayMS) * time.Millisecond,
			ClaimTimeout:      time.Duration(runtime.cfg.Worker.ClaimTimeoutSeconds) * time.Second,
			OnError: func(processErr error) {
				runtime.workerFailures.Add(1)
				runtime.setStorageReadiness(false, processErr)
				slog.Warn("call audit worker error", "error", safeStorageReadinessError(processErr))
			},
			OnDeadLetter: func(manifest Manifest, processErr error) {
				runtime.deadLettered.Add(1)
				runtime.setDeadLetterReadiness(false, errDeadLettersPresent)
				markCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := store.UpsertCall(markCtx, manifest, CaptureFailed, processErr); err != nil {
					runtime.workerFailures.Add(1)
					runtime.setStorageReadiness(false, err)
					slog.Warn("call audit dead-letter status deferred", "request_id", manifest.RequestID, "error", safeStorageReadinessError(err))
				}
			},
			OnSuccess: func(Manifest) {
				runtime.setStorageReadiness(true, nil)
			},
		})
		if workerErr != nil {
			_ = store.Close()
			runtime.failInitialization(workerErr)
			return runtime
		}
		runtime.worker = worker
		go runtime.run()
	} else {
		close(runtime.done)
	}
	go runtime.readinessMaintenance()
	return runtime
}

func (r *Runtime) failInitialization(err error) {
	r.initErr = err
	slog.Error("call audit initialized in degraded nonblocking mode", "error", err)
	close(r.done)
	close(r.maintenanceDone)
}

func (r *Runtime) run() {
	defer close(r.done)
	if err := r.worker.Run(r.ctx); err != nil && !errors.Is(err, context.Canceled) {
		r.workerFailures.Add(1)
		r.setStorageReadiness(false, err)
		slog.Error("call audit worker stopped", "error", safeStorageReadinessError(err))
	}
}

func (r *Runtime) readinessMaintenance() {
	defer close(r.maintenanceDone)
	if r == nil || r.store == nil || r.ctx == nil {
		return
	}
	check := func() {
		ctx, cancel := context.WithTimeout(r.ctx, 30*time.Second)
		defer cancel()
		if err := r.store.CheckReadiness(ctx, time.Now()); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			r.workerFailures.Add(1)
			r.setSchemaReadiness(false, err)
			slog.Warn("call audit storage readiness check failed", "error", safeStorageReadinessError(err))
			return
		}
		r.setSchemaReadiness(true, nil)
	}
	check()
	r.reconcileDeadLetters()
	readinessTicker := time.NewTicker(storageReadinessInterval)
	deadLetterTicker := time.NewTicker(time.Minute)
	defer readinessTicker.Stop()
	defer deadLetterTicker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-readinessTicker.C:
			check()
		case <-deadLetterTicker.C:
			r.reconcileDeadLetters()
		}
	}
}

func (r *Runtime) reconcileDeadLetters() {
	if r == nil || r.store == nil || r.spool == nil || r.ctx == nil {
		return
	}
	paths, err := r.spool.listManifestPaths(spoolDeadDir)
	if err != nil {
		r.workerFailures.Add(1)
		r.setDeadLetterReadiness(false, err)
		return
	}
	if len(paths) == 0 {
		r.setDeadLetterReadiness(true, nil)
		return
	}
	r.setDeadLetterReadiness(false, errDeadLettersPresent)
	for _, path := range paths {
		manifest, loadErr := r.spool.LoadManifest(path)
		if loadErr != nil || strings.TrimSpace(manifest.RequestID) == "" {
			continue
		}
		message := errors.New("audit capture failed")
		if manifest.LastErrorCode != "" {
			message = fmt.Errorf("audit capture failed: %s", manifest.LastErrorCode)
		}
		ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
		err = r.store.UpsertCall(ctx, manifest, CaptureFailed, message)
		cancel()
		if err != nil && !errors.Is(err, context.Canceled) {
			r.workerFailures.Add(1)
			r.setStorageReadiness(false, err)
			return
		}
	}
}

func (r *Runtime) Enabled() bool { return r != nil && r.cfg.Enabled }

func (r *Runtime) Ready() bool {
	return r != nil && r.cfg.Enabled && r.initErr == nil && r.spool != nil &&
		r.schemaReady.Load() && r.storageReady.Load() && r.deadLetterReady.Load()
}

func (r *Runtime) setSchemaReadiness(ready bool, cause error) {
	if r == nil {
		return
	}
	r.readinessMu.Lock()
	if ready {
		r.schemaReadinessErr = ""
	} else {
		r.schemaReadinessErr = safeStorageReadinessError(cause)
	}
	r.schemaReady.Store(ready)
	r.readinessMu.Unlock()
}

func (r *Runtime) setStorageReadiness(ready bool, cause error) {
	if r == nil {
		return
	}
	r.readinessMu.Lock()
	if ready {
		r.storageReadinessErr = ""
	} else {
		r.storageReadinessErr = safeStorageReadinessError(cause)
	}
	r.storageReady.Store(ready)
	r.readinessMu.Unlock()
}

func (r *Runtime) setDeadLetterReadiness(ready bool, cause error) {
	if r == nil {
		return
	}
	r.readinessMu.Lock()
	if ready {
		r.deadLetterErr = ""
	} else {
		r.deadLetterErr = safeStorageReadinessError(cause)
	}
	r.deadLetterReady.Store(ready)
	r.readinessMu.Unlock()
}

func (r *Runtime) readinessSnapshot() (schemaReady, storageReady, deadLetterReady bool, diagnostic string) {
	if r == nil {
		return false, false, false, ""
	}
	r.readinessMu.RLock()
	defer r.readinessMu.RUnlock()
	schemaReady = r.schemaReady.Load()
	storageReady = r.storageReady.Load()
	deadLetterReady = r.deadLetterReady.Load()
	issues := make([]string, 0, 3)
	appendIssue := func(ready bool, issue string) {
		if ready || issue == "" {
			return
		}
		for _, existing := range issues {
			if existing == issue {
				return
			}
		}
		issues = append(issues, issue)
	}
	appendIssue(schemaReady, r.schemaReadinessErr)
	appendIssue(storageReady, r.storageReadinessErr)
	appendIssue(deadLetterReady, r.deadLetterErr)
	return schemaReady, storageReady, deadLetterReady, strings.Join(issues, "; ")
}

// safeStorageReadinessError intentionally returns only stable, non-secret
// diagnostics. Database drivers and object clients may include connection
// strings or signed URLs in their original errors, which must not reach health
// responses.
func safeStorageReadinessError(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, errSchemaReadinessPending) {
		return errSchemaReadinessPending.Error()
	}
	if errors.Is(err, errDeadLetterReadinessPending) {
		return errDeadLetterReadinessPending.Error()
	}
	if errors.Is(err, errDeadLettersPresent) {
		return errDeadLettersPresent.Error()
	}
	var processErr *ProcessError
	if errors.As(err, &processErr) && strings.TrimSpace(processErr.Code) != "" {
		return "audit worker failure: " + safeErrorCode(processErr.Code)
	}
	message := err.Error()
	if strings.HasPrefix(message, "audit postgres is not ready:") {
		const maxDiagnosticBytes = 512
		if len(message) > maxDiagnosticBytes {
			return message[:maxDiagnosticBytes] + "..."
		}
		return message
	}
	for _, operation := range []string{
		"ping audit postgres",
		"check audit relations",
		"check audit columns",
		"check audit partitions",
		"check audit indexes",
	} {
		if strings.HasPrefix(message, operation+":") {
			return operation + " failed"
		}
	}
	return "audit storage operation failed"
}

func safeErrorCode(code string) string {
	if code == "" || len(code) > 64 {
		return "unknown"
	}
	for _, character := range code {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || character == '_' || character == '-') {
			return "unknown"
		}
	}
	return code
}

func (r *Runtime) MaxArtifactBytes() int64 {
	if r == nil {
		return 0
	}
	return r.cfg.MaxArtifactBytes
}

func (r *Runtime) StartSession(input ScopeInput) (*Session, error) {
	if r == nil || !r.cfg.Enabled {
		return nil, nil
	}
	if r.initErr != nil || r.spool == nil {
		r.captureFailures.Add(1)
		return nil, fmt.Errorf("call audit unavailable: %w", r.initErr)
	}
	scope, err := NewScope(input, r.cfg.RetentionDays, time.Now())
	if err != nil {
		r.captureFailures.Add(1)
		return nil, err
	}
	session, err := NewSession(scope, r.spool, time.Duration(r.cfg.UsageWaitTimeoutMS)*time.Millisecond)
	if err != nil {
		r.captureFailures.Add(1)
		return nil, err
	}
	if reason := r.diskPressureReason(); reason != "" {
		session.DisableArtifacts(reason)
		r.rawCaptureDisabled.Add(1)
	}
	r.finalizeMu.Lock()
	if r.closing {
		r.finalizeMu.Unlock()
		r.captureFailures.Add(1)
		return nil, errors.New("call audit runtime is shutting down")
	}
	r.sessionWG.Add(1)
	r.activeSessions.Add(1)
	session.releaseRuntime = func() {
		r.activeSessions.Add(-1)
		r.sessionWG.Done()
	}
	r.finalizeMu.Unlock()
	r.captured.Add(1)
	return session, nil
}

func (r *Runtime) RecordCaptureFailure(err error) {
	if r == nil || err == nil {
		return
	}
	r.captureFailures.Add(1)
	slog.Warn("call audit capture degraded", "error", err)
}

// RunFinalize keeps disk fsync/rename work off the client response path while
// allowing Shutdown to drain already accepted audit finalizers.
func (r *Runtime) RunFinalize(task func()) {
	if task == nil {
		return
	}
	if r == nil {
		task()
		return
	}
	r.finalizeMu.Lock()
	if r.finalizeSlots == nil {
		// Keep manually constructed runtimes used by focused tests and degraded
		// paths subject to the same process-wide execution bound.
		r.finalizeSlots = make(chan struct{}, maxConcurrentFinalizers)
	}
	finalizeSlots := r.finalizeSlots
	if r.closing {
		r.finalizeMu.Unlock()
		// HTTP shutdown normally drains requests before cleanup. A late request
		// still finalizes synchronously rather than being silently discarded, but
		// it must share the same execution permits as earlier finalizers.
		r.acquireFinalizeSlot(finalizeSlots)
		defer func() { <-finalizeSlots }()
		r.runFinalizeTask(task)
		return
	}
	r.finalizeWG.Add(1)
	r.finalizeMu.Unlock()
	select {
	case finalizeSlots <- struct{}{}:
		go func() {
			defer r.finalizeWG.Done()
			defer func() { <-finalizeSlots }()
			r.runFinalizeTask(task)
		}()
	default:
		r.finalizeSaturated.Add(1)
		// Saturation is exceptional. The response bytes have already been sent, so
		// wait in the existing handler goroutine rather than starting unbounded
		// disk work without a permit.
		finalizeSlots <- struct{}{}
		defer r.finalizeWG.Done()
		defer func() { <-finalizeSlots }()
		r.runFinalizeTask(task)
	}
}

func (r *Runtime) acquireFinalizeSlot(finalizeSlots chan struct{}) {
	select {
	case finalizeSlots <- struct{}{}:
	default:
		r.finalizeSaturated.Add(1)
		finalizeSlots <- struct{}{}
	}
}

func (r *Runtime) runFinalizeTask(task func()) {
	defer func() {
		if recovered := recover(); recovered != nil {
			r.RecordCaptureFailure(fmt.Errorf("call audit finalizer panic: %v", recovered))
		}
	}()
	task()
}

func (r *Runtime) Snapshot() RuntimeSnapshot {
	if r == nil {
		return RuntimeSnapshot{}
	}
	schemaReady, storageReady, deadLetterReady, readinessError := r.readinessSnapshot()
	snapshot := RuntimeSnapshot{
		Enabled:            r.cfg.Enabled,
		Ready:              r.cfg.Enabled && r.initErr == nil && r.spool != nil && schemaReady && storageReady && deadLetterReady,
		SchemaReady:        schemaReady,
		StorageReady:       storageReady,
		DeadLetterReady:    deadLetterReady,
		ReadinessError:     readinessError,
		Captured:           r.captured.Load(),
		CaptureFailures:    r.captureFailures.Load(),
		WorkerFailures:     r.workerFailures.Load(),
		DeadLettered:       r.deadLettered.Load(),
		RawCaptureDisabled: r.rawCaptureDisabled.Load(),
		ActiveSessions:     r.activeSessions.Load(),
		FinalizeSaturated:  r.finalizeSaturated.Load(),
	}
	if r != nil && r.initErr != nil {
		snapshot.Initialization = r.initErr.Error()
	}
	r.populateSpoolSnapshot(&snapshot)
	return snapshot
}

func (r *Runtime) populateSpoolSnapshot(snapshot *RuntimeSnapshot) {
	if r == nil || r.spool == nil || snapshot == nil {
		return
	}
	oldest := time.Time{}
	for _, directory := range []string{spoolReadyDir, spoolRetryDir, spoolProcessingDir} {
		paths, err := r.spool.listManifestPaths(directory)
		if err != nil {
			continue
		}
		snapshot.Backlog += len(paths)
		for _, path := range paths {
			if info, err := os.Stat(path); err == nil && (oldest.IsZero() || info.ModTime().Before(oldest)) {
				oldest = info.ModTime()
			}
		}
	}
	if paths, err := r.spool.listManifestPaths(spoolDeadDir); err == nil {
		snapshot.DeadLetterFiles = len(paths)
	}
	if !oldest.IsZero() {
		snapshot.OldestPendingSeconds = max(0, int64(time.Since(oldest).Seconds()))
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(r.spool.Root(), &stat); err == nil {
		blockSize := uint64(stat.Bsize)
		total := stat.Blocks * blockSize
		available := stat.Bavail * blockSize
		snapshot.DiskAvailableBytes = available
		if total > 0 {
			snapshot.DiskUsedPercent = int((total - available) * 100 / total)
		}
	}
}

func (r *Runtime) diskPressureReason() string {
	if r == nil || r.spool == nil {
		return "spool_unavailable"
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(r.spool.Root(), &stat); err != nil {
		return ""
	}
	blockSize := uint64(stat.Bsize)
	total := stat.Blocks * blockSize
	available := stat.Bavail * blockSize
	if r.cfg.DiskMinFreeBytes > 0 && available < uint64(r.cfg.DiskMinFreeBytes) {
		return "disk_min_free_bytes"
	}
	if total > 0 {
		usedPercent := int((total - available) * 100 / total)
		if usedPercent >= r.cfg.DiskHighWatermarkPercent {
			return "disk_high_watermark"
		}
	}
	return ""
}

func (r *Runtime) Shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	var result error
	r.closeOnce.Do(func() {
		r.finalizeMu.Lock()
		r.closing = true
		r.finalizeMu.Unlock()
		if r.cancel != nil {
			r.cancel()
		}
		select {
		case <-r.done:
		case <-ctx.Done():
			result = ctx.Err()
		}
		select {
		case <-r.maintenanceDone:
		case <-ctx.Done():
			result = errors.Join(result, ctx.Err())
		}
		finalizersDone := make(chan struct{})
		go func() {
			r.sessionWG.Wait()
			r.finalizeWG.Wait()
			close(finalizersDone)
		}()
		select {
		case <-finalizersDone:
		case <-ctx.Done():
			result = errors.Join(result, ctx.Err())
		}
		if r.store != nil {
			result = errors.Join(result, r.store.Close())
		}
	})
	return result
}
