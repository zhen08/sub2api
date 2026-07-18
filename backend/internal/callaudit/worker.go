package callaudit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type ManifestProcessor interface {
	Process(context.Context, Manifest) error
}

type ProcessorFunc func(context.Context, Manifest) error

func (fn ProcessorFunc) Process(ctx context.Context, manifest Manifest) error {
	return fn(ctx, manifest)
}

type ProcessError struct {
	Code      string
	Retryable bool
	Err       error
}

func (e *ProcessError) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return e.Code
}

func (e *ProcessError) Unwrap() error { return e.Err }

func Retryable(code string, err error) error {
	return &ProcessError{Code: code, Retryable: true, Err: err}
}

func Permanent(code string, err error) error {
	return &ProcessError{Code: code, Retryable: false, Err: err}
}

type WorkerOptions struct {
	MaxAttempts       int
	BatchSize         int
	PollInterval      time.Duration
	RetryInitialDelay time.Duration
	RetryMaxDelay     time.Duration
	ClaimTimeout      time.Duration
	Now               func() time.Time
	OnError           func(error)
	OnDeadLetter      func(Manifest, error)
	OnSuccess         func(Manifest)
}

type WorkerStats struct {
	Claimed      int
	Succeeded    int
	Retried      int
	DeadLettered int
	Recovered    int
	Cleaned      int
}

type Worker struct {
	spool     *Spool
	processor ManifestProcessor
	options   WorkerOptions
}

func NewWorker(spool *Spool, processor ManifestProcessor, options WorkerOptions) (*Worker, error) {
	if spool == nil {
		return nil, fmt.Errorf("audit spool is required")
	}
	if processor == nil {
		return nil, fmt.Errorf("audit manifest processor is required")
	}
	if options.MaxAttempts <= 0 || options.BatchSize <= 0 || options.PollInterval <= 0 ||
		options.RetryInitialDelay <= 0 || options.RetryMaxDelay <= 0 || options.ClaimTimeout <= 0 {
		return nil, fmt.Errorf("audit worker options must be positive")
	}
	if options.RetryMaxDelay < options.RetryInitialDelay {
		return nil, fmt.Errorf("audit retry maximum delay must be >= initial delay")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.OnError == nil {
		options.OnError = func(error) {}
	}
	if options.OnDeadLetter == nil {
		options.OnDeadLetter = func(Manifest, error) {}
	}
	if options.OnSuccess == nil {
		options.OnSuccess = func(Manifest) {}
	}
	return &Worker{spool: spool, processor: processor, options: options}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(w.options.PollInterval)
	defer ticker.Stop()
	for {
		if _, err := w.ProcessOnce(ctx); err != nil {
			w.options.OnError(err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (w *Worker) ProcessOnce(ctx context.Context) (WorkerStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	now := w.options.Now().UTC()
	stats := WorkerStats{}
	var joined error
	if recovered, err := w.recoverStale(now); err != nil {
		joined = errors.Join(joined, err)
	} else {
		stats.Recovered = recovered
	}
	if cleaned, err := w.cleanupCompleted(); err != nil {
		joined = errors.Join(joined, err)
	} else {
		stats.Cleaned = cleaned
	}

	candidates, err := w.candidates(now)
	if err != nil {
		return stats, errors.Join(joined, err)
	}
	if len(candidates) > w.options.BatchSize {
		candidates = candidates[:w.options.BatchSize]
	}
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return stats, errors.Join(joined, err)
		}
		result, err := w.processCandidate(ctx, candidate, now)
		if err != nil {
			joined = errors.Join(joined, err)
			w.options.OnError(err)
		}
		stats.Claimed += result.Claimed
		stats.Succeeded += result.Succeeded
		stats.Retried += result.Retried
		stats.DeadLettered += result.DeadLettered
	}
	return stats, joined
}

type manifestCandidate struct{ path string }

func (w *Worker) candidates(now time.Time) ([]manifestCandidate, error) {
	ready, err := w.spool.listManifestPaths(spoolReadyDir)
	if err != nil {
		return nil, err
	}
	retry, err := w.spool.listManifestPaths(spoolRetryDir)
	if err != nil {
		return nil, err
	}
	candidates := make([]manifestCandidate, 0, len(ready)+len(retry))
	for _, path := range ready {
		candidates = append(candidates, manifestCandidate{path: path})
	}
	for _, path := range retry {
		manifest, loadErr := w.spool.LoadManifest(path)
		if loadErr != nil || manifest.NextAttemptAt == nil || !manifest.NextAttemptAt.After(now) {
			candidates = append(candidates, manifestCandidate{path: path})
		}
	}
	return candidates, nil
}

func (w *Worker) processCandidate(ctx context.Context, candidate manifestCandidate, now time.Time) (WorkerStats, error) {
	stats := WorkerStats{}
	processingPath := filepath.Join(w.spool.root, spoolProcessingDir, filepath.Base(candidate.path))
	if err := renameDurable(candidate.path, processingPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return stats, nil
		}
		return stats, fmt.Errorf("claim audit manifest: %w", err)
	}
	stats.Claimed = 1
	manifest, err := w.spool.LoadManifest(processingPath)
	if err != nil {
		if moveErr := w.quarantineCorrupt(processingPath); moveErr != nil {
			return stats, errors.Join(err, moveErr)
		}
		stats.DeadLettered = 1
		return stats, fmt.Errorf("load claimed audit manifest: %w", err)
	}
	manifest.State = ManifestProcessing
	manifest.ClaimedAt = timePointer(now)
	manifest.EventSpoolPath = w.relative(processingPath)
	if err := w.writeManifest(processingPath, manifest); err != nil {
		return stats, err
	}

	if err := w.processor.Process(ctx, manifest); err != nil {
		// Processing failures are relevant to runtime storage health even when the
		// durable retry/dead-letter transition itself succeeds. Report them here;
		// ProcessOnce only reports transition errors returned below.
		w.options.OnError(err)
		retried, dead, transitionErr := w.transitionFailure(processingPath, manifest, err, now)
		stats.Retried = btoi(retried)
		stats.DeadLettered = btoi(dead)
		return stats, transitionErr
	}
	if err := w.complete(processingPath, manifest); err != nil {
		return stats, err
	}
	w.options.OnSuccess(manifest)
	stats.Succeeded = 1
	return stats, nil
}

func (w *Worker) transitionFailure(processingPath string, manifest Manifest, processErr error, now time.Time) (retried, dead bool, err error) {
	code, retryable := classifyProcessError(processErr)
	manifest.Attempts++
	manifest.LastAttemptAt = timePointer(now)
	manifest.ClaimedAt = nil
	manifest.LastErrorCode = code
	if !retryable || manifest.Attempts >= w.options.MaxAttempts {
		manifest.State = ManifestDeadLetter
		manifest.CaptureStatus = CaptureFailed
		manifest.FailedAt = timePointer(now)
		manifest.NextAttemptAt = nil
		target := filepath.Join(w.spool.root, spoolDeadDir, filepath.Base(processingPath))
		manifest.EventSpoolPath = w.relative(target)
		if err := w.writeManifest(processingPath, manifest); err != nil {
			return false, false, err
		}
		if err := renameDurable(processingPath, target); err != nil {
			return false, false, fmt.Errorf("move audit manifest to dead letter: %w", err)
		}
		w.options.OnDeadLetter(manifest, processErr)
		return false, true, nil
	}

	next := now.Add(w.retryDelay(manifest.Attempts))
	manifest.State = ManifestRetry
	manifest.CaptureStatus = CaptureRetrying
	manifest.NextAttemptAt = &next
	target := filepath.Join(w.spool.root, spoolRetryDir, filepath.Base(processingPath))
	manifest.EventSpoolPath = w.relative(target)
	if err := w.writeManifest(processingPath, manifest); err != nil {
		return false, false, err
	}
	if err := renameDurable(processingPath, target); err != nil {
		return false, false, fmt.Errorf("move audit manifest to retry: %w", err)
	}
	return true, false, nil
}

func (w *Worker) complete(processingPath string, manifest Manifest) error {
	target := filepath.Join(w.spool.root, spoolCompletedDir, filepath.Base(processingPath))
	manifest.State = ManifestProcessing
	manifest.CaptureStatus = CaptureStored
	manifest.ClaimedAt = nil
	manifest.EventSpoolPath = w.relative(target)
	if err := w.writeManifest(processingPath, manifest); err != nil {
		return err
	}
	if err := renameDurable(processingPath, target); err != nil {
		return fmt.Errorf("commit completed audit manifest: %w", err)
	}
	if err := w.spool.removeBundle(manifest); err != nil {
		return fmt.Errorf("remove completed audit artifacts: %w", err)
	}
	if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove completed audit manifest: %w", err)
	}
	return nil
}

func (w *Worker) recoverStale(now time.Time) (int, error) {
	paths, err := w.spool.listManifestPaths(spoolProcessingDir)
	if err != nil {
		return 0, err
	}
	recovered := 0
	var joined error
	for _, path := range paths {
		info, statErr := os.Stat(path)
		if statErr != nil {
			joined = errors.Join(joined, statErr)
			continue
		}
		if info.ModTime().After(now.Add(-w.options.ClaimTimeout)) {
			continue
		}
		manifest, loadErr := w.spool.LoadManifest(path)
		if loadErr != nil {
			joined = errors.Join(joined, loadErr, w.quarantineCorrupt(path))
			continue
		}
		_, _, transitionErr := w.transitionFailure(path, manifest, Retryable("claim_expired", errors.New("audit worker claim expired")), now)
		if transitionErr != nil {
			joined = errors.Join(joined, transitionErr)
			continue
		}
		recovered++
	}
	return recovered, joined
}

func (w *Worker) cleanupCompleted() (int, error) {
	paths, err := w.spool.listManifestPaths(spoolCompletedDir)
	if err != nil {
		return 0, err
	}
	cleaned := 0
	var joined error
	for _, path := range paths {
		manifest, loadErr := w.spool.LoadManifest(path)
		if loadErr != nil {
			joined = errors.Join(joined, loadErr)
			continue
		}
		if removeErr := w.spool.removeBundle(manifest); removeErr != nil {
			joined = errors.Join(joined, removeErr)
			continue
		}
		if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			joined = errors.Join(joined, removeErr)
			continue
		}
		cleaned++
	}
	return cleaned, joined
}

func (w *Worker) quarantineCorrupt(path string) error {
	target := filepath.Join(w.spool.root, spoolDeadDir, filepath.Base(path))
	if err := renameDurable(path, target); err != nil {
		return fmt.Errorf("quarantine corrupt audit manifest: %w", err)
	}
	return nil
}

func renameDurable(source, target string) error {
	if err := os.Rename(source, target); err != nil {
		return err
	}
	if err := syncDirectory(filepath.Dir(target)); err != nil {
		return fmt.Errorf("sync audit target directory: %w", err)
	}
	if filepath.Dir(source) != filepath.Dir(target) {
		if err := syncDirectory(filepath.Dir(source)); err != nil {
			return fmt.Errorf("sync audit source directory: %w", err)
		}
	}
	return nil
}

func (w *Worker) writeManifest(path string, manifest Manifest) error {
	raw, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	return atomicWriteFile(path, raw, 0o600, false)
}

func (w *Worker) retryDelay(attempt int) time.Duration {
	delay := w.options.RetryInitialDelay
	for index := 1; index < attempt && delay < w.options.RetryMaxDelay; index++ {
		if delay > w.options.RetryMaxDelay/2 {
			return w.options.RetryMaxDelay
		}
		delay *= 2
	}
	if delay > w.options.RetryMaxDelay {
		return w.options.RetryMaxDelay
	}
	return delay
}

func (w *Worker) relative(path string) string {
	relative, err := filepath.Rel(w.spool.root, path)
	if err != nil {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(relative)
}

var processErrorCodePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,63}$`)

func classifyProcessError(err error) (string, bool) {
	var processErr *ProcessError
	if errors.As(err, &processErr) {
		code := strings.ToLower(strings.TrimSpace(processErr.Code))
		if !processErrorCodePattern.MatchString(code) {
			code = "processing_failed"
		}
		return code, processErr.Retryable
	}
	return "processing_failed", true
}

func timePointer(value time.Time) *time.Time { return &value }

func btoi(value bool) int {
	if value {
		return 1
	}
	return 0
}
