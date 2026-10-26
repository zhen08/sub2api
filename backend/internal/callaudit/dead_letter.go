package callaudit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type DeadLetterRetryOptions struct {
	Enabled      bool
	MaxRetries   int
	BatchSize    int
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

// DeadLetterSnapshot is published by the sole queue owner. In-flight recovered
// bundles remain unresolved even while dead-letter/ is temporarily empty.
type DeadLetterSnapshot struct {
	Recovering       int
	Retryable        int
	Exhausted        int
	Permanent        int
	OldestSeconds    int64
	AutoRetryEnabled bool
	ScanFailed       bool
}

func (s DeadLetterSnapshot) unresolved() int {
	return s.Recovering + s.Retryable + s.Exhausted + s.Permanent
}

func deadLetterRetryable(manifest Manifest) bool {
	if manifest.LastFailureRetryable != nil {
		return *manifest.LastFailureRetryable
	}
	// Older manifests did not persist error classification. Only known storage
	// operations may opt in; corrupt/unknown records require operator review.
	switch manifest.LastErrorCode {
	case "postgres_upsert_failed", "postgres_commit_failed", "s3_upload_failed", "claim_expired":
		return true
	default:
		return false
	}
}

func deadLetterFirstFailure(manifest Manifest, fallback time.Time) time.Time {
	for _, value := range []*time.Time{manifest.FirstFailedAt, manifest.FailedAt, manifest.LastAttemptAt} {
		if value != nil && !value.IsZero() {
			return value.UTC()
		}
	}
	return fallback.UTC()
}

func (w *Worker) canRetryDeadLetter(manifest Manifest) bool {
	return w.options.DeadLetterRetry.Enabled && deadLetterRetryable(manifest) &&
		manifest.DeadLetterRetries >= 0 && manifest.DeadLetterRetries < w.options.DeadLetterRetry.MaxRetries
}

func (w *Worker) deadLetterDelay(retries int) time.Duration {
	delay := w.options.DeadLetterRetry.InitialDelay
	for i := 0; i < retries && delay < w.options.DeadLetterRetry.MaxDelay; i++ {
		if delay > w.options.DeadLetterRetry.MaxDelay/2 {
			return w.options.DeadLetterRetry.MaxDelay
		}
		delay *= 2
	}
	return min(delay, w.options.DeadLetterRetry.MaxDelay)
}

func (s *Spool) deadLetterSnapshot(now time.Time, options DeadLetterRetryOptions) DeadLetterSnapshot {
	result := DeadLetterSnapshot{AutoRetryEnabled: options.Enabled}
	oldest := time.Time{}
	// Automatic slow retries go directly dead-letter -> processing -> either
	// completed or dead-letter. Ordinary ready/retry queues need no JSON scan.
	for _, dir := range []string{spoolDeadDir, spoolProcessingDir} {
		paths, err := s.listManifestPaths(dir)
		if err != nil {
			result.ScanFailed = true
			continue
		}
		for _, path := range paths {
			manifest, loadErr := s.LoadManifest(path)
			if loadErr != nil && errors.Is(loadErr, os.ErrNotExist) {
				continue
			}
			if dir != spoolDeadDir && loadErr == nil && manifest.DeadLetterRetries == 0 &&
				manifest.State != ManifestDeadLetter && manifest.FirstFailedAt == nil {
				continue
			}
			info, err := os.Stat(path)
			if err != nil {
				result.ScanFailed = true
				continue
			}
			failedAt := deadLetterFirstFailure(manifest, info.ModTime())
			if oldest.IsZero() || failedAt.Before(oldest) {
				oldest = failedAt
			}
			switch {
			case loadErr != nil, manifest.DeadLetterRetries < 0:
				result.Permanent++
			case dir != spoolDeadDir:
				result.Recovering++
			case !deadLetterRetryable(manifest):
				result.Permanent++
			case options.MaxRetries > 0 && manifest.DeadLetterRetries >= options.MaxRetries:
				result.Exhausted++
			default:
				result.Retryable++
			}
		}
	}
	if !oldest.IsZero() {
		result.OldestSeconds = max(0, int64(now.Sub(oldest).Seconds()))
	}
	return result
}

func (w *Worker) publishDeadLetterSnapshot(now time.Time) {
	if !w.deadLetterSnapshotDirty && now.Before(w.nextDeadLetterSnapshot) {
		return
	}
	w.nextDeadLetterSnapshot = now.Add(30 * time.Second)
	w.deadLetterSnapshotDirty = false
	w.options.OnDeadLetterState(w.spool.deadLetterSnapshot(now, w.options.DeadLetterRetry))
}

// syncDeadLetter is called exclusively by ProcessOnce/transitionFailure while
// holding spool.workerMu. The callback must preserve an already stored row.
func (w *Worker) syncDeadLetter(ctx context.Context, manifest Manifest, now time.Time) bool {
	if w.options.SyncDeadLetter == nil || manifest.RequestID == "" {
		return false
	}
	key := spoolRequestFilePart(manifest.RequestID) + "-event.json"
	if last, ok := w.deadLetterSynced[key]; ok && now.Sub(last) < time.Minute {
		return false
	}
	w.deadLetterSynced[key] = now
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	message := errors.New("audit capture failed: " + safeErrorCode(manifest.LastErrorCode))
	if err := w.options.SyncDeadLetter(ctx, manifest, message); err != nil {
		w.options.OnError(err)
	}
	return true
}

func (w *Worker) processDeadLetters(ctx context.Context, now time.Time) (WorkerStats, error) {
	stats := WorkerStats{}
	if now.Before(w.nextDeadLetterScan) {
		return stats, nil
	}
	interval := 30 * time.Second
	if w.options.DeadLetterRetry.InitialDelay > 0 {
		interval = min(interval, w.options.DeadLetterRetry.InitialDelay)
	}
	w.nextDeadLetterScan = now.Add(interval)
	paths, err := w.spool.listManifestPaths(spoolDeadDir)
	if err != nil {
		return stats, err
	}
	// Unsynchronized/oldest records precede recently visited records. A fixed
	// filename prefix must not monopolize the bounded reconciliation budget.
	sort.SliceStable(paths, func(i, j int) bool {
		return w.deadLetterSynced[filepath.Base(paths[i])].Before(w.deadLetterSynced[filepath.Base(paths[j])])
	})
	remainingSync := max(1, w.options.DeadLetterRetry.BatchSize)
	remainingRetries := w.options.DeadLetterRetry.BatchSize
	var joined error
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return stats, errors.Join(joined, err)
		}
		manifest, err := w.spool.LoadManifest(path)
		if err != nil {
			continue
		} // Corrupt records are retained and reported by the snapshot.
		if !w.canRetryDeadLetter(manifest) || remainingRetries <= 0 {
			if remainingSync > 0 && w.syncDeadLetter(ctx, manifest, now) {
				remainingSync--
			}
			continue
		}
		info, err := os.Stat(path)
		if err != nil {
			joined = errors.Join(joined, err)
			continue
		}
		changed := false
		if manifest.EnqueuedAt == nil || manifest.EnqueuedAt.IsZero() {
			manifest.EnqueuedAt = timePointer(info.ModTime().UTC())
			changed = true
		}
		if manifest.FirstFailedAt == nil {
			manifest.FirstFailedAt = timePointer(deadLetterFirstFailure(manifest, info.ModTime()))
			changed = true
		}
		if manifest.LastFailureRetryable == nil {
			retryable := true
			manifest.LastFailureRetryable = &retryable
			changed = true
		}
		if manifest.NextAttemptAt == nil {
			lastFailed := info.ModTime()
			if manifest.FailedAt != nil {
				lastFailed = *manifest.FailedAt
			}
			manifest.NextAttemptAt = timePointer(lastFailed.Add(w.deadLetterDelay(manifest.DeadLetterRetries)))
			changed = true
		}
		if changed {
			if err := w.writeManifest(path, manifest); err != nil {
				joined = errors.Join(joined, err)
				continue
			}
		}
		if manifest.NextAttemptAt.After(now) {
			if remainingSync > 0 && w.syncDeadLetter(ctx, manifest, now) {
				remainingSync--
			}
			continue
		}
		remainingRetries--
		validationErr := validateReplayBundle(w.spool, manifest)
		if validationErr != nil {
			code, retryable := classifyProcessError(validationErr)
			if !retryable {
				w.deadLetterSnapshotDirty = true
				manifest.LastFailureRetryable = &retryable
				manifest.LastErrorCode = code
				manifest.NextAttemptAt = nil
				if err := w.writeManifest(path, manifest); err != nil {
					joined = errors.Join(joined, err)
					continue
				}
				w.options.OnError(validationErr)
				w.syncDeadLetter(ctx, manifest, now)
				continue
			}
		}
		result, err := w.processCandidate(ctx, manifestCandidate{path: path, deadLetter: true, preflightErr: validationErr}, w.options.Now().UTC())
		stats.Claimed += result.Claimed
		stats.Succeeded += result.Succeeded
		stats.DeadLettered += result.DeadLettered
		if err != nil {
			joined = errors.Join(joined, fmt.Errorf("retry audit dead letter: %w", err))
		}
	}
	// Bound bookkeeping to durable files still present after this pass.
	for name := range w.deadLetterSynced {
		if _, err := os.Stat(filepath.Join(w.spool.root, spoolDeadDir, name)); errors.Is(err, os.ErrNotExist) {
			delete(w.deadLetterSynced, name)
		}
	}
	return stats, joined
}
