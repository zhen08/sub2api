package callaudit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

var ErrSessionFinalizing = errors.New("audit session is finalizing")
var ErrArtifactsDisabled = errors.New("audit raw artifact capture is disabled")
var ErrArtifactWriterSaturated = errors.New("audit artifact writer capacity is saturated")

const maxAsyncArtifactWriters = 256

var asyncArtifactWriterSlots = make(chan struct{}, maxAsyncArtifactWriters)

// Session owns one request's mutable capture state. It is safe for relay,
// response, and asynchronous usage goroutines to call concurrently.
type Session struct {
	spool            *Spool
	scope            Scope
	usageWaitTimeout time.Duration

	mu                      sync.Mutex
	notify                  chan struct{}
	usage                   Usage
	entryModel              string
	artifacts               []ArtifactRef
	sequences               map[ArtifactKind]int
	artifactFailures        map[string]struct{}
	pendingUsage            int
	pendingArtifacts        int
	finalizing              bool
	finalized               bool
	manifestPath            string
	artifactsDisabledReason string
	releaseOnce             sync.Once
	releaseRuntime          func()
}

// Release marks the request lifecycle complete for Runtime shutdown tracking.
// Middleware calls it after the durable manifest finalization attempt. It is
// idempotent so panic/error paths cannot underflow the runtime wait group.
func (s *Session) Release() {
	if s == nil {
		return
	}
	s.releaseOnce.Do(func() {
		if s.releaseRuntime != nil {
			s.releaseRuntime()
		}
	})
}

func NewSession(scope Scope, spool *Spool, usageWaitTimeout time.Duration) (*Session, error) {
	if spool == nil {
		return nil, fmt.Errorf("audit spool is required")
	}
	if scope.RequestID == "" || scope.CreatedAt.IsZero() || scope.RetentionUntil.IsZero() {
		return nil, fmt.Errorf("valid audit scope is required")
	}
	if usageWaitTimeout < 0 {
		return nil, fmt.Errorf("usage wait timeout must be non-negative")
	}
	return &Session{
		spool:            spool,
		scope:            scope,
		usageWaitTimeout: usageWaitTimeout,
		notify:           make(chan struct{}),
		sequences:        make(map[ArtifactKind]int),
		artifactFailures: make(map[string]struct{}),
	}, nil
}

func (s *Session) Scope() Scope {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.scope
}

func (s *Session) MaxArtifactBytes() int64 {
	if s == nil || s.spool == nil {
		return 0
	}
	return s.spool.maxArtifactBytes
}

func (s *Session) SetStream(stream bool) {
	if !stream {
		return
	}
	s.mu.Lock()
	s.scope.Stream = true
	s.signalLocked()
	s.mu.Unlock()
}

func (s *Session) DisableArtifacts(reason string) {
	s.mu.Lock()
	if s.artifactsDisabledReason == "" {
		s.artifactsDisabledReason = reason
	}
	s.signalLocked()
	s.mu.Unlock()
}

func (s *Session) ArtifactsEnabled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.artifactsDisabledReason == ""
}

func (s *Session) UsageSnapshot() Usage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.usage
}

func (s *Session) MergeUsage(update Usage) {
	s.mu.Lock()
	s.usage.Merge(update)
	s.signalLocked()
	s.mu.Unlock()
}

// SetEntryModel records the model requested at the public gateway before any
// provider mapping or failover changes it. The final upstream model remains in
// Usage.Model; this value is stored separately in manifest metadata.
func (s *Session) SetEntryModel(model string) {
	model = strings.TrimSpace(model)
	if model == "" {
		return
	}
	if len(model) > 1024 {
		model = model[:1024]
	}
	s.mu.Lock()
	if s.entryModel == "" {
		s.entryModel = model
	}
	s.signalLocked()
	s.mu.Unlock()
}

// BeginUsage registers asynchronous usage work before it starts. The returned
// completion function is idempotent and merges its final cumulative snapshot.
func (s *Session) BeginUsage() (func(Usage), error) {
	s.mu.Lock()
	if s.finalizing || s.finalized {
		s.mu.Unlock()
		return nil, ErrSessionFinalizing
	}
	s.pendingUsage++
	s.signalLocked()
	s.mu.Unlock()

	var once sync.Once
	return func(update Usage) {
		once.Do(func() {
			s.mu.Lock()
			s.usage.Merge(update)
			s.pendingUsage--
			s.signalLocked()
			s.mu.Unlock()
		})
	}, nil
}

func (s *Session) WaitPendingUsage(ctx context.Context) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.pendingUsage > 0 {
		changed := s.notify
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			s.mu.Lock()
			return false
		}
		s.mu.Lock()
	}
	return true
}

func (s *Session) CaptureArtifact(kind ArtifactKind, payload any) (ArtifactRef, error) {
	s.mu.Lock()
	if s.finalizing || s.finalized {
		s.mu.Unlock()
		return ArtifactRef{}, ErrSessionFinalizing
	}
	if s.artifactsDisabledReason != "" {
		s.mu.Unlock()
		return ArtifactRef{}, ErrArtifactsDisabled
	}
	sequence := s.sequences[kind]
	s.sequences[kind] = sequence + 1
	s.pendingArtifacts++
	s.signalLocked()
	s.mu.Unlock()

	artifact, err := s.spool.WriteArtifact(s.scope.CreatedAt, s.scope.RequestID, kind, sequence, payload)

	s.mu.Lock()
	s.pendingArtifacts--
	if err == nil {
		s.artifacts = append(s.artifacts, artifact)
	} else {
		s.recordArtifactFailureLocked(kind, sequence)
	}
	s.signalLocked()
	s.mu.Unlock()
	return artifact, err
}

func (s *Session) CaptureArtifactStream(kind ArtifactKind, write func(io.Writer) error) (ArtifactRef, error) {
	s.mu.Lock()
	if s.finalizing || s.finalized {
		s.mu.Unlock()
		return ArtifactRef{}, ErrSessionFinalizing
	}
	if s.artifactsDisabledReason != "" {
		s.mu.Unlock()
		return ArtifactRef{}, ErrArtifactsDisabled
	}
	sequence := s.sequences[kind]
	s.sequences[kind] = sequence + 1
	s.pendingArtifacts++
	s.signalLocked()
	s.mu.Unlock()

	artifact, err := s.spool.WriteArtifactStream(s.scope.CreatedAt, s.scope.RequestID, kind, sequence, write)

	s.mu.Lock()
	s.pendingArtifacts--
	if err == nil {
		s.artifacts = append(s.artifacts, artifact)
	} else {
		s.recordArtifactFailureLocked(kind, sequence)
	}
	s.signalLocked()
	s.mu.Unlock()
	return artifact, err
}

// CaptureArtifactStreamAsync reserves the artifact sequence synchronously, then
// performs disk serialization outside the inference path. Finalize waits for
// the pending write, so the manifest cannot be published early.
func (s *Session) CaptureArtifactStreamAsync(kind ArtifactKind, write func(io.Writer) error, onComplete func(error)) error {
	if write == nil {
		return fmt.Errorf("audit artifact writer is required")
	}
	select {
	case asyncArtifactWriterSlots <- struct{}{}:
	default:
		return ErrArtifactWriterSaturated
	}
	s.mu.Lock()
	if s.finalizing || s.finalized {
		s.mu.Unlock()
		<-asyncArtifactWriterSlots
		return ErrSessionFinalizing
	}
	if s.artifactsDisabledReason != "" {
		s.mu.Unlock()
		<-asyncArtifactWriterSlots
		return ErrArtifactsDisabled
	}
	sequence := s.sequences[kind]
	s.sequences[kind] = sequence + 1
	s.pendingArtifacts++
	s.signalLocked()
	s.mu.Unlock()

	go func() {
		defer func() { <-asyncArtifactWriterSlots }()
		var artifact ArtifactRef
		var writeErr error
		defer func() {
			if recovered := recover(); recovered != nil {
				writeErr = fmt.Errorf("audit artifact writer panic: %v", recovered)
			}
			s.mu.Lock()
			s.pendingArtifacts--
			if writeErr == nil {
				s.artifacts = append(s.artifacts, artifact)
			} else {
				s.recordArtifactFailureLocked(kind, sequence)
			}
			s.signalLocked()
			s.mu.Unlock()
			if onComplete != nil {
				func() {
					defer func() { _ = recover() }()
					onComplete(writeErr)
				}()
			}
		}()
		artifact, writeErr = s.spool.WriteArtifactStream(s.scope.CreatedAt, s.scope.RequestID, kind, sequence, write)
	}()
	return nil
}

func (s *Session) CreateCaptureTemp(label string) (*os.File, error) {
	if s == nil || s.spool == nil {
		return nil, fmt.Errorf("audit spool is unavailable")
	}
	if !s.ArtifactsEnabled() {
		return nil, ErrArtifactsDisabled
	}
	return s.spool.CreateCaptureTemp(label)
}

// Finalize waits for in-flight artifact writes and, for a bounded period, usage
// callbacks. It atomically publishes exactly one ready manifest. A usage timeout
// does not lose the call; it is recorded as usageIncomplete in manifest meta.
func (s *Session) Finalize(ctx context.Context, outcome Outcome) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		s.mu.Lock()
		if s.finalized {
			path := s.manifestPath
			s.mu.Unlock()
			return path, nil
		}
		if !s.finalizing {
			s.finalizing = true
			s.signalLocked()
			s.mu.Unlock()
			break
		}
		changed := s.notify
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	if err := s.waitForArtifactWrites(ctx); err != nil {
		s.mu.Lock()
		s.finalizing = false
		s.signalLocked()
		s.mu.Unlock()
		return "", err
	}

	usageComplete := s.waitForUsage(ctx)
	s.mu.Lock()
	manifest := s.buildManifestLocked(outcome, usageComplete)
	s.mu.Unlock()

	path, err := s.spool.CommitManifest(manifest)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.finalizing = false
		s.signalLocked()
		return "", err
	}
	s.manifestPath = path
	s.finalized = true
	s.finalizing = false
	s.signalLocked()
	return path, nil
}

func (s *Session) waitForArtifactWrites(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.pendingArtifacts > 0 {
		changed := s.notify
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			s.mu.Lock()
			return ctx.Err()
		}
		s.mu.Lock()
	}
	return nil
}

func (s *Session) waitForUsage(ctx context.Context) bool {
	if s.usageWaitTimeout == 0 {
		s.mu.Lock()
		complete := s.pendingUsage == 0
		s.mu.Unlock()
		return complete
	}
	timer := time.NewTimer(s.usageWaitTimeout)
	defer timer.Stop()

	s.mu.Lock()
	defer s.mu.Unlock()
	for s.pendingUsage > 0 {
		changed := s.notify
		s.mu.Unlock()
		select {
		case <-changed:
		case <-timer.C:
			s.mu.Lock()
			return false
		case <-ctx.Done():
			s.mu.Lock()
			return false
		}
		s.mu.Lock()
	}
	return true
}

func (s *Session) buildManifestLocked(outcome Outcome, usageComplete bool) Manifest {
	status := outcome.Status
	if outcome.TerminationReason != "" {
		status = CallStatusAborted
	} else if status == "" && outcome.StatusCode != nil && *outcome.StatusCode >= 400 {
		status = CallStatusError
	} else if status == "" {
		status = CallStatusOK
	}
	meta := cloneMeta(outcome.Meta)
	if outcome.TerminationReason != "" {
		meta["terminationReason"] = outcome.TerminationReason
	}
	if !usageComplete {
		meta["usageIncomplete"] = true
	}
	if s.artifactsDisabledReason != "" {
		meta["rawCaptureDisabledReason"] = s.artifactsDisabledReason
	}
	identity := s.scope.Identity
	if identity.GroupID != "" {
		meta["groupId"] = identity.GroupID
	}
	if identity.GroupName != "" {
		meta["groupName"] = identity.GroupName
	}
	if identity.GroupPlatform != "" {
		meta["groupPlatform"] = identity.GroupPlatform
	}
	if s.entryModel != "" {
		meta["entryModel"] = s.entryModel
	}
	if len(s.artifactFailures) > 0 {
		failures := make([]string, 0, len(s.artifactFailures))
		for failure := range s.artifactFailures {
			failures = append(failures, failure)
		}
		sort.Strings(failures)
		meta["artifactWriteFailureCount"] = len(failures)
		meta["artifactWriteFailures"] = failures
	}
	artifacts := append([]ArtifactRef(nil), s.artifacts...)
	sort.SliceStable(artifacts, func(left, right int) bool {
		leftRank := artifactKindRank(artifacts[left].Kind)
		rightRank := artifactKindRank(artifacts[right].Kind)
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		return artifacts[left].Sequence < artifacts[right].Sequence
	})
	return Manifest{
		Version:          1,
		State:            ManifestReady,
		RequestID:        s.scope.RequestID,
		CreatedAt:        s.scope.CreatedAt,
		RequestStartedAt: s.scope.RequestStartedAt,
		RetentionUntil:   s.scope.RetentionUntil,
		Endpoint:         s.scope.Endpoint,
		Method:           s.scope.Method,
		Protocol:         s.scope.Protocol,
		APIKeyID:         identity.APIKeyID,
		APIKeyName:       identity.APIKeyName,
		UserID:           identity.UserID,
		UserUsername:     identity.UserUsername,
		Status:           status,
		StatusCode:       outcome.StatusCode,
		Stream:           s.scope.Stream,
		CaptureStatus:    CapturePending,
		Usage:            s.usage,
		Artifacts:        artifacts,
		Meta:             meta,
	}
}

func artifactKindRank(kind ArtifactKind) int {
	switch kind {
	case ArtifactClientRequest:
		return 0
	case ArtifactUpstreamRequest:
		return 1
	case ArtifactResponse:
		return 2
	default:
		return 3
	}
}

func (s *Session) recordArtifactFailureLocked(kind ArtifactKind, sequence int) {
	if s.artifactFailures == nil {
		s.artifactFailures = make(map[string]struct{})
	}
	s.artifactFailures[fmt.Sprintf("%s:%d", kind, sequence)] = struct{}{}
}

func cloneMeta(meta map[string]any) map[string]any {
	result := make(map[string]any, len(meta)+2)
	for key, value := range meta {
		result[key] = value
	}
	return result
}

func (s *Session) signalLocked() {
	close(s.notify)
	s.notify = make(chan struct{})
}
