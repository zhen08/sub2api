package callaudit

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

func newTestSession(t *testing.T, usageWait time.Duration) (*Session, *Spool) {
	t.Helper()
	spool := newTestSpool(t)
	scope, err := NewScope(ScopeInput{
		RequestID: "req-session",
		Endpoint:  "/v1/messages",
		Method:    "post",
		Protocol:  ProtocolAnthropic,
		Identity: IdentitySnapshot{
			APIKeyID:      "key-1",
			APIKeyName:    "production",
			UserID:        "user-1",
			UserUsername:  "alice",
			GroupID:       "group-1",
			GroupName:     "production-group",
			GroupPlatform: "anthropic",
		},
		Stream: true,
	}, 180, time.Date(2026, time.July, 18, 8, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewSession(scope, spool, usageWait)
	if err != nil {
		t.Fatal(err)
	}
	return session, spool
}

func TestSessionContextAndConcurrentUsageMerge(t *testing.T) {
	t.Parallel()
	session, _ := newTestSession(t, time.Second)
	ctx := WithSession(nil, session)
	got, ok := SessionFromContext(ctx)
	if !ok || got != session {
		t.Fatalf("SessionFromContext() = %p, %v", got, ok)
	}
	if _, ok := SessionFromContext(context.Background()); ok {
		t.Fatal("unexpected session in empty context")
	}

	var wait sync.WaitGroup
	for index := 1; index <= 100; index++ {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			session.MergeUsage(Usage{
				InputTokens:       int64(index),
				OutputTokens:      int64(index * 2),
				CacheReadTokens:   int64(index / 2),
				CacheCreateTokens: int64(index / 4),
				TotalTokens:       int64(index * 3),
			})
		}()
	}
	wait.Wait()
	usage := session.UsageSnapshot()
	if usage.InputTokens != 100 || usage.OutputTokens != 200 || usage.TotalTokens != 300 || usage.CacheReadTokens != 50 || usage.CacheCreateTokens != 25 {
		t.Fatalf("usage = %+v", usage)
	}
}

func TestSessionFinalizationWaitsForUsageAndPreservesSequences(t *testing.T) {
	t.Parallel()
	session, spool := newTestSession(t, time.Second)
	session.SetEntryModel("claude-entry")
	for _, kind := range []ArtifactKind{ArtifactUpstreamRequest, ArtifactUpstreamRequest, ArtifactResponse} {
		artifact, err := session.CaptureArtifact(kind, map[string]any{"kind": kind})
		if err != nil {
			t.Fatal(err)
		}
		wantSequence := 0
		if kind == ArtifactUpstreamRequest && len(session.artifacts) == 2 {
			wantSequence = 1
		}
		if artifact.Sequence != wantSequence {
			t.Fatalf("%s sequence = %d, want %d", kind, artifact.Sequence, wantSequence)
		}
	}
	completeUsage, err := session.BeginUsage()
	if err != nil {
		t.Fatal(err)
	}
	type finalizeResult struct {
		path string
		err  error
	}
	resultChannel := make(chan finalizeResult, 1)
	go func() {
		path, err := session.Finalize(context.Background(), Outcome{StatusCode: intPointer(200)})
		resultChannel <- finalizeResult{path: path, err: err}
	}()
	select {
	case got := <-resultChannel:
		t.Fatalf("Finalize returned before usage completed: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}
	completeUsage(Usage{Model: "claude-opus", InputTokens: 10, OutputTokens: 4, TotalTokens: 14})
	got := <-resultChannel
	if got.err != nil {
		t.Fatal(got.err)
	}
	manifest, err := spool.LoadManifest(got.path)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Usage.Model != "claude-opus" || manifest.Usage.TotalTokens != 14 || len(manifest.Artifacts) != 3 {
		t.Fatalf("manifest = %+v", manifest)
	}
	if manifest.APIKeyID != "key-1" || manifest.UserUsername != "alice" || manifest.Protocol != ProtocolAnthropic {
		t.Fatalf("identity/protocol snapshot missing: %+v", manifest)
	}
	if manifest.Meta["groupId"] != "group-1" || manifest.Meta["groupName"] != "production-group" ||
		manifest.Meta["groupPlatform"] != "anthropic" || manifest.Meta["entryModel"] != "claude-entry" {
		t.Fatalf("group/model snapshot missing: %+v", manifest.Meta)
	}
	if _, err := session.CaptureArtifact(ArtifactResponse, nil); !errors.Is(err, ErrSessionFinalizing) {
		t.Fatalf("CaptureArtifact after finalize error = %v", err)
	}
	pathAgain, err := session.Finalize(context.Background(), Outcome{Status: CallStatusError})
	if err != nil || pathAgain != got.path {
		t.Fatalf("second Finalize = %q, %v", pathAgain, err)
	}
}

func TestSessionUsageTimeoutIsRecorded(t *testing.T) {
	t.Parallel()
	session, spool := newTestSession(t, 5*time.Millisecond)
	complete, err := session.BeginUsage()
	if err != nil {
		t.Fatal(err)
	}
	defer complete(Usage{})
	path, err := session.Finalize(context.Background(), Outcome{TerminationReason: "client_disconnect"})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := spool.LoadManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Status != CallStatusAborted || manifest.Meta["terminationReason"] != "client_disconnect" || manifest.Meta["usageIncomplete"] != true {
		t.Fatalf("manifest = %+v", manifest)
	}
}

func TestSessionDiskPressureKeepsMetadataWithoutRawArtifacts(t *testing.T) {
	t.Parallel()
	session, spool := newTestSession(t, 0)
	session.DisableArtifacts("disk_high_watermark")
	if _, err := session.CaptureArtifact(ArtifactClientRequest, map[string]any{"secret": true}); !errors.Is(err, ErrArtifactsDisabled) {
		t.Fatalf("CaptureArtifact error = %v", err)
	}
	path, err := session.Finalize(context.Background(), Outcome{StatusCode: intPointer(200)})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := spool.LoadManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Artifacts) != 0 || manifest.Meta["rawCaptureDisabledReason"] != "disk_high_watermark" {
		t.Fatalf("manifest = %+v", manifest)
	}
}

func TestSessionAsyncArtifactsFinalizeInDeterministicOrder(t *testing.T) {
	t.Parallel()
	session, spool := newTestSession(t, 0)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	if err := session.CaptureArtifactStreamAsync(ArtifactUpstreamRequest, func(writer io.Writer) error {
		close(firstStarted)
		<-releaseFirst
		_, err := io.WriteString(writer, `{"sequence":0}`)
		return err
	}, nil); err != nil {
		t.Fatal(err)
	}
	<-firstStarted
	if err := session.CaptureArtifactStreamAsync(ArtifactUpstreamRequest, func(writer io.Writer) error {
		_, err := io.WriteString(writer, `{"sequence":1}`)
		return err
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := session.CaptureArtifactStreamAsync(ArtifactResponse, func(writer io.Writer) error {
		_, err := io.WriteString(writer, `{"response":true}`)
		return err
	}, nil); err != nil {
		t.Fatal(err)
	}

	result := make(chan string, 1)
	errors := make(chan error, 1)
	go func() {
		path, err := session.Finalize(context.Background(), Outcome{StatusCode: intPointer(200)})
		if err != nil {
			errors <- err
			return
		}
		result <- path
	}()
	select {
	case path := <-result:
		t.Fatalf("Finalize returned before the first artifact completed: %s", path)
	case err := <-errors:
		t.Fatalf("Finalize returned early with error: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseFirst)

	var path string
	select {
	case path = <-result:
	case err := <-errors:
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		t.Fatal("Finalize did not complete after async artifacts")
	}
	manifest, err := spool.LoadManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Artifacts) != 3 ||
		manifest.Artifacts[0].Kind != ArtifactUpstreamRequest || manifest.Artifacts[0].Sequence != 0 ||
		manifest.Artifacts[1].Kind != ArtifactUpstreamRequest || manifest.Artifacts[1].Sequence != 1 ||
		manifest.Artifacts[2].Kind != ArtifactResponse || manifest.Artifacts[2].Sequence != 0 {
		t.Fatalf("artifacts = %+v", manifest.Artifacts)
	}
}

func TestSessionAsyncArtifactPanicDoesNotBlockFinalize(t *testing.T) {
	t.Parallel()
	session, spool := newTestSession(t, 0)
	completed := make(chan error, 1)
	if err := session.CaptureArtifactStreamAsync(ArtifactUpstreamRequest, func(io.Writer) error {
		panic("writer failed")
	}, func(err error) {
		completed <- err
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-completed:
		if err == nil {
			t.Fatal("panic must be reported as an artifact error")
		}
	case <-time.After(time.Second):
		t.Fatal("async panic callback did not complete")
	}
	path, err := session.Finalize(context.Background(), Outcome{StatusCode: intPointer(200)})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := spool.LoadManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Artifacts) != 0 {
		t.Fatalf("failed artifact leaked into manifest: %+v", manifest.Artifacts)
	}
	if manifest.Meta["artifactWriteFailureCount"] != float64(1) {
		t.Fatalf("artifact failure metadata missing: %+v", manifest.Meta)
	}
}

func intPointer(value int) *int { return &value }
