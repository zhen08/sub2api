package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/callaudit"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
)

type auditedUpstreamStub struct{}

func (auditedUpstreamStub) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	req.Header.Set("X-Final-Header", "mapped")
	_, _ = io.Copy(io.Discard, req.Body)
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(nil))}, nil
}

func (stub auditedUpstreamStub) DoWithTLS(req *http.Request, proxy string, accountID int64, concurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return stub.Do(req, proxy, accountID, concurrency)
}

type earlyResponseUpstreamStub struct {
	started chan struct{}
	release chan struct{}
	done    chan struct{}
}

func (stub earlyResponseUpstreamStub) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	go func() {
		if stub.done != nil {
			defer close(stub.done)
		}
		close(stub.started)
		<-stub.release
		_, _ = io.Copy(io.Discard, req.Body)
		_ = req.Body.Close()
	}()
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(nil))}, nil
}

func (stub earlyResponseUpstreamStub) DoWithTLS(req *http.Request, proxy string, accountID int64, concurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return stub.Do(req, proxy, accountID, concurrency)
}

func TestCallAuditHTTPUpstreamCapturesFinalRequestAndRedactsSecrets(t *testing.T) {
	t.Parallel()
	spool, err := callaudit.NewSpool(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 18, 12, 0, 0, 0, time.UTC)
	scope, err := callaudit.NewScope(callaudit.ScopeInput{RequestID: "req-upstream", Endpoint: "/v1/messages", Method: "POST"}, 180, now)
	if err != nil {
		t.Fatal(err)
	}
	session, err := callaudit.NewSession(scope, spool, 0)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(callaudit.WithSession(context.Background(), session), http.MethodPost,
		"https://example.com/v1/messages?key=upstream-secret", bytes.NewReader([]byte(`{"model":"mapped-model"}`)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer upstream-secret")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Amz-Security-Token", "aws-secret")
	request.Header.Set("X-Relay-Token", "custom-secret")
	decorator := &callAuditHTTPUpstream{base: auditedUpstreamStub{}}
	response, err := decorator.Do(request, "", 42, 1)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	retryRequest, err := http.NewRequestWithContext(callaudit.WithSession(context.Background(), session), http.MethodPost,
		"https://example.com/v1/messages?key=retry-secret", bytes.NewReader([]byte(`{"model":"retry-model"}`)))
	if err != nil {
		t.Fatal(err)
	}
	retryRequest.Header = request.Header.Clone()
	retryResponse, err := decorator.Do(retryRequest, "", 43, 1)
	if err != nil {
		t.Fatal(err)
	}
	_ = retryResponse.Body.Close()
	manifestPath, err := session.Finalize(context.Background(), callaudit.Outcome{StatusCode: intPointerForAudit(200)})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := spool.LoadManifest(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Artifacts) != 2 || manifest.Artifacts[0].Kind != callaudit.ArtifactUpstreamRequest ||
		manifest.Artifacts[0].Sequence != 0 || manifest.Artifacts[1].Sequence != 1 {
		t.Fatalf("artifacts = %+v", manifest.Artifacts)
	}
	raw, err := spool.ReadArtifact(manifest.Artifacts[0])
	if err != nil {
		t.Fatal(err)
	}
	var artifact struct {
		Provider string         `json:"provider"`
		URL      string         `json:"url"`
		Headers  map[string]any `json:"headers"`
		Body     map[string]any `json:"body"`
	}
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.Provider != "unknown" || artifact.URL != "https://example.com/v1/messages?key=%5BREDACTED%5D" ||
		artifact.Headers["authorization"] != callaudit.RedactedValue ||
		artifact.Headers["x-amz-security-token"] != callaudit.RedactedValue ||
		artifact.Headers["x-relay-token"] != callaudit.RedactedValue ||
		artifact.Headers["x-final-header"] != "mapped" || artifact.Body["model"] != "mapped-model" {
		t.Fatalf("artifact = %+v", artifact)
	}
}

func TestCallAuditHTTPUpstreamDoesNotWaitForLateTransportBodyRead(t *testing.T) {
	t.Parallel()
	spool, err := callaudit.NewSpool(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := callaudit.NewScope(callaudit.ScopeInput{
		RequestID: "req-upstream-late-read",
		Endpoint:  "/v1/messages",
		Method:    http.MethodPost,
	}, 180, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	session, err := callaudit.NewSession(scope, spool, 0)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"model":"late-read"}`)
	request, err := http.NewRequestWithContext(
		callaudit.WithSession(context.Background(), session),
		http.MethodPost,
		"https://example.com/v1/messages",
		io.NopCloser(bytes.NewReader(payload)),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.GetBody = nil
	request.ContentLength = int64(len(payload))
	request.Header.Set("Content-Type", "application/json")
	started := make(chan struct{})
	release := make(chan struct{})
	decorator := &callAuditHTTPUpstream{base: earlyResponseUpstreamStub{started: started, release: release}}

	returned := make(chan error, 1)
	go func() {
		response, err := decorator.Do(request, "", 42, 1)
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		returned <- err
	}()
	select {
	case err := <-returned:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("upstream decorator blocked on audit serialization")
	}
	<-started
	close(release)

	manifestPath, err := session.Finalize(context.Background(), callaudit.Outcome{StatusCode: intPointerForAudit(200)})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := spool.LoadManifest(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Artifacts) != 1 || manifest.Artifacts[0].Kind != callaudit.ArtifactUpstreamRequest {
		t.Fatalf("artifacts = %+v", manifest.Artifacts)
	}
	raw, err := spool.ReadArtifact(manifest.Artifacts[0])
	if err != nil {
		t.Fatal(err)
	}
	var artifact struct {
		Body map[string]any `json:"body"`
	}
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	if artifact.Body["model"] != "late-read" {
		t.Fatalf("artifact body = %+v", artifact.Body)
	}
}

func TestCallAuditHTTPUpstreamAdmissionFailureReleasesStreamCapture(t *testing.T) {
	spool, err := callaudit.NewSpool(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	blockerScope, err := callaudit.NewScope(callaudit.ScopeInput{
		RequestID: "req-upstream-writer-blockers",
		Endpoint:  "/v1/messages",
		Method:    http.MethodPost,
	}, 180, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	blockerSession, err := callaudit.NewSession(blockerScope, spool, 0)
	if err != nil {
		t.Fatal(err)
	}

	releaseWriters := make(chan struct{})
	var releaseOnce sync.Once
	releaseAllWriters := func() { releaseOnce.Do(func() { close(releaseWriters) }) }
	defer func() {
		releaseAllWriters()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, finalizeErr := blockerSession.Finalize(ctx, callaudit.Outcome{StatusCode: intPointerForAudit(200)}); finalizeErr != nil {
			t.Errorf("finalize blocker session: %v", finalizeErr)
		}
	}()

	accepted := 0
	for accepted < 1024 {
		err = blockerSession.CaptureArtifactStreamAsync(callaudit.ArtifactUpstreamRequest, func(io.Writer) error {
			<-releaseWriters
			return errors.New("release saturated audit writer")
		}, nil)
		if errors.Is(err, callaudit.ErrArtifactWriterSaturated) {
			break
		}
		if err != nil {
			t.Fatalf("fill async artifact writer capacity: %v", err)
		}
		accepted++
	}
	if !errors.Is(err, callaudit.ErrArtifactWriterSaturated) || accepted == 0 {
		t.Fatalf("async artifact writer pool did not saturate after %d writers: %v", accepted, err)
	}

	targetScope, err := callaudit.NewScope(callaudit.ScopeInput{
		RequestID: "req-upstream-admission-failure",
		Endpoint:  "/v1/messages",
		Method:    http.MethodPost,
	}, 180, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	targetSession, err := callaudit.NewSession(targetScope, spool, 0)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"model":"late-admission-failure"}`)
	request, err := http.NewRequestWithContext(
		callaudit.WithSession(context.Background(), targetSession),
		http.MethodPost,
		"https://example.com/v1/messages",
		io.NopCloser(bytes.NewReader(payload)),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.GetBody = nil
	request.ContentLength = int64(len(payload))
	request.Header.Set("Content-Type", "application/json")

	baselineCaptures := callaudit.ActiveStreamCaptureCount()
	started := make(chan struct{})
	releaseBody := make(chan struct{})
	bodyDone := make(chan struct{})
	var releaseBodyOnce sync.Once
	releaseLateBody := func() { releaseBodyOnce.Do(func() { close(releaseBody) }) }
	defer releaseLateBody()
	decorator := &callAuditHTTPUpstream{base: earlyResponseUpstreamStub{
		started: started,
		release: releaseBody,
		done:    bodyDone,
	}}
	response, err := decorator.Do(request, "", 42, 1)
	if err != nil {
		t.Fatal(err)
	}
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("late request-body reader did not start")
	}
	if got := callaudit.ActiveStreamCaptureCount(); got != baselineCaptures {
		t.Fatalf("active stream captures = %d, want baseline %d", got, baselineCaptures)
	}
	inflight, err := os.ReadDir(filepath.Join(spool.Root(), "artifacts", "inflight"))
	if err != nil {
		t.Fatal(err)
	}
	if len(inflight) != 0 {
		t.Fatalf("capture temp files were not reclaimed: %+v", inflight)
	}

	releaseLateBody()
	select {
	case <-bodyDone:
	case <-time.After(time.Second):
		t.Fatal("late request-body reader did not finish")
	}
	if _, err := targetSession.Finalize(context.Background(), callaudit.Outcome{StatusCode: intPointerForAudit(200)}); err != nil {
		t.Fatal(err)
	}
}

func intPointerForAudit(value int) *int { return &value }
