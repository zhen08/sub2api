package middleware

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/callaudit"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func TestCallAuditBodyTeesNeverPropagateCaptureWriteFailures(t *testing.T) {
	temp, err := os.CreateTemp(t.TempDir(), "capture-*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if err := temp.Close(); err != nil {
		t.Fatal(err)
	}
	requestStream, err := callaudit.NewStreamCapture(temp, 1024)
	if err != nil {
		t.Fatal(err)
	}
	body := &callAuditRequestBody{
		source:  io.NopCloser(strings.NewReader("business-payload")),
		capture: temp,
		stream:  requestStream,
	}
	got, err := io.ReadAll(body)
	if err != nil || string(got) != "business-payload" {
		t.Fatalf("business read = %q, %v", got, err)
	}
	body.prepareCapture()
	if body.captureErr == nil {
		t.Fatal("closed capture file must record a best-effort write failure")
	}

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	responseStream, err := callaudit.NewStreamCapture(temp, 1024)
	if err != nil {
		t.Fatal(err)
	}
	writer := &callAuditResponseWriter{ResponseWriter: c.Writer, stream: responseStream}
	if _, ok := any(writer).(io.ReaderFrom); !ok {
		t.Fatal("response writer must preserve io.ReaderFrom")
	}
	if _, err := io.Copy(writer, strings.NewReader("client-response")); err != nil {
		t.Fatalf("client response failed because audit capture failed: %v", err)
	}
	writer.finishCapture()
	if recorder.Body.String() != "client-response" || writer.captureErr == nil {
		t.Fatalf("response=%q captureErr=%v", recorder.Body.String(), writer.captureErr)
	}
}

func TestExtractEntryModelUsesBoundedPrefix(t *testing.T) {
	t.Parallel()
	file, err := os.CreateTemp(t.TempDir(), "request-*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.WriteString(`{"metadata":{"source":"test"},"model":"claude-sonnet","stream":true,"messages":[]}`); err != nil {
		t.Fatal(err)
	}
	if got := extractEntryModel(file); got != "claude-sonnet" {
		t.Fatalf("extractEntryModel() = %q", got)
	}
	if model, stream := extractEntryAuditHints(file); model != "claude-sonnet" || !stream {
		t.Fatalf("extractEntryAuditHints() = %q/%v", model, stream)
	}
	if got := extractEntryModelFromPath("/v1beta/models/gemini-2.5-pro:streamGenerateContent"); got != "gemini-2.5-pro" {
		t.Fatalf("extractEntryModelFromPath() = %q", got)
	}
}

func TestCallAuditTerminationReasonRecognizesClientWriteFailure(t *testing.T) {
	t.Parallel()
	brokenPipe := &net.OpError{Err: &os.SyscallError{Syscall: "write", Err: syscall.EPIPE}}
	if got := callAuditTerminationReason(nil, brokenPipe); got != "client_disconnected" {
		t.Fatalf("broken-pipe panic reason = %q", got)
	}
	if got := callAuditTerminationReason(io.ErrClosedPipe, nil); got != "client_disconnected" {
		t.Fatalf("response write reason = %q", got)
	}
	if got := callAuditTerminationReason(nil, "ordinary panic"); got != "" {
		t.Fatalf("ordinary panic reason = %q", got)
	}
}

func TestCallAuditMiddlewareCapturesOneCallWithIdentityAndRedaction(t *testing.T) {
	spoolDir := t.TempDir()
	cfg := &config.Config{CallAudit: config.CallAuditConfig{
		Enabled:                  true,
		RetentionDays:            180,
		FailurePolicy:            "nonblocking",
		PostgresURL:              "postgres://audit:audit@127.0.0.1:1/audit?sslmode=disable&connect_timeout=1",
		SpoolDir:                 spoolDir,
		ObjectKeyPrefix:          callaudit.LegacyObjectPrefix,
		MaxArtifactBytes:         1 << 20,
		DiskHighWatermarkPercent: 99,
		UsageWaitTimeoutMS:       0,
		Worker: config.CallAuditWorkerConfig{
			Enabled:             false,
			MaxAttempts:         5,
			BatchSize:           1,
			PollIntervalMS:      10,
			RetryInitialDelayMS: 10,
			RetryMaxDelayMS:     100,
			ClaimTimeoutSeconds: 1,
		},
		S3: config.CallAuditS3Config{
			Bucket:         "audit",
			AccessKey:      "writer",
			SecretKey:      "secret",
			Region:         "us-east-1",
			ForcePathStyle: true,
		},
	}}
	runtime := callaudit.NewRuntime(cfg)
	if !runtime.Enabled() || runtime.Snapshot().Initialization != "" {
		t.Fatalf("runtime = %+v", runtime.Snapshot())
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := runtime.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown() = %v", err)
		}
	}()

	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(ClientRequestID())
	router.Use(func(c *gin.Context) {
		groupID := int64(9)
		c.Set(string(ContextKeyAPIKey), &service.APIKey{
			ID:      7,
			Name:    "migration-key",
			GroupID: &groupID,
			User:    &service.User{ID: 8, Username: "alice"},
			Group:   &service.Group{ID: groupID, Name: "migration", Platform: service.PlatformAnthropic},
		})
		c.Next()
	})
	router.Use(gin.HandlerFunc(NewCallAuditMiddleware(runtime)))
	router.POST("/v1/messages", func(c *gin.Context) {
		c.Header("Content-Type", "application/json")
		c.Header("Set-Cookie", "session=secret")
		c.Status(http.StatusOK)
		_, _ = io.Copy(c.Writer, c.Request.Body)
	})

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/messages?key=query-secret", strings.NewReader(`{"model":"claude-sonnet","messages":[]}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer client-secret")
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || recorder.Body.String() != `{"model":"claude-sonnet","messages":[]}` {
		t.Fatalf("response = %d %q", recorder.Code, recorder.Body.String())
	}

	entries := waitForReadyManifests(t, filepath.Join(spoolDir, "ready"), 1)
	rawManifest, err := os.ReadFile(filepath.Join(spoolDir, "ready", entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	var manifest callaudit.Manifest
	if err := json.Unmarshal(rawManifest, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.APIKeyID != "7" || manifest.UserUsername != "alice" || manifest.Meta["groupId"] != "9" || manifest.Meta["entryModel"] != "claude-sonnet" {
		t.Fatalf("manifest snapshot = %+v", manifest)
	}
	if len(manifest.Artifacts) != 2 {
		t.Fatalf("artifacts = %+v", manifest.Artifacts)
	}
	for _, artifact := range manifest.Artifacts {
		raw, err := os.ReadFile(filepath.Join(spoolDir, filepath.FromSlash(artifact.SpoolPath)))
		if err != nil {
			t.Fatal(err)
		}
		var payload map[string]any
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatalf("%s artifact is invalid JSON: %v\n%s", artifact.Kind, err, raw)
		}
		headers, _ := payload["headers"].(map[string]any)
		switch artifact.Kind {
		case callaudit.ArtifactClientRequest:
			if headers["authorization"] != callaudit.RedactedValue {
				t.Fatalf("client authorization leaked: %#v", headers)
			}
			query, _ := payload["query"].(map[string]any)
			if query["key"] != callaudit.RedactedValue {
				t.Fatalf("query key leaked: %#v", query)
			}
		case callaudit.ArtifactResponse:
			if headers["set-cookie"] != callaudit.RedactedValue {
				t.Fatalf("response cookie leaked: %#v", headers)
			}
		}
	}
}

func TestCallAuditMiddlewarePreservesSSEBytes(t *testing.T) {
	runtime, spoolDir := newCallAuditMiddlewareRuntime(t)
	router := newCallAuditMiddlewareRouter(runtime)
	want := "event: message\ndata: {\"delta\":\"one\"}\n\n" +
		"event: done\ndata: [DONE]\n\n"
	router.POST("/v1/messages", func(c *gin.Context) {
		c.Header("Content-Type", "text/event-stream")
		c.Status(http.StatusOK)
		for _, chunk := range []string{
			"event: message\ndata: {\"delta\":\"one\"}\n\n",
			"event: done\ndata: [DONE]\n\n",
		} {
			_, _ = c.Writer.WriteString(chunk)
			c.Writer.Flush()
		}
	})

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"claude","stream":true}`))
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, request)
	if recorder.Body.String() != want {
		t.Fatalf("SSE bytes changed:\n got %q\nwant %q", recorder.Body.String(), want)
	}
	manifest := loadOnlyReadyManifest(t, spoolDir)
	if !manifest.Stream || manifest.Status != callaudit.CallStatusOK {
		t.Fatalf("manifest = %+v", manifest)
	}
	response := readArtifactPayload(t, spoolDir, manifest, callaudit.ArtifactResponse)
	body, _ := response["body"].(map[string]any)
	if body["raw"] != want || body["encoding"] != "utf8" {
		t.Fatalf("captured SSE body = %#v", body)
	}
}

func TestCallAuditMiddlewareCapturesPromptBlockWithoutUpstreamArtifact(t *testing.T) {
	runtime, spoolDir := newCallAuditMiddlewareRuntime(t)
	router := newCallAuditMiddlewareRouter(runtime)
	router.POST("/v1/messages", func(c *gin.Context) {
		// Prompt Audit evaluates the parsed request, so the real gateway has
		// consumed the body before it returns a blocking response. Mirror that
		// behavior here so request model/stream intent and body capture are tested.
		_, _ = io.ReadAll(c.Request.Body)
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
			"type":  "error",
			"error": gin.H{"type": "permission_error", "message": "blocked by prompt audit"},
		})
	})

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"claude","stream":true,"messages":[]}`))
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d", recorder.Code)
	}
	manifest := loadOnlyReadyManifest(t, spoolDir)
	if manifest.Status != callaudit.CallStatusError || manifest.StatusCode == nil || *manifest.StatusCode != http.StatusForbidden || !manifest.Stream {
		t.Fatalf("manifest = %+v", manifest)
	}
	if len(manifest.Artifacts) != 2 || manifest.Artifacts[0].Kind != callaudit.ArtifactClientRequest || manifest.Artifacts[1].Kind != callaudit.ArtifactResponse {
		t.Fatalf("Prompt Audit block artifacts = %+v", manifest.Artifacts)
	}
}

func TestCallAuditMiddlewareMarksCanceledClientAborted(t *testing.T) {
	runtime, spoolDir := newCallAuditMiddlewareRuntime(t)
	router := newCallAuditMiddlewareRouter(runtime)
	router.POST("/v1/messages", func(*gin.Context) {})

	request := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"claude"}`))
	request.Header.Set("Content-Type", "application/json")
	ctx, cancel := context.WithCancel(request.Context())
	cancel()
	request = request.WithContext(ctx)
	router.ServeHTTP(httptest.NewRecorder(), request)

	manifest := loadOnlyReadyManifest(t, spoolDir)
	if manifest.Status != callaudit.CallStatusAborted || manifest.Meta["terminationReason"] != "client_disconnected" {
		t.Fatalf("manifest = %+v", manifest)
	}
}

func TestCallAuditMiddlewareCapturesPanicResponse(t *testing.T) {
	runtime, spoolDir := newCallAuditMiddlewareRuntime(t)
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(Recovery())
	router.Use(ClientRequestID())
	router.Use(func(c *gin.Context) {
		groupID := int64(9)
		c.Set(string(ContextKeyAPIKey), &service.APIKey{
			ID: 7, GroupID: &groupID,
			User:  &service.User{ID: 8, Username: "alice"},
			Group: &service.Group{ID: groupID, Platform: service.PlatformAnthropic},
		})
		c.Next()
	})
	router.Use(gin.HandlerFunc(NewCallAuditMiddleware(runtime)))
	router.POST("/v1/messages", func(*gin.Context) { panic("audit panic") })

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"claude"}`))
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("panic status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	manifest := loadOnlyReadyManifest(t, spoolDir)
	if manifest.Status != callaudit.CallStatusError || manifest.StatusCode == nil || *manifest.StatusCode != http.StatusInternalServerError {
		t.Fatalf("manifest = %+v", manifest)
	}
	response := readArtifactPayload(t, spoolDir, manifest, callaudit.ArtifactResponse)
	var clientBody any
	if err := json.Unmarshal(recorder.Body.Bytes(), &clientBody); err != nil {
		t.Fatal(err)
	}
	if response["body"] == nil || !reflect.DeepEqual(response["body"], clientBody) {
		t.Fatalf("panic artifact body=%#v client=%#v", response["body"], clientBody)
	}
}

func newCallAuditMiddlewareRuntime(t *testing.T) (*callaudit.Runtime, string) {
	t.Helper()
	spoolDir := t.TempDir()
	cfg := &config.Config{CallAudit: config.CallAuditConfig{
		Enabled:                  true,
		RetentionDays:            180,
		FailurePolicy:            "nonblocking",
		PostgresURL:              "postgres://audit:audit@127.0.0.1:1/audit?sslmode=disable&connect_timeout=1",
		SpoolDir:                 spoolDir,
		ObjectKeyPrefix:          callaudit.LegacyObjectPrefix,
		MaxArtifactBytes:         1 << 20,
		DiskHighWatermarkPercent: 99,
		UsageWaitTimeoutMS:       0,
		Worker: config.CallAuditWorkerConfig{
			Enabled:             false,
			MaxAttempts:         5,
			BatchSize:           1,
			PollIntervalMS:      10,
			RetryInitialDelayMS: 10,
			RetryMaxDelayMS:     100,
			ClaimTimeoutSeconds: 1,
		},
		S3: config.CallAuditS3Config{
			Bucket: "audit", AccessKey: "writer", SecretKey: "secret", Region: "us-east-1", ForcePathStyle: true,
		},
	}}
	runtime := callaudit.NewRuntime(cfg)
	if !runtime.Enabled() || runtime.Snapshot().Initialization != "" {
		t.Fatalf("runtime = %+v", runtime.Snapshot())
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := runtime.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown() = %v", err)
		}
	})
	return runtime, spoolDir
}

func newCallAuditMiddlewareRouter(runtime *callaudit.Runtime) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(ClientRequestID())
	router.Use(func(c *gin.Context) {
		groupID := int64(9)
		c.Set(string(ContextKeyAPIKey), &service.APIKey{
			ID: 7, Name: "migration-key", GroupID: &groupID,
			User:  &service.User{ID: 8, Username: "alice"},
			Group: &service.Group{ID: groupID, Name: "migration", Platform: service.PlatformAnthropic},
		})
		c.Next()
	})
	router.Use(gin.HandlerFunc(NewCallAuditMiddleware(runtime)))
	return router
}

func loadOnlyReadyManifest(t *testing.T, spoolDir string) callaudit.Manifest {
	t.Helper()
	entries := waitForReadyManifests(t, filepath.Join(spoolDir, "ready"), 1)
	raw, err := os.ReadFile(filepath.Join(spoolDir, "ready", entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	var manifest callaudit.Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func readArtifactPayload(t *testing.T, spoolDir string, manifest callaudit.Manifest, kind callaudit.ArtifactKind) map[string]any {
	t.Helper()
	for _, artifact := range manifest.Artifacts {
		if artifact.Kind != kind {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(spoolDir, filepath.FromSlash(artifact.SpoolPath)))
		if err != nil {
			t.Fatal(err)
		}
		var payload map[string]any
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatal(err)
		}
		return payload
	}
	t.Fatalf("artifact %s not found in %+v", kind, manifest.Artifacts)
	return nil
}

func waitForReadyManifests(t *testing.T, directory string, want int) []os.DirEntry {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		entries, err := os.ReadDir(directory)
		if err == nil && len(entries) == want {
			return entries
		}
		if time.Now().After(deadline) {
			t.Fatalf("ready manifests in %s = %v, %v; want %d", directory, entries, err, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
