package repository

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/callaudit"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

type callAuditHTTPUpstream struct {
	base    service.HTTPUpstream
	runtime *callaudit.Runtime
}

// ProvideHTTPUpstream decorates only context-scoped inference requests. Health
// probes, account tests and background jobs share HTTPUpstream but have no audit
// Session in context and therefore remain untouched.
func ProvideHTTPUpstream(cfg *config.Config, runtime *callaudit.Runtime) service.HTTPUpstream {
	return &callAuditHTTPUpstream{base: NewHTTPUpstream(cfg), runtime: runtime}
}

func (u *callAuditHTTPUpstream) Do(req *http.Request, proxyURL string, accountID int64, accountConcurrency int) (*http.Response, error) {
	return u.do(req, proxyURL, accountID, func() (*http.Response, error) {
		return u.base.Do(req, proxyURL, accountID, accountConcurrency)
	})
}

func (u *callAuditHTTPUpstream) DoWithTLS(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, profile *tlsfingerprint.Profile) (*http.Response, error) {
	return u.do(req, proxyURL, accountID, func() (*http.Response, error) {
		return u.base.DoWithTLS(req, proxyURL, accountID, accountConcurrency, profile)
	})
}

func (u *callAuditHTTPUpstream) do(req *http.Request, proxyURL string, accountID int64, invoke func() (*http.Response, error)) (*http.Response, error) {
	if req == nil {
		return invoke()
	}
	session, ok := callaudit.SessionFromContext(req.Context())
	if !ok || !session.ArtifactsEnabled() {
		return invoke()
	}
	bodyLimit := upstreamArtifactBodyLimit(u.runtime, session)

	var captureTemp *os.File
	var captureStream *callaudit.StreamCapture
	var teeBody *auditTeeReadCloser
	var captureSetupErr error
	if req.Body != nil && req.GetBody == nil {
		if temp, err := session.CreateCaptureTemp("upstream"); err == nil {
			stream, streamErr := callaudit.NewStreamCapture(temp, bodyLimit)
			if streamErr != nil {
				name := temp.Name()
				_ = temp.Close()
				_ = os.Remove(name)
				captureSetupErr = streamErr
				if u.runtime != nil {
					u.runtime.RecordCaptureFailure(streamErr)
				}
			} else {
				captureTemp = temp
				captureStream = stream
				teeBody = &auditTeeReadCloser{
					source:        req.Body,
					capture:       stream,
					contentLength: req.ContentLength,
					done:          make(chan struct{}),
				}
				req.Body = teeBody
			}
		} else {
			captureSetupErr = err
			if u.runtime != nil {
				u.runtime.RecordCaptureFailure(err)
			}
		}
	}

	response, upstreamErr := invoke()
	getBody := req.GetBody
	contentLength := req.ContentLength
	contentType := req.Header.Get("Content-Type")
	upstreamHost := ""
	if req.URL != nil {
		upstreamHost = req.URL.Hostname()
	}
	baseFields := map[string]any{
		"kind":       string(callaudit.ArtifactUpstreamRequest),
		"capturedAt": time.Now().UTC(),
		"requestId":  session.Scope().RequestID,
		"provider":   legacyAuditProvider(session.Scope(), upstreamHost),
		"method":     req.Method,
		"url":        callaudit.RedactURL(req.URL),
		"headers":    callaudit.AuditHeaders(req.Header),
		"meta": map[string]any{
			"accountId":       strconv.FormatInt(accountID, 10),
			"proxyConfigured": proxyURL != "",
			"upstreamHost":    upstreamHost,
		},
	}
	cleanupCapture := func() {
		if captureTemp == nil {
			return
		}
		name := captureTemp.Name()
		_ = captureTemp.Close()
		_ = os.Remove(name)
	}
	onComplete := func(captureErr error) {
		if captureErr == nil {
			return
		}
		session.DisableArtifacts("artifact_write_failed")
		if u.runtime != nil {
			u.runtime.RecordCaptureFailure(captureErr)
		}
	}
	artifactErr := session.CaptureArtifactStreamAsync(callaudit.ArtifactUpstreamRequest, func(writer io.Writer) error {
		defer cleanupCapture()

		fields := cloneAuditFields(baseFields)
		var body io.Reader
		var bodyClose func()
		var bodyErr error
		captureErr := captureSetupErr
		truncated := contentLength > bodyLimit && bodyLimit > 0
		incomplete := false

		if teeBody != nil {
			// A transport may return response headers before its request-body writer
			// exits. Waiting here is off the inference path. On timeout, disabling
			// the sink under the same lock prevents a late read racing the flush.
			if !teeBody.finishCapture(5 * time.Second) {
				incomplete = true
			}
			completeSnapshot, sourceErr := teeBody.snapshot()
			incomplete = incomplete || !completeSnapshot
			bodyErr = sourceErr
			if captureStream != nil {
				streamSnapshot := captureStream.Finish()
				truncated = streamSnapshot.Truncated
				incomplete = incomplete || streamSnapshot.Incomplete
				captureErr = errors.Join(captureErr, streamSnapshot.Err)
			}
			if captureTemp != nil {
				if _, err := captureTemp.Seek(0, io.SeekStart); err != nil {
					captureErr = errors.Join(captureErr, err)
				} else {
					body = captureTemp
				}
			}
		} else {
			body, bodyClose, bodyErr = upstreamAuditBody(getBody, nil)
			if bodyClose != nil {
				defer bodyClose()
			}
		}

		if bodyErr != nil {
			fields["captureReadError"] = true
		}
		if captureErr != nil {
			fields["captureWriteError"] = true
		}
		if truncated {
			fields["captureTruncated"] = true
		}
		if incomplete {
			fields["captureIncomplete"] = true
		}
		if body != nil {
			body = io.LimitReader(body, bodyLimit)
		}
		encoding := callaudit.BodyJSON
		if truncated || incomplete || bodyErr != nil || captureErr != nil || !strings.Contains(strings.ToLower(contentType), "json") {
			encoding = callaudit.BodyUTF8Raw
		}
		return callaudit.WriteArtifactEnvelope(writer, fields, body, encoding)
	}, onComplete)
	if artifactErr != nil {
		// CaptureArtifactStreamAsync may reject before taking ownership (for
		// example when its bounded writer pool is saturated). Stop late transport
		// reads from enqueueing more bytes and synchronously join the capture
		// worker before closing/removing its file, otherwise the process-wide
		// stream slot and goroutine would leak until restart.
		if captureStream != nil {
			captureStream.Disable(artifactErr)
			_ = captureStream.Finish()
		}
		cleanupCapture()
		onComplete(artifactErr)
	}
	return response, upstreamErr
}

func upstreamAuditBody(getBody func() (io.ReadCloser, error), fallback *os.File) (io.Reader, func(), error) {
	if getBody != nil {
		body, err := getBody()
		if err == nil {
			return body, func() { _ = body.Close() }, nil
		}
		return fallback, nil, fmt.Errorf("reopen upstream audit body: %w", err)
	}
	if fallback != nil {
		return fallback, nil, nil
	}
	return nil, nil, nil
}

type auditTeeReadCloser struct {
	source        io.ReadCloser
	capture       *callaudit.StreamCapture
	contentLength int64
	done          chan struct{}
	doneOnce      sync.Once

	mu        sync.Mutex
	active    int
	readBytes int64
	eof       bool
	closed    bool
	sourceErr error
}

func (r *auditTeeReadCloser) Read(payload []byte) (int, error) {
	r.mu.Lock()
	r.active++
	r.mu.Unlock()
	n, err := r.source.Read(payload)
	r.mu.Lock()
	if n > 0 {
		r.readBytes += int64(n)
		r.capture.WriteCapture(payload[:n])
	}
	if err == io.EOF {
		r.eof = true
	} else if err != nil && r.sourceErr == nil {
		r.sourceErr = err
	}
	r.active--
	complete := r.captureTerminalLocked() && r.active == 0
	r.mu.Unlock()
	if complete {
		r.signalDone()
	}
	return n, err
}

func (r *auditTeeReadCloser) Close() error {
	err := r.source.Close()
	r.mu.Lock()
	r.closed = true
	complete := r.active == 0
	r.mu.Unlock()
	if complete {
		r.signalDone()
	}
	return err
}

func (r *auditTeeReadCloser) finishCapture(timeout time.Duration) bool {
	if r == nil {
		return true
	}
	if timeout <= 0 {
		timeout = time.Second
	}
	r.mu.Lock()
	alreadyComplete := r.captureTerminalLocked() && r.active == 0
	r.mu.Unlock()
	if alreadyComplete {
		r.signalDone()
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-r.done:
		return true
	case <-timer.C:
		r.mu.Lock()
		if r.capture != nil {
			r.capture.Disable(callaudit.ErrCaptureProducerTimeout)
		}
		r.mu.Unlock()
		return false
	}
}

func (r *auditTeeReadCloser) snapshot() (complete bool, sourceErr error) {
	if r == nil {
		return true, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.captureCompleteLocked(), r.sourceErr
}

func (r *auditTeeReadCloser) captureCompleteLocked() bool {
	return r.eof || (r.contentLength >= 0 && r.readBytes >= r.contentLength)
}

func (r *auditTeeReadCloser) captureTerminalLocked() bool {
	return r.closed || r.captureCompleteLocked()
}

func (r *auditTeeReadCloser) signalDone() {
	r.doneOnce.Do(func() { close(r.done) })
}

func cloneAuditFields(source map[string]any) map[string]any {
	clone := make(map[string]any, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func upstreamArtifactBodyLimit(runtime *callaudit.Runtime, session *callaudit.Session) int64 {
	const envelopeReserve = int64(64 << 10)
	maximum := int64(0)
	if runtime != nil {
		maximum = runtime.MaxArtifactBytes()
	}
	if maximum <= 0 && session != nil {
		maximum = session.MaxArtifactBytes()
	}
	if maximum <= 0 {
		return 0
	}
	if maximum <= envelopeReserve {
		return maximum / 2
	}
	return maximum - envelopeReserve
}

func legacyAuditProvider(scope callaudit.Scope, host string) string {
	normalizedHost := strings.ToLower(strings.TrimSpace(host))
	switch {
	case strings.Contains(normalizedHost, "bedrock") || strings.Contains(normalizedHost, "amazonaws.com"):
		return "bedrock"
	case strings.Contains(normalizedHost, "azure"):
		return "azure-openai"
	case strings.Contains(normalizedHost, "anthropic"):
		return "claude-official"
	case strings.Contains(normalizedHost, "openai") || strings.Contains(normalizedHost, "chatgpt"):
		return "openai-responses"
	case strings.Contains(normalizedHost, "googleapis"):
		switch scope.Protocol {
		case callaudit.ProtocolAntigravity:
			return "antigravity"
		case callaudit.ProtocolGeminiCLI:
			return "gemini-cli"
		default:
			return "gemini-api"
		}
	case scope.Protocol != "" && scope.Protocol != callaudit.ProtocolUnknown:
		return string(scope.Protocol)
	default:
		return "unknown"
	}
}
