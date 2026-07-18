package middleware

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/callaudit"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/gin-gonic/gin"
)

type CallAuditMiddleware gin.HandlerFunc

func NewCallAuditMiddleware(runtime *callaudit.Runtime) CallAuditMiddleware {
	return CallAuditMiddleware(func(c *gin.Context) {
		if c == nil {
			return
		}
		if runtime == nil || !runtime.Enabled() || c.Request == nil {
			c.Next()
			return
		}
		classification := callaudit.ClassifyRoute(c.Request.Method, c.Request.URL.RequestURI())
		if !classification.Eligible {
			c.Next()
			return
		}
		apiKey, ok := GetAPIKeyFromContext(c)
		if !ok || apiKey == nil || apiKey.User == nil {
			// The middleware is deliberately mounted after successful API-key auth.
			c.Next()
			return
		}

		identity := callaudit.IdentitySnapshot{
			APIKeyID:     strconv.FormatInt(apiKey.ID, 10),
			APIKeyName:   apiKey.Name,
			UserID:       strconv.FormatInt(apiKey.User.ID, 10),
			UserUsername: apiKey.User.Username,
		}
		if apiKey.Group != nil {
			if apiKey.Group.ID != 0 {
				identity.GroupID = strconv.FormatInt(apiKey.Group.ID, 10)
			} else if apiKey.GroupID != nil {
				identity.GroupID = strconv.FormatInt(*apiKey.GroupID, 10)
			}
			identity.GroupName = apiKey.Group.Name
			identity.GroupPlatform = apiKey.Group.Platform
		} else if apiKey.GroupID != nil {
			identity.GroupID = strconv.FormatInt(*apiKey.GroupID, 10)
		}
		requestID, _ := c.Request.Context().Value(ctxkey.ClientRequestID).(string)
		session, err := runtime.StartSession(callaudit.ScopeInput{
			RequestID: requestID,
			Endpoint:  classification.Path,
			Method:    c.Request.Method,
			Protocol:  classification.Protocol,
			Identity:  identity,
		})
		if err != nil || session == nil {
			if err != nil {
				runtime.RecordCaptureFailure(err)
			}
			c.Next()
			return
		}
		if model := extractEntryModelFromPath(classification.Path); model != "" {
			session.SetEntryModel(model)
		}

		bodyLimit := artifactBodyLimit(runtime.MaxArtifactBytes())
		var requestCapture *callAuditRequestBody
		requestCaptureUnavailable := false
		if session.ArtifactsEnabled() {
			requestTemp, captureErr := session.CreateCaptureTemp("request")
			if captureErr != nil {
				runtime.RecordCaptureFailure(captureErr)
				requestCaptureUnavailable = true
			} else {
				streamCapture, streamErr := callaudit.NewStreamCapture(requestTemp, bodyLimit)
				if streamErr != nil {
					cleanupCaptureTemp(requestTemp)
					runtime.RecordCaptureFailure(streamErr)
					requestCaptureUnavailable = true
				} else {
					requestCapture = &callAuditRequestBody{
						source:  c.Request.Body,
						capture: requestTemp,
						stream:  streamCapture,
					}
					if c.Request.Body != nil {
						c.Request.Body = requestCapture
					}
				}
			}
		}

		var responseTemp *os.File
		responseCaptureUnavailable := false
		if session.ArtifactsEnabled() {
			responseTemp, err = session.CreateCaptureTemp("response")
			if err != nil {
				runtime.RecordCaptureFailure(err)
				responseTemp = nil
				responseCaptureUnavailable = true
			}
		}
		captureWriter := &callAuditResponseWriter{
			ResponseWriter: c.Writer,
		}
		if responseTemp != nil {
			streamCapture, streamErr := callaudit.NewStreamCapture(responseTemp, bodyLimit)
			if streamErr != nil {
				cleanupCaptureTemp(responseTemp)
				responseTemp = nil
				responseCaptureUnavailable = true
				runtime.RecordCaptureFailure(streamErr)
			} else {
				captureWriter.stream = streamCapture
				c.Writer = captureWriter
			}
		}
		c.Request = c.Request.WithContext(callaudit.WithSession(c.Request.Context(), session))

		defer func() {
			recovered := recover()
			if recovered != nil {
				writeRecoveredHTTPResponse(c, recovered)
			}
			terminationReason := callAuditTerminationReason(captureWriter.writeErr, recovered)
			if terminationReason == "" && c.Request.Context().Err() != nil {
				if requestCapture != nil && requestCapture.sourceErr != nil && !c.Writer.Written() {
					terminationReason = "client_aborted"
				} else {
					terminationReason = "client_disconnected"
				}
			}
			if strings.Contains(strings.ToLower(c.Writer.Header().Get("Content-Type")), "text/event-stream") {
				session.SetStream(true)
			}
			var statusCode *int
			if c.Writer.Written() || terminationReason == "" {
				status := c.Writer.Status()
				statusCode = &status
			}
			if recovered != nil && !c.Writer.Written() {
				status := http.StatusInternalServerError
				statusCode = &status
			}
			if requestCapture != nil {
				requestCapture.finishSource()
			}
			requestSnapshot := cloneAuditRequest(c.Request)
			responseSnapshot := callAuditResponseSnapshot{
				Status:  c.Writer.Status(),
				Written: c.Writer.Written(),
				Header:  c.Writer.Header().Clone(),
			}
			meta := map[string]any{}
			if requestCaptureUnavailable {
				meta["requestCaptureUnavailable"] = true
			}
			if responseCaptureUnavailable {
				meta["responseCaptureUnavailable"] = true
			}
			if captureWriter.writeErr != nil {
				meta["clientResponseWriteError"] = true
			}
			status := callaudit.CallStatus("")
			if recovered != nil {
				status = callaudit.CallStatusError
				meta["panicRecovered"] = true
			}
			runtime.RunFinalize(func() {
				defer session.Release()
				defer cleanupCaptureTemp(requestCaptureFile(requestCapture))
				defer cleanupCaptureTemp(responseTemp)

				if requestCapture != nil {
					requestCapture.prepareCapture()
					if requestCapture.captureErr != nil {
						runtime.RecordCaptureFailure(requestCapture.captureErr)
						meta["requestCaptureWriteError"] = true
					}
					if requestCapture.sourceErr != nil {
						runtime.RecordCaptureFailure(requestCapture.sourceErr)
					}
					model, streamRequested := extractEntryAuditHints(requestCapture.capture)
					if model != "" {
						session.SetEntryModel(model)
					}
					session.SetStream(streamRequested)
					if session.ArtifactsEnabled() {
						if err := captureClientRequest(session, requestSnapshot, requestCapture, statusCode); err != nil {
							runtime.RecordCaptureFailure(err)
							meta["clientRequestArtifactError"] = true
						}
					}
				}
				if responseTemp != nil {
					captureWriter.finishCapture()
					if captureWriter.truncated {
						meta["responseCaptureTruncated"] = true
					}
					if captureWriter.incomplete {
						meta["responseCaptureIncomplete"] = true
					}
					if captureWriter.captureErr != nil {
						runtime.RecordCaptureFailure(captureWriter.captureErr)
						meta["responseCaptureWriteError"] = true
					}
				}
				if session.ArtifactsEnabled() {
					if err := captureClientResponse(session, responseSnapshot, responseTemp, terminationReason, captureWriter.truncated, captureWriter.incomplete, captureWriter.captureErr, statusCode); err != nil {
						runtime.RecordCaptureFailure(err)
						meta["responseArtifactError"] = true
					}
				}
				// Artifact writers are bounded and upstream tee completion has its own
				// timeout. Do not abandon a durable manifest because of an arbitrary
				// request-local deadline; Runtime.Shutdown provides the process-level
				// drain bound.
				if _, err := session.Finalize(context.Background(), callaudit.Outcome{
					Status:            status,
					StatusCode:        statusCode,
					TerminationReason: terminationReason,
					Meta:              meta,
				}); err != nil {
					runtime.RecordCaptureFailure(err)
				}
			})
			if recovered != nil {
				panic(recovered)
			}
		}()

		c.Next()
	})
}

func cloneAuditRequest(request *http.Request) *http.Request {
	if request == nil {
		return &http.Request{Header: make(http.Header)}
	}
	clone := request.Clone(context.Background())
	clone.Body = nil
	return clone
}

func captureClientRequest(session *callaudit.Session, request *http.Request, capture *callAuditRequestBody, statusCode *int) error {
	complete := capture.complete(request.ContentLength) && !capture.incomplete
	fields := map[string]any{
		"kind":       string(callaudit.ArtifactClientRequest),
		"capturedAt": time.Now().UTC(),
		"requestId":  session.Scope().RequestID,
		"method":     request.Method,
		"endpoint":   session.Scope().Endpoint,
		"protocol":   session.Scope().Protocol,
		"headers":    callaudit.AuditHeaders(request.Header),
		"query":      callaudit.AuditQuery(request.URL.Query()),
	}
	if capture.sourceErr != nil {
		fields["captureReadError"] = true
	}
	if capture.captureErr != nil {
		fields["captureWriteError"] = true
	}
	if capture.truncated {
		fields["captureTruncated"] = true
	}
	if capture.incomplete {
		fields["captureIncomplete"] = true
	}
	if !complete {
		fields["captureIncomplete"] = true
	}

	var bodyReader io.Reader
	if capture.capture != nil {
		info, err := capture.capture.Stat()
		if err != nil {
			return err
		}
		if info.Size() > 0 {
			if _, err := capture.capture.Seek(0, io.SeekStart); err != nil {
				return err
			}
			bodyReader = capture.capture
		}
	}
	encoding := callaudit.BodyJSON
	if capture.truncated || capture.sourceErr != nil || capture.captureErr != nil || !complete ||
		!strings.Contains(strings.ToLower(request.Header.Get("Content-Type")), "json") ||
		(statusCode != nil && *statusCode >= http.StatusBadRequest) {
		encoding = callaudit.BodyUTF8Raw
	}
	_, err := session.CaptureArtifactStream(callaudit.ArtifactClientRequest, func(writer io.Writer) error {
		return callaudit.WriteArtifactEnvelope(writer, fields, bodyReader, encoding)
	})
	if capture.capture != nil {
		_, _ = capture.capture.Seek(0, io.SeekStart)
	}
	return err
}

type callAuditResponseSnapshot struct {
	Status  int
	Written bool
	Header  http.Header
}

func captureClientResponse(session *callaudit.Session, response callAuditResponseSnapshot, body *os.File, terminationReason string, truncated bool, incomplete bool, captureErr error, statusOverride *int) error {
	statusCode := any(response.Status)
	if statusOverride != nil {
		statusCode = *statusOverride
	}
	if terminationReason != "" && !response.Written {
		statusCode = nil
	}
	fields := map[string]any{
		"kind":              string(callaudit.ArtifactResponse),
		"capturedAt":        time.Now().UTC(),
		"requestId":         session.Scope().RequestID,
		"statusCode":        statusCode,
		"headers":           callaudit.AuditHeaders(response.Header),
		"contentType":       response.Header.Get("Content-Type"),
		"terminationReason": nullableAuditString(terminationReason),
	}
	if truncated {
		fields["captureTruncated"] = true
	}
	if captureErr != nil {
		fields["captureWriteError"] = true
	}
	if incomplete {
		fields["captureIncomplete"] = true
	}
	var reader io.Reader
	if body != nil {
		if _, err := body.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if info, err := body.Stat(); err == nil && info.Size() > 0 {
			reader = body
		}
	}
	encoding := callaudit.BodyUTF8Raw
	if terminationReason == "" && !truncated && !incomplete && captureErr == nil && strings.Contains(strings.ToLower(response.Header.Get("Content-Type")), "json") {
		encoding = callaudit.BodyJSON
	}
	_, err := session.CaptureArtifactStream(callaudit.ArtifactResponse, func(output io.Writer) error {
		return callaudit.WriteArtifactEnvelope(output, fields, reader, encoding)
	})
	return err
}

func artifactBodyLimit(max int64) int64 {
	const envelopeReserve = int64(64 << 10)
	if max <= 0 {
		return 0
	}
	if max <= envelopeReserve {
		return max / 2
	}
	return max - envelopeReserve
}

func nullableAuditString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

type callAuditRequestBody struct {
	source     io.ReadCloser
	capture    *os.File
	stream     *callaudit.StreamCapture
	readBytes  int64
	eof        bool
	closed     bool
	truncated  bool
	incomplete bool
	sourceErr  error
	captureErr error
}

func (r *callAuditRequestBody) Read(payload []byte) (int, error) {
	if r.source == nil {
		r.eof = true
		return 0, io.EOF
	}
	n, err := r.source.Read(payload)
	if n > 0 {
		r.readBytes += int64(n)
		r.captureBytes(payload[:n])
	}
	if err == io.EOF {
		r.eof = true
	} else if err != nil && r.sourceErr == nil {
		r.sourceErr = err
	}
	return n, err
}

func (r *callAuditRequestBody) Close() error {
	if r.source == nil || r.closed {
		return nil
	}
	r.closed = true
	return r.source.Close()
}

func (r *callAuditRequestBody) finishSource() {
	if r == nil {
		return
	}
	// Never drain an unread client body for auditing: doing so can delay the
	// response and violates the nonblocking failure policy. Gateway handlers read
	// the JSON bodies they consume; otherwise the artifact is marked incomplete.
	_ = r.Close()
}

func (r *callAuditRequestBody) prepareCapture() {
	if r == nil || r.stream == nil {
		return
	}
	snapshot := r.stream.Finish()
	r.truncated = snapshot.Truncated
	r.incomplete = snapshot.Incomplete
	r.captureErr = snapshot.Err
}

func (r *callAuditRequestBody) complete(contentLength int64) bool {
	if r == nil || r.source == nil {
		return true
	}
	if r.eof {
		return true
	}
	return contentLength >= 0 && r.readBytes >= contentLength
}

func (r *callAuditRequestBody) captureBytes(payload []byte) {
	if r == nil || r.stream == nil || len(payload) == 0 {
		return
	}
	r.stream.WriteCapture(payload)
}

func requestCaptureFile(capture *callAuditRequestBody) *os.File {
	if capture == nil {
		return nil
	}
	return capture.capture
}

func cleanupCaptureTemp(file *os.File) {
	if file == nil {
		return
	}
	name := file.Name()
	_ = file.Close()
	_ = os.Remove(name)
}

func extractEntryModel(file *os.File) string {
	model, _ := extractEntryAuditHints(file)
	return model
}

func extractEntryAuditHints(file *os.File) (string, bool) {
	if file == nil {
		return "", false
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", false
	}
	defer func() { _, _ = file.Seek(0, io.SeekStart) }()
	decoder := json.NewDecoder(io.LimitReader(file, 64<<10))
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return "", false
	}
	var model string
	var stream bool
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return model, stream
		}
		switch key {
		case "model":
			value, err := decoder.Token()
			if err != nil {
				return model, stream
			}
			model, _ = value.(string)
			model = strings.TrimSpace(model)
		case "stream":
			value, err := decoder.Token()
			if err != nil {
				return model, stream
			}
			stream, _ = value.(bool)
		default:
			if err := skipJSONValue(decoder); err != nil {
				return model, stream
			}
		}
	}
	return model, stream
}

func callAuditTerminationReason(responseWriteErr error, recovered any) string {
	if responseWriteErr != nil {
		return "client_disconnected"
	}
	if recoveredErr, ok := recovered.(error); ok && isBrokenPipe(recoveredErr) {
		return "client_disconnected"
	}
	return ""
}

func extractEntryModelFromPath(path string) string {
	const marker = "/models/"
	index := strings.LastIndex(strings.ToLower(path), marker)
	if index < 0 {
		return ""
	}
	model := path[index+len(marker):]
	if separator := strings.IndexAny(model, ":/"); separator >= 0 {
		model = model[:separator]
	}
	return strings.TrimSpace(model)
}

func skipJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok || (delim != '{' && delim != '[') {
		return nil
	}
	depth := 1
	for depth > 0 {
		token, err = decoder.Token()
		if err != nil {
			return err
		}
		if nested, ok := token.(json.Delim); ok {
			switch nested {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
		}
	}
	return nil
}

type callAuditResponseWriter struct {
	gin.ResponseWriter
	stream     *callaudit.StreamCapture
	truncated  bool
	incomplete bool
	captureErr error
	writeErr   error
}

func (w *callAuditResponseWriter) finishCapture() {
	if w == nil || w.stream == nil {
		return
	}
	snapshot := w.stream.Finish()
	w.truncated = snapshot.Truncated
	w.incomplete = w.incomplete || snapshot.Incomplete
	w.captureErr = snapshot.Err
}

func (w *callAuditResponseWriter) Write(payload []byte) (int, error) {
	n, err := w.ResponseWriter.Write(payload)
	if n > len(payload) {
		n = len(payload)
	}
	if n > 0 {
		w.captureBytes(payload[:n])
	}
	if err != nil || n != len(payload) {
		w.incomplete = true
		if err != nil {
			w.writeErr = err
		} else {
			w.writeErr = io.ErrShortWrite
		}
	}
	return n, err
}

func (w *callAuditResponseWriter) WriteString(payload string) (int, error) {
	n, err := w.ResponseWriter.WriteString(payload)
	if n > len(payload) {
		n = len(payload)
	}
	if n > 0 {
		w.captureBytes([]byte(payload[:n]))
	}
	if err != nil || n != len(payload) {
		w.incomplete = true
		if err != nil {
			w.writeErr = err
		} else {
			w.writeErr = io.ErrShortWrite
		}
	}
	return n, err
}

// ReadFrom preserves io.ReaderFrom for handlers that use io.Copy while still
// routing successful bytes through Write, so the client and capture stay exact.
func (w *callAuditResponseWriter) ReadFrom(reader io.Reader) (int64, error) {
	return io.Copy(struct{ io.Writer }{Writer: w}, reader)
}

// Unwrap lets net/http.ResponseController reach the underlying writer.
func (w *callAuditResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *callAuditResponseWriter) captureBytes(payload []byte) {
	if w == nil || w.stream == nil || len(payload) == 0 {
		return
	}
	w.stream.WriteCapture(payload)
}
