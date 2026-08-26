package callaudit

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"
)

type blockingCaptureWriter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	buffer  bytes.Buffer
}

type delayedCaptureWriter struct {
	delay  time.Duration
	buffer bytes.Buffer
}

func (w *delayedCaptureWriter) Write(payload []byte) (int, error) {
	time.Sleep(w.delay)
	return w.buffer.Write(payload)
}

func (w *blockingCaptureWriter) Write(payload []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return w.buffer.Write(payload)
}

func TestStreamCaptureNeverBlocksProducerOnSlowDisk(t *testing.T) {
	t.Parallel()
	writer := &blockingCaptureWriter{started: make(chan struct{}), release: make(chan struct{})}
	budget := newCaptureBufferBudget(2 << 20)
	capture, err := newStreamCapture(writer, 8<<20, 8, budget)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("x"), 1<<20)
	startedAt := time.Now()
	capture.WriteCapture(payload)
	if elapsed := time.Since(startedAt); elapsed > 250*time.Millisecond {
		t.Fatalf("producer blocked on capture disk I/O for %s", elapsed)
	}
	finished := make(chan StreamCaptureSnapshot, 1)
	go func() { finished <- capture.Finish() }()
	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("capture writer did not start")
	}

	close(writer.release)
	snapshot := <-finished
	if !snapshot.Incomplete || !snapshot.Truncated || !errors.Is(snapshot.Err, ErrCaptureBackpressure) {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if used := budget.usedBytes(); used != 0 {
		t.Fatalf("buffer budget leaked %d bytes", used)
	}
}

func TestStreamCapturePreservesCompletePayload(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	capture, err := NewStreamCapture(&output, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("audit"), 10_000)
	capture.WriteCapture(payload)
	snapshot := capture.Finish()
	if snapshot.Err != nil || snapshot.Incomplete || snapshot.Truncated {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if !bytes.Equal(output.Bytes(), payload) {
		t.Fatalf("captured %d bytes, want %d", output.Len(), len(payload))
	}
}

func TestStreamCaptureAbsorbsShortStorageLatencyBurst(t *testing.T) {
	t.Parallel()
	writer := &delayedCaptureWriter{delay: 25 * time.Millisecond}
	capture, err := NewStreamCapture(writer, 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("burst"), (1<<20)/len("burst"))
	capture.WriteCapture(payload)
	snapshot := capture.Finish()
	if snapshot.Err != nil || snapshot.Incomplete || snapshot.Truncated {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if !bytes.Equal(writer.buffer.Bytes(), payload) {
		t.Fatalf("captured %d bytes, want %d", writer.buffer.Len(), len(payload))
	}
}

func TestStreamCaptureAbsorbsLargeRequestDuringSlowStorage(t *testing.T) {
	t.Parallel()
	writer := &delayedCaptureWriter{delay: 25 * time.Millisecond}
	payload := bytes.Repeat([]byte("large-audit-request"), (20<<20)/len("large-audit-request"))
	capture, err := NewStreamCapture(writer, int64(len(payload))+1)
	if err != nil {
		t.Fatal(err)
	}
	startedAt := time.Now()
	capture.WriteCapture(payload)
	if elapsed := time.Since(startedAt); elapsed > 250*time.Millisecond {
		t.Fatalf("producer blocked on capture disk I/O for %s", elapsed)
	}
	snapshot := capture.Finish()
	if snapshot.Err != nil || snapshot.Incomplete || snapshot.Truncated {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if snapshot.Accepted != int64(len(payload)) || snapshot.Written != int64(len(payload)) {
		t.Fatalf("snapshot bytes = accepted %d written %d, want %d", snapshot.Accepted, snapshot.Written, len(payload))
	}
	if !bytes.Equal(writer.buffer.Bytes(), payload) {
		t.Fatalf("captured %d bytes, want %d", writer.buffer.Len(), len(payload))
	}
}

func TestStreamCaptureGlobalBufferBudgetIsReleasedAfterBackpressure(t *testing.T) {
	t.Parallel()
	budget := newCaptureBufferBudget(2 * captureChunkBytes)
	writer := &blockingCaptureWriter{started: make(chan struct{}), release: make(chan struct{})}
	capture, err := newStreamCapture(writer, 1<<20, 16, budget)
	if err != nil {
		t.Fatal(err)
	}
	capture.WriteCapture(bytes.Repeat([]byte("x"), 4*captureChunkBytes))
	finished := make(chan StreamCaptureSnapshot, 1)
	go func() { finished <- capture.Finish() }()
	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("capture writer did not start")
	}
	close(writer.release)
	snapshot := <-finished
	if !snapshot.Incomplete || !snapshot.Truncated || !errors.Is(snapshot.Err, ErrCaptureBackpressure) {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if used := budget.usedBytes(); used != 0 {
		t.Fatalf("buffer budget leaked %d bytes", used)
	}

	var output bytes.Buffer
	second, err := newStreamCapture(&output, 1<<20, 16, budget)
	if err != nil {
		t.Fatal(err)
	}
	second.WriteCapture([]byte("budget-reused"))
	secondSnapshot := second.Finish()
	if secondSnapshot.Err != nil || output.String() != "budget-reused" {
		t.Fatalf("second capture snapshot = %+v output = %q", secondSnapshot, output.String())
	}
}
