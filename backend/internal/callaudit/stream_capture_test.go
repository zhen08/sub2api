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
	capture, err := NewStreamCapture(writer, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("x"), 8<<20)
	startedAt := time.Now()
	capture.WriteCapture(payload)
	if elapsed := time.Since(startedAt); elapsed > 250*time.Millisecond {
		t.Fatalf("producer blocked on capture disk I/O for %s", elapsed)
	}
	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("capture writer did not start")
	}

	finished := make(chan StreamCaptureSnapshot, 1)
	go func() { finished <- capture.Finish() }()
	close(writer.release)
	snapshot := <-finished
	if !snapshot.Incomplete || !snapshot.Truncated || !errors.Is(snapshot.Err, ErrCaptureBackpressure) {
		t.Fatalf("snapshot = %+v", snapshot)
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
