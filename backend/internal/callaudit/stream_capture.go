package callaudit

import (
	"bufio"
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

const (
	captureChunkBytes              = 32 << 10
	maxStreamCaptureBufferedBytes  = 64 << 20
	captureQueueDepth              = maxStreamCaptureBufferedBytes / captureChunkBytes
	captureWriterBufferBytes       = 256 << 10
	maxProcessCaptureBufferedBytes = 256 << 20
	maxStreamCaptures              = 512
)

var (
	ErrCaptureBackpressure    = errors.New("audit capture queue is saturated")
	ErrCaptureProducerTimeout = errors.New("audit capture producer did not finish before timeout")
	ErrCaptureWorkerSaturated = errors.New("audit capture worker capacity is saturated")
)

var streamCaptureSlots = make(chan struct{}, maxStreamCaptures)

var activeStreamCaptures atomic.Int64

// captureBufferBudget bounds queued payload bytes across every active request,
// response, and upstream capture. A large per-stream queue can therefore absorb
// multi-megabyte request bursts without multiplying that capacity by the global
// stream limit. Bytes copied into bufio.Writer are no longer charged here; its
// fixed per-stream allocation is bounded separately by maxStreamCaptures.
type captureBufferBudget struct {
	limit int64
	used  atomic.Int64
}

func newCaptureBufferBudget(limit int64) *captureBufferBudget {
	return &captureBufferBudget{limit: limit}
}

func (b *captureBufferBudget) tryAcquire(bytes int64) bool {
	if b == nil || bytes <= 0 || bytes > b.limit {
		return false
	}
	for {
		used := b.used.Load()
		if used > b.limit-bytes {
			return false
		}
		if b.used.CompareAndSwap(used, used+bytes) {
			return true
		}
	}
}

func (b *captureBufferBudget) release(bytes int64) {
	if b == nil || bytes <= 0 {
		return
	}
	b.used.Add(-bytes)
}

func (b *captureBufferBudget) usedBytes() int64 {
	if b == nil {
		return 0
	}
	return b.used.Load()
}

var processCaptureBufferBudget = newCaptureBufferBudget(maxProcessCaptureBufferedBytes)

// ActiveStreamCaptureCount returns the number of capture workers currently
// holding a global stream slot. It is intentionally process-wide because the
// capacity guard is process-wide as well.
func ActiveStreamCaptureCount() int64 {
	return activeStreamCaptures.Load()
}

// StreamCaptureSnapshot describes a bounded best-effort capture. Err and
// Incomplete never propagate to the inference request; callers persist them as
// audit degradation metadata instead.
type StreamCaptureSnapshot struct {
	Accepted   int64
	Written    int64
	Truncated  bool
	Incomplete bool
	Err        error
}

// StreamCapture decouples request/SSE transport goroutines from local disk I/O.
// Producers copy into a per-stream queue guarded by the process-wide memory
// budget and never wait for file.Write. If either bound saturates (for example
// because the persistent volume stalls), raw capture stops for this artifact
// while the business stream continues.
type StreamCapture struct {
	writer io.Writer
	limit  int64
	queue  chan []byte
	done   chan struct{}
	budget *captureBufferBudget

	mu         sync.Mutex
	finishOnce sync.Once
	accepted   int64
	written    int64
	closed     bool
	disabled   bool
	truncated  bool
	incomplete bool
	err        error
}

func NewStreamCapture(writer io.Writer, limit int64) (*StreamCapture, error) {
	return newStreamCapture(writer, limit, captureQueueDepth, processCaptureBufferBudget)
}

func newStreamCapture(writer io.Writer, limit int64, queueDepth int, budget *captureBufferBudget) (*StreamCapture, error) {
	if writer == nil {
		return nil, errors.New("audit capture writer is required")
	}
	if limit <= 0 {
		return nil, errors.New("audit capture byte limit must be positive")
	}
	if queueDepth <= 0 {
		return nil, errors.New("audit capture queue depth must be positive")
	}
	if budget == nil || budget.limit <= 0 {
		return nil, errors.New("audit capture buffer budget is required")
	}
	select {
	case streamCaptureSlots <- struct{}{}:
	default:
		return nil, ErrCaptureWorkerSaturated
	}
	capture := &StreamCapture{
		writer: writer,
		limit:  limit,
		queue:  make(chan []byte, queueDepth),
		done:   make(chan struct{}),
		budget: budget,
	}
	activeStreamCaptures.Add(1)
	go capture.run()
	return capture, nil
}

// WriteCapture is intentionally best effort and has no error return. It never
// performs disk I/O and never waits for queue capacity.
func (c *StreamCapture) WriteCapture(payload []byte) {
	if c == nil || len(payload) == 0 {
		return
	}
	for len(payload) > 0 {
		c.mu.Lock()
		if c.closed || c.disabled {
			c.mu.Unlock()
			return
		}
		remaining := c.limit - c.accepted
		if remaining <= 0 {
			c.truncated = true
			c.disabled = true
			c.mu.Unlock()
			return
		}
		chunkSize := len(payload)
		if chunkSize > captureChunkBytes {
			chunkSize = captureChunkBytes
		}
		if int64(chunkSize) > remaining {
			chunkSize = int(remaining)
			c.truncated = true
		}
		if !c.budget.tryAcquire(int64(chunkSize)) {
			c.disabled = true
			c.truncated = true
			c.incomplete = true
			if c.err == nil {
				c.err = ErrCaptureBackpressure
			}
			c.mu.Unlock()
			return
		}
		chunk := append([]byte(nil), payload[:chunkSize]...)
		select {
		case c.queue <- chunk:
			c.accepted += int64(chunkSize)
			payload = payload[chunkSize:]
			if c.accepted >= c.limit {
				c.truncated = c.truncated || len(payload) > 0
				c.disabled = true
			}
			c.mu.Unlock()
		default:
			c.budget.release(int64(chunkSize))
			c.disabled = true
			c.truncated = true
			c.incomplete = true
			if c.err == nil {
				c.err = ErrCaptureBackpressure
			}
			c.mu.Unlock()
			return
		}
	}
}

// Disable stops accepting bytes without closing the queue. Finish still drains
// already accepted chunks and flushes them before returning.
func (c *StreamCapture) Disable(reason error) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.disabled = true
	c.truncated = true
	c.incomplete = true
	if reason != nil && c.err == nil {
		c.err = reason
	}
	c.mu.Unlock()
}

// Finish is called only after the producer is done (or has been disabled). It
// may wait for disk I/O, so callers run it in the audit finalizer path.
func (c *StreamCapture) Finish() StreamCaptureSnapshot {
	if c == nil {
		return StreamCaptureSnapshot{}
	}
	c.finishOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		close(c.queue)
		c.mu.Unlock()
	})
	<-c.done
	c.mu.Lock()
	defer c.mu.Unlock()
	return StreamCaptureSnapshot{
		Accepted:   c.accepted,
		Written:    c.written,
		Truncated:  c.truncated,
		Incomplete: c.incomplete,
		Err:        c.err,
	}
}

func (c *StreamCapture) run() {
	defer func() {
		activeStreamCaptures.Add(-1)
		<-streamCaptureSlots
		close(c.done)
	}()
	// Absorb short storage-latency spikes in memory and flush larger writes.
	// The queue remains bounded and producers remain strictly non-blocking.
	buffered := bufio.NewWriterSize(c.writer, captureWriterBufferBytes)
	for chunk := range c.queue {
		c.mu.Lock()
		failed := c.err != nil && !errors.Is(c.err, ErrCaptureBackpressure) && !errors.Is(c.err, ErrCaptureProducerTimeout)
		c.mu.Unlock()
		if failed {
			c.budget.release(int64(len(chunk)))
			continue
		}
		n, err := buffered.Write(chunk)
		c.budget.release(int64(len(chunk)))
		c.mu.Lock()
		c.written += int64(n)
		if err != nil || n != len(chunk) {
			if err == nil {
				err = io.ErrShortWrite
			}
			if c.err == nil || errors.Is(c.err, ErrCaptureBackpressure) || errors.Is(c.err, ErrCaptureProducerTimeout) {
				c.err = err
			}
			c.disabled = true
			c.incomplete = true
		}
		c.mu.Unlock()
	}
	if err := buffered.Flush(); err != nil {
		c.mu.Lock()
		if c.err == nil || errors.Is(c.err, ErrCaptureBackpressure) || errors.Is(c.err, ErrCaptureProducerTimeout) {
			c.err = err
		}
		c.disabled = true
		c.incomplete = true
		c.mu.Unlock()
	}
}
