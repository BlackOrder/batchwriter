package batchwriter

import (
	"context"
)

// Push enqueues one event. It blocks when the queue is full.
// If the writer's shutdown context or the call context is canceled,
// the item is dumped and the corresponding error is returned.
func (w *Writer[T]) Push(ctx context.Context, evt T) error {
	// Fast path: try non-blocking enqueue. This allows enqueueing even if
	// shutdown context is already canceled, as long as there's still buffer space.
	select {
	case w.ch <- evt:
		return nil
	default:
		// would block, fall through to blocking select with cancellation awareness
	}

	select {
	case w.ch <- evt:
		return nil
	case <-w.ctx.Done():
		w.dumpFailed([]T{evt}, "enqueue_after_shutdown", w.ctx.Err())
		return w.ctx.Err()
	case <-ctx.Done():
		w.dumpFailed([]T{evt}, "enqueue_timeout", ctx.Err())
		return ctx.Err()
	}
}

// PushMany enqueues multiple events with backpressure. It blocks when the queue is full.
// If either the writer's shutdown context or the call context is canceled,
// the *remaining* items are dumped and the function returns the relevant error.
func (w *Writer[T]) PushMany(ctx context.Context, evts []T) error {
	for i := range evts {
		select {
		case w.ch <- evts[i]:
			// sent
		default:
			// queue is full: block until we can send OR either context cancels
			select {
			case w.ch <- evts[i]:
				// sent after waiting; continue
			case <-w.ctx.Done():
				w.dumpFailed(evts[i:], "enqueue_after_shutdown", w.ctx.Err())
				return w.ctx.Err()
			case <-ctx.Done():
				w.dumpFailed(evts[i:], "enqueue_timeout", ctx.Err())
				return ctx.Err()
			}
		}
	}
	return nil
}

func (w *Writer[T]) Close(ctx context.Context) error {
	// Wait for workers to exit (they stop when shutdown ctx is canceled).
	done := make(chan struct{})
	go func() { w.wg.Wait(); close(done) }()

	select {
	case <-done:
		w.l.Info("All workers finished, closing writer")
		w.sinkMu.Lock()
		if w.sink != nil {
			if err := w.sink.Close(); err != nil {
				w.l.Err(err).Error("Failed to close sink")
			}
		}
		w.sinkMu.Unlock()
		return nil
	case <-ctx.Done():
		w.l.With("reason", "shutdown").Warn("Writer shutdown context canceled, draining remaining items")
		// Force drain whatever is still in the channel and dump as "shutdown"
		var rem []T
		for {
			select {
			case evt := <-w.ch:
				rem = append(rem, evt)
			default:
				if len(rem) > 0 {
					w.dumpFailed(rem, "shutdown", ctx.Err())
				}
				w.l.Info("Writer shutdown complete")
				w.sinkMu.Lock()
				if w.sink != nil {
					if err := w.sink.Close(); err != nil {
						w.l.Err(err).Error("Failed to close sink")
					}
				}
				w.sinkMu.Unlock()
				return ctx.Err()
			}
		}
	}
}

// HealthStatus provides operational metrics for monitoring and health checks
type HealthStatus struct {
	QueueDepth     int  `json:"queue_depth"`     // Current items in queue
	QueueCapacity  int  `json:"queue_capacity"`  // Maximum queue capacity
	QueueUtilPct   int  `json:"queue_util_pct"`  // Queue utilization percentage
	Workers        int  `json:"workers"`         // Number of active workers
	IsShuttingDown bool `json:"is_shutting_down"` // Whether shutdown has been initiated
}

// Health returns current operational status for monitoring and health checks.
// This method is safe to call concurrently and provides non-blocking health information.
func (w *Writer[T]) Health() HealthStatus {
	queueDepth := len(w.ch)
	queueCapacity := cap(w.ch)
	utilizationPct := 0
	if queueCapacity > 0 {
		utilizationPct = (queueDepth * 100) / queueCapacity
	}
	
	// Check if shutdown context is done
	isShuttingDown := false
	select {
	case <-w.ctx.Done():
		isShuttingDown = true
	default:
	}
	
	return HealthStatus{
		QueueDepth:     queueDepth,
		QueueCapacity:  queueCapacity,
		QueueUtilPct:   utilizationPct,
		Workers:        w.cfg.Workers,
		IsShuttingDown: isShuttingDown,
	}
}

func (w *Writer[T]) dumpFailed(batch []T, reason string, cause error) {
	if len(batch) == 0 {
		return
	}

	w.sinkMu.Lock()
	defer w.sinkMu.Unlock()

	if w.sink != nil {
		w.l.With("reason", reason).With("count", len(batch)).Warn("Dumping failed batch")
		w.sink.Dump(reason, cause, batch)
	}
}
