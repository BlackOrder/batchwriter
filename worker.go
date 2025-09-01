package batchwriter

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MongoDB error codes
const (
	DuplicateKey           = 11000
	DuplicateKeyLegacy     = 11001
	DuplicateKeyUpdateConf = 12582
)

func (w *Writer[T]) worker() {
	defer w.wg.Done()

	batch := make([]T, 0, w.cfg.MaxBatch)
	timer := time.NewTimer(w.cfg.MaxDelay)
	defer timer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		remaining, lastErr := w.insertWithRetry(batch)
		if len(remaining) > 0 {
			w.dumpFailed(remaining, "insert_failed", lastErr)
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-w.ctx.Done():
			// drain channel without blocking, flush once
			for {
				select {
				case evt := <-w.ch:
					batch = append(batch, evt)
					if len(batch) >= w.cfg.MaxBatch {
						flush()
					}
				default:
					flush()
					return
				}
			}

		case evt := <-w.ch:
			batch = append(batch, evt)
			if len(batch) >= w.cfg.MaxBatch {
				flush()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(w.cfg.MaxDelay)
			}

		case <-timer.C:
			flush()
			timer.Reset(w.cfg.MaxDelay)
		}
	}
}

func (w *Writer[T]) insertWithRetry(docs []T) ([]T, error) {
	remaining := docs
	backoff := w.cfg.RetryBackoffMin
	var lastErr error

	// Apply reasonable limits to prevent infinite retry loops
	maxRetries := w.cfg.Retries
	if maxRetries < 0 {
		maxRetries = 1000 // Reasonable upper bound for "unlimited" retries
	}
	
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if len(remaining) == 0 {
			return nil, nil
		}
		// Use writer shutdown context as parent, so attempts abort promptly on shutdown.
		ctx, cancel := context.WithTimeout(w.ctx, w.cfg.RetryTimeout)
		_, err := w.coll.InsertMany(ctx, remaining, options.InsertMany().SetOrdered(false))
		cancel() // cancel the context to avoid leaks
		if err == nil {
			return nil, nil
		}
		lastErr = err

		// If we're shutting down, stop retrying and return what remains to be dumped.
		select {
		case <-w.ctx.Done():
			return remaining, lastErr
		default:
		}

		var bwe mongo.BulkWriteException
		if errors.As(err, &bwe) {
			drop := make(map[int]struct{})
			for _, e := range bwe.WriteErrors {
				switch e.Code {
				case DuplicateKey, DuplicateKeyLegacy, DuplicateKeyUpdateConf:
					drop[e.Index] = struct{}{}
				}
			}
			remaining = filterOutByIndices(remaining, drop)

			// Retry on network/timeout or write-concern errors; otherwise give back the remainder
			retryable := mongo.IsNetworkError(err) || mongo.IsTimeout(err) || (bwe.WriteConcernError != nil)
			if !retryable {
				return remaining, lastErr
			}
		} else {
			if mongo.IsDuplicateKeyError(err) {
				return nil, nil
			}
			if !(mongo.IsNetworkError(err) || mongo.IsTimeout(err)) {
				return remaining, lastErr
			}
		}

		// Use context-aware sleep to respect shutdown signals
		select {
		case <-time.After(jitter(backoff, w.cfg.RetryBackoffMax)):
			// backoff completed normally
		case <-w.ctx.Done():
			// shutdown requested during backoff
			return remaining, w.ctx.Err()
		}
		
		backoff *= 2
		if backoff > w.cfg.RetryBackoffMax {
			backoff = w.cfg.RetryBackoffMax
		}
	}
	return remaining, lastErr
}

func filterOutByIndices[T any](in []T, drop map[int]struct{}) []T {
	if len(drop) == 0 {
		return in
	}
	out := make([]T, 0, len(in)-len(drop))
	for i, v := range in {
		if _, ok := drop[i]; !ok {
			out = append(out, v)
		}
	}
	return out
}

// jitter returns a duration that is randomly adjusted by up to ±25%
// while respecting the provided maximum
func jitter(d, maxDuration time.Duration) time.Duration {
	// Cap at maximum if needed
	if d > maxDuration {
		d = maxDuration
	}

	// Generate a random factor between 0.75 and 1.25
	// #nosec G404 - math/rand is sufficient for jitter timing, crypto/rand not needed
	factor := 0.75 + rand.Float64()*0.5

	// Apply the factor and ensure we don't exceed max
	result := time.Duration(float64(d) * factor)
	if result > maxDuration {
		return maxDuration
	}
	return result
}
