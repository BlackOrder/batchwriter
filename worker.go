package batchwriter

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo/options"
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
		_ = w.insertWithRetry(batch)
		batch = batch[:0]
	}

	for {
		select {
		case <-w.stopC:
			// drain remaining items
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
					<-timer.C
				}
				timer.Reset(w.cfg.MaxDelay)
			}

		case <-timer.C:
			flush()
			timer.Reset(w.cfg.MaxDelay)
		}
	}
}

func (w *Writer[T]) insertWithRetry(docs []T) error {
	var err error
	backoff := w.cfg.RetryBackoffMin
	for attempt := 0; attempt <= w.cfg.Retries || w.cfg.Retries < 0; attempt++ {
		_, err = w.coll.InsertMany(context.Background(), docs, options.InsertMany().SetOrdered(false))
		if err == nil {
			return nil
		}
		// classify error: if not transient, break early
		// (left simple—use mongo.IsNetworkError, write concern, etc., in prod)
		time.Sleep(jitter(backoff, w.cfg.RetryBackoffMax))
		backoff *= 2
		if backoff > w.cfg.RetryBackoffMax {
			backoff = w.cfg.RetryBackoffMax
		}
	}
	// log it; optionally push to a dead-letter store
	return err
}

func jitter(d time.Duration, max time.Duration) time.Duration {
	// very small jitter; plug in math/rand if you want
	if d > max {
		return max
	}
	return d + time.Duration(int64(d)/10)
}
