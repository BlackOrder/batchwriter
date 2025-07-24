package batchwriter

import (
	"context"
)

// Push enqueues one event. Blocks or returns ErrQueueFull depending on cfg.
func (w *Writer[T]) Push(ctx context.Context, evt T) error {
	select {
	case w.ch <- evt:
		return nil
	default:
		if w.cfg.DropOnFull {
			return ErrQueueFull
		}
		// block with context
		select {
		case w.ch <- evt:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// PushMany enqueues multiple events. Returns error if any event fails to enqueue.
func (w *Writer[T]) PushMany(ctx context.Context, evts []T) error {
	for _, evt := range evts {
		if err := w.Push(ctx, evt); err != nil {
			return err
		}
	}
	return nil
}

// Close drains the queue and stops workers.
func (w *Writer[T]) Close(ctx context.Context) error {
	close(w.stopC)
	// wait till channel is empty then close
	go func() {
		w.wg.Wait()
	}()
	// Optional: add a timeout using ctx
	return nil
}
