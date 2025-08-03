package batchwriter

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// FailureSink receives items that could not be persisted (e.g., after retries)
// or that remained in the queue at shutdown. Implementations must be thread-safe.
type FailureSink[T any] interface {
	// Dump must be non-blocking or fast; it's called in hot paths.
	// 'reason' examples: "insert_failed", "enqueue_after_shutdown", "shutdown".
	Dump(reason string, cause error, batch []T)
	Close() error
}

// If you chose Config.Sink as FailureSink[any], use this adapter:
type typedAdapter[T any] struct{ s FailureSink[any] }

func (a typedAdapter[T]) Dump(reason string, cause error, batch []T) {
	// unsafe cast to []any copy to avoid aliasing; cheap for failure volumes
	items := make([]any, len(batch))
	for i, v := range batch {
		items[i] = v
	}
	a.s.Dump(reason, cause, items)
}
func (a typedAdapter[T]) Close() error { return a.s.Close() }

type jsonSink[T any] struct {
	mu         sync.Mutex
	cfg        JSONDumpConfig
	f          *os.File
	w          *bufio.Writer
	bytesWrote int64
	flushCount int
	l          Logger
}

func newJSONSink[T any](cfg *JSONDumpConfig, logger Logger) (*jsonSink[T], error) {
	if cfg == nil || cfg.Dir == "" {
		return nil, nil // disabled
	}

	// Validate directory path to prevent path traversal attacks
	cleanDir := filepath.Clean(cfg.Dir)
	if strings.Contains(cleanDir, "..") {
		return nil, fmt.Errorf("invalid directory path: %s", cfg.Dir)
	}
	cfg.Dir = cleanDir

	if cfg.FilePrefix == "" {
		cfg.FilePrefix = "batchwriter"
	}
	// Validate file prefix to prevent path traversal
	if strings.Contains(cfg.FilePrefix, "/") || strings.Contains(cfg.FilePrefix, "\\") {
		return nil, fmt.Errorf("invalid file prefix: %s", cfg.FilePrefix)
	}
	if cfg.FsyncEvery <= 0 {
		cfg.FsyncEvery = 200 // flush every 200 records
	}
	if cfg.RotateBytes <= 0 {
		cfg.RotateBytes = 0 // no rotation by default
	} else if cfg.RotateBytes < 1<<20 { // minimum 1 MiB
		cfg.RotateBytes = 1 << 20 // enforce minimum size
	}
	if logger == nil {
		logger = nopLogger{} // no-op logger
	}
	if err := os.MkdirAll(cfg.Dir, 0o750); err != nil {
		return nil, err
	}
	js := &jsonSink[T]{cfg: *cfg, l: logger}
	if err := js.rotateLocked(); err != nil {
		return nil, err
	}
	return js, nil
}

func (js *jsonSink[T]) rotateLocked() error {
	if js.f != nil {
		if err := js.w.Flush(); err != nil {
			js.l.Err(err).Error("Failed to flush JSON sink")
		}
		if err := js.f.Sync(); err != nil {
			js.l.Err(err).Error("Failed to sync JSON sink")
		}
		if err := js.f.Close(); err != nil {
			js.l.Err(err).Error("Failed to close JSON sink")
		}
	}
	name := time.Now().UTC().Format("20060102-150405")
	path := filepath.Join(js.cfg.Dir, fmt.Sprintf("%s-%s.ndjson", js.cfg.FilePrefix, name))
	// #nosec G304 - path is constructed from validated config (no path traversal) and timestamp
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	js.f = f
	js.w = bufio.NewWriterSize(f, 1<<20)
	js.bytesWrote = 0
	js.flushCount = 0

	// New: visibility for ops
	js.l.With("file", path).Info("JSON sink rotated")
	return nil
}

func (js *jsonSink[T]) Dump(reason string, cause error, batch []T) {
	if js == nil || len(batch) == 0 {
		return
	}
	js.mu.Lock()
	defer js.mu.Unlock()

	var errStr string
	if cause != nil {
		errStr = cause.Error()
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)

	for _, v := range batch {
		// Envelope keeps metadata separate from the user payload.
		rec := map[string]any{
			"_ts":  now,
			"_doc": v,
		}
		if js.cfg.IncludeReason {
			rec["_reason"] = reason
			if errStr != "" {
				rec["_error"] = errStr
			}
		}
		b, err := json.Marshal(rec)
		if err != nil {
			js.l.With("rec", rec).Error("Failed to marshal JSON record, skipping")
			continue // skip this record, but keep going
		}
		n, err := js.w.Write(append(b, '\n'))
		if err != nil {
			js.l.With("rec", rec).Err(err).Error("Failed to write JSON record, skipping")
			continue
		}
		js.bytesWrote += int64(n)
		js.flushCount++

		if js.cfg.FsyncEvery > 0 && js.flushCount >= js.cfg.FsyncEvery {
			if err := js.w.Flush(); err != nil {
				js.l.With("count", js.flushCount).Err(err).Error("Failed to flush JSON sink")
			}
			if err := js.f.Sync(); err != nil {
				js.l.With("count", js.flushCount).Err(err).Error("Failed to sync JSON sink")
			}
			js.flushCount = 0
		}
		if js.cfg.RotateBytes > 0 && js.bytesWrote >= js.cfg.RotateBytes {
			if err := js.w.Flush(); err != nil {
				js.l.With("bytes", js.bytesWrote).Err(err).Error("Failed to flush JSON sink")
			}
			if err := js.f.Sync(); err != nil {
				js.l.With("bytes", js.bytesWrote).Err(err).Error("Failed to sync JSON sink")
			}
			if err := js.rotateLocked(); err != nil {
				js.l.With("bytes", js.bytesWrote).Err(err).Error("Failed to rotate JSON sink")
			}
		}
	}
}

func (js *jsonSink[T]) Close() error {
	if js == nil {
		return nil
	}
	js.mu.Lock()
	defer js.mu.Unlock()
	if js.w != nil {
		if err := js.w.Flush(); err != nil {
			js.l.Err(err).Error("Failed to flush JSON sink on close")
		}
	}
	if js.f != nil {
		if err := js.f.Sync(); err != nil {
			js.l.Err(err).Error("Failed to sync JSON sink on close")
		}
		return js.f.Close()
	}
	return nil
}
