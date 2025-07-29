package batchwriter

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

type Logger interface {
	Debug(msg string)
	Info(msg string)
	Warn(msg string)
	Error(msg string)
	With(key string, value any) Logger
	Err(err error) Logger
}

type nopLogger struct{}

func (n nopLogger) Debug(msg string)                  {}
func (n nopLogger) Info(msg string)                   {}
func (n nopLogger) Warn(msg string)                   {}
func (n nopLogger) Error(msg string)                  {}
func (n nopLogger) With(key string, value any) Logger { return n }
func (n nopLogger) Err(err error) Logger              { return n }

type JSONDumpConfig struct {
	Dir           string // required
	FilePrefix    string // e.g. "batchwriter"
	FsyncEvery    int    // 0=never; e.g. 200 flushes then Sync()
	RotateBytes   int64  // 0=disable rotation; else create new file when exceeded
	IncludeReason bool   // include _reason and _error fields in JSON
}

type Config struct {
	MaxBatch        int           // e.g. 500
	MaxDelay        time.Duration // e.g. 100 * time.Millisecond
	QueueSize       int           // e.g. 100_000
	Retries         int           // e.g. 5
	RetryBackoffMin time.Duration // e.g. 50 * time.Millisecond
	RetryBackoffMax time.Duration // e.g. 2 * time.Second
	RetryTimeout    time.Duration // e.g. 5 * time.Second
	Workers         int           // e.g. 2-4

	// Provide either a custom sink OR configure the built-in JSON sink.
	Sink     FailureSink[any] // if set, used as-is (preferred for custom formats)
	JSONDump *JSONDumpConfig  // if Sink is nil and JSONDump != nil, enable built-in JSON dumping

	Logger Logger // optional; if nil, no logging
}

type Writer[T any] struct {
	cfg  Config
	coll *mongo.Collection
	ch   chan T
	wg   sync.WaitGroup
	// ctx is the shutdown context passed to NewWriter.
	ctx context.Context

	sink   FailureSink[T] // nil => dumping disabled
	sinkMu sync.Mutex     // protects concurrent sink access
	l      Logger         // nil => no logging; set by NewWriter
}

// Contract: cancel shutdownCtx to stop workers; after cancel, call Close(waitCtx)
// to wait for drain. If waitCtx times out, remaining items are dumped and Close returns waitCtx.Err().
// NewWriter starts the background workers immediately.
func NewWriter[T any](shutdownCtx context.Context, coll *mongo.Collection, cfg Config) (*Writer[T], error) {
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = 500
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 100 * time.Millisecond
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 100_000
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.RetryBackoffMin <= 0 {
		cfg.RetryBackoffMin = 100 * time.Millisecond
	}
	if cfg.RetryBackoffMax <= 0 {
		cfg.RetryBackoffMax = 5 * time.Second
	}
	if cfg.RetryTimeout <= 0 {
		cfg.RetryTimeout = 5 * time.Second
	}

	w := &Writer[T]{
		cfg:  cfg,
		coll: coll,
		ch:   make(chan T, cfg.QueueSize),
		ctx:  shutdownCtx,
	}

	// Set up logger
	if cfg.Logger != nil {
		w.l = cfg.Logger
	} else {
		w.l = nopLogger{} // no-op logger
	}

	// Choose sink: prefer user-provided sink; else built-in JSON if configured.
	if cfg.Sink != nil {
		// If you kept Sink as FailureSink[any] in Config, adapt with a thin wrapper:
		w.sink = typedAdapter[T]{cfg.Sink}
	} else if cfg.JSONDump != nil {
		js, err := newJSONSink[T](cfg.JSONDump, w.l)
		if err != nil {
			return nil, err
		}
		w.sink = js
	}

	for i := 0; i < w.cfg.Workers; i++ {
		w.wg.Add(1)
		go w.worker()
	}
	return w, nil
}
