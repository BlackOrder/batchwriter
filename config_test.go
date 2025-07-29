package batchwriter

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// Shared test types used across multiple test files

// testDoc represents a test document for MongoDB operations
type testDoc struct {
	ID   int    `bson:"_id"`
	Name string `bson:"name"`
}

// testSink is a thread-safe test implementation of the Sink interface
type testSink struct {
	mu    sync.Mutex
	dumps []dumpCall
}

// dumpCall represents a call to the Dump method
type dumpCall struct {
	reason string
	cause  error
	batch  []any
}

func (ts *testSink) Dump(reason string, cause error, batch []any) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.dumps = append(ts.dumps, dumpCall{reason, cause, batch})
}

func (ts *testSink) Close() error {
	return nil
}

func (ts *testSink) getDumps() []dumpCall {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	// Return a copy to avoid race conditions
	result := make([]dumpCall, len(ts.dumps))
	copy(result, ts.dumps)
	return result
}

func TestNewWriter(t *testing.T) {
	coll := setupTestMongo(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("with defaults", func(t *testing.T) {
		cfg := Config{}
		writer, err := NewWriter[testDoc](ctx, coll, cfg)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		// Check defaults were applied
		if writer.cfg.MaxBatch != 500 {
			t.Errorf("Expected default MaxBatch 500, got %d", writer.cfg.MaxBatch)
		}
		if writer.cfg.MaxDelay != 100*time.Millisecond {
			t.Errorf("Expected default MaxDelay 100ms, got %v", writer.cfg.MaxDelay)
		}
		if writer.cfg.QueueSize != 100_000 {
			t.Errorf("Expected default QueueSize 100000, got %d", writer.cfg.QueueSize)
		}
		if writer.cfg.Workers != 1 {
			t.Errorf("Expected default Workers 1, got %d", writer.cfg.Workers)
		}
	})

	t.Run("with custom config", func(t *testing.T) {
		cfg := Config{
			MaxBatch:        100,
			MaxDelay:        50 * time.Millisecond,
			QueueSize:       1000,
			Workers:         2,
			Retries:         3,
			RetryBackoffMin: 10 * time.Millisecond,
			RetryBackoffMax: 1 * time.Second,
			RetryTimeout:    2 * time.Second,
		}

		writer, err := NewWriter[testDoc](ctx, coll, cfg)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		// Check custom values were preserved
		if writer.cfg.MaxBatch != 100 {
			t.Errorf("Expected MaxBatch 100, got %d", writer.cfg.MaxBatch)
		}
		if writer.cfg.MaxDelay != 50*time.Millisecond {
			t.Errorf("Expected MaxDelay 50ms, got %v", writer.cfg.MaxDelay)
		}
		if writer.cfg.QueueSize != 1000 {
			t.Errorf("Expected QueueSize 1000, got %d", writer.cfg.QueueSize)
		}
		if writer.cfg.Workers != 2 {
			t.Errorf("Expected Workers 2, got %d", writer.cfg.Workers)
		}
	})

	t.Run("with custom sink", func(t *testing.T) {
		sink := &testSink{}
		cfg := Config{
			Sink: sink,
		}

		writer, err := NewWriter[testDoc](ctx, coll, cfg)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		if writer.sink == nil {
			t.Error("Expected sink to be set")
		}
	})

	t.Run("with JSON dump config", func(t *testing.T) {
		tmpDir := t.TempDir()
		cfg := Config{
			JSONDump: &JSONDumpConfig{
				Dir:           tmpDir,
				FilePrefix:    "test",
				FsyncEvery:    100,
				RotateBytes:   1024 * 1024,
				IncludeReason: true,
			},
		}

		writer, err := NewWriter[testDoc](ctx, coll, cfg)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		if writer.sink == nil {
			t.Error("Expected JSON sink to be created")
		}
	})

	t.Run("with logger", func(t *testing.T) {
		logger := &testLogger{}
		cfg := Config{
			Logger: logger,
		}

		writer, err := NewWriter[testDoc](ctx, coll, cfg)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		if writer.l != logger {
			t.Error("Expected custom logger to be set")
		}
	})
}

func TestConfigDefaults(t *testing.T) {
	tests := []struct {
		name     string
		input    Config
		expected Config
	}{
		{
			name:  "all zeros",
			input: Config{},
			expected: Config{
				MaxBatch:        500,
				MaxDelay:        100 * time.Millisecond,
				QueueSize:       100_000,
				Workers:         1,
				RetryBackoffMin: 100 * time.Millisecond,
				RetryBackoffMax: 5 * time.Second,
				RetryTimeout:    5 * time.Second,
			},
		},
		{
			name: "partial config",
			input: Config{
				MaxBatch:  200,
				QueueSize: 50_000,
			},
			expected: Config{
				MaxBatch:        200, // preserved
				MaxDelay:        100 * time.Millisecond,
				QueueSize:       50_000, // preserved
				Workers:         1,
				RetryBackoffMin: 100 * time.Millisecond,
				RetryBackoffMax: 5 * time.Second,
				RetryTimeout:    5 * time.Second,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coll := setupTestMongo(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			writer, err := NewWriter[testDoc](ctx, coll, tt.input)
			if err != nil {
				t.Fatalf("Failed to create writer: %v", err)
			}

			if writer.cfg.MaxBatch != tt.expected.MaxBatch {
				t.Errorf("MaxBatch: expected %d, got %d", tt.expected.MaxBatch, writer.cfg.MaxBatch)
			}
			if writer.cfg.MaxDelay != tt.expected.MaxDelay {
				t.Errorf("MaxDelay: expected %v, got %v", tt.expected.MaxDelay, writer.cfg.MaxDelay)
			}
			if writer.cfg.QueueSize != tt.expected.QueueSize {
				t.Errorf("QueueSize: expected %d, got %d", tt.expected.QueueSize, writer.cfg.QueueSize)
			}
			if writer.cfg.Workers != tt.expected.Workers {
				t.Errorf("Workers: expected %d, got %d", tt.expected.Workers, writer.cfg.Workers)
			}
			if writer.cfg.RetryBackoffMin != tt.expected.RetryBackoffMin {
				t.Errorf("RetryBackoffMin: expected %v, got %v", tt.expected.RetryBackoffMin, writer.cfg.RetryBackoffMin)
			}
			if writer.cfg.RetryBackoffMax != tt.expected.RetryBackoffMax {
				t.Errorf("RetryBackoffMax: expected %v, got %v", tt.expected.RetryBackoffMax, writer.cfg.RetryBackoffMax)
			}
			if writer.cfg.RetryTimeout != tt.expected.RetryTimeout {
				t.Errorf("RetryTimeout: expected %v, got %v", tt.expected.RetryTimeout, writer.cfg.RetryTimeout)
			}
		})
	}
}

// Test logger implementation for testing
type testLogger struct {
	logs []logEntry
}

type logEntry struct {
	level   string
	message string
	fields  map[string]any
	err     error
}

func (tl *testLogger) Debug(msg string) {
	tl.logs = append(tl.logs, logEntry{level: "debug", message: msg})
}

func (tl *testLogger) Info(msg string) {
	tl.logs = append(tl.logs, logEntry{level: "info", message: msg})
}

func (tl *testLogger) Warn(msg string) {
	tl.logs = append(tl.logs, logEntry{level: "warn", message: msg})
}

func (tl *testLogger) Error(msg string) {
	tl.logs = append(tl.logs, logEntry{level: "error", message: msg})
}

func (tl *testLogger) With(key string, value any) Logger {
	newLogger := &testLogger{logs: make([]logEntry, len(tl.logs))}
	copy(newLogger.logs, tl.logs)
	// For simplicity, just track the last field
	if len(newLogger.logs) > 0 {
		lastEntry := &newLogger.logs[len(newLogger.logs)-1]
		if lastEntry.fields == nil {
			lastEntry.fields = make(map[string]any)
		}
		lastEntry.fields[key] = value
	}
	return newLogger
}

func (tl *testLogger) Err(err error) Logger {
	newLogger := &testLogger{logs: make([]logEntry, len(tl.logs))}
	copy(newLogger.logs, tl.logs)
	if len(newLogger.logs) > 0 {
		newLogger.logs[len(newLogger.logs)-1].err = err
	}
	return newLogger
}

func TestTypedAdapter(t *testing.T) {
	sink := &testSink{}
	adapter := typedAdapter[testDoc]{sink}

	docs := []testDoc{
		{ID: 1, Name: "test1"},
		{ID: 2, Name: "test2"},
	}

	adapter.Dump("test_reason", nil, docs)

	dumps := sink.getDumps()
	if len(dumps) != 1 {
		t.Fatalf("Expected 1 dump call, got %d", len(dumps))
	}

	dump := dumps[0]
	if dump.reason != "test_reason" {
		t.Fatalf("Expected reason 'test_reason', got %s", dump.reason)
	}

	if len(dump.batch) != 2 {
		t.Fatalf("Expected 2 items in batch, got %d", len(dump.batch))
	}

	// Check type conversion
	for i, item := range dump.batch {
		doc, ok := item.(testDoc)
		if !ok {
			t.Fatalf("Expected testDoc, got %T", item)
		}
		if doc.ID != docs[i].ID {
			t.Fatalf("Expected ID %d, got %d", docs[i].ID, doc.ID)
		}
	}
}

func TestNopLogger(t *testing.T) {
	logger := nopLogger{}

	// These should not panic
	logger.Debug("test")
	logger.Info("test")
	logger.Warn("test")
	logger.Error("test")

	logger2 := logger.With("key", "value")
	if logger2 != logger {
		t.Error("Expected nopLogger.With to return same instance")
	}

	logger3 := logger.Err(errors.New("test"))
	if logger3 != logger {
		t.Error("Expected nopLogger.Err to return same instance")
	}
}

func TestMultipleWorkers(t *testing.T) {
	coll := setupTestMongo(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := Config{
		MaxBatch:  5,
		MaxDelay:  100 * time.Millisecond,
		QueueSize: 100,
		Workers:   3, // Multiple workers
	}

	writer, err := NewWriter[testDoc](ctx, coll, cfg)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Push many documents quickly
	docs := make([]testDoc, 50)
	for i := range docs {
		docs[i] = testDoc{ID: 1000 + i, Name: "concurrent"}
	}

	err = writer.PushMany(context.Background(), docs)
	if err != nil {
		t.Fatalf("PushMany failed: %v", err)
	}

	// Wait for processing
	time.Sleep(300 * time.Millisecond)

	// Verify all documents were inserted
	count, err := coll.CountDocuments(context.Background(), map[string]any{"name": "concurrent"})
	if err != nil {
		t.Fatalf("Failed to count documents: %v", err)
	}
	if count != 50 {
		t.Fatalf("Expected 50 documents, got %d", count)
	}
}
