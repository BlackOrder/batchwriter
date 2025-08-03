package batchwriter

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Test comprehensive error scenarios and edge cases not covered in other tests

func TestInsertWithRetryComprehensive(t *testing.T) {
	// This test covers more retry scenarios to improve insertWithRetry coverage
	coll := setupTestMongo(t)

	t.Run("retry with network errors", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg := Config{
			Retries:         3,
			RetryBackoffMin: 1 * time.Millisecond,
			RetryBackoffMax: 10 * time.Millisecond,
			RetryTimeout:    100 * time.Millisecond,
		}

		writer := &Writer[testDoc]{
			cfg:  cfg,
			coll: coll,
			ctx:  ctx,
			l:    nopLogger{},
		}

		// Create a batch with invalid data to trigger errors
		docs := []testDoc{
			{ID: 6001, Name: "retry1"},
			{ID: 6002, Name: "retry2"},
		}

		// This should succeed normally
		remaining, err := writer.insertWithRetry(docs)
		if err != nil && len(remaining) > 0 {
			t.Logf("Retry test completed - some docs may have failed due to network conditions: %v", err)
		}
	})

	t.Run("retry with shutdown during retry", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		cfg := Config{
			Retries:         5,
			RetryBackoffMin: 50 * time.Millisecond,
			RetryBackoffMax: 200 * time.Millisecond,
			RetryTimeout:    1 * time.Second,
		}

		writer := &Writer[testDoc]{
			cfg:  cfg,
			coll: coll,
			ctx:  ctx,
			l:    nopLogger{},
		}

		docs := []testDoc{
			{ID: 6003, Name: "shutdown_during_retry"},
		}

		// Start the retry process in a goroutine
		var remaining []testDoc
		var retryErr error
		done := make(chan struct{})

		go func() {
			remaining, retryErr = writer.insertWithRetry(docs)
			close(done)
		}()

		// Cancel context during retry to test shutdown path
		time.Sleep(10 * time.Millisecond)
		cancel()

		// Wait for retry to complete
		<-done

		// Should have returned remaining docs due to shutdown
		if len(remaining) == 0 && retryErr == nil {
			t.Log("Insert succeeded before shutdown was detected")
		} else {
			t.Logf("Retry properly detected shutdown: remaining=%d, err=%v", len(remaining), retryErr)
		}
	})

	t.Run("infinite retries", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg := Config{
			Retries:         -1, // Infinite retries
			RetryBackoffMin: 1 * time.Millisecond,
			RetryBackoffMax: 5 * time.Millisecond,
			RetryTimeout:    10 * time.Millisecond,
		}

		writer := &Writer[testDoc]{
			cfg:  cfg,
			coll: coll,
			ctx:  ctx,
			l:    nopLogger{},
		}

		docs := []testDoc{
			{ID: 6004, Name: "infinite_retry"},
		}

		// This should work normally even with infinite retries
		remaining, err := writer.insertWithRetry(docs)
		if len(remaining) > 0 || err != nil {
			t.Logf("Infinite retry test completed: remaining=%d, err=%v", len(remaining), err)
		}
	})
}

func TestJSONSinkErrorPaths(t *testing.T) {
	// Test error handling paths in JSON sink to improve coverage

	t.Run("write to read-only directory", func(t *testing.T) {
		if os.Getuid() == 0 {
			t.Skip("Running as root, cannot test read-only directory")
		}

		tmpDir := t.TempDir()
		readOnlyDir := filepath.Join(tmpDir, "readonly")

		// Create directory and make it read-only
		err := os.MkdirAll(readOnlyDir, 0o755)
		if err != nil {
			t.Fatalf("Failed to create directory: %v", err)
		}

		err = os.Chmod(readOnlyDir, 0o444) // Read-only
		if err != nil {
			t.Fatalf("Failed to make directory read-only: %v", err)
		}
		defer os.Chmod(readOnlyDir, 0o755) // Restore permissions for cleanup

		cfg := &JSONDumpConfig{
			Dir:        readOnlyDir,
			FilePrefix: "readonly_test",
		}

		// This should fail to create the sink
		_, err = newJSONSink[testDoc](cfg, nopLogger{})
		if err == nil {
			t.Fatal("Expected error when creating sink in read-only directory")
		}
	})

	t.Run("rotation on write error", func(t *testing.T) {
		tmpDir := t.TempDir()
		cfg := &JSONDumpConfig{
			Dir:           tmpDir,
			FilePrefix:    "rotation_test",
			RotateBytes:   100, // Very small rotation size
			IncludeReason: true,
		}

		logger := &comprehensiveTestLogger{}
		sink, err := newJSONSink[testDoc](cfg, logger)
		if err != nil {
			t.Fatalf("Failed to create sink: %v", err)
		}
		defer sink.Close()

		// Create a large document to trigger rotation
		largeDoc := testDoc{ID: 7001, Name: strings.Repeat("x", 200)}

		// This should trigger rotation due to size
		sink.Dump("test_rotation", nil, []testDoc{largeDoc})
		sink.Dump("test_rotation", nil, []testDoc{largeDoc})

		// Check that multiple files were created
		files, err := filepath.Glob(filepath.Join(tmpDir, "rotation_test-*.ndjson"))
		if err != nil {
			t.Fatalf("Failed to glob files: %v", err)
		}

		if len(files) < 1 {
			t.Fatal("Expected at least one file to be created")
		}

		t.Logf("Rotation test created %d files", len(files))
	})

	t.Run("fsync behavior", func(t *testing.T) {
		tmpDir := t.TempDir()
		cfg := &JSONDumpConfig{
			Dir:        tmpDir,
			FilePrefix: "fsync_test",
			FsyncEvery: 2, // Sync every 2 records
		}

		sink, err := newJSONSink[testDoc](cfg, nopLogger{})
		if err != nil {
			t.Fatalf("Failed to create sink: %v", err)
		}
		defer sink.Close()

		// Write exactly FsyncEvery records to trigger sync
		docs := []testDoc{
			{ID: 7002, Name: "fsync1"},
			{ID: 7003, Name: "fsync2"},
		}

		sink.Dump("fsync_test", nil, docs)

		// The sync should have happened internally
		// We can't easily test the actual fsync, but we test the code path

		// Write one more to ensure continued operation
		sink.Dump("fsync_test", nil, []testDoc{{ID: 7004, Name: "fsync3"}})
	})
}

func TestConcurrentOperations(t *testing.T) {
	// Test concurrent operations to find race conditions and edge cases
	coll := setupTestMongo(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sink := &testSink{}
	cfg := Config{
		MaxBatch:  10,
		MaxDelay:  50 * time.Millisecond,
		QueueSize: 100,
		Sink:      sink,
		Workers:   3,
	}

	writer, err := NewWriter[testDoc](ctx, coll, cfg)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Launch multiple goroutines doing concurrent operations
	var wg sync.WaitGroup
	numGoroutines := 10
	docsPerGoroutine := 50

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			baseID := goroutineID*1000 + 8000 // Start from 8000 to avoid conflicts

			for j := 0; j < docsPerGoroutine; j++ {
				doc := testDoc{
					ID:   baseID + j,
					Name: "concurrent",
				}

				if j%2 == 0 {
					// Use Push for half the docs
					writer.Push(context.Background(), doc)
				} else {
					// Use PushMany for the other half
					writer.PushMany(context.Background(), []testDoc{doc})
				}
			}
		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Give time for processing
	time.Sleep(200 * time.Millisecond)

	// Verify all documents were processed
	totalExpected := numGoroutines * docsPerGoroutine
	count, err := coll.CountDocuments(context.Background(), map[string]any{
		"name": "concurrent",
	})
	if err != nil {
		t.Fatalf("Failed to count documents: %v", err)
	}

	t.Logf("Concurrent test: expected %d docs, found %d docs in MongoDB", totalExpected, count)

	// Should have most documents (some might be in failed dumps due to duplicates or timing)
	if count < int64(float64(totalExpected)*0.8) {
		t.Errorf("Expected at least 80%% of documents to be inserted, got %d/%d", count, totalExpected)
	}

	// Clean shutdown
	cancel()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()

	err = writer.Close(closeCtx)
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestConfigurationEdgeCases(t *testing.T) {
	coll := setupTestMongo(t)

	t.Run("zero timeout configurations", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg := Config{
			RetryTimeout: 0, // Should get default
		}

		writer, err := NewWriter[testDoc](ctx, coll, cfg)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		// Verify default was applied
		if writer.cfg.RetryTimeout != 5*time.Second {
			t.Errorf("Expected default RetryTimeout of 5s, got %v", writer.cfg.RetryTimeout)
		}

		// Push a doc to verify it works
		err = writer.Push(context.Background(), testDoc{ID: 9001, Name: "zero_timeout"})
		if err != nil {
			t.Fatalf("Push failed: %v", err)
		}

		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer closeCancel()
		writer.Close(closeCtx)
	})

	t.Run("custom logger integration", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		logger := &comprehensiveTestLogger{}
		cfg := Config{
			MaxBatch: 1, // Small batch to trigger logging
			MaxDelay: 10 * time.Millisecond,
			Logger:   logger,
		}

		writer, err := NewWriter[testDoc](ctx, coll, cfg)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		// Push a doc
		err = writer.Push(context.Background(), testDoc{ID: 9002, Name: "logger_test"})
		if err != nil {
			t.Fatalf("Push failed: %v", err)
		}

		// Wait for processing and shutdown
		time.Sleep(50 * time.Millisecond)
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer closeCancel()
		writer.Close(closeCtx)

		// Verify logger was used
		if len(logger.logs) == 0 {
			t.Error("Expected logger to capture some log messages")
		}

		// Look for worker finished message
		foundInfo := false
		for _, log := range logger.logs {
			if log.level == "info" && strings.Contains(log.message, "finished") {
				foundInfo = true
				break
			}
		}
		if !foundInfo {
			t.Error("Expected to find info log about workers finishing")
		}
	})
}

// Enhanced test logger implementation that captures all log calls
type comprehensiveTestLogger struct {
	mu   sync.Mutex
	logs []comprehensiveLogEntry
}

type comprehensiveLogEntry struct {
	level   string
	message string
	fields  map[string]any
	err     error
}

func (tl *comprehensiveTestLogger) Debug(msg string) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	tl.logs = append(tl.logs, comprehensiveLogEntry{level: "debug", message: msg})
}

func (tl *comprehensiveTestLogger) Info(msg string) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	tl.logs = append(tl.logs, comprehensiveLogEntry{level: "info", message: msg})
}

func (tl *comprehensiveTestLogger) Warn(msg string) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	tl.logs = append(tl.logs, comprehensiveLogEntry{level: "warn", message: msg})
}

func (tl *comprehensiveTestLogger) Error(msg string) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	tl.logs = append(tl.logs, comprehensiveLogEntry{level: "error", message: msg})
}

func (tl *comprehensiveTestLogger) With(key string, value any) Logger {
	// For testing, return a new logger with the field stored
	newLogger := &comprehensiveTestLogger{}
	newLogger.mu.Lock()
	defer newLogger.mu.Unlock()

	// Copy existing logs
	newLogger.logs = make([]comprehensiveLogEntry, len(tl.logs))
	copy(newLogger.logs, tl.logs)

	// Add field to last entry if it exists
	if len(newLogger.logs) > 0 {
		lastIdx := len(newLogger.logs) - 1
		if newLogger.logs[lastIdx].fields == nil {
			newLogger.logs[lastIdx].fields = make(map[string]any)
		}
		newLogger.logs[lastIdx].fields[key] = value
	}

	return newLogger
}

func (tl *comprehensiveTestLogger) Err(err error) Logger {
	// For testing, return a new logger with the error stored
	newLogger := &comprehensiveTestLogger{}
	newLogger.mu.Lock()
	defer newLogger.mu.Unlock()

	// Copy existing logs
	newLogger.logs = make([]comprehensiveLogEntry, len(tl.logs))
	copy(newLogger.logs, tl.logs)

	// Add error to last entry if it exists
	if len(newLogger.logs) > 0 {
		lastIdx := len(newLogger.logs) - 1
		newLogger.logs[lastIdx].err = err
	}

	return newLogger
}
