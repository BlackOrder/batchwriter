package batchwriter

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// Test the production readiness fixes
func TestProductionReadinessFixes(t *testing.T) {

	t.Run("context aware backoff respects shutdown", func(t *testing.T) {
		coll := setupTestMongo(t)
		ctx, cancel := context.WithCancel(context.Background())

		cfg := Config{
			MaxBatch:        10,
			MaxDelay:        1 * time.Hour,  // Long delay to avoid timer flushes
			QueueSize:       100,
			Retries:         5,
			RetryBackoffMin: 1 * time.Second,   // Long backoff to test cancellation
			RetryBackoffMax: 10 * time.Second,
			RetryTimeout:    100 * time.Millisecond, // Short timeout to force retries
			Workers:         1,
		}

		writer, err := NewWriter[testDoc](ctx, coll, cfg)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		// Create documents that will likely timeout and trigger retries
		docs := []testDoc{{ID: 90001, Name: "backoff_test"}}

		// Start the insert in a goroutine
		done := make(chan struct{})
		go func() {
			defer close(done)
			// This should trigger retry attempts with backoff
			writer.insertWithRetry(docs)
		}()

		// Let it start retrying
		time.Sleep(100 * time.Millisecond)

		// Cancel context during backoff period
		cancel()

		// Should exit quickly due to context cancellation during backoff
		select {
		case <-done:
			// Good - function returned quickly due to context cancellation
		case <-time.After(2 * time.Second):
			t.Error("insertWithRetry did not respect context cancellation during backoff")
		}
	})

	t.Run("unlimited retries are properly bounded", func(t *testing.T) {
		coll := setupTestMongo(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg := Config{
			MaxBatch:        10,
			MaxDelay:        1 * time.Hour,
			QueueSize:       100,
			Retries:         -1, // "Unlimited" retries
			RetryBackoffMin: 1 * time.Millisecond, // Fast backoff for testing
			RetryBackoffMax: 5 * time.Millisecond,
			RetryTimeout:    1 * time.Millisecond, // Very short timeout to force failures
			Workers:         1,
		}

		writer := &Writer[testDoc]{
			cfg:  cfg,
			coll: coll,
			ctx:  ctx,
			l:    nopLogger{},
		}

		docs := []testDoc{{ID: 90002, Name: "unlimited_retry_test"}}

		start := time.Now()

		// This should not run forever, should be bounded at 1000 attempts
		remaining, err := writer.insertWithRetry(docs)

		elapsed := time.Since(start)

		// Should not take too long even with 1000 attempts due to short backoff
		if elapsed > 30*time.Second {
			t.Errorf("Unlimited retries took too long: %v", elapsed)
		}

		// Should eventually give up
		if len(remaining) == 0 && err == nil {
			t.Error("Expected unlimited retries to eventually fail and return remaining docs")
		}

		t.Logf("Unlimited retry test completed in %v with %d remaining docs", elapsed, len(remaining))
	})

	t.Run("memory based batching configuration", func(t *testing.T) {
		// Test that the new MaxBatchBytes field is available and configurable
		cfg := Config{
			MaxBatch:      100,
			MaxBatchBytes: 1024 * 1024, // 1MB
			MaxDelay:      100 * time.Millisecond,
			QueueSize:     1000,
		}

		// Should be able to create a writer with the new field
		coll := setupTestMongo(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		writer, err := NewWriter[testDoc](ctx, coll, cfg)
		if err != nil {
			t.Fatalf("Failed to create writer with MaxBatchBytes: %v", err)
		}

		// Verify the config is stored
		if writer.cfg.MaxBatchBytes != 1024*1024 {
			t.Errorf("Expected MaxBatchBytes=1048576, got %d", writer.cfg.MaxBatchBytes)
		}
	})

	t.Run("production configuration examples are valid", func(t *testing.T) {
		// Test that all the example configurations are valid and can create writers
		coll := setupTestMongo(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		configs := map[string]Config{
			"HighThroughputSmallDocuments": {
				MaxBatch:        1000,
				MaxBatchBytes:   10 * 1024 * 1024,
				MaxDelay:        50 * time.Millisecond,
				QueueSize:       10_000,
				Retries:         3,
				RetryBackoffMin: 10 * time.Millisecond,
				RetryBackoffMax: 500 * time.Millisecond,
				RetryTimeout:    2 * time.Second,
				Workers:         4,
			},
			"LargeDocumentsLowThroughput": {
				MaxBatch:        50,
				MaxBatchBytes:   50 * 1024 * 1024,
				MaxDelay:        500 * time.Millisecond,
				QueueSize:       1_000,
				Retries:         5,
				RetryBackoffMin: 100 * time.Millisecond,
				RetryBackoffMax: 10 * time.Second,
				RetryTimeout:    30 * time.Second,
				Workers:         2,
			},
			"CriticalReliability": {
				MaxBatch:        100,
				MaxBatchBytes:   5 * 1024 * 1024,
				MaxDelay:        1 * time.Second,
				QueueSize:       5_000,
				Retries:         10,
				RetryBackoffMin: 200 * time.Millisecond,
				RetryBackoffMax: 30 * time.Second,
				RetryTimeout:    60 * time.Second,
				Workers:         3,
			},
		}

		for name, cfg := range configs {
			t.Run(name, func(t *testing.T) {
				writer, err := NewWriter[testDoc](ctx, coll, cfg)
				if err != nil {
					t.Fatalf("Failed to create writer with %s config: %v", name, err)
				}

				// Test basic functionality
				doc := testDoc{ID: 91000, Name: name}
				err = writer.Push(context.Background(), doc)
				if err != nil {
					t.Errorf("Push failed with %s config: %v", name, err)
				}
			})
		}
	})

	t.Run("configuration validates memory usage expectations", func(t *testing.T) {
		// Test memory usage calculation from the examples
		cfg := Config{
			MaxBatch:      1000,
			MaxBatchBytes: 10 * 1024 * 1024, // 10MB
			QueueSize:     10_000,
			Workers:       4,
		}

		// Simulate average document size of 1KB
		avgDocSize := 1024
		
		// Calculate expected memory usage
		// Memory ≈ QueueSize * avg_doc_size + (Workers * MaxBatch * avg_doc_size) + 30% overhead
		queueMemory := cfg.QueueSize * avgDocSize
		workerMemory := cfg.Workers * cfg.MaxBatch * avgDocSize
		expectedMemory := int64(float64(queueMemory+workerMemory) * 1.3)

		t.Logf("Memory calculation for config:")
		t.Logf("  Queue memory: %d bytes (~%d MB)", queueMemory, queueMemory/(1024*1024))
		t.Logf("  Worker memory: %d bytes (~%d MB)", workerMemory, workerMemory/(1024*1024))  
		t.Logf("  Expected total: %d bytes (~%d MB)", expectedMemory, expectedMemory/(1024*1024))
		t.Logf("  MaxBatchBytes limit: %d bytes (~%d MB)", cfg.MaxBatchBytes, cfg.MaxBatchBytes/(1024*1024))

		// Verify MaxBatchBytes is reasonable for the configuration
		maxBatchMemory := int64(cfg.MaxBatch * avgDocSize)
		if cfg.MaxBatchBytes < maxBatchMemory {
			t.Errorf("MaxBatchBytes (%d) is smaller than a full batch (%d)", cfg.MaxBatchBytes, maxBatchMemory)
		}
	})
}

// TestGoVersionCompatibility verifies the updated Go version requirement
func TestGoVersionCompatibility(t *testing.T) {
	// Test that we can use Go 1.22+ features without requiring 1.24.5
	
	// Test math/rand/v2 usage (available in Go 1.22+)
	duration := jitter(100*time.Millisecond, 200*time.Millisecond)
	if duration < 0 || duration > 200*time.Millisecond {
		t.Errorf("jitter function failed: %v", duration)
	}

	// Test that we can marshal/unmarshal without requiring latest Go version
	testStruct := struct {
		Field1 string `json:"field1"`
		Field2 int    `json:"field2"`
	}{
		Field1: "test",
		Field2: 42,
	}

	data, err := json.Marshal(testStruct)
	if err != nil {
		t.Errorf("JSON marshal failed: %v", err)
	}

	var result struct {
		Field1 string `json:"field1"`
		Field2 int    `json:"field2"`
	}

	err = json.Unmarshal(data, &result)
	if err != nil {
		t.Errorf("JSON unmarshal failed: %v", err)
	}

	if result != testStruct {
		t.Errorf("JSON round-trip failed: expected %+v, got %+v", testStruct, result)
	}
}

// TestHealthCheck verifies the new health check functionality  
func TestHealthCheck(t *testing.T) {
	coll := setupTestMongo(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := Config{
		MaxBatch:  10,
		MaxDelay:  1 * time.Hour, // Long delay to keep items in queue
		QueueSize: 100,
		Workers:   3,
	}

	writer, err := NewWriter[testDoc](ctx, coll, cfg)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Initially should have empty queue
	health := writer.Health()
	if health.QueueDepth != 0 {
		t.Errorf("Expected empty queue, got depth %d", health.QueueDepth)
	}
	if health.QueueCapacity != 100 {
		t.Errorf("Expected capacity 100, got %d", health.QueueCapacity)
	}
	if health.QueueUtilPct != 0 {
		t.Errorf("Expected 0%% utilization, got %d%%", health.QueueUtilPct)
	}
	if health.Workers != 3 {
		t.Errorf("Expected 3 workers, got %d", health.Workers)
	}
	if health.IsShuttingDown {
		t.Error("Expected not shutting down initially")
	}

	// Add some items to the queue
	for i := 0; i < 25; i++ {
		doc := testDoc{ID: 92000 + i, Name: "health_test"}
		err = writer.Push(context.Background(), doc)
		if err != nil {
			t.Fatalf("Push failed: %v", err)
		}
	}

	// Check queue depth
	health = writer.Health()
	if health.QueueDepth == 0 {
		t.Error("Expected items in queue after pushing")
	}
	if health.QueueUtilPct == 0 {
		t.Error("Expected non-zero utilization after pushing")
	}
	
	t.Logf("Health after pushing 25 items: %+v", health)

	// Test shutdown detection
	cancel()
	
	// Give a moment for context cancellation to propagate
	time.Sleep(10 * time.Millisecond)
	
	health = writer.Health()
	if !health.IsShuttingDown {
		t.Error("Expected IsShuttingDown=true after context cancellation")
	}

	t.Logf("Health during shutdown: %+v", health)
}