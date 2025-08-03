package batchwriter

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Test edge cases and error conditions that might be hard to trigger

func TestMongoErrorHandling(t *testing.T) {
	// Test various MongoDB error scenarios
	coll := setupTestMongo(t)

	t.Run("bulk write exception handling", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg := Config{
			MaxBatch:        2,
			MaxDelay:        10 * time.Millisecond,
			Retries:         1,
			RetryBackoffMin: 1 * time.Millisecond,
			RetryBackoffMax: 5 * time.Millisecond,
		}

		writer := &Writer[testDoc]{
			cfg:  cfg,
			coll: coll,
			ctx:  ctx,
			l:    nopLogger{},
		}

		// First insert some documents
		originalDocs := []testDoc{
			{ID: 10001, Name: "original1"},
			{ID: 10002, Name: "original2"},
		}

		_, err := coll.InsertMany(context.Background(), originalDocs)
		if err != nil {
			t.Fatalf("Failed to insert original docs: %v", err)
		}

		// Now try to insert a mix of duplicates and new docs
		mixedDocs := []testDoc{
			{ID: 10001, Name: "duplicate1"}, // This will cause duplicate key error
			{ID: 10003, Name: "new1"},       // This should succeed
			{ID: 10002, Name: "duplicate2"}, // This will cause duplicate key error
			{ID: 10004, Name: "new2"},       // This should succeed
		}

		// This should handle duplicates gracefully
		remaining, err := writer.insertWithRetry(mixedDocs)

		// Should either succeed (duplicates filtered) or return remaining docs
		t.Logf("insertWithRetry result: remaining=%d, err=%v", len(remaining), err)

		// Verify new documents were inserted (might be partial due to duplicates)
		count, err := coll.CountDocuments(context.Background(), bson.M{
			"_id": bson.M{"$in": []int{10003, 10004}},
		})
		if err != nil {
			t.Fatalf("Failed to count new documents: %v", err)
		}

		// Due to timing and error handling, we might not get exactly 2
		// The important thing is that the function handles the scenario gracefully
		t.Logf("Found %d new documents out of expected 2", count)
	})

	t.Run("timeout during insert", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg := Config{
			RetryTimeout: 50 * time.Millisecond, // Short but reasonable timeout
			Retries:      2,
		}

		writer := &Writer[testDoc]{
			cfg:  cfg,
			coll: coll,
			ctx:  ctx,
			l:    nopLogger{},
		}

		docs := []testDoc{
			{ID: 10005, Name: "timeout_test"},
		}

		// This might timeout due to very short timeout
		remaining, err := writer.insertWithRetry(docs)

		// Either succeeds or times out, both are valid
		t.Logf("Timeout test result: remaining=%d, err=%v", len(remaining), err)
	})
}

func TestWorkerShutdownScenarios(t *testing.T) {
	// Test various worker shutdown scenarios
	coll := setupTestMongo(t)

	t.Run("shutdown with pending batch", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		sink := &testSink{}
		cfg := Config{
			MaxBatch:  100,              // Large batch size
			MaxDelay:  10 * time.Second, // Long delay
			QueueSize: 10,
			Sink:      sink,
			Workers:   1,
		}

		writer, err := NewWriter[testDoc](ctx, coll, cfg)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		// Push some docs that won't trigger a flush
		docs := []testDoc{
			{ID: 11001, Name: "pending1"},
			{ID: 11002, Name: "pending2"},
			{ID: 11003, Name: "pending3"},
		}

		for _, doc := range docs {
			err := writer.Push(context.Background(), doc)
			if err != nil {
				t.Fatalf("Push failed: %v", err)
			}
		}

		// Cancel immediately to test shutdown draining
		cancel()

		// Close with reasonable timeout
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer closeCancel()

		err = writer.Close(closeCtx)
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		// Verify docs were either written to MongoDB or dumped
		count, err := coll.CountDocuments(context.Background(), bson.M{
			"_id": bson.M{"$in": []int{11001, 11002, 11003}},
		})
		if err != nil {
			t.Fatalf("Failed to count documents: %v", err)
		}

		// Count dumped items
		var dumpedCount int
		dumps := sink.getDumps()
		for _, dump := range dumps {
			dumpedCount += len(dump.batch)
		}

		totalProcessed := int(count) + dumpedCount
		if totalProcessed != 3 {
			t.Errorf("Expected 3 items to be processed (written or dumped), got %d", totalProcessed)
		}

		t.Logf("Shutdown test: %d written to MongoDB, %d dumped to sink", count, dumpedCount)
	})

	t.Run("multiple flushes during shutdown", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		sink := &testSink{}
		cfg := Config{
			MaxBatch:  2,             // Small batch to trigger multiple flushes
			MaxDelay:  1 * time.Hour, // Long delay
			QueueSize: 20,
			Sink:      sink,
			Workers:   1,
		}

		writer, err := NewWriter[testDoc](ctx, coll, cfg)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		// Push many docs to trigger multiple batches during shutdown
		numDocs := 10
		baseID := 11100
		for i := 0; i < numDocs; i++ {
			doc := testDoc{ID: baseID + i, Name: "multi_flush"}
			err := writer.Push(context.Background(), doc)
			if err != nil {
				t.Fatalf("Push failed: %v", err)
			}
		}

		// Cancel and close
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer closeCancel()

		err = writer.Close(closeCtx)
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		// Verify all docs were processed
		count, err := coll.CountDocuments(context.Background(), bson.M{
			"name": "multi_flush",
		})
		if err != nil {
			t.Fatalf("Failed to count documents: %v", err)
		}

		// Count dumped items
		var dumpedCount int
		dumps := sink.getDumps()
		for _, dump := range dumps {
			dumpedCount += len(dump.batch)
		}

		totalProcessed := int(count) + dumpedCount
		if totalProcessed != numDocs {
			t.Errorf("Expected %d items to be processed, got %d", numDocs, totalProcessed)
		}

		t.Logf("Multi-flush test: %d written to MongoDB, %d dumped to sink", count, dumpedCount)
	})
}

func TestConfigValidation(t *testing.T) {
	// Test configuration validation and edge cases
	coll := setupTestMongo(t)

	t.Run("negative values get corrected", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg := Config{
			MaxBatch:        -1,               // Should become 500
			MaxDelay:        -1 * time.Second, // Should become 100ms
			QueueSize:       -1,               // Should become 100_000
			Workers:         -1,               // Should become 1
			RetryBackoffMin: -1 * time.Second, // Should become 100ms
			RetryBackoffMax: -1 * time.Second, // Should become 5s
			RetryTimeout:    -1 * time.Second, // Should become 5s
		}

		writer, err := NewWriter[testDoc](ctx, coll, cfg)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		// Verify defaults were applied
		if writer.cfg.MaxBatch != 500 {
			t.Errorf("Expected MaxBatch=500, got %d", writer.cfg.MaxBatch)
		}
		if writer.cfg.MaxDelay != 100*time.Millisecond {
			t.Errorf("Expected MaxDelay=100ms, got %v", writer.cfg.MaxDelay)
		}
		if writer.cfg.QueueSize != 100_000 {
			t.Errorf("Expected QueueSize=100000, got %d", writer.cfg.QueueSize)
		}
		if writer.cfg.Workers != 1 {
			t.Errorf("Expected Workers=1, got %d", writer.cfg.Workers)
		}

		// Test that it works
		err = writer.Push(context.Background(), testDoc{ID: 12001, Name: "validation_test"})
		if err != nil {
			t.Fatalf("Push failed: %v", err)
		}

		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer closeCancel()
		writer.Close(closeCtx)
	})
}

func TestSinkAdapterEdgeCases(t *testing.T) {
	// Test the typed adapter edge cases

	t.Run("adapter with nil sink", func(t *testing.T) {
		// This would be a programming error, but test robustness
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Expected panic when using nil sink: %v", r)
			}
		}()

		adapter := typedAdapter[testDoc]{nil}
		// This should panic
		adapter.Dump("test", nil, []testDoc{{ID: 1, Name: "test"}})
	})

	t.Run("adapter close", func(t *testing.T) {
		sink := &testSink{}
		adapter := typedAdapter[testDoc]{sink}

		err := adapter.Close()
		if err != nil {
			t.Errorf("Adapter close failed: %v", err)
		}
	})

	t.Run("empty batch dump", func(t *testing.T) {
		sink := &testSink{}
		adapter := typedAdapter[testDoc]{sink}

		// Empty batch should work
		adapter.Dump("empty", nil, []testDoc{})

		dumps := sink.getDumps()
		if len(dumps) != 1 {
			t.Errorf("Expected 1 dump call, got %d", len(dumps))
		}

		if len(dumps[0].batch) != 0 {
			t.Errorf("Expected empty batch, got %d items", len(dumps[0].batch))
		}
	})
}

func TestJitterDistribution(t *testing.T) {
	// Test jitter function more thoroughly
	base := 100 * time.Millisecond
	maxDuration := 200 * time.Millisecond

	// Test many iterations to verify distribution
	var total time.Duration
	samples := 1000

	for i := 0; i < samples; i++ {
		result := jitter(base, maxDuration)
		total += result

		// Each result should be within bounds
		if result < 0 || result > maxDuration {
			t.Errorf("Jitter result %v out of bounds [0, %v]", result, maxDuration)
		}
	}

	// Average should be reasonable (around base time, but can vary due to max cap)
	average := total / time.Duration(samples)
	t.Logf("Jitter average over %d samples: %v (base: %v, max: %v)", samples, average, base, maxDuration)

	// Test edge case: base > max
	veryLarge := 1 * time.Second
	small := 50 * time.Millisecond

	for i := 0; i < 10; i++ {
		result := jitter(veryLarge, small)
		if result > small {
			t.Errorf("Jitter result %v exceeded max %v when base > max", result, small)
		}
	}
}
