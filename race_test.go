package batchwriter

import (
	"context"
	"sync"
	"testing"
	"time"
)

// This test specifically stresses race conditions to ensure the package is thread-safe
// It uses a deliberately non-thread-safe sink to ensure the package protects it properly

type racyTestSink struct {
	// Deliberately NO MUTEX - this sink is NOT thread-safe
	// If the package doesn't protect it properly, we'll get race conditions
	dumps []dumpCall
}

func (rs *racyTestSink) Dump(reason string, cause error, batch []any) {
	// This is deliberately racy - if called concurrently without protection, it will race
	rs.dumps = append(rs.dumps, dumpCall{reason, cause, batch})
}

func (rs *racyTestSink) Close() error {
	return nil
}

func TestPackageRaceSafety(t *testing.T) {
	// This test verifies that the batchwriter package properly protects sink access
	// even when the sink implementation itself is NOT thread-safe

	coll := setupTestMongo(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use a deliberately non-thread-safe sink
	racySink := &racyTestSink{}

	cfg := Config{
		MaxBatch:  5,
		MaxDelay:  10 * time.Millisecond, // Short delay to trigger timeouts
		QueueSize: 2,                     // Small queue to force blocking
		Sink:      racySink,
		Workers:   4, // Multiple workers to increase concurrency
	}

	writer, err := NewWriter[testDoc](ctx, coll, cfg)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Create high contention scenario with multiple goroutines
	// trying to trigger dumpFailed calls simultaneously

	const numGoroutines = 20
	const itemsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Start multiple goroutines that will all try to:
	// 1. Fill the queue to cause blocking
	// 2. Use short timeouts to trigger dumps
	// 3. Call dumpFailed concurrently from multiple sources

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < itemsPerGoroutine; j++ {
				// Use a very short timeout to force dump operations
				shortCtx, shortCancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)

				doc := testDoc{
					ID:   goroutineID*1000 + j,
					Name: "race_test",
				}

				// This will likely fail due to timeout, triggering dumpFailed
				_ = writer.Push(shortCtx, doc)
				shortCancel()

				// Also try PushMany to trigger different code paths
				if j%10 == 0 {
					docs := []testDoc{
						{ID: goroutineID*1000 + j + 10000, Name: "race_batch1"},
						{ID: goroutineID*1000 + j + 20000, Name: "race_batch2"},
					}
					shortCtx2, shortCancel2 := context.WithTimeout(context.Background(), 1*time.Nanosecond)
					_ = writer.PushMany(shortCtx2, docs)
					shortCancel2()
				}
			}
		}(i)
	}

	// While the goroutines are running, also trigger shutdown scenarios
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel() // This will cause workers to dump remaining items
	}()

	// Wait for all goroutines to complete
	wg.Wait()

	// Close the writer which may also trigger dumps
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer closeCancel()

	_ = writer.Close(closeCtx)

	// If we get here without a race condition, the package is properly protecting
	// the sink access even though the sink itself is not thread-safe

	// Verify we actually exercised the dump functionality
	if len(racySink.dumps) == 0 {
		t.Log("No dumps occurred - test may not have exercised race conditions enough")
	} else {
		t.Logf("Successfully handled %d dump operations without race conditions", len(racySink.dumps))
	}
}

func TestConcurrentDumpFailedCalls(t *testing.T) {
	// This test directly exercises the dumpFailed method from multiple goroutines
	// to ensure it's properly protected by the mutex

	coll := setupTestMongo(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	racySink := &racyTestSink{}
	cfg := Config{
		MaxBatch:  10,
		MaxDelay:  1 * time.Hour, // Long delay so we don't get timer-based flushes
		QueueSize: 10,
		Sink:      racySink,
	}

	writer, err := NewWriter[testDoc](ctx, coll, cfg)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Directly call dumpFailed from multiple goroutines simultaneously
	const numConcurrentCalls = 50
	const itemsPerCall = 10

	var wg sync.WaitGroup
	wg.Add(numConcurrentCalls)

	for i := 0; i < numConcurrentCalls; i++ {
		go func(callID int) {
			defer wg.Done()

			// Create a batch of items
			batch := make([]testDoc, itemsPerCall)
			for j := 0; j < itemsPerCall; j++ {
				batch[j] = testDoc{
					ID:   callID*1000 + j,
					Name: "concurrent_dump",
				}
			}

			// Call dumpFailed directly - this should be protected by the package's mutex
			writer.dumpFailed(batch, "test_concurrent_dump", context.Canceled)
		}(i)
	}

	// Wait for all concurrent calls to complete
	wg.Wait()

	// Verify we got the expected number of dump calls
	expectedDumps := numConcurrentCalls
	actualDumps := len(racySink.dumps)

	if actualDumps != expectedDumps {
		t.Errorf("Expected %d dump calls, got %d", expectedDumps, actualDumps)
	}

	// Calculate total items dumped
	totalItems := 0
	for _, dump := range racySink.dumps {
		totalItems += len(dump.batch)
	}

	expectedItems := numConcurrentCalls * itemsPerCall
	if totalItems != expectedItems {
		t.Errorf("Expected %d total items dumped, got %d", expectedItems, totalItems)
	}

	t.Logf("Successfully handled %d concurrent dumpFailed calls with %d total items", actualDumps, totalItems)
}
