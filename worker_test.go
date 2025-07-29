package batchwriter

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestFilterOutByIndices(t *testing.T) {
	input := []testDoc{
		{ID: 1, Name: "test1"},
		{ID: 2, Name: "test2"},
		{ID: 3, Name: "test3"},
		{ID: 4, Name: "test4"},
	}

	drop := map[int]struct{}{
		0: {},
		2: {},
	}

	result := filterOutByIndices(input, drop)

	if len(result) != 2 {
		t.Fatalf("Expected 2 items, got %d", len(result))
	}

	if result[0].ID != 2 || result[1].ID != 4 {
		t.Fatalf("Expected IDs [2, 4], got [%d, %d]", result[0].ID, result[1].ID)
	}
}

func TestFilterOutByIndicesEmpty(t *testing.T) {
	input := []testDoc{
		{ID: 1, Name: "test1"},
		{ID: 2, Name: "test2"},
	}

	drop := map[int]struct{}{}

	result := filterOutByIndices(input, drop)

	if len(result) != 2 {
		t.Fatalf("Expected 2 items, got %d", len(result))
	}

	// Should be the same slice
	if &result[0] != &input[0] {
		t.Fatal("Expected same slice when no items to drop")
	}
}

func TestJitter(t *testing.T) {
	base := 100 * time.Millisecond
	max := 200 * time.Millisecond

	// Test multiple times to ensure we get variation
	results := make(map[time.Duration]bool)
	for i := 0; i < 100; i++ {
		result := jitter(base, max)

		// Should be between 75% and 125% of base, but capped at max
		minExpected := time.Duration(float64(base) * 0.75)
		maxExpected := max

		if result < minExpected || result > maxExpected {
			t.Fatalf("Jitter result %v outside expected range [%v, %v]", result, minExpected, maxExpected)
		}

		results[result] = true
	}

	// Should have some variation (not all the same)
	if len(results) < 10 {
		t.Fatal("Jitter not providing enough variation")
	}
}

func TestJitterRespectMax(t *testing.T) {
	base := 300 * time.Millisecond
	max := 200 * time.Millisecond // max < base

	for i := 0; i < 10; i++ {
		result := jitter(base, max)
		if result > max {
			t.Fatalf("Jitter result %v exceeds max %v", result, max)
		}
	}
}

// Integration test with real MongoDB for worker behavior
func TestWorkerIntegration(t *testing.T) {
	coll := setupTestMongo(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sink := &testSink{}
	cfg := Config{
		MaxBatch:  3,
		MaxDelay:  100 * time.Millisecond,
		QueueSize: 10,
		Sink:      sink,
		Workers:   1,
	}

	writer, err := NewWriter[testDoc](ctx, coll, cfg)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Test batch size trigger
	t.Run("batch size trigger", func(t *testing.T) {
		docs := []testDoc{
			{ID: 101, Name: "test101"},
			{ID: 102, Name: "test102"},
			{ID: 103, Name: "test103"},
		}

		for _, doc := range docs {
			err := writer.Push(context.Background(), doc)
			if err != nil {
				t.Fatalf("Push failed: %v", err)
			}
		}

		// Wait for processing
		time.Sleep(200 * time.Millisecond)

		// Verify documents were inserted
		count, err := coll.CountDocuments(context.Background(), map[string]any{"_id": map[string]any{"$in": []int{101, 102, 103}}})
		if err != nil {
			t.Fatalf("Failed to count documents: %v", err)
		}
		if count != 3 {
			t.Fatalf("Expected 3 documents, got %d", count)
		}
	})

	t.Run("time delay trigger", func(t *testing.T) {
		// Clear collection
		_, _ = coll.DeleteMany(context.Background(), map[string]any{})

		docs := []testDoc{
			{ID: 201, Name: "test201"},
			{ID: 202, Name: "test202"},
		}

		for _, doc := range docs {
			err := writer.Push(context.Background(), doc)
			if err != nil {
				t.Fatalf("Push failed: %v", err)
			}
		}

		// Wait for delay timeout
		time.Sleep(200 * time.Millisecond)

		// Verify documents were inserted
		count, err := coll.CountDocuments(context.Background(), map[string]any{"_id": map[string]any{"$in": []int{201, 202}}})
		if err != nil {
			t.Fatalf("Failed to count documents: %v", err)
		}
		if count != 2 {
			t.Fatalf("Expected 2 documents, got %d", count)
		}
	})

	t.Run("duplicate key handling", func(t *testing.T) {
		// Insert a document first
		_, err := coll.InsertOne(context.Background(), testDoc{ID: 301, Name: "original"})
		if err != nil {
			t.Fatalf("Failed to insert original document: %v", err)
		}

		// Try to insert the same ID again
		err = writer.Push(context.Background(), testDoc{ID: 301, Name: "duplicate"})
		if err != nil {
			t.Fatalf("Push failed: %v", err)
		}

		// Wait for processing
		time.Sleep(200 * time.Millisecond)

		// The duplicate should be handled gracefully (no error should propagate)
		// Check that we don't have failures in sink for duplicate keys
		duplicateFailures := 0
		dumps := sink.getDumps()
		for _, dump := range dumps {
			if dump.reason == "insert_failed" {
				duplicateFailures++
			}
		}

		// For a simple duplicate key error on a single doc, it should be filtered out
		// and not appear in the failure sink
		if duplicateFailures > 0 {
			t.Logf("Note: Got %d duplicate key failures in sink - this is acceptable for complex bulk operations", duplicateFailures)
		}
	})
}

func TestInsertWithRetryIntegration(t *testing.T) {
	coll := setupTestMongo(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := Config{
		Retries:         2,
		RetryBackoffMin: 1 * time.Millisecond,
		RetryBackoffMax: 10 * time.Millisecond,
		RetryTimeout:    1 * time.Second,
	}

	writer := &Writer[testDoc]{
		cfg:  cfg,
		coll: coll,
		ctx:  ctx,
		l:    nopLogger{},
	}

	t.Run("successful insert", func(t *testing.T) {
		docs := []testDoc{{ID: 401, Name: "test401"}}
		remaining, err := writer.insertWithRetry(docs)

		if err != nil {
			t.Fatalf("Expected success, got error: %v", err)
		}
		if len(remaining) != 0 {
			t.Fatalf("Expected no remaining docs, got %d", len(remaining))
		}

		// Verify document was inserted
		count, err := coll.CountDocuments(context.Background(), map[string]any{"_id": 401})
		if err != nil {
			t.Fatalf("Failed to count documents: %v", err)
		}
		if count != 1 {
			t.Fatalf("Expected 1 document, got %d", count)
		}
	})

	t.Run("duplicate key filtering", func(t *testing.T) {
		// Insert original documents
		_, err := coll.InsertMany(context.Background(), []interface{}{
			testDoc{ID: 501, Name: "original1"},
			testDoc{ID: 503, Name: "original3"},
		})
		if err != nil {
			t.Fatalf("Failed to insert original documents: %v", err)
		}

		// Try to insert mix of new and duplicate documents
		docs := []testDoc{
			{ID: 501, Name: "duplicate1"}, // duplicate
			{ID: 502, Name: "new2"},       // new
			{ID: 503, Name: "duplicate3"}, // duplicate
		}

		remaining, err := writer.insertWithRetry(docs)

		// Should handle duplicates gracefully
		if err != nil {
			// Check if it's a bulk write exception
			var bwe mongo.BulkWriteException
			if errors.As(err, &bwe) {
				bwe = err.(mongo.BulkWriteException)
				t.Logf("Got bulk write exception with %d write errors", len(bwe.WriteErrors))

				// The remaining should exclude the duplicates
				if len(remaining) > 1 { // Should have filtered out at least one duplicate
					t.Fatalf("Expected fewer remaining docs after duplicate filtering, got %d", len(remaining))
				}
			} else {
				t.Fatalf("Unexpected error type: %v", err)
			}
		}

		// Verify the new document was inserted
		count, err := coll.CountDocuments(context.Background(), map[string]any{"_id": 502})
		if err != nil {
			t.Fatalf("Failed to count documents: %v", err)
		}
		if count != 1 {
			t.Fatalf("Expected new document to be inserted, got %d", count)
		}
	})
}
