package batchwriter

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func setupTestMongoBench(b *testing.B) *mongo.Collection {
	b.Helper()

	mongoURI := getMongoURI()
	client, err := mongo.Connect(options.Client().ApplyURI(mongoURI))
	if err != nil {
		b.Skipf("MongoDB not available for benchmarking (URI: %s): %v", mongoURI, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx, nil); err != nil {
		b.Skipf("MongoDB not available for benchmarking (URI: %s): %v", mongoURI, err)
	}

	db := client.Database("batchwriter_bench")
	coll := db.Collection("bench_docs")

	// Clean up collection
	_, _ = coll.DeleteMany(ctx, map[string]any{})

	return coll
}

func BenchmarkPush(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	coll := setupTestMongoBench(b)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := Config{
		MaxBatch:  1000,
		MaxDelay:  1 * time.Second,
		QueueSize: 100000,
		Workers:   4,
	}

	writer, err := NewWriter[testDoc](ctx, coll, cfg)
	if err != nil {
		b.Fatalf("Failed to create writer: %v", err)
	}

	defer func() {
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		writer.Close(closeCtx)
	}()

	doc := testDoc{ID: 1, Name: "benchmark"}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			doc.ID = i
			writer.Push(context.Background(), doc)
		}
	})
}

func BenchmarkPushMany(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	coll := setupTestMongoBench(b)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := Config{
		MaxBatch:  1000,
		MaxDelay:  1 * time.Second,
		QueueSize: 100000,
		Workers:   4,
	}

	writer, err := NewWriter[testDoc](ctx, coll, cfg)
	if err != nil {
		b.Fatalf("Failed to create writer: %v", err)
	}

	defer func() {
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		writer.Close(closeCtx)
	}()

	batchSize := 100
	docs := make([]testDoc, batchSize)
	for i := range docs {
		docs[i] = testDoc{ID: i, Name: "benchmark"}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		baseID := 0
		for pb.Next() {
			// Update IDs to avoid duplicates
			for i := range docs {
				docs[i].ID = baseID + i
			}
			baseID += batchSize

			writer.PushMany(context.Background(), docs)
		}
	})
}

func BenchmarkJSONSinkDump(b *testing.B) {
	tmpDir := b.TempDir()
	cfg := &JSONDumpConfig{
		Dir:        tmpDir,
		FilePrefix: "bench",
	}

	sink, err := newJSONSink[testDoc](cfg, nopLogger{})
	if err != nil {
		b.Fatalf("Failed to create sink: %v", err)
	}
	defer sink.Close()

	docs := []testDoc{
		{ID: 1, Name: "benchmark_doc_1"},
		{ID: 2, Name: "benchmark_doc_2"},
		{ID: 3, Name: "benchmark_doc_3"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink.Dump("benchmark", nil, docs)
	}
}

func BenchmarkFilterOutByIndices(b *testing.B) {
	input := make([]testDoc, 1000)
	for i := range input {
		input[i] = testDoc{ID: i, Name: "benchmark"}
	}

	// Create a drop map with every 10th element
	drop := make(map[int]struct{})
	for i := 0; i < 1000; i += 10 {
		drop[i] = struct{}{}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = filterOutByIndices(input, drop)
	}
}

func BenchmarkJitter(b *testing.B) {
	base := 100 * time.Millisecond
	max := 1 * time.Second

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = jitter(base, max)
	}
}
