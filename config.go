package batchwriter

import (
	"errors"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

type Config struct {
	MaxBatch        int           // e.g. 500
	MaxDelay        time.Duration // e.g. 100 * time.Millisecond
	QueueSize       int           // e.g. 100_000
	Retries         int           // e.g. 5
	RetryBackoffMin time.Duration // e.g. 50 * time.Millisecond
	RetryBackoffMax time.Duration // e.g. 2 * time.Second
	Workers         int           // e.g. 2-4
	DropOnFull      bool          // false = block, true = return ErrQueueFull
}

var ErrQueueFull = errors.New("event queue full")

type Writer[T any] struct {
	cfg   Config
	coll  *mongo.Collection
	ch    chan T
	wg    sync.WaitGroup
	stopC chan struct{}
}

// NewWriter starts the background workers immediately.
func NewWriter[T any](coll *mongo.Collection, cfg Config) *Writer[T] {
	w := &Writer[T]{
		cfg:   cfg,
		coll:  coll,
		ch:    make(chan T, cfg.QueueSize),
		stopC: make(chan struct{}),
	}
	if w.cfg.Workers <= 0 {
		w.cfg.Workers = 1
	}
	for i := 0; i < w.cfg.Workers; i++ {
		w.wg.Add(1)
		go w.worker()
	}
	return w
}
