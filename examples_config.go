// Package batchwriter provides production-ready configuration examples
package batchwriter

import (
	"time"
)

// Production configuration examples for different use cases
// Copy and adapt these configurations based on your specific requirements

// HighThroughputSmallDocuments - Optimized for high throughput with small documents (< 1KB each)
// Suitable for: Logging, events, metrics, small transactions
var HighThroughputSmallDocuments = Config{
	MaxBatch:        1000,                      // Larger batches for efficiency
	MaxBatchBytes:   10 * 1024 * 1024,         // 10MB memory limit
	MaxDelay:        50 * time.Millisecond,    // Fast flush for real-time needs
	QueueSize:       10_000,                   // Reasonable memory usage
	Retries:         3,                        // Quick failure for high throughput
	RetryBackoffMin: 10 * time.Millisecond,   // Fast retry
	RetryBackoffMax: 500 * time.Millisecond,  // Don't wait too long
	RetryTimeout:    2 * time.Second,         // Quick timeout
	Workers:         4,                        // Parallel processing

	// Failure handling for debugging
	JSONDump: &JSONDumpConfig{
		Dir:           "/var/log/batchwriter/failed",
		FilePrefix:    "high-throughput-failures",
		FsyncEvery:    100,   // Frequent sync for reliability
		RotateBytes:   100 * 1024 * 1024, // 100MB rotation
		IncludeReason: true,
	},
}

// LargeDocumentsLowThroughput - Optimized for large documents (> 10KB each)
// Suitable for: User profiles, large records, file metadata, complex documents  
var LargeDocumentsLowThroughput = Config{
	MaxBatch:        50,                        // Smaller batches for large docs
	MaxBatchBytes:   50 * 1024 * 1024,         // 50MB memory limit 
	MaxDelay:        500 * time.Millisecond,   // Allow time for batching
	QueueSize:       1_000,                    // Smaller queue for memory management
	Retries:         5,                        // More retries for expensive operations
	RetryBackoffMin: 100 * time.Millisecond,  // Slower retry
	RetryBackoffMax: 10 * time.Second,        // Allow longer backoff
	RetryTimeout:    30 * time.Second,        // Longer timeout for large batches
	Workers:         2,                        // Fewer workers to manage memory

	JSONDump: &JSONDumpConfig{
		Dir:           "/var/log/batchwriter/failed",
		FilePrefix:    "large-docs-failures",
		FsyncEvery:    10,    // Less frequent sync due to larger records
		RotateBytes:   500 * 1024 * 1024, // 500MB rotation
		IncludeReason: true,
	},
}

// CriticalReliability - Maximum reliability with extensive retry and failure handling
// Suitable for: Financial transactions, audit logs, critical business data
var CriticalReliability = Config{
	MaxBatch:        100,                      // Moderate batch size
	MaxBatchBytes:   5 * 1024 * 1024,         // 5MB memory limit
	MaxDelay:        1 * time.Second,         // Allow time for full batches
	QueueSize:       5_000,                   // Moderate queue size
	Retries:         10,                      // Extensive retries
	RetryBackoffMin: 200 * time.Millisecond, // Conservative retry timing
	RetryBackoffMax: 30 * time.Second,       // Long backoff for persistent issues
	RetryTimeout:    60 * time.Second,       // Very long timeout
	Workers:         3,                       // Multiple workers for reliability

	JSONDump: &JSONDumpConfig{
		Dir:           "/var/log/batchwriter/critical-failures",
		FilePrefix:    "critical-failures",
		FsyncEvery:    1,     // Sync every record for maximum reliability
		RotateBytes:   10 * 1024 * 1024, // 10MB rotation for detailed analysis
		IncludeReason: true,
	},
}

// DevelopmentTesting - Configuration for development and testing environments
// Suitable for: Local development, integration tests, debugging
var DevelopmentTesting = Config{
	MaxBatch:        10,                       // Small batches for immediate visibility
	MaxBatchBytes:   1 * 1024 * 1024,         // 1MB memory limit
	MaxDelay:        100 * time.Millisecond,  // Fast flush for debugging
	QueueSize:       100,                     // Small queue for quick testing
	Retries:         1,                       // Fast failure for debugging
	RetryBackoffMin: 50 * time.Millisecond,  
	RetryBackoffMax: 1 * time.Second,
	RetryTimeout:    5 * time.Second,
	Workers:         1,                       // Single worker for predictable behavior

	JSONDump: &JSONDumpConfig{
		Dir:           "/tmp/batchwriter-dev",
		FilePrefix:    "dev-failures", 
		FsyncEvery:    1,     // Immediate flush for debugging
		RotateBytes:   1 * 1024 * 1024, // 1MB rotation
		IncludeReason: true,
	},
}

// Production configuration guidelines:
//
// Memory Usage Calculation:
// - Approximate memory = QueueSize * average_document_size + (Workers * MaxBatch * average_document_size)
// - Add 20-50% overhead for Go runtime and other allocations
// - Example: QueueSize=10000, Workers=4, MaxBatch=1000, avg_doc_size=1KB
//   Memory ≈ (10000 + 4*1000) * 1KB * 1.3 = ~18MB
//
// Performance Tuning:
// - Increase Workers for CPU-bound workloads
// - Increase MaxBatch for better MongoDB efficiency (but watch memory)
// - Decrease MaxDelay for lower latency requirements  
// - Increase QueueSize for bursty workloads (but watch memory)
//
// Monitoring Recommendations:
// - Monitor queue depth: should stay well below QueueSize
// - Monitor retry rates: high retry rates indicate MongoDB issues
// - Monitor dump rates: frequent dumps indicate configuration issues
// - Monitor memory usage: should match calculated estimates
//
// Failure Recovery:
// - Always configure JSONDump for production deployments
// - Monitor dump files and set up alerts for dump activity
// - Consider implementing custom sink for integration with monitoring systems