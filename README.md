# BatchWriter

A thread-safe, batch processing writer for MongoDB with configurable batching, timeouts, and failure handling.

## Features

- **Thread-safe**: Safe for concurrent use across multiple goroutines
- **Configurable batching**: Set maximum batch size and flush delays
- **Failure handling**: Configurable sink for handling failed batches
- **Graceful shutdown**: Proper cleanup and remaining item processing
- **MongoDB integration**: Built specifically for MongoDB collections
 - **Non-blocking first push**: (v0.1.1) `Push` mirrors `PushMany` semantics—if there is room in the queue it enqueues even after shutdown has begun; cancellation is only honored when the call would block.

## Testing Requirements

To run the full test suite, you need a MongoDB instance. The connection can be configured via environment variables.

### Quick Setup (Interactive)

For first-time setup, you can use the interactive configuration script:

```bash
./setup-mongo.sh
```

This will guide you through setting up MongoDB configuration and save it to a `.env` file.

### Environment Variables

The tests automatically load configuration from a `.env` file if present, or from environment variables:

- `MONGO_HOST` - MongoDB host (default: `localhost`)
- `MONGO_PORT` - MongoDB port (default: `27017`)
- `MONGO_USERNAME` - MongoDB username (default: `testuser`)
- `MONGO_PASSWORD` - MongoDB password (default: `testpass`)
- `MONGO_AUTH_DATABASE` - Authentication database (default: `admin`)

### Setup Examples

**Using .env file (recommended):**
```bash
# Create .env file
cat > .env << EOF
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_USERNAME=testuser
MONGO_PASSWORD=testpass
EOF

# Tests automatically load .env file
go test ./...
```

**Using Docker (recommended for testing):**
```bash
# Start MongoDB with default credentials
docker run -d \
  --name mongo-test \
  -p 27017:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=testuser \
  -e MONGO_INITDB_ROOT_PASSWORD=testpass \
  mongo:7

# Run tests (will use .env or defaults)
go test ./...
```

**Using custom MongoDB instance:**
```bash
# Run tests with custom MongoDB configuration
MONGO_HOST=my-mongo-server \
MONGO_PORT=27018 \
MONGO_USERNAME=testuser \
MONGO_PASSWORD=testpass \
go test ./...
```

**Using MongoDB Atlas or remote instance:**
```bash
# For cloud MongoDB instances, you might need different auth setup
MONGO_HOST=cluster0.mongodb.net \
MONGO_PORT=27017 \
MONGO_USERNAME=myuser \
MONGO_PASSWORD=mypass \
MONGO_AUTH_DATABASE=admin \
go test ./...
```

If MongoDB is not available, the tests will be skipped automatically with a message indicating the connection details that were attempted.

## Usage

```go
package main

import (
    "context"
    "time"
    "github.com/blackorder/batchwriter"
    "go.mongodb.org/mongo-driver/v2/mongo"
    "go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Document struct {
    ID   int    `bson:"_id"`
    Name string `bson:"name"`
}

func main() {
    // Connect to MongoDB
    client, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
    if err != nil {
        panic(err)
    }
    defer client.Disconnect(context.Background())
    
    collection := client.Database("mydb").Collection("documents")
    
    // Create shutdown context
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    // Configure the writer
    cfg := batchwriter.Config{
        MaxBatch:  100,                    // Flush when 100 items accumulated
        MaxDelay:  5 * time.Second,        // Flush every 5 seconds
        QueueSize: 1000,                   // Queue up to 1000 items
        Workers:   4,                      // Use 4 worker goroutines
        Sink:      nil,                    // Optional: handle failed batches
    }
    
    // Create the writer
    writer, err := batchwriter.NewWriter[Document](ctx, collection, cfg)
    if err != nil {
        panic(err)
    }
    
    // Push documents
    doc := Document{ID: 1, Name: "example"}
    if err := writer.Push(context.Background(), doc); err != nil {
        // Handle error (item was dumped to sink if configured)
    }
    
    // Push multiple documents
    docs := []Document{
        {ID: 2, Name: "batch1"},
        {ID: 3, Name: "batch2"},
    }
    if err := writer.PushMany(context.Background(), docs); err != nil {
        // Handle error
    }
    
    // Graceful shutdown
    cancel() // Signal shutdown
    if err := writer.Close(context.Background()); err != nil {
        // Handle shutdown error
    }
}
```

## Configuration

- `MaxBatch`: Maximum number of items per batch (default: 100)
- `MaxDelay`: Maximum time to wait before flushing (default: 5s)
- `QueueSize`: Size of the internal queue (default: 1000)
- `Workers`: Number of worker goroutines (default: 1)
- `Sink`: Optional handler for failed batches

## Thread Safety

The library is fully thread-safe. You can safely call `Push()` and `PushMany()` from multiple goroutines simultaneously.

## Error Handling

When operations fail (timeouts, shutdown, database errors), items are sent to the configured `Sink` for custom handling. If no sink is configured, failed items are logged and discarded.

## License

[Add your license here]
