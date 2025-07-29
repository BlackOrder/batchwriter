package batchwriter

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// loadEnvFile loads environment variables from .env file if it exists
func loadEnvFile() {
	file, err := os.Open(".env")
	if err != nil {
		return // .env file doesn't exist, use system env vars
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Parse KEY=VALUE format
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Only set if not already in environment
		if os.Getenv(key) == "" {
			os.Setenv(key, value)
		}
	}
}

// getMongoURI builds the MongoDB connection URI from environment variables
// Falls back to default values if environment variables are not set
func getMongoURI() string {
	// Load .env file if it exists
	loadEnvFile()

	host := os.Getenv("MONGO_HOST")
	if host == "" {
		host = "localhost"
	}

	port := os.Getenv("MONGO_PORT")
	if port == "" {
		port = "27017"
	}

	username := os.Getenv("MONGO_USERNAME")
	if username == "" {
		username = "testuser"
	}

	password := os.Getenv("MONGO_PASSWORD")
	if password == "" {
		password = "testpass"
	}

	database := os.Getenv("MONGO_AUTH_DATABASE")
	if database == "" {
		database = "admin"
	}

	// Build URI with authentication
	return fmt.Sprintf("mongodb://%s:%s@%s:%s/?authSource=%s",
		username, password, host, port, database)
}

// setupTestMongo sets up a MongoDB collection for testing
// MongoDB connection can be configured via environment variables:
//
//	MONGO_HOST (default: localhost)
//	MONGO_PORT (default: 27017)
//	MONGO_USERNAME (default: testuser)
//	MONGO_PASSWORD (default: testpass)
//	MONGO_AUTH_DATABASE (default: admin)
//
// The function also automatically loads a .env file if present in the current directory.
//
// Example usage:
//
//	MONGO_HOST=my-mongo-server MONGO_PORT=27018 go test ./...
//
// Or create a .env file:
//
//	MONGO_HOST=my-mongo-server
//	MONGO_PORT=27018
//	MONGO_USERNAME=myuser
//	MONGO_PASSWORD=mypass
func setupTestMongo(t *testing.T) *mongo.Collection {
	t.Helper()

	mongoURI := getMongoURI()
	client, err := mongo.Connect(options.Client().ApplyURI(mongoURI))
	if err != nil {
		t.Skipf("MongoDB not available for testing (URI: %s): %v", mongoURI, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx, nil); err != nil {
		t.Skipf("MongoDB not available for testing (URI: %s): %v", mongoURI, err)
	}

	db := client.Database("batchwriter_test")
	coll := db.Collection("test_docs")

	// Clean up collection before each test
	_, _ = coll.DeleteMany(ctx, map[string]any{})

	return coll
}
