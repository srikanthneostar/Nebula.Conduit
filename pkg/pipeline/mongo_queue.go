package pipeline

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDBQueue implements DataQueue using MongoDB
type MongoDBQueue struct {
	client       *mongo.Client
	dbName       string
	collectionDB *mongo.Database
}

// NewMongoDBQueue creates a new MongoDB queue
func NewMongoDBQueue(connectionString, dbName, username, password string) (*MongoDBQueue, error) {
	// Build connection options with auth if credentials provided
	clientOpts := options.Client().ApplyURI(connectionString)

	if username != "" && password != "" {
		cred := &options.Credential{
			AuthMechanism: "SCRAM-SHA-1",
			Username:      username,
			Password:      password,
			AuthSource:    "admin",
		}
		clientOpts.Auth = cred
	}

	client, err := mongo.Connect(context.Background(), clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	if err := client.Ping(context.Background(), nil); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	return &MongoDBQueue{
		client:       client,
		dbName:       dbName,
		collectionDB: client.Database(dbName),
	}, nil
}

// Enqueue adds data to the queue
func (q *MongoDBQueue) Enqueue(ctx context.Context, componentID string, data Data) error {
	collection := q.collectionDB.Collection(fmt.Sprintf("queue_%s", componentID))

	doc := bson.M{
		"component_id": componentID,
		"data":         data,
		"created_at":   time.Now(),
		"enqueued_at":  time.Now(),
	}

	_, err := collection.InsertOne(ctx, doc)
	return err
}

// Dequeue removes and returns data from the queue
func (q *MongoDBQueue) Dequeue(ctx context.Context, componentID string) (Data, bool, error) {
	collection := q.collectionDB.Collection(fmt.Sprintf("queue_%s", componentID))

	var result bson.M
	err := collection.FindOne(ctx, bson.M{}).Decode(&result)
	if err == mongo.ErrNoDocuments {
		return Data{}, false, nil
	}
	if err != nil {
		return Data{}, false, err
	}

	// Extract data from result
	dataBytes, ok := result["data"].(bson.M)
	if !ok {
		return Data{}, false, fmt.Errorf("invalid data format in queue")
	}

	var data Data
	// Convert bson.M to Data struct
	if payload, ok := dataBytes["payload"]; ok {
		data.Payload = payload
	}
	if traceID, ok := dataBytes["trace_id"]; ok {
		data.TraceID = traceID.(string)
	}
	if timestamp, ok := dataBytes["timestamp"]; ok {
		data.Timestamp = timestamp.(time.Time)
	}
	if metadata, ok := dataBytes["metadata"]; ok {
		data.Metadata = metadata.(map[string]string)
	}

	// Delete the processed document
	_, err = collection.DeleteOne(ctx, bson.M{"_id": result["_id"]})
	if err != nil {
		return Data{}, false, fmt.Errorf("failed to delete processed item: %w", err)
	}

	return data, true, nil
}

// Size returns the current queue size
func (q *MongoDBQueue) Size(ctx context.Context, componentID string) (int, error) {
	collection := q.collectionDB.Collection(fmt.Sprintf("queue_%s", componentID))
	count, err := collection.CountDocuments(ctx, bson.M{})
	return int(count), err
}

// Clear removes all data from the queue
func (q *MongoDBQueue) Clear(ctx context.Context, componentID string) error {
	collection := q.collectionDB.Collection(fmt.Sprintf("queue_%s", componentID))
	_, err := collection.DeleteMany(ctx, bson.M{})
	return err
}

// Close closes the MongoDB connection
func (q *MongoDBQueue) Close() error {
	return q.client.Disconnect(context.Background())
}
