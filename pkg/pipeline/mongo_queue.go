package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDBQueue implements DataQueue using MongoDB
type MongoDBQueue struct {
	client       *mongo.Client
	dbName       string
	collectionDB *mongo.Database
}

type queueEnvelope struct {
	ID          interface{} `bson:"_id,omitempty"`
	ComponentID string      `bson:"component_id"`
	Data        storedData  `bson:"data"`
	CreatedAt   time.Time   `bson:"created_at"`
	EnqueuedAt  time.Time   `bson:"enqueued_at"`
}

type storedData struct {
	Payload   []byte            `bson:"payload"`
	Metadata  map[string]string `bson:"metadata"`
	Timestamp time.Time         `bson:"timestamp"`
	TraceID   string            `bson:"trace_id"`
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

	stored, err := newStoredData(data)
	if err != nil {
		return fmt.Errorf("failed to serialize queue data: %w", err)
	}

	now := time.Now()
	doc := queueEnvelope{
		ComponentID: componentID,
		Data:        stored,
		CreatedAt:   now,
		EnqueuedAt:  now,
	}

	_, err = collection.InsertOne(ctx, doc)
	return err
}

// Dequeue removes and returns data from the queue
func (q *MongoDBQueue) Dequeue(ctx context.Context, componentID string) (Data, bool, error) {
	collection := q.collectionDB.Collection(fmt.Sprintf("queue_%s", componentID))

	var result queueEnvelope
	err := collection.FindOne(ctx, bson.M{}).Decode(&result)
	if err == mongo.ErrNoDocuments {
		return Data{}, false, nil
	}
	if err != nil {
		// Fall back to the legacy bson.M representation so existing queued
		// documents are still readable after the safer format ships.
		var legacyResult bson.M
		if legacyErr := collection.FindOne(ctx, bson.M{}).Decode(&legacyResult); legacyErr != nil {
			return Data{}, false, err
		}
		data, id, legacyErr := decodeLegacyQueueDocument(legacyResult)
		if legacyErr != nil {
			return Data{}, false, legacyErr
		}
		if err := q.deleteQueueDocument(ctx, collection, id); err != nil {
			return Data{}, false, err
		}
		return data, true, nil
	}

	data, err := result.Data.toData()
	if err != nil {
		return Data{}, false, fmt.Errorf("failed to decode queue data: %w", err)
	}

	if err := q.deleteQueueDocument(ctx, collection, result.ID); err != nil {
		return Data{}, false, err
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

func newStoredData(data Data) (storedData, error) {
	payloadBytes, err := json.Marshal(data.Payload)
	if err != nil {
		return storedData{}, err
	}

	metadata := data.Metadata
	if metadata == nil {
		metadata = map[string]string{}
	}

	return storedData{
		Payload:   payloadBytes,
		Metadata:  metadata,
		Timestamp: data.Timestamp,
		TraceID:   data.TraceID,
	}, nil
}

func (d storedData) toData() (Data, error) {
	var payload interface{}
	if len(d.Payload) > 0 {
		if err := json.Unmarshal(d.Payload, &payload); err != nil {
			return Data{}, err
		}
	}

	metadata := d.Metadata
	if metadata == nil {
		metadata = map[string]string{}
	}

	return Data{
		Payload:   payload,
		Metadata:  metadata,
		Timestamp: d.Timestamp,
		TraceID:   d.TraceID,
	}, nil
}

func decodeLegacyQueueDocument(result bson.M) (Data, interface{}, error) {
	dataMap, ok := asBSONMap(result["data"])
	if !ok {
		return Data{}, nil, fmt.Errorf("invalid data format in queue")
	}

	metadata, err := coerceMetadataMap(dataMap["metadata"])
	if err != nil {
		return Data{}, nil, err
	}

	timestamp, err := coerceTime(dataMap["timestamp"])
	if err != nil {
		return Data{}, nil, err
	}

	traceID, err := coerceString(dataMap["trace_id"])
	if err != nil {
		return Data{}, nil, err
	}

	return Data{
		Payload:   dataMap["payload"],
		Metadata:  metadata,
		Timestamp: timestamp,
		TraceID:   traceID,
	}, result["_id"], nil
}

func (q *MongoDBQueue) deleteQueueDocument(ctx context.Context, collection *mongo.Collection, id interface{}) error {
	_, err := collection.DeleteOne(ctx, bson.M{"_id": id})
	if err != nil {
		return fmt.Errorf("failed to delete processed item: %w", err)
	}
	return nil
}

func asBSONMap(value interface{}) (bson.M, bool) {
	switch v := value.(type) {
	case bson.M:
		return v, true
	case map[string]interface{}:
		return bson.M(v), true
	default:
		return nil, false
	}
}

func coerceString(value interface{}) (string, error) {
	if value == nil {
		return "", nil
	}

	switch v := value.(type) {
	case string:
		return v, nil
	default:
		return "", fmt.Errorf("expected string value, got %T", value)
	}
}

func coerceTime(value interface{}) (time.Time, error) {
	if value == nil {
		return time.Time{}, nil
	}

	switch v := value.(type) {
	case time.Time:
		return v, nil
	case primitive.DateTime:
		return v.Time(), nil
	default:
		return time.Time{}, fmt.Errorf("expected time value, got %T", value)
	}
}

func coerceMetadataMap(value interface{}) (map[string]string, error) {
	if value == nil {
		return map[string]string{}, nil
	}

	switch v := value.(type) {
	case map[string]string:
		return v, nil
	case bson.M:
		return coerceMetadataMap(map[string]interface{}(v))
	case map[string]interface{}:
		metadata := make(map[string]string, len(v))
		for key, raw := range v {
			strValue, ok := raw.(string)
			if !ok {
				return nil, fmt.Errorf("metadata value for %q must be a string, got %T", key, raw)
			}
			metadata[key] = strValue
		}
		return metadata, nil
	default:
		return nil, fmt.Errorf("expected metadata map, got %T", value)
	}
}
