package pipeline

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestStoredDataRoundTrip(t *testing.T) {
	timestamp := time.Date(2026, 4, 22, 16, 30, 0, 0, time.UTC)
	original := Data{
		Payload: map[string]interface{}{
			"count": float64(3),
			"label": "queued",
		},
		Metadata: map[string]string{
			"source": "test",
		},
		Timestamp: timestamp,
		TraceID:   "trace-123",
	}

	stored, err := newStoredData(original)
	if err != nil {
		t.Fatalf("newStoredData() error = %v", err)
	}

	restored, err := stored.toData()
	if err != nil {
		t.Fatalf("stored.toData() error = %v", err)
	}

	if restored.TraceID != original.TraceID {
		t.Fatalf("expected trace id %q, got %q", original.TraceID, restored.TraceID)
	}
	if !restored.Timestamp.Equal(original.Timestamp) {
		t.Fatalf("expected timestamp %v, got %v", original.Timestamp, restored.Timestamp)
	}
	if restored.Metadata["source"] != "test" {
		t.Fatalf("expected metadata to survive round-trip, got %#v", restored.Metadata)
	}

	payloadMap, ok := restored.Payload.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map payload, got %T", restored.Payload)
	}
	if payloadMap["label"] != "queued" {
		t.Fatalf("expected payload label %q, got %#v", "queued", payloadMap["label"])
	}
}

func TestDecodeLegacyQueueDocumentHandlesTypicalBSONShapes(t *testing.T) {
	timestamp := time.Date(2026, 4, 22, 16, 45, 0, 0, time.UTC)
	document := bson.M{
		"_id": primitive.NewObjectID(),
		"data": bson.M{
			"payload": bson.M{
				"message": "ok",
			},
			"trace_id":  "legacy-trace",
			"timestamp": primitive.NewDateTimeFromTime(timestamp),
			"metadata": primitive.M{
				"tenant": "acme",
			},
		},
	}

	data, _, err := decodeLegacyQueueDocument(document)
	if err != nil {
		t.Fatalf("decodeLegacyQueueDocument() error = %v", err)
	}

	if data.TraceID != "legacy-trace" {
		t.Fatalf("expected trace id legacy-trace, got %q", data.TraceID)
	}
	if !data.Timestamp.Equal(timestamp) {
		t.Fatalf("expected timestamp %v, got %v", timestamp, data.Timestamp)
	}
	if data.Metadata["tenant"] != "acme" {
		t.Fatalf("expected metadata tenant acme, got %#v", data.Metadata)
	}

	payloadMap, ok := data.Payload.(bson.M)
	if !ok {
		t.Fatalf("expected bson.M payload, got %T", data.Payload)
	}
	if payloadMap["message"] != "ok" {
		t.Fatalf("expected payload message ok, got %#v", payloadMap["message"])
	}
}

func TestDecodeLegacyQueueDocumentRejectsInvalidMetadataTypes(t *testing.T) {
	document := bson.M{
		"_id": primitive.NewObjectID(),
		"data": bson.M{
			"payload":   "value",
			"trace_id":  "trace",
			"timestamp": time.Now(),
			"metadata": bson.M{
				"attempts": 3,
			},
		},
	}

	_, _, err := decodeLegacyQueueDocument(document)
	if err == nil {
		t.Fatal("expected invalid metadata type to return an error")
	}
}
