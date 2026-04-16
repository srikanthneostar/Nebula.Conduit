package components

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

func TestJSONExtractorComponent_MaxModified(t *testing.T) {
	// Simulate Frappe API response
	frappeResponse := map[string]interface{}{
		"data": []interface{}{
			map[string]interface{}{"name": "HR-EMP-00001", "modified": "2026-04-10 08:30:00"},
			map[string]interface{}{"name": "HR-EMP-00002", "modified": "2026-04-15 14:22:00"},
			map[string]interface{}{"name": "HR-EMP-00003", "modified": "2026-04-12 11:00:00"},
		},
	}
	payload, _ := json.Marshal(frappeResponse)

	config := pipeline.ComponentConfig{
		ID:   "extract_max_modified",
		Type: pipeline.ComponentTypeJSONExtractor,
		Parameters: map[string]interface{}{
			"extractions": []interface{}{
				map[string]interface{}{
					"array_path": "data",
					"field":      "modified",
					"operation":  "max",
					"output_key": "last_modified",
				},
			},
		},
	}

	comp, err := NewJSONExtractorComponent(config)
	if err != nil {
		t.Fatalf("failed to create component: %v", err)
	}

	// Feed data through the component
	input := make(chan pipeline.Data, 1)
	input <- pipeline.Data{
		Payload:   payload,
		Metadata:  map[string]string{},
		Timestamp: time.Now(),
	}
	close(input)

	ctx := context.Background()
	output, err := comp.Execute(ctx, input)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	result := <-output
	got := result.Metadata["last_modified"]
	want := "2026-04-15 14:22:00"
	if got != want {
		t.Errorf("last_modified = %q, want %q", got, want)
	}
}

func TestJSONExtractorComponent_Count(t *testing.T) {
	payload := `{"data": [{"a":1},{"a":2},{"a":3}]}`

	config := pipeline.ComponentConfig{
		ID:   "count_items",
		Type: pipeline.ComponentTypeJSONExtractor,
		Parameters: map[string]interface{}{
			"extractions": []interface{}{
				map[string]interface{}{
					"array_path": "data",
					"field":      "",
					"operation":  "count",
					"output_key": "record_count",
				},
			},
		},
	}

	comp, err := NewJSONExtractorComponent(config)
	if err != nil {
		t.Fatalf("failed to create component: %v", err)
	}

	input := make(chan pipeline.Data, 1)
	input <- pipeline.Data{Payload: []byte(payload), Metadata: map[string]string{}, Timestamp: time.Now()}
	close(input)

	output, err := comp.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	result := <-output
	if result.Metadata["record_count"] != "3" {
		t.Errorf("record_count = %q, want %q", result.Metadata["record_count"], "3")
	}
}

func TestJSONExtractorComponent_FirstLast(t *testing.T) {
	payload := `{"items": [{"id":"A"},{"id":"B"},{"id":"C"}]}`

	config := pipeline.ComponentConfig{
		ID:   "first_last",
		Type: pipeline.ComponentTypeJSONExtractor,
		Parameters: map[string]interface{}{
			"extractions": []interface{}{
				map[string]interface{}{
					"array_path": "items",
					"field":      "id",
					"operation":  "first",
					"output_key": "first_id",
				},
				map[string]interface{}{
					"array_path": "items",
					"field":      "id",
					"operation":  "last",
					"output_key": "last_id",
				},
			},
		},
	}

	comp, err := NewJSONExtractorComponent(config)
	if err != nil {
		t.Fatalf("failed to create component: %v", err)
	}

	input := make(chan pipeline.Data, 1)
	input <- pipeline.Data{Payload: []byte(payload), Metadata: map[string]string{}, Timestamp: time.Now()}
	close(input)

	output, err := comp.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	result := <-output
	if result.Metadata["first_id"] != "A" {
		t.Errorf("first_id = %q, want %q", result.Metadata["first_id"], "A")
	}
	if result.Metadata["last_id"] != "C" {
		t.Errorf("last_id = %q, want %q", result.Metadata["last_id"], "C")
	}
}

func TestJSONExtractorComponent_RootArray(t *testing.T) {
	// Test with root-level array (no array_path)
	payload := `[{"val":"x"},{"val":"z"},{"val":"y"}]`

	config := pipeline.ComponentConfig{
		ID:   "root_array",
		Type: pipeline.ComponentTypeJSONExtractor,
		Parameters: map[string]interface{}{
			"extractions": []interface{}{
				map[string]interface{}{
					"array_path": "",
					"field":      "val",
					"operation":  "max",
					"output_key": "max_val",
				},
			},
		},
	}

	comp, err := NewJSONExtractorComponent(config)
	if err != nil {
		t.Fatalf("failed to create component: %v", err)
	}

	input := make(chan pipeline.Data, 1)
	input <- pipeline.Data{Payload: []byte(payload), Metadata: map[string]string{}, Timestamp: time.Now()}
	close(input)

	output, err := comp.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	result := <-output
	if result.Metadata["max_val"] != "z" {
		t.Errorf("max_val = %q, want %q", result.Metadata["max_val"], "z")
	}
}

func TestJSONExtractorComponent_ValidationErrors(t *testing.T) {
	tests := []struct {
		name   string
		params map[string]interface{}
	}{
		{
			name:   "missing extractions",
			params: map[string]interface{}{},
		},
		{
			name: "missing output_key",
			params: map[string]interface{}{
				"extractions": []interface{}{
					map[string]interface{}{"field": "x", "operation": "max"},
				},
			},
		},
		{
			name: "missing operation",
			params: map[string]interface{}{
				"extractions": []interface{}{
					map[string]interface{}{"field": "x", "output_key": "y"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := pipeline.ComponentConfig{
				ID:         "test",
				Type:       pipeline.ComponentTypeJSONExtractor,
				Parameters: tt.params,
			}
			_, err := NewJSONExtractorComponent(config)
			if err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}
