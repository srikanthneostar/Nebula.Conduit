package components

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

// helper to run a single Data through the transform component and return the parsed result
func runTransform(t *testing.T, params map[string]interface{}, payload string, metadata map[string]string) map[string]interface{} {
	t.Helper()
	config := pipeline.ComponentConfig{
		ID:         "test_transform",
		Type:       pipeline.ComponentTypeJSONTransform,
		Parameters: params,
	}
	comp, err := NewJSONTransformComponent(config)
	if err != nil {
		t.Fatalf("create component: %v", err)
	}

	input := make(chan pipeline.Data, 1)
	if metadata == nil {
		metadata = map[string]string{}
	}
	input <- pipeline.Data{Payload: []byte(payload), Metadata: metadata, Timestamp: time.Now()}
	close(input)

	out, err := comp.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	result := <-out

	var parsed map[string]interface{}
	raw, _ := toJSONBytes(result.Payload)
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	return parsed
}

func runTransformArray(t *testing.T, params map[string]interface{}, payload string) []interface{} {
	t.Helper()
	config := pipeline.ComponentConfig{
		ID:         "test_transform",
		Type:       pipeline.ComponentTypeJSONTransform,
		Parameters: params,
	}
	comp, err := NewJSONTransformComponent(config)
	if err != nil {
		t.Fatalf("create component: %v", err)
	}

	input := make(chan pipeline.Data, 1)
	input <- pipeline.Data{Payload: []byte(payload), Metadata: map[string]string{}, Timestamp: time.Now()}
	close(input)

	out, err := comp.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	result := <-out

	var parsed []interface{}
	raw, _ := toJSONBytes(result.Payload)
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	return parsed
}

func TestJSONTransform_Rename(t *testing.T) {
	params := map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{"operation": "rename", "field": "employee_name", "to": "full_name"},
		},
	}
	result := runTransform(t, params, `{"employee_name":"John","age":30}`, nil)

	if _, exists := result["employee_name"]; exists {
		t.Error("old field 'employee_name' should be removed")
	}
	if result["full_name"] != "John" {
		t.Errorf("full_name = %v, want John", result["full_name"])
	}
}

func TestJSONTransform_Remove(t *testing.T) {
	params := map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{"operation": "remove", "field": "secret"},
		},
	}
	result := runTransform(t, params, `{"name":"Alice","secret":"xyz"}`, nil)

	if _, exists := result["secret"]; exists {
		t.Error("field 'secret' should be removed")
	}
	if result["name"] != "Alice" {
		t.Errorf("name = %v, want Alice", result["name"])
	}
}

func TestJSONTransform_Add(t *testing.T) {
	params := map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{"operation": "add", "field": "source", "value": "frappe"},
		},
	}
	result := runTransform(t, params, `{"name":"Bob"}`, nil)

	if result["source"] != "frappe" {
		t.Errorf("source = %v, want frappe", result["source"])
	}
}

func TestJSONTransform_AddWithTemplate(t *testing.T) {
	params := map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{"operation": "add", "field": "greeting", "value": "Hello {{name}} from {{company}}"},
		},
	}
	result := runTransform(t, params, `{"name":"Alice","company":"Acme"}`, nil)

	if result["greeting"] != "Hello Alice from Acme" {
		t.Errorf("greeting = %v, want 'Hello Alice from Acme'", result["greeting"])
	}
}

func TestJSONTransform_Copy(t *testing.T) {
	params := map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{"operation": "copy", "field": "email", "to": "primary_email"},
		},
	}
	result := runTransform(t, params, `{"email":"a@b.com"}`, nil)

	if result["email"] != "a@b.com" {
		t.Error("original field should remain")
	}
	if result["primary_email"] != "a@b.com" {
		t.Errorf("primary_email = %v, want a@b.com", result["primary_email"])
	}
}

func TestJSONTransform_MapValues(t *testing.T) {
	params := map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{
				"operation": "map_values",
				"field":     "status",
				"mapping":   map[string]interface{}{"Active": "1", "Left": "0"},
				"default":   "unknown",
			},
		},
	}

	// Test mapped value
	result := runTransform(t, params, `{"status":"Active"}`, nil)
	if result["status"] != "1" {
		t.Errorf("status = %v, want 1", result["status"])
	}

	// Test default
	result2 := runTransform(t, params, `{"status":"Suspended"}`, nil)
	if result2["status"] != "unknown" {
		t.Errorf("status = %v, want unknown", result2["status"])
	}
}

func TestJSONTransform_Convert(t *testing.T) {
	params := map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{"operation": "convert", "field": "age", "to_type": "string"},
		},
	}
	result := runTransform(t, params, `{"age":30}`, nil)

	if result["age"] != "30" {
		t.Errorf("age = %v (%T), want string '30'", result["age"], result["age"])
	}
}

func TestJSONTransform_Flatten(t *testing.T) {
	params := map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{"operation": "flatten", "field": "address", "separator": "_"},
		},
	}
	result := runTransform(t, params, `{"name":"X","address":{"city":"NY","zip":"10001"}}`, nil)

	if _, exists := result["address"]; exists {
		t.Error("nested 'address' should be removed after flatten")
	}
	if result["address_city"] != "NY" {
		t.Errorf("address_city = %v, want NY", result["address_city"])
	}
	if result["address_zip"] != "10001" {
		t.Errorf("address_zip = %v, want 10001", result["address_zip"])
	}
}

func TestJSONTransform_ArrayPayload(t *testing.T) {
	params := map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{"operation": "rename", "field": "old", "to": "new"},
			map[string]interface{}{"operation": "remove", "field": "drop"},
		},
	}
	payload := `[{"old":"v1","drop":"x","keep":"y"},{"old":"v2","drop":"z","keep":"w"}]`
	result := runTransformArray(t, params, payload)

	if len(result) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(result))
	}
	elem0 := result[0].(map[string]interface{})
	if elem0["new"] != "v1" {
		t.Errorf("elem[0].new = %v, want v1", elem0["new"])
	}
	if _, exists := elem0["old"]; exists {
		t.Error("elem[0].old should be renamed")
	}
	if _, exists := elem0["drop"]; exists {
		t.Error("elem[0].drop should be removed")
	}
}

func TestJSONTransform_NestedArrayPath(t *testing.T) {
	params := map[string]interface{}{
		"array_path": "data",
		"rules": []interface{}{
			map[string]interface{}{"operation": "add", "field": "processed", "value": "true"},
		},
	}
	payload := `{"data":[{"id":1},{"id":2}],"meta":"keep"}`
	result := runTransform(t, params, payload, nil)

	// meta should be preserved
	if result["meta"] != "keep" {
		t.Errorf("meta = %v, want keep", result["meta"])
	}

	arr, ok := result["data"].([]interface{})
	if !ok {
		t.Fatal("data should be an array")
	}
	for i, elem := range arr {
		obj := elem.(map[string]interface{})
		if obj["processed"] != "true" {
			t.Errorf("data[%d].processed = %v, want true", i, obj["processed"])
		}
	}
}

func TestJSONTransform_MultipleRulesChained(t *testing.T) {
	// Simulate Frappe employee transformation
	params := map[string]interface{}{
		"rules": []interface{}{
			map[string]interface{}{"operation": "rename", "field": "employee_name", "to": "full_name"},
			map[string]interface{}{"operation": "rename", "field": "company_email", "to": "email"},
			map[string]interface{}{"operation": "remove", "field": "modified_by"},
			map[string]interface{}{
				"operation": "map_values",
				"field":     "status",
				"mapping":   map[string]interface{}{"Active": "active", "Left": "inactive"},
				"default":   "unknown",
			},
			map[string]interface{}{"operation": "add", "field": "source_system", "value": "frappe_hr"},
		},
	}

	payload := `{"employee_name":"John Doe","company_email":"john@acme.com","status":"Active","modified_by":"admin","department":"Engineering"}`
	result := runTransform(t, params, payload, nil)

	if result["full_name"] != "John Doe" {
		t.Errorf("full_name = %v", result["full_name"])
	}
	if result["email"] != "john@acme.com" {
		t.Errorf("email = %v", result["email"])
	}
	if result["status"] != "active" {
		t.Errorf("status = %v", result["status"])
	}
	if _, exists := result["modified_by"]; exists {
		t.Error("modified_by should be removed")
	}
	if result["source_system"] != "frappe_hr" {
		t.Errorf("source_system = %v", result["source_system"])
	}
	if result["department"] != "Engineering" {
		t.Error("untouched fields should be preserved")
	}
}

func TestJSONTransform_ValidationErrors(t *testing.T) {
	tests := []struct {
		name   string
		params map[string]interface{}
	}{
		{"missing rules", map[string]interface{}{}},
		{"empty rules", map[string]interface{}{"rules": []interface{}{}}},
		{"rename missing to", map[string]interface{}{"rules": []interface{}{
			map[string]interface{}{"operation": "rename", "field": "x"},
		}}},
		{"unknown operation", map[string]interface{}{"rules": []interface{}{
			map[string]interface{}{"operation": "nope", "field": "x"},
		}}},
		{"convert bad type", map[string]interface{}{"rules": []interface{}{
			map[string]interface{}{"operation": "convert", "field": "x", "to_type": "date"},
		}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := pipeline.ComponentConfig{ID: "t", Type: pipeline.ComponentTypeJSONTransform, Parameters: tt.params}
			_, err := NewJSONTransformComponent(config)
			if err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}
