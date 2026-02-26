package pipeline

import (
	"encoding/json"
	"fmt"
	"time"
)

// --- Parameter extraction helpers ---
// Use these in your component constructor to pull values from ComponentConfig.Parameters
// with type safety, defaults, and clear error messages.

// ParamString extracts a string parameter. Returns defaultVal if not present.
// Returns an error if the key exists but is not a string.
func ParamString(params map[string]interface{}, key string, defaultVal string) (string, error) {
	v, ok := params[key]
	if !ok {
		return defaultVal, nil
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("parameter %q must be a string, got %T", key, v)
	}
	return s, nil
}

// ParamStringRequired extracts a required string parameter.
func ParamStringRequired(params map[string]interface{}, key string) (string, error) {
	v, ok := params[key]
	if !ok {
		return "", fmt.Errorf("parameter %q is required", key)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("parameter %q must be a string, got %T", key, v)
	}
	if s == "" {
		return "", fmt.Errorf("parameter %q cannot be empty", key)
	}
	return s, nil
}

// ParamInt extracts an int parameter. JSON numbers arrive as float64.
func ParamInt(params map[string]interface{}, key string, defaultVal int) (int, error) {
	v, ok := params[key]
	if !ok {
		return defaultVal, nil
	}
	switch n := v.(type) {
	case float64:
		return int(n), nil
	case int:
		return n, nil
	case json.Number:
		i, err := n.Int64()
		return int(i), err
	default:
		return 0, fmt.Errorf("parameter %q must be a number, got %T", key, v)
	}
}

// ParamFloat extracts a float64 parameter.
func ParamFloat(params map[string]interface{}, key string, defaultVal float64) (float64, error) {
	v, ok := params[key]
	if !ok {
		return defaultVal, nil
	}
	switch n := v.(type) {
	case float64:
		return n, nil
	case int:
		return float64(n), nil
	case json.Number:
		return n.Float64()
	default:
		return 0, fmt.Errorf("parameter %q must be a number, got %T", key, v)
	}
}

// ParamBool extracts a bool parameter.
func ParamBool(params map[string]interface{}, key string, defaultVal bool) (bool, error) {
	v, ok := params[key]
	if !ok {
		return defaultVal, nil
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("parameter %q must be a bool, got %T", key, v)
	}
	return b, nil
}

// ParamStringSlice extracts a []string parameter (JSON arrays arrive as []interface{}).
func ParamStringSlice(params map[string]interface{}, key string, defaultVal []string) ([]string, error) {
	v, ok := params[key]
	if !ok {
		return defaultVal, nil
	}
	switch arr := v.(type) {
	case []interface{}:
		result := make([]string, 0, len(arr))
		for i, item := range arr {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("parameter %q[%d] must be a string, got %T", key, i, item)
			}
			result = append(result, s)
		}
		return result, nil
	case []string:
		return arr, nil
	default:
		return nil, fmt.Errorf("parameter %q must be an array of strings, got %T", key, v)
	}
}

// ParamMap extracts a map[string]string parameter.
func ParamMap(params map[string]interface{}, key string, defaultVal map[string]string) (map[string]string, error) {
	v, ok := params[key]
	if !ok {
		return defaultVal, nil
	}
	m, ok := v.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("parameter %q must be an object, got %T", key, v)
	}
	result := make(map[string]string, len(m))
	for k, val := range m {
		s, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("parameter %q.%s must be a string, got %T", key, k, val)
		}
		result[k] = s
	}
	return result, nil
}

// ParamDuration extracts a time.Duration from a string parameter (e.g. "30s", "5m").
func ParamDuration(params map[string]interface{}, key string, defaultVal time.Duration) (time.Duration, error) {
	v, ok := params[key]
	if !ok {
		return defaultVal, nil
	}
	s, ok := v.(string)
	if !ok {
		return 0, fmt.Errorf("parameter %q must be a duration string, got %T", key, v)
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("parameter %q: invalid duration %q: %w", key, s, err)
	}
	return d, nil
}

// --- Data conversion helpers ---
// Use these to convert Data.Payload to/from common types.

// DataToJSON marshals Data.Payload to a JSON byte slice.
func DataToJSON(d Data) ([]byte, error) {
	return json.Marshal(d.Payload)
}

// DataFromJSON creates a Data with Payload unmarshaled from JSON bytes.
func DataFromJSON(raw []byte) (Data, error) {
	var payload interface{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return Data{}, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}
	return Data{
		Payload:   payload,
		Metadata:  make(map[string]string),
		Timestamp: time.Now(),
	}, nil
}

// DataToBytes converts Data.Payload to []byte.
func DataToBytes(d Data) ([]byte, error) {
	switch v := d.Payload.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return json.Marshal(v)
	}
}

// DataFromBytes creates a Data with a []byte payload.
func DataFromBytes(b []byte) Data {
	return Data{
		Payload:   b,
		Metadata:  make(map[string]string),
		Timestamp: time.Now(),
	}
}

// DataToString converts Data.Payload to a string.
func DataToString(d Data) (string, error) {
	switch v := d.Payload.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
}

// NewData is a convenience constructor for creating Data with payload and optional metadata.
func NewData(payload interface{}, metadata map[string]string) Data {
	if metadata == nil {
		metadata = make(map[string]string)
	}
	return Data{
		Payload:   payload,
		Metadata:  metadata,
		Timestamp: time.Now(),
	}
}
