package components

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rs/zerolog"

	"github.com/Xecutables/Nebula.Conduit/pkg/logger"
	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

// JSONExtractorComponent is a processor that extracts values from JSON payloads
// and writes them into metadata for downstream template resolution.
//
// Supported operations:
//   - "max": finds the maximum string/datetime value of a field across an array
//   - "min": finds the minimum string/datetime value of a field across an array
//   - "first": takes the value from the first element
//   - "last": takes the value from the last element
//   - "count": returns the number of elements in the array
//
// The extracted value is written to metadata under the configured output key,
// making it available as {{output_key}} in downstream components.
type JSONExtractorComponent struct {
	config      pipeline.ComponentConfig
	extractions []ExtractionRule
	logger      zerolog.Logger
	stateStore  pipeline.StateStore
	pipelineID  string
}

// ExtractionRule defines a single extraction operation
type ExtractionRule struct {
	// ArrayPath is the dot-notation path to the JSON array (e.g. "data" or "response.items").
	// If empty, the root payload is treated as the array.
	ArrayPath string `json:"array_path"`

	// Field is the name of the field to extract from each array element (e.g. "modified").
	Field string `json:"field"`

	// Operation is the aggregation to apply: max, min, first, last, count.
	Operation string `json:"operation"`

	// OutputKey is the metadata key where the result is stored.
	OutputKey string `json:"output_key"`
}

// NewJSONExtractorComponent creates a new JSON Extractor component
func NewJSONExtractorComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	rulesParam, ok := config.Parameters["extractions"]
	if !ok {
		return nil, fmt.Errorf("extractions parameter is required")
	}

	rulesList, ok := rulesParam.([]interface{})
	if !ok {
		return nil, fmt.Errorf("extractions must be an array")
	}

	var extractions []ExtractionRule
	for i, item := range rulesList {
		m, ok := item.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("extractions[%d] must be an object", i)
		}

		rule := ExtractionRule{
			ArrayPath: stringFromMap(m, "array_path"),
			Field:     stringFromMap(m, "field"),
			Operation: stringFromMap(m, "operation"),
			OutputKey: stringFromMap(m, "output_key"),
		}

		if rule.Field == "" && rule.Operation != "count" {
			return nil, fmt.Errorf("extractions[%d]: field is required for operation %q", i, rule.Operation)
		}
		if rule.Operation == "" {
			return nil, fmt.Errorf("extractions[%d]: operation is required", i)
		}
		if rule.OutputKey == "" {
			return nil, fmt.Errorf("extractions[%d]: output_key is required", i)
		}

		extractions = append(extractions, rule)
	}

	if len(extractions) == 0 {
		return nil, fmt.Errorf("at least one extraction rule is required")
	}

	return &JSONExtractorComponent{
		config:      config,
		extractions: extractions,
		logger:      logger.InitLogger(),
	}, nil
}

// Execute runs the JSON Extractor component
func (j *JSONExtractorComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data, 100)

	go func() {
		defer close(output)

		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-input:
				if !ok {
					return
				}

				if err := j.extract(&data); err != nil {
					j.logger.Error().Err(err).
						Str("component_id", j.config.ID).
						Msg("JSON extraction failed")
					if !j.config.ContinueOnError {
						return
					}
					// Still forward the data even on error
				}

				select {
				case output <- data:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return output, nil
}

// extract parses the payload and applies all extraction rules
func (j *JSONExtractorComponent) extract(data *pipeline.Data) error {
	// Parse payload into generic JSON
	raw, err := toJSONBytes(data.Payload)
	if err != nil {
		return fmt.Errorf("failed to convert payload to JSON: %w", err)
	}

	var parsed interface{}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return fmt.Errorf("failed to parse JSON payload: %w", err)
	}

	if data.Metadata == nil {
		data.Metadata = make(map[string]string)
	}

	for i, rule := range j.extractions {
		result, err := j.applyRule(parsed, rule)
		if err != nil {
			return fmt.Errorf("extractions[%d] (%s): %w", i, rule.OutputKey, err)
		}
		data.Metadata[rule.OutputKey] = result
		j.logger.Debug().
			Str("component_id", j.config.ID).
			Str("output_key", rule.OutputKey).
			Str("value", result).
			Msg("Extracted value")

		// Persist extracted value so source components can use it on the next run
		if j.stateStore != nil && j.pipelineID != "" {
			if err := j.stateStore.SaveState(context.Background(), j.pipelineID, j.config.ID, rule.OutputKey, result); err != nil {
				j.logger.Warn().Err(err).
					Str("component_id", j.config.ID).
					Str("output_key", rule.OutputKey).
					Msg("Failed to persist extracted value")
			} else {
				j.logger.Info().
					Str("component_id", j.config.ID).
					Str("output_key", rule.OutputKey).
					Str("value", result).
					Msg("Persisted extracted value for next run")
			}
		}
	}

	return nil
}

// applyRule executes a single extraction rule against the parsed JSON
func (j *JSONExtractorComponent) applyRule(parsed interface{}, rule ExtractionRule) (string, error) {
	// Navigate to the array using the path
	target := parsed
	if rule.ArrayPath != "" {
		var err error
		target, err = navigatePath(parsed, rule.ArrayPath)
		if err != nil {
			return "", fmt.Errorf("cannot navigate to %q: %w", rule.ArrayPath, err)
		}
	}

	arr, ok := target.([]interface{})
	if !ok {
		return "", fmt.Errorf("value at path %q is not an array", rule.ArrayPath)
	}

	if len(arr) == 0 {
		return "", fmt.Errorf("array at path %q is empty", rule.ArrayPath)
	}

	switch rule.Operation {
	case "count":
		return fmt.Sprintf("%d", len(arr)), nil
	case "max":
		return findExtreme(arr, rule.Field, true)
	case "min":
		return findExtreme(arr, rule.Field, false)
	case "first":
		return getFieldFromElement(arr[0], rule.Field)
	case "last":
		return getFieldFromElement(arr[len(arr)-1], rule.Field)
	default:
		return "", fmt.Errorf("unsupported operation %q", rule.Operation)
	}
}

// navigatePath walks a dot-separated path through nested maps
func navigatePath(obj interface{}, path string) (interface{}, error) {
	parts := strings.Split(path, ".")
	current := obj
	for _, part := range parts {
		m, ok := current.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected object at %q, got %T", part, current)
		}
		val, exists := m[part]
		if !exists {
			return nil, fmt.Errorf("key %q not found", part)
		}
		current = val
	}
	return current, nil
}

// findExtreme finds the max or min string value of a field across array elements.
// Works for datetime strings (ISO/Frappe format) and plain strings via lexicographic comparison.
func findExtreme(arr []interface{}, field string, findMax bool) (string, error) {
	var result string
	found := false

	for _, elem := range arr {
		val, err := getFieldFromElement(elem, field)
		if err != nil {
			continue // skip elements missing the field
		}
		if !found {
			result = val
			found = true
			continue
		}
		if findMax && val > result {
			result = val
		} else if !findMax && val < result {
			result = val
		}
	}

	if !found {
		return "", fmt.Errorf("field %q not found in any array element", field)
	}
	return result, nil
}

// getFieldFromElement extracts a string value from a map element
func getFieldFromElement(elem interface{}, field string) (string, error) {
	m, ok := elem.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("array element is not an object")
	}
	val, exists := m[field]
	if !exists {
		return "", fmt.Errorf("field %q not found", field)
	}
	switch v := val.(type) {
	case string:
		return v, nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

// toJSONBytes converts various payload types to JSON bytes
func toJSONBytes(payload interface{}) ([]byte, error) {
	switch v := payload.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return json.Marshal(v)
	}
}

// stringFromMap safely extracts a string from a map
func stringFromMap(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

// Validate checks if the component configuration is valid
func (j *JSONExtractorComponent) Validate() error {
	if len(j.extractions) == 0 {
		return fmt.Errorf("at least one extraction rule is required")
	}
	validOps := map[string]bool{"max": true, "min": true, "first": true, "last": true, "count": true}
	for i, rule := range j.extractions {
		if !validOps[rule.Operation] {
			return fmt.Errorf("extractions[%d]: unsupported operation %q", i, rule.Operation)
		}
		if rule.OutputKey == "" {
			return fmt.Errorf("extractions[%d]: output_key is required", i)
		}
	}
	return nil
}

// Type returns the component type identifier
func (j *JSONExtractorComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeJSONExtractor
}

// ID returns the unique component instance identifier
func (j *JSONExtractorComponent) ID() string {
	return j.config.ID
}

// Config returns the component configuration
func (j *JSONExtractorComponent) Config() pipeline.ComponentConfig {
	return j.config
}

// SetStateStore injects the StateStore dependency.
func (j *JSONExtractorComponent) SetStateStore(store pipeline.StateStore) {
	j.stateStore = store
}

// SetPipelineID injects the pipeline ID for state scoping.
func (j *JSONExtractorComponent) SetPipelineID(pipelineID string) {
	j.pipelineID = pipelineID
}

// Ensure compile-time interface compliance
var _ pipeline.Component = (*JSONExtractorComponent)(nil)
var _ pipeline.StateStoreInjectable = (*JSONExtractorComponent)(nil)
