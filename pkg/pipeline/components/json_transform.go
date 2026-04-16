package components

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/Xecutables/Nebula.Conduit/pkg/logger"
	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

// JSONTransformComponent is a processor that applies field-level transformation
// rules to JSON payloads. It auto-detects whether the payload (or a nested path
// within it) is a single object or an array and applies every rule to each object.
//
// Supported rule operations:
//
//	rename      – rename a field:          { "operation":"rename",  "field":"old_name", "to":"new_name" }
//	remove      – delete a field:          { "operation":"remove",  "field":"unwanted_field" }
//	add         – add/overwrite a field:   { "operation":"add",     "field":"status", "value":"active" }
//	             value supports {{var}} templates resolved from the object itself and from pipeline metadata.
//	copy        – copy one field to another: { "operation":"copy",  "field":"source", "to":"dest" }
//	map_values  – map discrete values:     { "operation":"map_values", "field":"status",
//	                                          "mapping":{"Active":"1","Left":"0"}, "default":"unknown" }
//	convert     – type conversion:         { "operation":"convert", "field":"age", "to_type":"string" }
//	             supported to_type: string, number, bool
//	flatten     – pull nested fields up:   { "operation":"flatten", "field":"address", "separator":"_" }
//	             e.g. {"address":{"city":"NY"}} → {"address_city":"NY"}
type JSONTransformComponent struct {
	config    pipeline.ComponentConfig
	rules     []TransformRule
	arrayPath string // dot-path to the array inside the payload; empty = root
	logger    zerolog.Logger
}

// TransformRule defines a single transformation operation.
type TransformRule struct {
	Operation string            `json:"operation"`
	Field     string            `json:"field"`
	To        string            `json:"to,omitempty"`        // rename / copy target
	Value     string            `json:"value,omitempty"`     // add: static or template value
	ToType    string            `json:"to_type,omitempty"`   // convert target type
	Mapping   map[string]string `json:"mapping,omitempty"`   // map_values lookup
	Default   string            `json:"default,omitempty"`   // map_values fallback
	Separator string            `json:"separator,omitempty"` // flatten separator
}

// NewJSONTransformComponent creates a new JSON Transform component.
func NewJSONTransformComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	rulesParam, ok := config.Parameters["rules"]
	if !ok {
		return nil, fmt.Errorf("rules parameter is required")
	}
	rulesList, ok := rulesParam.([]interface{})
	if !ok {
		return nil, fmt.Errorf("rules must be an array")
	}

	var rules []TransformRule
	for i, item := range rulesList {
		m, ok := item.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("rules[%d] must be an object", i)
		}
		rule, err := parseTransformRule(i, m)
		if err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}
	if len(rules) == 0 {
		return nil, fmt.Errorf("at least one transformation rule is required")
	}

	arrayPath := ""
	if ap, ok := config.Parameters["array_path"].(string); ok {
		arrayPath = ap
	}

	return &JSONTransformComponent{
		config:    config,
		rules:     rules,
		arrayPath: arrayPath,
		logger:    logger.InitLogger(),
	}, nil
}

// parseTransformRule validates and builds a TransformRule from a raw map.
func parseTransformRule(idx int, m map[string]interface{}) (TransformRule, error) {
	op := stringFromMap(m, "operation")
	if op == "" {
		return TransformRule{}, fmt.Errorf("rules[%d]: operation is required", idx)
	}

	rule := TransformRule{
		Operation: op,
		Field:     stringFromMap(m, "field"),
		To:        stringFromMap(m, "to"),
		Value:     stringFromMap(m, "value"),
		ToType:    stringFromMap(m, "to_type"),
		Default:   stringFromMap(m, "default"),
		Separator: stringFromMap(m, "separator"),
	}

	// Parse mapping for map_values
	if mapParam, ok := m["mapping"].(map[string]interface{}); ok {
		rule.Mapping = make(map[string]string, len(mapParam))
		for k, v := range mapParam {
			rule.Mapping[k] = fmt.Sprintf("%v", v)
		}
	}

	switch op {
	case "rename":
		if rule.Field == "" || rule.To == "" {
			return TransformRule{}, fmt.Errorf("rules[%d]: rename requires 'field' and 'to'", idx)
		}
	case "remove":
		if rule.Field == "" {
			return TransformRule{}, fmt.Errorf("rules[%d]: remove requires 'field'", idx)
		}
	case "add":
		if rule.Field == "" {
			return TransformRule{}, fmt.Errorf("rules[%d]: add requires 'field'", idx)
		}
	case "copy":
		if rule.Field == "" || rule.To == "" {
			return TransformRule{}, fmt.Errorf("rules[%d]: copy requires 'field' and 'to'", idx)
		}
	case "map_values":
		if rule.Field == "" || len(rule.Mapping) == 0 {
			return TransformRule{}, fmt.Errorf("rules[%d]: map_values requires 'field' and 'mapping'", idx)
		}
	case "convert":
		if rule.Field == "" || rule.ToType == "" {
			return TransformRule{}, fmt.Errorf("rules[%d]: convert requires 'field' and 'to_type'", idx)
		}
		validTypes := map[string]bool{"string": true, "number": true, "bool": true}
		if !validTypes[rule.ToType] {
			return TransformRule{}, fmt.Errorf("rules[%d]: unsupported to_type %q (use string, number, bool)", idx, rule.ToType)
		}
	case "flatten":
		if rule.Field == "" {
			return TransformRule{}, fmt.Errorf("rules[%d]: flatten requires 'field'", idx)
		}
		if rule.Separator == "" {
			rule.Separator = "_"
		}
	default:
		return TransformRule{}, fmt.Errorf("rules[%d]: unsupported operation %q", idx, op)
	}

	return rule, nil
}

// Execute runs the JSON Transform component.
func (j *JSONTransformComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
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

				transformed, err := j.transform(data)
				if err != nil {
					j.logger.Error().Err(err).
						Str("component_id", j.config.ID).
						Msg("JSON transformation failed")
					if !j.config.ContinueOnError {
						return
					}
					// Forward original data on error
					transformed = data
				}

				select {
				case output <- transformed:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return output, nil
}

// transform parses the payload, applies rules, and returns a new Data.
func (j *JSONTransformComponent) transform(data pipeline.Data) (pipeline.Data, error) {
	raw, err := toJSONBytes(data.Payload)
	if err != nil {
		return data, fmt.Errorf("failed to convert payload: %w", err)
	}

	var parsed interface{}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return data, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Navigate to the target if array_path is set
	if j.arrayPath != "" {
		transformed, err := j.transformAtPath(parsed, data.Metadata)
		if err != nil {
			return data, err
		}
		parsed = transformed
	} else {
		// Root-level: detect array vs object
		transformed, err := j.applyToValue(parsed, data.Metadata)
		if err != nil {
			return data, err
		}
		parsed = transformed
	}

	// Marshal back
	out, err := json.Marshal(parsed)
	if err != nil {
		return data, fmt.Errorf("failed to marshal transformed JSON: %w", err)
	}

	result := pipeline.Data{
		Payload:   out,
		Metadata:  make(map[string]string),
		Timestamp: time.Now(),
		TraceID:   data.TraceID,
	}
	for k, v := range data.Metadata {
		result.Metadata[k] = v
	}
	result.Metadata["json_transformed"] = "true"

	return result, nil
}

// transformAtPath navigates to array_path, transforms the value there, and
// re-assembles the full structure.
func (j *JSONTransformComponent) transformAtPath(root interface{}, metadata map[string]string) (interface{}, error) {
	parts := strings.Split(j.arrayPath, ".")
	return j.transformNested(root, parts, metadata)
}

// transformNested recursively walks the path, transforms the leaf, and rebuilds.
func (j *JSONTransformComponent) transformNested(current interface{}, path []string, metadata map[string]string) (interface{}, error) {
	if len(path) == 0 {
		return j.applyToValue(current, metadata)
	}

	m, ok := current.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected object at path segment %q, got %T", path[0], current)
	}

	key := path[0]
	child, exists := m[key]
	if !exists {
		return nil, fmt.Errorf("key %q not found in object", key)
	}

	transformed, err := j.transformNested(child, path[1:], metadata)
	if err != nil {
		return nil, err
	}

	// Rebuild the map with the transformed child
	result := make(map[string]interface{}, len(m))
	for k, v := range m {
		result[k] = v
	}
	result[key] = transformed
	return result, nil
}

// applyToValue detects array vs single object and applies rules accordingly.
func (j *JSONTransformComponent) applyToValue(val interface{}, metadata map[string]string) (interface{}, error) {
	switch v := val.(type) {
	case []interface{}:
		for i, elem := range v {
			obj, ok := elem.(map[string]interface{})
			if !ok {
				continue // skip non-object array elements
			}
			transformed, err := j.applyRulesToObject(obj, metadata)
			if err != nil {
				return nil, fmt.Errorf("element[%d]: %w", i, err)
			}
			v[i] = transformed
		}
		return v, nil
	case map[string]interface{}:
		return j.applyRulesToObject(v, metadata)
	default:
		return nil, fmt.Errorf("payload is neither an object nor an array (got %T)", val)
	}
}

// applyRulesToObject applies all transformation rules to a single JSON object.
func (j *JSONTransformComponent) applyRulesToObject(obj map[string]interface{}, metadata map[string]string) (map[string]interface{}, error) {
	for _, rule := range j.rules {
		var err error
		obj, err = applyOneRule(obj, rule, metadata)
		if err != nil {
			return nil, fmt.Errorf("operation %q on field %q: %w", rule.Operation, rule.Field, err)
		}
	}
	return obj, nil
}

// applyOneRule applies a single transformation rule to an object.
func applyOneRule(obj map[string]interface{}, rule TransformRule, metadata map[string]string) (map[string]interface{}, error) {
	switch rule.Operation {
	case "rename":
		if val, exists := obj[rule.Field]; exists {
			delete(obj, rule.Field)
			obj[rule.To] = val
		}

	case "remove":
		delete(obj, rule.Field)

	case "add":
		// Resolve template variables from the object fields and metadata
		vars := buildTemplateVars(obj, metadata)
		obj[rule.Field] = resolveTemplate(rule.Value, vars)

	case "copy":
		if val, exists := obj[rule.Field]; exists {
			obj[rule.To] = val
		}

	case "map_values":
		if val, exists := obj[rule.Field]; exists {
			strVal := fmt.Sprintf("%v", val)
			if mapped, ok := rule.Mapping[strVal]; ok {
				obj[rule.Field] = mapped
			} else if rule.Default != "" {
				obj[rule.Field] = rule.Default
			}
		}

	case "convert":
		if val, exists := obj[rule.Field]; exists {
			converted, err := convertValue(val, rule.ToType)
			if err != nil {
				return obj, err
			}
			obj[rule.Field] = converted
		}

	case "flatten":
		if nested, ok := obj[rule.Field].(map[string]interface{}); ok {
			delete(obj, rule.Field)
			for k, v := range nested {
				obj[rule.Field+rule.Separator+k] = v
			}
		}
	}

	return obj, nil
}

// buildTemplateVars creates a flat string map from the object fields and metadata
// for template resolution in "add" operations.
func buildTemplateVars(obj map[string]interface{}, metadata map[string]string) map[string]string {
	vars := make(map[string]string)
	for k, v := range metadata {
		vars[k] = v
	}
	for k, v := range obj {
		switch val := v.(type) {
		case string:
			vars[k] = val
		default:
			vars[k] = fmt.Sprintf("%v", val)
		}
	}
	return vars
}

// convertValue converts a value to the target type.
func convertValue(val interface{}, toType string) (interface{}, error) {
	switch toType {
	case "string":
		return fmt.Sprintf("%v", val), nil
	case "number":
		switch v := val.(type) {
		case float64:
			return v, nil
		case string:
			var f float64
			if _, err := fmt.Sscanf(v, "%f", &f); err != nil {
				return nil, fmt.Errorf("cannot convert %q to number", v)
			}
			return f, nil
		case bool:
			if v {
				return float64(1), nil
			}
			return float64(0), nil
		default:
			return nil, fmt.Errorf("cannot convert %T to number", val)
		}
	case "bool":
		switch v := val.(type) {
		case bool:
			return v, nil
		case string:
			switch strings.ToLower(v) {
			case "true", "1", "yes":
				return true, nil
			case "false", "0", "no", "":
				return false, nil
			default:
				return nil, fmt.Errorf("cannot convert %q to bool", v)
			}
		case float64:
			return v != 0, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to bool", val)
		}
	default:
		return nil, fmt.Errorf("unsupported target type %q", toType)
	}
}

// Validate checks if the component configuration is valid.
func (j *JSONTransformComponent) Validate() error {
	if len(j.rules) == 0 {
		return fmt.Errorf("at least one transformation rule is required")
	}
	return nil
}

func (j *JSONTransformComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeJSONTransform
}

func (j *JSONTransformComponent) ID() string                       { return j.config.ID }
func (j *JSONTransformComponent) Config() pipeline.ComponentConfig { return j.config }

var _ pipeline.Component = (*JSONTransformComponent)(nil)
