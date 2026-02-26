package components

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

var templateVarRegex = regexp.MustCompile(`\{\{(\w+(?:\.\w+)*)\}\}`)

// AttributeUpdateComponent is a processor that creates or updates data attributes
// by combining upstream payload/metadata fields using template expressions.
// Downstream components can reference these as {{variable}} in their parameters.
type AttributeUpdateComponent struct {
	config   pipeline.ComponentConfig
	mappings []AttributeMapping
}

// AttributeMapping defines a single attribute derivation rule
type AttributeMapping struct {
	Name       string `json:"name"`       // target attribute name (written to metadata)
	Expression string `json:"expression"` // template expression, e.g. "/logs/{{department}}_report.log"
}

// NewAttributeUpdateComponent creates a new Attribute Update component
func NewAttributeUpdateComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	mappingsParam, ok := config.Parameters["mappings"]
	if !ok {
		return nil, fmt.Errorf("mappings parameter is required")
	}

	mappingsList, ok := mappingsParam.([]interface{})
	if !ok {
		return nil, fmt.Errorf("mappings must be an array")
	}

	var mappings []AttributeMapping
	for i, item := range mappingsList {
		m, ok := item.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("mapping[%d] must be an object with 'name' and 'expression'", i)
		}

		name, _ := m["name"].(string)
		expression, _ := m["expression"].(string)

		if name == "" {
			return nil, fmt.Errorf("mapping[%d] 'name' is required", i)
		}
		if expression == "" {
			return nil, fmt.Errorf("mapping[%d] 'expression' is required", i)
		}

		mappings = append(mappings, AttributeMapping{Name: name, Expression: expression})
	}

	if len(mappings) == 0 {
		return nil, fmt.Errorf("at least one mapping is required")
	}

	return &AttributeUpdateComponent{config: config, mappings: mappings}, nil
}

// Execute runs the Attribute Update component
func (a *AttributeUpdateComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	return a.Process(ctx, input)
}

// Process transforms each data item by evaluating mappings and adding results to metadata
func (a *AttributeUpdateComponent) Process(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
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

				// Build a lookup of all available variables from payload and metadata
				vars := buildVarLookup(data)

				// Ensure metadata map exists
				if data.Metadata == nil {
					data.Metadata = make(map[string]string)
				}

				// Evaluate each mapping and write result to metadata
				for _, m := range a.mappings {
					resolved := resolveTemplate(m.Expression, vars)
					data.Metadata[m.Name] = resolved
					// Also add to vars so later mappings can reference earlier ones
					vars[m.Name] = resolved
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

// buildVarLookup creates a flat key→value map from the Data's payload and metadata.
// Payload fields are accessed by their key name directly.
// Nested map fields are accessed with dot notation: "parent.child".
func buildVarLookup(data pipeline.Data) map[string]string {
	vars := make(map[string]string)

	// Add metadata fields
	for k, v := range data.Metadata {
		vars[k] = v
	}

	// Add built-in variables
	vars["_timestamp"] = data.Timestamp.Format(time.RFC3339)
	vars["_trace_id"] = data.TraceID

	// Flatten payload into string values
	switch payload := data.Payload.(type) {
	case map[string]interface{}:
		flattenMap("", payload, vars)
	case map[string]string:
		for k, v := range payload {
			vars[k] = v
		}
	}

	return vars
}

// flattenMap recursively flattens a nested map into dot-notation keys
func flattenMap(prefix string, m map[string]interface{}, out map[string]string) {
	for k, v := range m {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}
		switch val := v.(type) {
		case map[string]interface{}:
			flattenMap(key, val, out)
		case string:
			out[key] = val
		default:
			out[key] = fmt.Sprintf("%v", val)
		}
	}
}

// resolveTemplate replaces all {{variable}} placeholders in the expression
// with their values from the vars map. Unresolved variables are left as-is.
func resolveTemplate(expression string, vars map[string]string) string {
	return templateVarRegex.ReplaceAllStringFunc(expression, func(match string) string {
		// Strip {{ and }}
		varName := strings.TrimSuffix(strings.TrimPrefix(match, "{{"), "}}")
		if val, ok := vars[varName]; ok {
			return val
		}
		return match // leave unresolved
	})
}

func (a *AttributeUpdateComponent) Validate() error {
	if len(a.mappings) == 0 {
		return fmt.Errorf("at least one mapping is required")
	}
	return nil
}

func (a *AttributeUpdateComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeAttributeUpdate
}

func (a *AttributeUpdateComponent) ID() string                       { return a.config.ID }
func (a *AttributeUpdateComponent) Config() pipeline.ComponentConfig { return a.config }
