package components

import (
	"testing"
	"time"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

func TestRegisterComponents(t *testing.T) {
	factory := pipeline.NewComponentFactory()

	// Register all components
	RegisterComponents(factory)

	// Verify HTTP GET component is registered
	types := factory.ListTypes()
	if len(types) == 0 {
		t.Errorf("expected at least one component type to be registered")
	}

	// Verify we can create an HTTP GET component
	config := pipeline.ComponentConfig{
		ID:   "test-http",
		Type: pipeline.ComponentTypeHTTPGet,
		Parameters: map[string]interface{}{
			"url": "https://api.example.com/data",
		},
		Timeout: 10 * time.Second,
	}

	component, err := factory.Create(config)
	if err != nil {
		t.Fatalf("failed to create HTTP GET component: %v", err)
	}

	if component == nil {
		t.Errorf("expected component but got nil")
	}

	if component.Type() != pipeline.ComponentTypeHTTPGet {
		t.Errorf("expected component type %s, got %s", pipeline.ComponentTypeHTTPGet, component.Type())
	}
}

func TestRegisterComponents_HTTPGetIntegration(t *testing.T) {
	factory := pipeline.NewComponentFactory()
	RegisterComponents(factory)

	// Test creating HTTP GET component with various configurations
	tests := []struct {
		name        string
		config      pipeline.ComponentConfig
		expectError bool
	}{
		{
			name: "valid HTTP GET with headers",
			config: pipeline.ComponentConfig{
				ID:   "http-1",
				Type: pipeline.ComponentTypeHTTPGet,
				Parameters: map[string]interface{}{
					"url": "https://api.example.com/data",
					"headers": map[string]interface{}{
						"Authorization": "Bearer token",
					},
				},
			},
			expectError: false,
		},
		{
			name: "valid HTTP GET with interval",
			config: pipeline.ComponentConfig{
				ID:   "http-2",
				Type: pipeline.ComponentTypeHTTPGet,
				Parameters: map[string]interface{}{
					"url":      "https://api.example.com/data",
					"interval": "30s",
				},
			},
			expectError: false,
		},
		{
			name: "invalid HTTP GET missing URL",
			config: pipeline.ComponentConfig{
				ID:         "http-3",
				Type:       pipeline.ComponentTypeHTTPGet,
				Parameters: map[string]interface{}{},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			component, err := factory.Create(tt.config)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if component == nil {
					t.Errorf("expected component but got nil")
				}
			}
		})
	}
}

func TestRegisterComponents_HTTPPostIntegration(t *testing.T) {
	factory := pipeline.NewComponentFactory()
	RegisterComponents(factory)

	// Test creating HTTP POST component with various configurations
	tests := []struct {
		name        string
		config      pipeline.ComponentConfig
		expectError bool
	}{
		{
			name: "valid HTTP POST with all parameters",
			config: pipeline.ComponentConfig{
				ID:   "post-1",
				Type: pipeline.ComponentTypeHTTPPost,
				Parameters: map[string]interface{}{
					"url":          "https://api.example.com/data",
					"content_type": "application/json",
					"headers": map[string]interface{}{
						"Authorization": "Bearer token",
					},
				},
			},
			expectError: false,
		},
		{
			name: "valid HTTP POST with minimal parameters",
			config: pipeline.ComponentConfig{
				ID:   "post-2",
				Type: pipeline.ComponentTypeHTTPPost,
				Parameters: map[string]interface{}{
					"url":          "https://api.example.com/data",
					"content_type": "application/json",
				},
			},
			expectError: false,
		},
		{
			name: "invalid HTTP POST missing URL",
			config: pipeline.ComponentConfig{
				ID:   "post-3",
				Type: pipeline.ComponentTypeHTTPPost,
				Parameters: map[string]interface{}{
					"content_type": "application/json",
				},
			},
			expectError: true,
		},
		{
			name: "invalid HTTP POST missing content_type",
			config: pipeline.ComponentConfig{
				ID:   "post-4",
				Type: pipeline.ComponentTypeHTTPPost,
				Parameters: map[string]interface{}{
					"url": "https://api.example.com/data",
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			component, err := factory.Create(tt.config)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if component == nil {
					t.Errorf("expected component but got nil")
				}
				if component.Type() != pipeline.ComponentTypeHTTPPost {
					t.Errorf("expected component type %s, got %s", pipeline.ComponentTypeHTTPPost, component.Type())
				}
			}
		})
	}
}

func TestRegisterComponents_LogIntegration(t *testing.T) {
	factory := pipeline.NewComponentFactory()
	RegisterComponents(factory)

	// Test creating Log component with various configurations
	tests := []struct {
		name        string
		config      pipeline.ComponentConfig
		expectError bool
	}{
		{
			name: "valid Log with all parameters",
			config: pipeline.ComponentConfig{
				ID:   "log-1",
				Type: pipeline.ComponentTypeLog,
				Parameters: map[string]interface{}{
					"log_level":   "info",
					"format":      "json",
					"sample_size": 1000,
					"truncate":    true,
				},
			},
			expectError: false,
		},
		{
			name: "valid Log with minimal parameters",
			config: pipeline.ComponentConfig{
				ID:   "log-2",
				Type: pipeline.ComponentTypeLog,
				Parameters: map[string]interface{}{
					"log_level": "debug",
				},
			},
			expectError: false,
		},
		{
			name: "invalid Log missing log_level",
			config: pipeline.ComponentConfig{
				ID:         "log-3",
				Type:       pipeline.ComponentTypeLog,
				Parameters: map[string]interface{}{},
			},
			expectError: true,
		},
		{
			name: "invalid Log with invalid log_level",
			config: pipeline.ComponentConfig{
				ID:   "log-4",
				Type: pipeline.ComponentTypeLog,
				Parameters: map[string]interface{}{
					"log_level": "invalid",
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			component, err := factory.Create(tt.config)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if component == nil {
					t.Errorf("expected component but got nil")
				}
				if component.Type() != pipeline.ComponentTypeLog {
					t.Errorf("expected component type %s, got %s", pipeline.ComponentTypeLog, component.Type())
				}
			}
		})
	}
}

func TestRegisterComponents_AllTypesRegistered(t *testing.T) {
	factory := pipeline.NewComponentFactory()
	RegisterComponents(factory)

	// Verify all components are registered (8 total)
	types := factory.ListTypes()

	expectedTypes := map[pipeline.ComponentType]bool{
		pipeline.ComponentTypeHTTPGet:         false,
		pipeline.ComponentTypeHTTPPost:        false,
		pipeline.ComponentTypeLog:             false,
		pipeline.ComponentTypeSQLQuery:        false,
		pipeline.ComponentTypeCSVReader:       false,
		pipeline.ComponentTypePythonCodeBlock: false,
		pipeline.ComponentTypeTCPRead:         false,
		pipeline.ComponentTypeTCPWrite:        false,
	}

	for _, componentType := range types {
		if _, exists := expectedTypes[componentType]; exists {
			expectedTypes[componentType] = true
		}
	}

	// Check that all expected types were found
	for componentType, found := range expectedTypes {
		if !found {
			t.Errorf("component type %s was not registered", componentType)
		}
	}

	// Verify we have exactly 8 types registered
	if len(types) != 8 {
		t.Errorf("expected 8 component types to be registered, got %d", len(types))
	}
}
