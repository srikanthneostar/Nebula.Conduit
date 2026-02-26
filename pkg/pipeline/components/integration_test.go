package components

import (
	"context"
	"testing"
	"time"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

// TestBasicComponentsIntegration tests that all three basic components
// (HTTP GET, HTTP POST, Log) can be created through the factory and work together
func TestBasicComponentsIntegration(t *testing.T) {
	factory := pipeline.NewComponentFactory()
	RegisterComponents(factory)

	// Verify all components are registered (15 total)
	types := factory.ListTypes()
	if len(types) != 15 {
		t.Fatalf("expected 15 component types, got %d", len(types))
	}

	// Test 1: Create HTTP GET component
	httpGetConfig := pipeline.ComponentConfig{
		ID:   "http-get-1",
		Type: pipeline.ComponentTypeHTTPGet,
		Parameters: map[string]interface{}{
			"url": "https://api.example.com/data",
			"headers": map[string]interface{}{
				"Authorization": "Bearer token123",
			},
		},
		Timeout: 30 * time.Second,
	}

	httpGetComponent, err := factory.Create(httpGetConfig)
	if err != nil {
		t.Fatalf("failed to create HTTP GET component: %v", err)
	}

	if httpGetComponent.Type() != pipeline.ComponentTypeHTTPGet {
		t.Errorf("expected HTTP GET component type, got %s", httpGetComponent.Type())
	}

	if err := httpGetComponent.Validate(); err != nil {
		t.Errorf("HTTP GET component validation failed: %v", err)
	}

	// Test 2: Create Log component
	logConfig := pipeline.ComponentConfig{
		ID:   "log-1",
		Type: pipeline.ComponentTypeLog,
		Parameters: map[string]interface{}{
			"log_level":   "info",
			"format":      "json",
			"sample_size": 1000,
			"truncate":    true,
		},
	}

	logComponent, err := factory.Create(logConfig)
	if err != nil {
		t.Fatalf("failed to create Log component: %v", err)
	}

	if logComponent.Type() != pipeline.ComponentTypeLog {
		t.Errorf("expected Log component type, got %s", logComponent.Type())
	}

	if err := logComponent.Validate(); err != nil {
		t.Errorf("Log component validation failed: %v", err)
	}

	// Test 3: Create HTTP POST component
	httpPostConfig := pipeline.ComponentConfig{
		ID:   "http-post-1",
		Type: pipeline.ComponentTypeHTTPPost,
		Parameters: map[string]interface{}{
			"url":          "https://api.example.com/sink",
			"content_type": "application/json",
			"headers": map[string]interface{}{
				"Authorization": "Bearer token456",
			},
		},
		Timeout: 30 * time.Second,
	}

	httpPostComponent, err := factory.Create(httpPostConfig)
	if err != nil {
		t.Fatalf("failed to create HTTP POST component: %v", err)
	}

	if httpPostComponent.Type() != pipeline.ComponentTypeHTTPPost {
		t.Errorf("expected HTTP POST component type, got %s", httpPostComponent.Type())
	}

	if err := httpPostComponent.Validate(); err != nil {
		t.Errorf("HTTP POST component validation failed: %v", err)
	}

	// Test 4: Verify component IDs match configuration
	if httpGetComponent.ID() != httpGetConfig.ID {
		t.Errorf("HTTP GET component ID mismatch: expected %s, got %s", httpGetConfig.ID, httpGetComponent.ID())
	}

	if logComponent.ID() != logConfig.ID {
		t.Errorf("Log component ID mismatch: expected %s, got %s", logConfig.ID, logComponent.ID())
	}

	if httpPostComponent.ID() != httpPostConfig.ID {
		t.Errorf("HTTP POST component ID mismatch: expected %s, got %s", httpPostConfig.ID, httpPostComponent.ID())
	}

	// Test 5: Verify component configs are preserved
	if httpGetComponent.Config().ID != httpGetConfig.ID {
		t.Errorf("HTTP GET component config not preserved")
	}

	if logComponent.Config().ID != logConfig.ID {
		t.Errorf("Log component config not preserved")
	}

	if httpPostComponent.Config().ID != httpPostConfig.ID {
		t.Errorf("HTTP POST component config not preserved")
	}
}

// TestComponentTypeConstants verifies that all component type constants are defined
func TestComponentTypeConstants(t *testing.T) {
	// Verify basic component type constants exist and have correct values
	tests := []struct {
		name     string
		constant pipeline.ComponentType
		expected string
	}{
		{"HTTP GET", pipeline.ComponentTypeHTTPGet, "http_get"},
		{"HTTP POST", pipeline.ComponentTypeHTTPPost, "http_post"},
		{"Log", pipeline.ComponentTypeLog, "log"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.constant) != tt.expected {
				t.Errorf("expected component type %s, got %s", tt.expected, string(tt.constant))
			}
		})
	}
}

// TestFactoryWithRealComponents tests the factory with actual component implementations
func TestFactoryWithRealComponents(t *testing.T) {
	factory := pipeline.NewComponentFactory()
	RegisterComponents(factory)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a Log component and test its execution
	logConfig := pipeline.ComponentConfig{
		ID:   "test-log",
		Type: pipeline.ComponentTypeLog,
		Parameters: map[string]interface{}{
			"log_level": "info",
			"format":    "json",
		},
	}

	logComponent, err := factory.Create(logConfig)
	if err != nil {
		t.Fatalf("failed to create log component: %v", err)
	}

	// Create input channel and send test data
	input := make(chan pipeline.Data, 1)
	testData := pipeline.Data{
		Payload:   []byte("test payload"),
		Metadata:  map[string]string{"test": "metadata"},
		Timestamp: time.Now(),
		TraceID:   "test-trace-id",
	}
	input <- testData
	close(input)

	// Execute the component
	output, err := logComponent.Execute(ctx, input)
	if err != nil {
		t.Fatalf("failed to execute log component: %v", err)
	}

	// Verify output data matches input (Log component passes data unchanged)
	select {
	case data, ok := <-output:
		if !ok {
			t.Fatal("output channel closed unexpectedly")
		}
		if string(data.Payload.([]byte)) != string(testData.Payload.([]byte)) {
			t.Errorf("expected payload %s, got %s", testData.Payload, data.Payload)
		}
		if data.TraceID != testData.TraceID {
			t.Errorf("expected trace ID %s, got %s", testData.TraceID, data.TraceID)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for output data")
	}
}

// TestComponentCreationErrors tests error handling in component creation
func TestComponentCreationErrors(t *testing.T) {
	factory := pipeline.NewComponentFactory()
	RegisterComponents(factory)

	tests := []struct {
		name        string
		config      pipeline.ComponentConfig
		expectError bool
		errorMsg    string
	}{
		{
			name: "unregistered component type",
			config: pipeline.ComponentConfig{
				ID:   "test",
				Type: "nonexistent_type",
			},
			expectError: true,
			errorMsg:    "component type nonexistent_type is not registered",
		},
		{
			name: "HTTP GET missing URL",
			config: pipeline.ComponentConfig{
				ID:         "test",
				Type:       pipeline.ComponentTypeHTTPGet,
				Parameters: map[string]interface{}{},
			},
			expectError: true,
		},
		{
			name: "HTTP POST missing content_type",
			config: pipeline.ComponentConfig{
				ID:   "test",
				Type: pipeline.ComponentTypeHTTPPost,
				Parameters: map[string]interface{}{
					"url": "https://example.com",
				},
			},
			expectError: true,
		},
		{
			name: "Log missing log_level",
			config: pipeline.ComponentConfig{
				ID:         "test",
				Type:       pipeline.ComponentTypeLog,
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
				if tt.errorMsg != "" && err.Error() != tt.errorMsg {
					t.Logf("expected error: %s", tt.errorMsg)
					t.Logf("got error: %s", err.Error())
				}
				if component != nil {
					t.Errorf("expected nil component on error")
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
