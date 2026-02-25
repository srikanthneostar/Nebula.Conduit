package pipeline

import (
	"testing"
)

func TestNewComponentFactory(t *testing.T) {
	factory := NewComponentFactory()
	if factory == nil {
		t.Fatal("NewComponentFactory returned nil")
	}

	// Verify factory starts with no registered types
	types := factory.ListTypes()
	if len(types) != 0 {
		t.Errorf("Expected 0 registered types, got %d", len(types))
	}
}

func TestComponentFactory_Register(t *testing.T) {
	factory := NewComponentFactory()

	// Register a component type
	factory.Register(ComponentTypeHTTPGet, mockConstructor)

	// Verify it's registered
	types := factory.ListTypes()
	if len(types) != 1 {
		t.Fatalf("Expected 1 registered type, got %d", len(types))
	}
	if types[0] != ComponentTypeHTTPGet {
		t.Errorf("Expected type %s, got %s", ComponentTypeHTTPGet, types[0])
	}
}

func TestComponentFactory_RegisterMultiple(t *testing.T) {
	factory := NewComponentFactory()

	// Register multiple component types
	factory.Register(ComponentTypeHTTPGet, mockConstructor)
	factory.Register(ComponentTypeHTTPPost, mockConstructor)
	factory.Register(ComponentTypeLog, mockConstructor)

	// Verify all are registered
	types := factory.ListTypes()
	if len(types) != 3 {
		t.Fatalf("Expected 3 registered types, got %d", len(types))
	}

	// Verify all expected types are present
	typeMap := make(map[ComponentType]bool)
	for _, typ := range types {
		typeMap[typ] = true
	}

	expectedTypes := []ComponentType{
		ComponentTypeHTTPGet,
		ComponentTypeHTTPPost,
		ComponentTypeLog,
	}

	for _, expected := range expectedTypes {
		if !typeMap[expected] {
			t.Errorf("Expected type %s not found in registered types", expected)
		}
	}
}

func TestComponentFactory_Create(t *testing.T) {
	factory := NewComponentFactory()
	factory.Register(ComponentTypeHTTPGet, mockConstructor)

	config := ComponentConfig{
		ID:   "test-component",
		Type: ComponentTypeHTTPGet,
		Parameters: map[string]interface{}{
			"url": "https://example.com",
		},
	}

	component, err := factory.Create(config)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if component == nil {
		t.Fatal("Create returned nil component")
	}

	if component.ID() != config.ID {
		t.Errorf("Expected component ID %s, got %s", config.ID, component.ID())
	}

	if component.Type() != config.Type {
		t.Errorf("Expected component type %s, got %s", config.Type, component.Type())
	}
}

func TestComponentFactory_CreateUnregisteredType(t *testing.T) {
	factory := NewComponentFactory()

	config := ComponentConfig{
		ID:   "test-component",
		Type: ComponentTypeHTTPGet,
	}

	component, err := factory.Create(config)
	if err == nil {
		t.Fatal("Expected error for unregistered component type, got nil")
	}

	if component != nil {
		t.Error("Expected nil component for unregistered type")
	}

	expectedError := "component type http_get is not registered"
	if err.Error() != expectedError {
		t.Errorf("Expected error message %q, got %q", expectedError, err.Error())
	}
}

func TestComponentFactory_CreateWithFailingConstructor(t *testing.T) {
	factory := NewComponentFactory()
	factory.Register(ComponentTypeHTTPGet, failingConstructor)

	config := ComponentConfig{
		ID:   "test-component",
		Type: ComponentTypeHTTPGet,
	}

	component, err := factory.Create(config)
	if err == nil {
		t.Fatal("Expected error from failing constructor, got nil")
	}

	if component != nil {
		t.Error("Expected nil component from failing constructor")
	}
}

func TestComponentFactory_RegisterOverwrite(t *testing.T) {
	factory := NewComponentFactory()

	// Register a component type
	factory.Register(ComponentTypeHTTPGet, mockConstructor)

	// Register the same type again with a different constructor
	factory.Register(ComponentTypeHTTPGet, failingConstructor)

	// Verify only one type is registered
	types := factory.ListTypes()
	if len(types) != 1 {
		t.Fatalf("Expected 1 registered type, got %d", len(types))
	}

	// Verify the second constructor is used
	config := ComponentConfig{
		ID:   "test-component",
		Type: ComponentTypeHTTPGet,
	}

	_, err := factory.Create(config)
	if err == nil {
		t.Error("Expected error from failing constructor, got nil")
	}
}

func TestComponentFactory_ConcurrentAccess(t *testing.T) {
	factory := NewComponentFactory()

	// Register initial types
	factory.Register(ComponentTypeHTTPGet, mockConstructor)
	factory.Register(ComponentTypeHTTPPost, mockConstructor)

	// Test concurrent registration and creation
	done := make(chan bool)

	// Goroutine 1: Register types
	go func() {
		for i := 0; i < 100; i++ {
			factory.Register(ComponentTypeLog, mockConstructor)
		}
		done <- true
	}()

	// Goroutine 2: Create components
	go func() {
		for i := 0; i < 100; i++ {
			config := ComponentConfig{
				ID:   "test-component",
				Type: ComponentTypeHTTPGet,
			}
			_, _ = factory.Create(config)
		}
		done <- true
	}()

	// Goroutine 3: List types
	go func() {
		for i := 0; i < 100; i++ {
			_ = factory.ListTypes()
		}
		done <- true
	}()

	// Wait for all goroutines to complete
	<-done
	<-done
	<-done

	// Verify factory is still functional
	types := factory.ListTypes()
	if len(types) < 2 {
		t.Errorf("Expected at least 2 registered types, got %d", len(types))
	}
}
