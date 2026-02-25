package pipeline

import (
	"context"
	"errors"
)

// mockComponent is a simple mock component for testing
type mockComponent struct {
	id            string
	componentType ComponentType
	config        ComponentConfig
	executeFunc   func(ctx context.Context, input <-chan Data) (<-chan Data, error)
	validateFunc  func() error
}

func (m *mockComponent) Execute(ctx context.Context, input <-chan Data) (<-chan Data, error) {
	if m.executeFunc != nil {
		return m.executeFunc(ctx, input)
	}
	output := make(chan Data)
	close(output)
	return output, nil
}

func (m *mockComponent) Validate() error {
	if m.validateFunc != nil {
		return m.validateFunc()
	}
	return nil
}

func (m *mockComponent) Type() ComponentType {
	return m.componentType
}

func (m *mockComponent) ID() string {
	return m.id
}

func (m *mockComponent) Config() ComponentConfig {
	return m.config
}

// mockSourceComponent is a mock source component
type mockSourceComponent struct {
	mockComponent
	startFunc func(ctx context.Context) (<-chan Data, error)
}

func (m *mockSourceComponent) Start(ctx context.Context) (<-chan Data, error) {
	if m.startFunc != nil {
		return m.startFunc(ctx)
	}
	output := make(chan Data)
	go func() {
		defer close(output)
		output <- Data{Payload: "test data"}
	}()
	return output, nil
}

// mockSinkComponent is a mock sink component
type mockSinkComponent struct {
	mockComponent
	writeFunc func(ctx context.Context, input <-chan Data) error
}

func (m *mockSinkComponent) Write(ctx context.Context, input <-chan Data) error {
	if m.writeFunc != nil {
		return m.writeFunc(ctx, input)
	}
	// Consume all input
	for range input {
	}
	return nil
}

// mockConstructor creates a mock component
func mockConstructor(config ComponentConfig) (Component, error) {
	return &mockComponent{
		id:            config.ID,
		componentType: config.Type,
		config:        config,
	}, nil
}

// failingConstructor always returns an error
func failingConstructor(config ComponentConfig) (Component, error) {
	return nil, errors.New("constructor failed")
}
