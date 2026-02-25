package pipeline

import (
	"fmt"
	"sync"
)

// ComponentConstructor is a function that creates a component from configuration
type ComponentConstructor func(config ComponentConfig) (Component, error)

// ComponentFactory creates component instances from configurations
type ComponentFactory interface {
	// Create instantiates a component from its configuration
	Create(config ComponentConfig) (Component, error)

	// Register adds a new component type to the factory
	Register(componentType ComponentType, constructor ComponentConstructor)

	// ListTypes returns all registered component types
	ListTypes() []ComponentType
}

// defaultComponentFactory is the default implementation of ComponentFactory
type defaultComponentFactory struct {
	mu           sync.RWMutex
	constructors map[ComponentType]ComponentConstructor
}

// NewComponentFactory creates a new component factory instance
func NewComponentFactory() ComponentFactory {
	return &defaultComponentFactory{
		constructors: make(map[ComponentType]ComponentConstructor),
	}
}

// Create instantiates a component from its configuration
func (f *defaultComponentFactory) Create(config ComponentConfig) (Component, error) {
	f.mu.RLock()
	constructor, exists := f.constructors[config.Type]
	f.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("component type %s is not registered", config.Type)
	}

	component, err := constructor(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create component %s: %w", config.Type, err)
	}

	return component, nil
}

// Register adds a new component type to the factory
func (f *defaultComponentFactory) Register(componentType ComponentType, constructor ComponentConstructor) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.constructors[componentType] = constructor
}

// ListTypes returns all registered component types
func (f *defaultComponentFactory) ListTypes() []ComponentType {
	f.mu.RLock()
	defer f.mu.RUnlock()

	types := make([]ComponentType, 0, len(f.constructors))
	for componentType := range f.constructors {
		types = append(types, componentType)
	}
	return types
}
