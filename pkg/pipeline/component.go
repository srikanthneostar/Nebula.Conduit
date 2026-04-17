package pipeline

import "context"

// Component is the base interface all pipeline components must implement
type Component interface {
	// Execute runs the component logic with the given context and input channel
	// Returns an output channel for downstream components
	Execute(ctx context.Context, input <-chan Data) (<-chan Data, error)

	// Validate checks if the component configuration is valid
	Validate() error

	// Type returns the component type identifier
	Type() ComponentType

	// ID returns the unique component instance identifier
	ID() string

	// Config returns the component configuration
	Config() ComponentConfig
}

// SourceComponent generates or reads data from external systems
type SourceComponent interface {
	Component
	// Start begins data generation/reading
	Start(ctx context.Context) (<-chan Data, error)
}

// ProcessorComponent transforms data between sources and sinks
type ProcessorComponent interface {
	Component
	// Process transforms input data to output data
	Process(ctx context.Context, input <-chan Data) (<-chan Data, error)
}

// SinkComponent writes or sends data to external systems
type SinkComponent interface {
	Component
	// Write consumes data and writes to destination
	Write(ctx context.Context, input <-chan Data) error
}

// LogStoreInjectable is implemented by components that need a LogStore
// (e.g. print_log). The executor injects the store before execution.
type LogStoreInjectable interface {
	SetLogStore(store LogStore)
}
