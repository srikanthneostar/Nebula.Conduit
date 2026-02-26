package pipeline

import (
	"context"
	"fmt"
	"time"
)

// RetryConfig holds retry configuration
type RetryConfig struct {
	MaxRetries int
	Delay      time.Duration
	Backoff    bool // Use exponential backoff
}

// RetryableFunc is a function that can be retried
type RetryableFunc func(ctx context.Context) error

// WithRetry executes a function with retry logic
func WithRetry(ctx context.Context, config RetryConfig, fn RetryableFunc) error {
	var lastErr error

	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		// Check context before attempting
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Execute the function
		err := fn(ctx)
		if err == nil {
			return nil
		}

		lastErr = err

		// Don't retry if this was the last attempt
		if attempt == config.MaxRetries {
			break
		}

		// Calculate delay with exponential backoff if enabled
		delay := config.Delay
		if config.Backoff && attempt > 0 {
			delay = config.Delay * time.Duration(1<<uint(attempt))
		}

		// Wait before retrying
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			// Continue to next attempt
		}
	}

	return fmt.Errorf("failed after %d attempts: %w", config.MaxRetries+1, lastErr)
}

// ComponentExecutor wraps component execution with retry and error handling
type ComponentExecutor struct {
	component Component
	config    ComponentConfig
}

// NewComponentExecutor creates a new component executor
func NewComponentExecutor(component Component, config ComponentConfig) *ComponentExecutor {
	return &ComponentExecutor{
		component: component,
		config:    config,
	}
}

// Execute runs the component with retry logic
func (ce *ComponentExecutor) Execute(ctx context.Context, input <-chan Data) (<-chan Data, error) {
	retryConfig := RetryConfig{
		MaxRetries: ce.config.RetryCount,
		Delay:      ce.config.RetryDelay,
		Backoff:    true,
	}

	var output <-chan Data
	var execErr error

	err := WithRetry(ctx, retryConfig, func(ctx context.Context) error {
		var err error
		output, err = ce.component.Execute(ctx, input)
		execErr = err
		return err
	})

	if err != nil {
		if ce.config.ContinueOnError {
			// Log error but return empty channel to continue pipeline
			fmt.Printf("component %s failed but continuing: %v\n", ce.component.ID(), err)
			// Return empty channel that closes immediately
			emptyChan := make(chan Data)
			close(emptyChan)
			return emptyChan, nil
		}
		return nil, err
	}

	return output, execErr
}

// ExecuteSource runs a source component with retry logic
func (ce *ComponentExecutor) ExecuteSource(ctx context.Context) (<-chan Data, error) {
	fmt.Printf("    ComponentExecutor[%s]: ExecuteSource called\n", ce.component.ID())

	sourceComp, ok := ce.component.(SourceComponent)
	if !ok {
		return nil, fmt.Errorf("component %s is not a source component", ce.component.ID())
	}

	retryConfig := RetryConfig{
		MaxRetries: ce.config.RetryCount,
		Delay:      ce.config.RetryDelay,
		Backoff:    true,
	}

	var output <-chan Data
	var execErr error

	err := WithRetry(ctx, retryConfig, func(ctx context.Context) error {
		fmt.Printf("    ComponentExecutor[%s]: Calling Start() method\n", ce.component.ID())
		var err error
		output, err = sourceComp.Start(ctx)
		execErr = err
		if err != nil {
			fmt.Printf("    ComponentExecutor[%s]: Start() failed: %v\n", ce.component.ID(), err)
		} else {
			fmt.Printf("    ComponentExecutor[%s]: Start() succeeded\n", ce.component.ID())
		}
		return err
	})

	if err != nil {
		if ce.config.ContinueOnError {
			fmt.Printf("source component %s failed but continuing: %v\n", ce.component.ID(), err)
			emptyChan := make(chan Data)
			close(emptyChan)
			return emptyChan, nil
		}
		return nil, err
	}

	return output, execErr
}

// ExecuteProcessor runs a processor component with retry logic
func (ce *ComponentExecutor) ExecuteProcessor(ctx context.Context, input <-chan Data) (<-chan Data, error) {
	processorComp, ok := ce.component.(ProcessorComponent)
	if !ok {
		return nil, fmt.Errorf("component %s is not a processor component", ce.component.ID())
	}

	retryConfig := RetryConfig{
		MaxRetries: ce.config.RetryCount,
		Delay:      ce.config.RetryDelay,
		Backoff:    true,
	}

	var output <-chan Data
	var execErr error

	err := WithRetry(ctx, retryConfig, func(ctx context.Context) error {
		var err error
		output, err = processorComp.Process(ctx, input)
		execErr = err
		return err
	})

	if err != nil {
		if ce.config.ContinueOnError {
			fmt.Printf("processor component %s failed but continuing: %v\n", ce.component.ID(), err)
			emptyChan := make(chan Data)
			close(emptyChan)
			return emptyChan, nil
		}
		return nil, err
	}

	return output, execErr
}

// ExecuteSink runs a sink component with retry logic
func (ce *ComponentExecutor) ExecuteSink(ctx context.Context, input <-chan Data) error {
	fmt.Printf("    ComponentExecutor[%s]: ExecuteSink called\n", ce.component.ID())

	sinkComp, ok := ce.component.(SinkComponent)
	if !ok {
		return fmt.Errorf("component %s is not a sink component", ce.component.ID())
	}

	retryConfig := RetryConfig{
		MaxRetries: ce.config.RetryCount,
		Delay:      ce.config.RetryDelay,
		Backoff:    true,
	}

	err := WithRetry(ctx, retryConfig, func(ctx context.Context) error {
		fmt.Printf("    ComponentExecutor[%s]: Calling Write() method\n", ce.component.ID())
		err := sinkComp.Write(ctx, input)
		if err != nil {
			fmt.Printf("    ComponentExecutor[%s]: Write() failed: %v\n", ce.component.ID(), err)
		} else {
			fmt.Printf("    ComponentExecutor[%s]: Write() completed successfully\n", ce.component.ID())
		}
		return err
	})

	if err != nil {
		if ce.config.ContinueOnError {
			fmt.Printf("sink component %s failed but continuing: %v\n", ce.component.ID(), err)
			return nil
		}
		return err
	}

	return nil
}
