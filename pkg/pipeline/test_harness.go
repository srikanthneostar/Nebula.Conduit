package pipeline

import (
	"context"
	"time"
)

// ComponentTestHarness lets you test a component in isolation without
// setting up a full pipeline. Feed it input data, run the component,
// and collect the output.
//
// Usage:
//
//	harness := pipeline.NewTestHarness(myComponent)
//	harness.SendInput(pipeline.NewData("hello", nil))
//	harness.CloseInput()
//	results, err := harness.Run(context.Background())
type ComponentTestHarness struct {
	component Component
	input     chan Data
	timeout   time.Duration
}

// NewTestHarness creates a test harness for the given component.
func NewTestHarness(component Component) *ComponentTestHarness {
	return &ComponentTestHarness{
		component: component,
		input:     make(chan Data, 100),
		timeout:   30 * time.Second,
	}
}

// WithTimeout sets the maximum time the harness will wait for the component to finish.
func (h *ComponentTestHarness) WithTimeout(d time.Duration) *ComponentTestHarness {
	h.timeout = d
	return h
}

// SendInput queues a Data item to be fed to the component.
func (h *ComponentTestHarness) SendInput(d Data) {
	h.input <- d
}

// CloseInput signals that no more input will be sent.
// Call this after all SendInput calls for processors and sinks.
func (h *ComponentTestHarness) CloseInput() {
	close(h.input)
}

// Run executes the component and collects all output Data items.
// For source components, input is ignored.
// For sink components, output will be empty (returns nil slice, nil error on success).
func (h *ComponentTestHarness) Run(ctx context.Context) ([]Data, error) {
	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	// Try as source first
	if src, ok := h.component.(SourceComponent); ok {
		outCh, err := src.Start(ctx)
		if err != nil {
			return nil, err
		}
		return collectChannel(ctx, outCh), nil
	}

	// Try as processor
	if proc, ok := h.component.(ProcessorComponent); ok {
		outCh, err := proc.Process(ctx, h.input)
		if err != nil {
			return nil, err
		}
		return collectChannel(ctx, outCh), nil
	}

	// Try as sink
	if sink, ok := h.component.(SinkComponent); ok {
		err := sink.Write(ctx, h.input)
		return nil, err
	}

	// Fallback to generic Execute
	outCh, err := h.component.Execute(ctx, h.input)
	if err != nil {
		return nil, err
	}
	if outCh == nil {
		return nil, nil
	}
	return collectChannel(ctx, outCh), nil
}

// RunSource is a convenience method for testing source components with no input.
func (h *ComponentTestHarness) RunSource(ctx context.Context) ([]Data, error) {
	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	src, ok := h.component.(SourceComponent)
	if !ok {
		// Fall back to Execute with nil input
		outCh, err := h.component.Execute(ctx, nil)
		if err != nil {
			return nil, err
		}
		return collectChannel(ctx, outCh), nil
	}

	outCh, err := src.Start(ctx)
	if err != nil {
		return nil, err
	}
	return collectChannel(ctx, outCh), nil
}

func collectChannel(ctx context.Context, ch <-chan Data) []Data {
	var results []Data
	for {
		select {
		case d, ok := <-ch:
			if !ok {
				return results
			}
			results = append(results, d)
		case <-ctx.Done():
			return results
		}
	}
}
