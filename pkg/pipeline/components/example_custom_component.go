package components

// This file demonstrates how to build a custom pipeline component using the framework.
// Use this as a template when creating your own source, processor, or sink.
//
// The framework provides three base structs:
//   - pipeline.BaseSource    → implement Generate(ctx, emit) error
//   - pipeline.BaseProcessor → implement Transform(ctx, input) (Data, error)
//   - pipeline.BaseSink      → implement Consume(ctx, input) error
//
// Each base struct handles all the boilerplate: channel setup, goroutine lifecycle,
// context cancellation, ID/Type/Config methods. You only write the business logic.
//
// To self-register (so importing the package auto-registers the component):
//   func init() {
//       pipeline.DefaultRegistry.Register("my_type", NewMyComponent)
//   }

import (
	"context"
	"fmt"
	"strings"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

// ---------------------------------------------------------------------------
// EXAMPLE 1: Custom Source — generates a fixed list of strings
// ---------------------------------------------------------------------------

func NewExampleSource(config pipeline.ComponentConfig) (pipeline.Component, error) {
	// Use helpers to extract parameters with type safety
	message, err := pipeline.ParamString(config.Parameters, "message", "hello world")
	if err != nil {
		return nil, err
	}
	count, err := pipeline.ParamInt(config.Parameters, "count", 1)
	if err != nil {
		return nil, err
	}

	return &pipeline.BaseSource{
		BaseComponent: pipeline.BaseComponent{Cfg: config},
		Generate: func(ctx context.Context, emit func(pipeline.Data) error) error {
			for i := 0; i < count; i++ {
				if err := emit(pipeline.NewData(
					fmt.Sprintf("%s #%d", message, i+1),
					map[string]string{"index": fmt.Sprintf("%d", i)},
				)); err != nil {
					return err
				}
			}
			return nil
		},
	}, nil
}

// ---------------------------------------------------------------------------
// EXAMPLE 2: Custom Processor — uppercases string payloads
// ---------------------------------------------------------------------------

func NewExampleUppercaseProcessor(config pipeline.ComponentConfig) (pipeline.Component, error) {
	return &pipeline.BaseProcessor{
		BaseComponent: pipeline.BaseComponent{Cfg: config},
		Transform: func(ctx context.Context, input pipeline.Data) (pipeline.Data, error) {
			s, err := pipeline.DataToString(input)
			if err != nil {
				return pipeline.Data{}, fmt.Errorf("expected string payload: %w", err)
			}
			return pipeline.NewData(strings.ToUpper(s), input.Metadata), nil
		},
	}, nil
}

// ---------------------------------------------------------------------------
// EXAMPLE 3: Custom Sink — collects data into a slice (useful for testing)
// ---------------------------------------------------------------------------

// CollectorSink collects all received data into Results for inspection.
type CollectorSink struct {
	pipeline.BaseSink
	Results []pipeline.Data
}

func NewExampleCollectorSink(config pipeline.ComponentConfig) (pipeline.Component, error) {
	sink := &CollectorSink{}
	sink.BaseComponent = pipeline.BaseComponent{Cfg: config}
	sink.Consume = func(ctx context.Context, input pipeline.Data) error {
		sink.Results = append(sink.Results, input)
		return nil
	}
	return sink, nil
}

// ---------------------------------------------------------------------------
// Self-registration: uncomment the init() below to auto-register these
// components when this package is imported.
// ---------------------------------------------------------------------------
//
// func init() {
//     pipeline.DefaultRegistry.Register("example_source", NewExampleSource)
//     pipeline.DefaultRegistry.Register("example_uppercase", NewExampleUppercaseProcessor)
//     pipeline.DefaultRegistry.Register("example_collector", NewExampleCollectorSink)
// }
