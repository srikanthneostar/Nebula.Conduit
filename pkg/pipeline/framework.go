package pipeline

import (
	"context"
	"fmt"
	"time"
)

// --- Base Component (shared by all three categories) ---

// BaseComponent provides common boilerplate for all component types.
// Embed this in your custom component struct to get ID(), Type(), Config(),
// and a default Validate() for free.
type BaseComponent struct {
	Cfg ComponentConfig
}

func (b *BaseComponent) ID() string              { return b.Cfg.ID }
func (b *BaseComponent) Type() ComponentType     { return b.Cfg.Type }
func (b *BaseComponent) Config() ComponentConfig { return b.Cfg }
func (b *BaseComponent) Validate() error         { return nil }

// --- Source Framework ---

// SourceFunc is the only method you implement for a custom source.
// Call emit() for each piece of data you want to send downstream.
// Return nil when done, or return an error to signal failure.
type SourceFunc func(ctx context.Context, emit func(Data) error) error

// BaseSource is an embeddable base for source components.
// Set the Generate field to your SourceFunc and you're done.
type BaseSource struct {
	BaseComponent
	Generate SourceFunc
}

func (s *BaseSource) Execute(ctx context.Context, input <-chan Data) (<-chan Data, error) {
	return s.Start(ctx)
}

func (s *BaseSource) Start(ctx context.Context) (<-chan Data, error) {
	if s.Generate == nil {
		return nil, fmt.Errorf("component %s: Generate function is not set", s.ID())
	}
	output := make(chan Data, 100)
	go func() {
		defer close(output)
		emit := func(d Data) error {
			if d.Timestamp.IsZero() {
				d.Timestamp = time.Now()
			}
			if d.Metadata == nil {
				d.Metadata = make(map[string]string)
			}
			select {
			case output <- d:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		_ = s.Generate(ctx, emit)
	}()
	return output, nil
}

// --- Processor Framework ---

// TransformFunc is the only method you implement for a custom processor.
// It receives one Data item and returns the transformed Data.
type TransformFunc func(ctx context.Context, input Data) (Data, error)

// BaseProcessor is an embeddable base for processor components.
// Set the Transform field to your TransformFunc and you're done.
type BaseProcessor struct {
	BaseComponent
	Transform TransformFunc
}

func (p *BaseProcessor) Execute(ctx context.Context, input <-chan Data) (<-chan Data, error) {
	return p.Process(ctx, input)
}

func (p *BaseProcessor) Process(ctx context.Context, input <-chan Data) (<-chan Data, error) {
	if p.Transform == nil {
		return nil, fmt.Errorf("component %s: Transform function is not set", p.ID())
	}
	output := make(chan Data, 100)
	go func() {
		defer close(output)
		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-input:
				if !ok {
					return
				}
				result, err := p.Transform(ctx, d)
				if err != nil {
					if !p.Cfg.ContinueOnError {
						return
					}
					continue
				}
				if result.Timestamp.IsZero() {
					result.Timestamp = time.Now()
				}
				if result.Metadata == nil {
					result.Metadata = d.Metadata
				}
				select {
				case output <- result:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output, nil
}

// --- Sink Framework ---

// ConsumeFunc is the only method you implement for a custom sink.
// It receives one Data item and writes it to the destination.
type ConsumeFunc func(ctx context.Context, input Data) error

// BaseSink is an embeddable base for sink components.
// Set the Consume field to your ConsumeFunc and you're done.
type BaseSink struct {
	BaseComponent
	Consume ConsumeFunc
}

func (s *BaseSink) Execute(ctx context.Context, input <-chan Data) (<-chan Data, error) {
	err := s.Write(ctx, input)
	return nil, err
}

func (s *BaseSink) Write(ctx context.Context, input <-chan Data) error {
	if s.Consume == nil {
		return fmt.Errorf("component %s: Consume function is not set", s.ID())
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-input:
			if !ok {
				return nil
			}
			if err := s.Consume(ctx, d); err != nil {
				if !s.Cfg.ContinueOnError {
					return err
				}
			}
		}
	}
}
