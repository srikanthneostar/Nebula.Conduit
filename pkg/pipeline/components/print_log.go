package components

import (
	"context"
	"fmt"
	"time"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

// PrintLogComponent is a processor that logs pipeline data to the database
// so it can be queried and displayed in the React frontend.
// Data passes through unchanged to downstream components.
type PrintLogComponent struct {
	config     pipeline.ComponentConfig
	logLevel   string
	message    string // optional static or template message
	pipelineID string
	logStore   pipeline.LogStore
}

// NewPrintLogComponent creates a new Print Log component.
// The LogStore is injected after creation via SetLogStore before execution.
func NewPrintLogComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	logLevel := "info"
	if ll, ok := config.Parameters["log_level"].(string); ok && ll != "" {
		validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
		if !validLevels[ll] {
			return nil, fmt.Errorf("invalid log_level %q: must be one of debug, info, warn, error", ll)
		}
		logLevel = ll
	}

	message := ""
	if msg, ok := config.Parameters["message"].(string); ok {
		message = msg
	}

	pipelineID := ""
	if pid, ok := config.Parameters["pipeline_id"].(string); ok {
		pipelineID = pid
	}

	return &PrintLogComponent{
		config:     config,
		logLevel:   logLevel,
		message:    message,
		pipelineID: pipelineID,
	}, nil
}

// SetLogStore injects the LogStore dependency. Called by the executor before running.
func (p *PrintLogComponent) SetLogStore(store pipeline.LogStore) {
	p.logStore = store
}

// Execute logs each data item to the database and passes it through unchanged.
func (p *PrintLogComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data, 100)

	go func() {
		defer close(output)

		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-input:
				if !ok {
					return
				}

				// Log to database if store is available
				if p.logStore != nil {
					p.logToStore(ctx, data)
				}

				// Always print to stdout for visibility
				p.printToStdout(data)

				// Pass data through unchanged
				select {
				case output <- data:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return output, nil
}

func (p *PrintLogComponent) logToStore(ctx context.Context, data pipeline.Data) {
	// Resolve message template if present
	msg := p.message
	if msg != "" && data.Metadata != nil {
		msg = replaceTemplateVars(msg, data.Metadata)
	}

	entry := pipeline.PipelineLogEntry{
		PipelineID:  p.pipelineID,
		ExecutionID: data.Metadata["execution_id"],
		ComponentID: p.config.ID,
		LogLevel:    p.logLevel,
		Message:     msg,
		Payload:     pipeline.PayloadToLogString(data.Payload),
		Metadata:    pipeline.MetadataToLogString(data.Metadata),
		TraceID:     data.TraceID,
	}

	if err := p.logStore.InsertLog(ctx, entry); err != nil {
		fmt.Printf("PrintLog[%s]: failed to write log to database: %v\n", p.config.ID, err)
	}
}

func (p *PrintLogComponent) printToStdout(data pipeline.Data) {
	ts := time.Now().Format(time.RFC3339)
	payloadStr := pipeline.PayloadToLogString(data.Payload)

	// Truncate for stdout display
	if len(payloadStr) > 500 {
		payloadStr = payloadStr[:500] + "...(truncated)"
	}

	fmt.Printf("[%s] [%s] [%s] trace=%s %s payload=%s\n",
		ts, p.logLevel, p.config.ID, data.TraceID, p.message, payloadStr)
}

func (p *PrintLogComponent) Validate() error {
	validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLevels[p.logLevel] {
		return fmt.Errorf("invalid log_level %q", p.logLevel)
	}
	return nil
}

func (p *PrintLogComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypePrintLog
}

func (p *PrintLogComponent) ID() string                       { return p.config.ID }
func (p *PrintLogComponent) Config() pipeline.ComponentConfig { return p.config }

var _ pipeline.Component = (*PrintLogComponent)(nil)
