package components

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Xecutables/Nebula.Conduit/pkg/logger"
	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
	"github.com/rs/zerolog"
)

// LogComponent logs data for inspection and passes it through unchanged
type LogComponent struct {
	config     pipeline.ComponentConfig
	logLevel   string
	format     string
	sampleSize int
	truncate   bool
	logger     zerolog.Logger
}

// NewLogComponent creates a new Log component
func NewLogComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	// Initialize logger
	appLogger := logger.InitLogger()

	// Extract log level from parameters
	logLevel, ok := config.Parameters["log_level"].(string)
	if !ok || logLevel == "" {
		return nil, fmt.Errorf("log_level parameter is required and must be a string")
	}

	// Validate log level
	validLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}
	if !validLevels[logLevel] {
		return nil, fmt.Errorf("invalid log_level: must be one of debug, info, warn, error")
	}

	// Extract format (optional, defaults to "json")
	format := "json"
	if formatParam, ok := config.Parameters["format"].(string); ok {
		if formatParam != "json" && formatParam != "text" {
			return nil, fmt.Errorf("invalid format: must be either json or text")
		}
		format = formatParam
	}

	// Extract sample size (optional, defaults to 0 for no sampling)
	sampleSize := 0
	if sampleSizeParam, ok := config.Parameters["sample_size"].(float64); ok {
		sampleSize = int(sampleSizeParam)
	} else if sampleSizeParam, ok := config.Parameters["sample_size"].(int); ok {
		sampleSize = sampleSizeParam
	}

	// Extract truncate flag (optional, defaults to false)
	truncate := false
	if truncateParam, ok := config.Parameters["truncate"].(bool); ok {
		truncate = truncateParam
	}

	return &LogComponent{
		config:     config,
		logLevel:   logLevel,
		format:     format,
		sampleSize: sampleSize,
		truncate:   truncate,
		logger:     appLogger,
	}, nil
}

// Execute runs the Log component logic
func (l *LogComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data, 10)

	go func() {
		defer close(output)

		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-input:
				if !ok {
					// Input channel closed, we're done
					return
				}

				// Log the data
				l.logData(data)

				// Pass data unchanged to downstream components
				select {
				case output <- data:
					// Data sent successfully
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return output, nil
}

// logData logs the data at the configured log level
func (l *LogComponent) logData(data pipeline.Data) {
	// Prepare the log data
	logData := l.prepareLogData(data)

	// Create log event based on log level
	var event *zerolog.Event
	switch l.logLevel {
	case "debug":
		event = l.logger.Debug()
	case "info":
		event = l.logger.Info()
	case "warn":
		event = l.logger.Warn()
	case "error":
		event = l.logger.Error()
	default:
		event = l.logger.Info()
	}

	// Add component context
	event = event.
		Str("component_id", l.config.ID).
		Str("component_type", string(l.Type())).
		Str("trace_id", data.TraceID).
		Time("data_timestamp", data.Timestamp)

	// Add metadata
	if len(data.Metadata) > 0 {
		event = event.Interface("metadata", data.Metadata)
	}

	// Add payload based on format
	if l.format == "json" {
		event = event.Interface("payload", logData)
	} else {
		event = event.Str("payload", fmt.Sprintf("%v", logData))
	}

	// Send the log
	event.Msg("Pipeline data logged")
}

// prepareLogData prepares the data for logging with sampling and truncation
func (l *LogComponent) prepareLogData(data pipeline.Data) interface{} {
	payload := data.Payload

	// Apply truncation if enabled
	if l.truncate && l.sampleSize > 0 {
		switch v := payload.(type) {
		case []byte:
			if len(v) > l.sampleSize {
				truncated := make([]byte, l.sampleSize)
				copy(truncated, v)
				return map[string]interface{}{
					"data":          truncated,
					"truncated":     true,
					"original_size": len(v),
				}
			}
		case string:
			if len(v) > l.sampleSize {
				return map[string]interface{}{
					"data":          v[:l.sampleSize],
					"truncated":     true,
					"original_size": len(v),
				}
			}
		case map[string]interface{}:
			// For JSON objects, try to serialize and check size
			jsonBytes, err := json.Marshal(v)
			if err == nil && len(jsonBytes) > l.sampleSize {
				return map[string]interface{}{
					"data":          string(jsonBytes[:l.sampleSize]),
					"truncated":     true,
					"original_size": len(jsonBytes),
				}
			}
		}
	}

	return payload
}

// Validate checks if the component configuration is valid
func (l *LogComponent) Validate() error {
	if l.logLevel == "" {
		return fmt.Errorf("log_level is required")
	}

	validLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}
	if !validLevels[l.logLevel] {
		return fmt.Errorf("invalid log_level: must be one of debug, info, warn, error")
	}

	if l.format != "json" && l.format != "text" {
		return fmt.Errorf("invalid format: must be either json or text")
	}

	if l.sampleSize < 0 {
		return fmt.Errorf("sample_size must be non-negative")
	}

	return nil
}

// Type returns the component type identifier
func (l *LogComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeLog
}

// ID returns the unique component instance identifier
func (l *LogComponent) ID() string {
	return l.config.ID
}

// Config returns the component configuration
func (l *LogComponent) Config() pipeline.ComponentConfig {
	return l.config
}
