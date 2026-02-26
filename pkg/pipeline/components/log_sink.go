package components

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

// LogSinkComponent writes pipeline data to a text log file
type LogSinkComponent struct {
	config   pipeline.ComponentConfig
	filePath string
	logLevel string
	format   string // "json" or "text"
}

// NewLogSinkComponent creates a new Log Sink component
func NewLogSinkComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	filePath, ok := config.Parameters["file_path"].(string)
	if !ok || filePath == "" {
		return nil, fmt.Errorf("file_path parameter is required")
	}

	logLevel, ok := config.Parameters["log_level"].(string)
	if !ok || logLevel == "" {
		return nil, fmt.Errorf("log_level parameter is required")
	}

	format := "text"
	if f, ok := config.Parameters["format"].(string); ok && f != "" {
		format = f
	}

	return &LogSinkComponent{
		config:   config,
		filePath: filePath,
		logLevel: logLevel,
		format:   format,
	}, nil
}

// Execute runs the Log Sink component
func (l *LogSinkComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	return nil, l.Write(ctx, input)
}

// Write consumes data and writes each item to the log file.
// The file_path supports {{variable}} placeholders resolved from data metadata.
// If the path is static (no templates), the file is opened once.
// If dynamic, a new file may be opened per unique resolved path.
func (l *LogSinkComponent) Write(ctx context.Context, input <-chan pipeline.Data) error {
	// For static paths (no templates), open once
	if !templateVarRegex.MatchString(l.filePath) {
		return l.writeStatic(ctx, input)
	}
	return l.writeDynamic(ctx, input)
}

// writeStatic writes all data to a single file
func (l *LogSinkComponent) writeStatic(ctx context.Context, input <-chan pipeline.Data) error {
	if dir := filepath.Dir(l.filePath); dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create log directory: %w", err)
		}
	}

	file, err := os.OpenFile(l.filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}
	defer file.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case data, ok := <-input:
			if !ok {
				return nil
			}
			if _, err := fmt.Fprintln(file, l.formatLine(data)); err != nil {
				if !l.config.ContinueOnError {
					return fmt.Errorf("failed to write to log file: %w", err)
				}
			}
		}
	}
}

// writeDynamic resolves the file path per message from data metadata
func (l *LogSinkComponent) writeDynamic(ctx context.Context, input <-chan pipeline.Data) error {
	openFiles := make(map[string]*os.File)
	defer func() {
		for _, f := range openFiles {
			f.Close()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case data, ok := <-input:
			if !ok {
				return nil
			}

			resolvedPath := resolveTemplate(l.filePath, data.Metadata)

			file, exists := openFiles[resolvedPath]
			if !exists {
				if dir := filepath.Dir(resolvedPath); dir != "" {
					if err := os.MkdirAll(dir, 0755); err != nil {
						if !l.config.ContinueOnError {
							return fmt.Errorf("failed to create directory %s: %w", dir, err)
						}
						continue
					}
				}
				var err error
				file, err = os.OpenFile(resolvedPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
				if err != nil {
					if !l.config.ContinueOnError {
						return fmt.Errorf("failed to open log file %s: %w", resolvedPath, err)
					}
					continue
				}
				openFiles[resolvedPath] = file
			}

			if _, err := fmt.Fprintln(file, l.formatLine(data)); err != nil {
				if !l.config.ContinueOnError {
					return fmt.Errorf("failed to write to log file: %w", err)
				}
			}
		}
	}
}

// formatLine formats a single Data item as a log line
func (l *LogSinkComponent) formatLine(data pipeline.Data) string {
	ts := data.Timestamp.Format(time.RFC3339)

	if l.format == "json" {
		entry := map[string]interface{}{
			"timestamp": ts,
			"level":     l.logLevel,
			"trace_id":  data.TraceID,
			"metadata":  data.Metadata,
			"payload":   data.Payload,
		}
		b, err := json.Marshal(entry)
		if err != nil {
			return fmt.Sprintf("[%s] [%s] marshal error: %v", ts, l.logLevel, err)
		}
		return string(b)
	}

	// Plain text format
	return fmt.Sprintf("[%s] [%s] trace=%s payload=%v", ts, l.logLevel, data.TraceID, data.Payload)
}

func (l *LogSinkComponent) Validate() error {
	if l.filePath == "" {
		return fmt.Errorf("file_path is required")
	}
	if l.logLevel == "" {
		return fmt.Errorf("log_level is required")
	}
	return nil
}

func (l *LogSinkComponent) Type() pipeline.ComponentType     { return pipeline.ComponentTypeLogSink }
func (l *LogSinkComponent) ID() string                       { return l.config.ID }
func (l *LogSinkComponent) Config() pipeline.ComponentConfig { return l.config }
