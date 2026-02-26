package components

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

// LogSinkComponent writes pipeline data to a text log file
type LogSinkComponent struct {
	config        pipeline.ComponentConfig
	filePath      string
	logLevel      string
	format        string // "json" or "text"
	includeData   bool   // if true, include full data payload; if false, only log metadata
	decodePayload bool   // if true, decode JSON byte arrays to objects; if false, keep as-is
}

// NewLogSinkComponent creates a new Log Sink component
func NewLogSinkComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	fmt.Printf("LogSink: NewLogSinkComponent called with config ID: %s\n", config.ID)
	fmt.Printf("LogSink: Parameters: %+v\n", config.Parameters)

	filePath, ok := config.Parameters["file_path"].(string)
	if !ok || filePath == "" {
		return nil, fmt.Errorf("file_path parameter is required")
	}
	fmt.Printf("LogSink: file_path = %s\n", filePath)

	logLevel, ok := config.Parameters["log_level"].(string)
	if !ok || logLevel == "" {
		return nil, fmt.Errorf("log_level parameter is required")
	}
	fmt.Printf("LogSink: log_level = %s\n", logLevel)

	format := "text"
	if f, ok := config.Parameters["format"].(string); ok && f != "" {
		format = f
	}
	fmt.Printf("LogSink: format = %s\n", format)

	// Extract include_data flag (optional, defaults to true for backward compatibility)
	includeData := true
	if includeDataParam, ok := config.Parameters["include_data"].(bool); ok {
		includeData = includeDataParam
	}
	fmt.Printf("LogSink: include_data = %v\n", includeData)

	// Extract decode_payload flag (optional, defaults to true for backward compatibility)
	decodePayload := true
	if decodePayloadParam, ok := config.Parameters["decode_payload"].(bool); ok {
		decodePayload = decodePayloadParam
	}
	fmt.Printf("LogSink: decode_payload = %v\n", decodePayload)

	component := &LogSinkComponent{
		config:        config,
		filePath:      filePath,
		logLevel:      logLevel,
		format:        format,
		includeData:   includeData,
		decodePayload: decodePayload,
	}

	fmt.Printf("LogSink: Component created successfully\n")
	return component, nil
}

// Execute runs the Log Sink component
func (l *LogSinkComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	fmt.Printf("LogSink[%s]: Execute called\n", l.config.ID)
	fmt.Printf("LogSink[%s]: Calling Write method\n", l.config.ID)
	return nil, l.Write(ctx, input)
}

// Write consumes data and writes each item to the log file.
// The file_path supports {{variable}} placeholders resolved from data metadata.
// If the path is static (no templates), the file is opened once.
// If dynamic, a new file may be opened per unique resolved path.
func (l *LogSinkComponent) Write(ctx context.Context, input <-chan pipeline.Data) error {
	fmt.Printf("LogSink[%s]: Write method started\n", l.config.ID)
	fmt.Printf("LogSink[%s]: File path: %s\n", l.config.ID, l.filePath)

	// For static paths (no templates), open once
	if !templateVarRegex.MatchString(l.filePath) {
		fmt.Printf("LogSink[%s]: Using STATIC write mode (no template variables)\n", l.config.ID)
		return l.writeStatic(ctx, input)
	}
	fmt.Printf("LogSink[%s]: Using DYNAMIC write mode (has template variables)\n", l.config.ID)
	return l.writeDynamic(ctx, input)
}

// writeStatic writes all data to a single file
func (l *LogSinkComponent) writeStatic(ctx context.Context, input <-chan pipeline.Data) error {
	fmt.Printf("LogSink[%s]: writeStatic started\n", l.config.ID)
	cleanPath := filepath.Clean(l.filePath)
	fmt.Printf("LogSink[%s]: Cleaned path: %s\n", l.config.ID, cleanPath)

	dir := filepath.Dir(cleanPath)
	if dir != "" && dir != "." {
		fmt.Printf("LogSink[%s]: Creating directory: %s\n", l.config.ID, dir)
		if err := os.MkdirAll(dir, 0755); err != nil {
			fmt.Printf("LogSink[%s]: FAILED to create directory: %v\n", l.config.ID, err)
			return fmt.Errorf("failed to create log directory: %w", err)
		}
		fmt.Printf("LogSink[%s]: Directory created/verified\n", l.config.ID)
	}

	fmt.Printf("LogSink[%s]: Opening file: %s\n", l.config.ID, cleanPath)
	file, err := os.OpenFile(cleanPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("LogSink[%s]: FAILED to open file: %v\n", l.config.ID, err)
		return fmt.Errorf("failed to open log file: %w", err)
	}
	defer file.Close()
	fmt.Printf("LogSink[%s]: File opened successfully, waiting for data...\n", l.config.ID)

	dataCount := 0
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("LogSink[%s]: Context cancelled, wrote %d records\n", l.config.ID, dataCount)
			return ctx.Err()
		case data, ok := <-input:
			if !ok {
				fmt.Printf("LogSink[%s]: Input channel closed, wrote %d records total\n", l.config.ID, dataCount)
				return nil
			}
			fmt.Printf("LogSink[%s]: Received data item %d (TraceID: %s)\n", l.config.ID, dataCount+1, data.TraceID)

			line := l.formatLine(data)
			if _, err := fmt.Fprintln(file, line); err != nil {
				if !l.config.ContinueOnError {
					fmt.Printf("LogSink[%s]: FAILED to write: %v\n", l.config.ID, err)
					return fmt.Errorf("failed to write to log file: %w", err)
				}
				fmt.Printf("LogSink[%s]: Write error (continuing): %v\n", l.config.ID, err)
			} else {
				dataCount++
				fmt.Printf("LogSink[%s]: Successfully wrote record %d\n", l.config.ID, dataCount)
				file.Sync() // Flush to disk
			}
		}
	}
}

// writeDynamic resolves the file path per message from data metadata
func (l *LogSinkComponent) writeDynamic(ctx context.Context, input <-chan pipeline.Data) error {
	fmt.Printf("LogSink[%s]: writeDynamic started\n", l.config.ID)
	openFiles := make(map[string]*os.File)
	defer func() {
		fmt.Printf("LogSink[%s]: Closing %d open files\n", l.config.ID, len(openFiles))
		for path, f := range openFiles {
			fmt.Printf("LogSink[%s]: Closing file: %s\n", l.config.ID, path)
			f.Close()
		}
	}()

	dataCount := 0
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("LogSink[%s]: Context cancelled, wrote %d records\n", l.config.ID, dataCount)
			return ctx.Err()
		case data, ok := <-input:
			if !ok {
				fmt.Printf("LogSink[%s]: Input channel closed, wrote %d records total\n", l.config.ID, dataCount)
				return nil
			}

			fmt.Printf("LogSink[%s]: Received data item %d (TraceID: %s)\n", l.config.ID, dataCount+1, data.TraceID)
			fmt.Printf("LogSink[%s]: Data metadata: %+v\n", l.config.ID, data.Metadata)

			resolvedPath := resolveTemplate(l.filePath, data.Metadata)
			fmt.Printf("LogSink[%s]: Resolved path: %s (from template: %s)\n", l.config.ID, resolvedPath, l.filePath)

			file, exists := openFiles[resolvedPath]
			if !exists {
				fmt.Printf("LogSink[%s]: File not in cache, opening: %s\n", l.config.ID, resolvedPath)

				dir := filepath.Dir(resolvedPath)
				if dir != "" && dir != "." {
					fmt.Printf("LogSink[%s]: Creating directory: %s\n", l.config.ID, dir)
					if err := os.MkdirAll(dir, 0755); err != nil {
						if !l.config.ContinueOnError {
							fmt.Printf("LogSink[%s]: FAILED to create directory: %v\n", l.config.ID, err)
							return fmt.Errorf("failed to create directory %s: %w", dir, err)
						}
						fmt.Printf("LogSink[%s]: Directory creation error (continuing): %v\n", l.config.ID, err)
						continue
					}
					fmt.Printf("LogSink[%s]: Directory created/verified\n", l.config.ID)
				}

				var err error
				file, err = os.OpenFile(resolvedPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
				if err != nil {
					if !l.config.ContinueOnError {
						fmt.Printf("LogSink[%s]: FAILED to open file: %v\n", l.config.ID, err)
						return fmt.Errorf("failed to open log file %s: %w", resolvedPath, err)
					}
					fmt.Printf("LogSink[%s]: File open error (continuing): %v\n", l.config.ID, err)
					continue
				}
				openFiles[resolvedPath] = file
				fmt.Printf("LogSink[%s]: File opened and cached: %s\n", l.config.ID, resolvedPath)
			} else {
				fmt.Printf("LogSink[%s]: Using cached file handle for: %s\n", l.config.ID, resolvedPath)
			}

			line := l.formatLine(data)
			if _, err := fmt.Fprintln(file, line); err != nil {
				if !l.config.ContinueOnError {
					fmt.Printf("LogSink[%s]: FAILED to write: %v\n", l.config.ID, err)
					return fmt.Errorf("failed to write to log file: %w", err)
				}
				fmt.Printf("LogSink[%s]: Write error (continuing): %v\n", l.config.ID, err)
			} else {
				dataCount++
				fmt.Printf("LogSink[%s]: Successfully wrote record %d\n", l.config.ID, dataCount)
				file.Sync() // Flush to disk
			}
		}
	}
}

// formatLine formats a single Data item as a log line
func (l *LogSinkComponent) formatLine(data pipeline.Data) string {
	ts := data.Timestamp.Format(time.RFC3339)

	// Resolve log_level from metadata if it's a template variable
	logLevel := l.logLevel
	if strings.HasPrefix(logLevel, "{{") && strings.HasSuffix(logLevel, "}}") {
		varName := strings.TrimSuffix(strings.TrimPrefix(logLevel, "{{"), "}}")
		if val, ok := data.Metadata[varName]; ok {
			logLevel = val
		}
	}

	if l.format == "json" {
		entry := map[string]interface{}{
			"timestamp": ts,
			"level":     logLevel,
			"trace_id":  data.TraceID,
			"metadata":  data.Metadata,
		}

		// Include payload only if include_data is true
		if l.includeData {
			// Decode payload if decode_payload is true and it's JSON bytes
			var payloadData interface{}
			if l.decodePayload {
				if jsonBytes, ok := data.Payload.([]byte); ok {
					var decoded interface{}
					if err := json.Unmarshal(jsonBytes, &decoded); err == nil {
						payloadData = decoded
					} else {
						payloadData = string(jsonBytes)
					}
				} else {
					payloadData = data.Payload
				}
			} else {
				// Keep payload as-is without decoding
				payloadData = data.Payload
			}
			entry["payload"] = payloadData
		} else {
			entry["message"] = "Data logged (payload excluded)"
		}

		b, err := json.Marshal(entry)
		if err != nil {
			return fmt.Sprintf("[%s] [%s] marshal error: %v", ts, logLevel, err)
		}
		return string(b)
	}

	// Plain text format
	if l.includeData {
		// Decode payload for text format if decode_payload is true
		var payloadStr string
		if l.decodePayload {
			if jsonBytes, ok := data.Payload.([]byte); ok {
				var decoded interface{}
				if err := json.Unmarshal(jsonBytes, &decoded); err == nil {
					payloadStr = fmt.Sprintf("%v", decoded)
				} else {
					payloadStr = string(jsonBytes)
				}
			} else {
				payloadStr = fmt.Sprintf("%v", data.Payload)
			}
		} else {
			// Keep payload as-is without decoding
			payloadStr = fmt.Sprintf("%v", data.Payload)
		}
		return fmt.Sprintf("[%s] [%s] trace=%s payload=%s", ts, logLevel, data.TraceID, payloadStr)
	} else {
		return fmt.Sprintf("[%s] [%s] trace=%s message=Data logged (payload excluded)", ts, logLevel, data.TraceID)
	}
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
