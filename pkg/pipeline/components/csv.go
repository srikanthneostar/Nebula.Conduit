package components

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

// CSVReaderComponent reads data from CSV files
type CSVReaderComponent struct {
	config    pipeline.ComponentConfig
	filePath  string
	delimiter rune
	hasHeader bool
	encoding  string
}

// NewCSVReaderComponent creates a new CSV Reader component
func NewCSVReaderComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	// Extract file path from parameters
	filePath, ok := config.Parameters["file_path"].(string)
	if !ok || filePath == "" {
		return nil, fmt.Errorf("file_path parameter is required and must be a string")
	}

	// Extract delimiter (optional, defaults to comma)
	delimiter := ','
	if delimiterParam, ok := config.Parameters["delimiter"].(string); ok && len(delimiterParam) > 0 {
		delimiter = rune(delimiterParam[0])
	}

	// Extract has_header flag (optional, defaults to true)
	hasHeader := true
	if hasHeaderParam, ok := config.Parameters["has_header"].(bool); ok {
		hasHeader = hasHeaderParam
	}

	// Extract encoding (optional, defaults to UTF-8)
	encoding := "utf-8"
	if encodingParam, ok := config.Parameters["encoding"].(string); ok && encodingParam != "" {
		encoding = encodingParam
	}

	return &CSVReaderComponent{
		config:    config,
		filePath:  filePath,
		delimiter: delimiter,
		hasHeader: hasHeader,
		encoding:  encoding,
	}, nil
}

// Execute runs the CSV Reader component logic
func (c *CSVReaderComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data, 10)

	go func() {
		defer close(output)

		if err := c.readAndSend(ctx, output); err != nil {
			// Log error but continue if continue_on_error is true
			if !c.config.ContinueOnError {
				return
			}
		}
	}()

	return output, nil
}

// readAndSend reads the CSV file and sends data to output channel
func (c *CSVReaderComponent) readAndSend(ctx context.Context, output chan<- pipeline.Data) error {
	// Open the CSV file
	file, err := os.Open(c.filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)
	reader.Comma = c.delimiter
	reader.TrimLeadingSpace = true

	// Read header if present
	var headers []string
	if c.hasHeader {
		headers, err = reader.Read()
		if err != nil {
			return fmt.Errorf("failed to read header: %w", err)
		}
	}

	// Read all rows
	var results []map[string]interface{}
	rowNumber := 0

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read row %d: %w", rowNumber, err)
		}

		rowNumber++

		// Create row map
		var rowMap map[string]interface{}
		if c.hasHeader && len(headers) > 0 {
			// Use headers as keys
			rowMap = make(map[string]interface{})
			for i, value := range record {
				if i < len(headers) {
					rowMap[headers[i]] = value
				} else {
					// Handle extra columns beyond headers
					rowMap[fmt.Sprintf("column_%d", i)] = value
				}
			}
		} else {
			// Use column indices as keys
			rowMap = make(map[string]interface{})
			for i, value := range record {
				rowMap[fmt.Sprintf("column_%d", i)] = value
			}
		}

		results = append(results, rowMap)
	}

	// Convert results to JSON bytes
	jsonData, err := json.Marshal(results)
	if err != nil {
		return fmt.Errorf("failed to marshal results to JSON: %w", err)
	}

	// Create data payload
	data := pipeline.Data{
		Payload:   jsonData,
		Metadata:  make(map[string]string),
		Timestamp: time.Now(),
		TraceID:   c.config.ID,
	}

	// Add metadata
	data.Metadata["row_count"] = fmt.Sprintf("%d", len(results))
	data.Metadata["file_path"] = c.filePath
	data.Metadata["has_header"] = fmt.Sprintf("%t", c.hasHeader)
	if c.hasHeader && len(headers) > 0 {
		data.Metadata["column_count"] = fmt.Sprintf("%d", len(headers))
	}

	// Send data to output channel
	select {
	case output <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Validate checks if the component configuration is valid
func (c *CSVReaderComponent) Validate() error {
	if c.filePath == "" {
		return fmt.Errorf("file_path is required")
	}

	// Check if file exists
	if _, err := os.Stat(c.filePath); os.IsNotExist(err) {
		return fmt.Errorf("file does not exist: %s", c.filePath)
	}

	return nil
}

// Type returns the component type identifier
func (c *CSVReaderComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeCSVReader
}

// ID returns the unique component instance identifier
func (c *CSVReaderComponent) ID() string {
	return c.config.ID
}

// Config returns the component configuration
func (c *CSVReaderComponent) Config() pipeline.ComponentConfig {
	return c.config
}
