package components

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

// CSVReaderComponent reads data from CSV files
type CSVReaderComponent struct {
	config        pipeline.ComponentConfig
	filePath      string
	delimiter     rune
	hasHeader     bool
	encoding      string
	archiveOnRead bool
	moveOnError   bool
	archiveFolder string
	errorFolder   string
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

	// Extract archive_on_read flag (optional, defaults to false)
	archiveOnRead := false
	if archiveParam, ok := config.Parameters["archive_on_read"].(bool); ok {
		archiveOnRead = archiveParam
	}

	// Extract move_on_error flag (optional, defaults to false)
	moveOnError := false
	if errorParam, ok := config.Parameters["move_on_error"].(bool); ok {
		moveOnError = errorParam
	}

	// Determine archive and error folder paths
	fileDir := filepath.Dir(filePath)
	archiveFolder := filepath.Join(fileDir, "archive")
	errorFolder := filepath.Join(fileDir, "error")

	// Allow custom archive folder path
	if customArchive, ok := config.Parameters["archive_folder"].(string); ok && customArchive != "" {
		archiveFolder = customArchive
	}

	// Allow custom error folder path
	if customError, ok := config.Parameters["error_folder"].(string); ok && customError != "" {
		errorFolder = customError
	}

	return &CSVReaderComponent{
		config:        config,
		filePath:      filePath,
		delimiter:     delimiter,
		hasHeader:     hasHeader,
		encoding:      encoding,
		archiveOnRead: archiveOnRead,
		moveOnError:   moveOnError,
		archiveFolder: archiveFolder,
		errorFolder:   errorFolder,
	}, nil
}

// Execute runs the CSV Reader component logic
func (c *CSVReaderComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	return c.Start(ctx)
}

// Start implements SourceComponent interface
func (c *CSVReaderComponent) Start(ctx context.Context) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data, 10)

	go func() {
		defer close(output)

		err := c.readAndSend(ctx, output)

		if err != nil {
			fmt.Printf("CSVReader error: %v\n", err)

			// Move file to error folder if move_on_error is enabled
			if c.moveOnError {
				if moveErr := c.moveToErrorFolder(); moveErr != nil {
					fmt.Printf("CSVReader: Failed to move file to error folder: %v\n", moveErr)
				} else {
					fmt.Printf("CSVReader: File moved to error folder successfully\n")
				}
			}

			// Log error but continue if continue_on_error is true
			if !c.config.ContinueOnError {
				return
			}
		} else {
			// Move file to archive folder if archive_on_read is enabled and read was successful
			if c.archiveOnRead {
				if archiveErr := c.moveToArchiveFolder(); archiveErr != nil {
					fmt.Printf("CSVReader: Failed to move file to archive folder: %v\n", archiveErr)
				} else {
					fmt.Printf("CSVReader: File moved to archive folder successfully\n")
				}
			}
		}
	}()

	return output, nil
}

// readAndSend reads the CSV file and sends data to output channel
func (c *CSVReaderComponent) readAndSend(ctx context.Context, output chan<- pipeline.Data) error {
	fmt.Printf("CSVReader: Opening file: %s\n", c.filePath)

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
		fmt.Printf("CSVReader: Read headers: %v\n", headers)
	}

	// Read and send rows one by one
	rowNumber := 0

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			fmt.Printf("CSVReader: Context cancelled after %d rows\n", rowNumber)
			return ctx.Err()
		default:
		}

		record, err := reader.Read()
		if err == io.EOF {
			fmt.Printf("CSVReader: Finished reading %d rows\n", rowNumber)
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

		// Convert row to JSON bytes
		jsonData, err := json.Marshal(rowMap)
		if err != nil {
			return fmt.Errorf("failed to marshal row %d to JSON: %w", rowNumber, err)
		}

		// Create data payload for this row
		data := pipeline.Data{
			Payload:   jsonData,
			Metadata:  make(map[string]string),
			Timestamp: time.Now(),
			TraceID:   fmt.Sprintf("%s-row-%d", c.config.ID, rowNumber),
		}

		// Add metadata
		data.Metadata["row_number"] = fmt.Sprintf("%d", rowNumber)
		data.Metadata["file_path"] = c.filePath
		if c.hasHeader && len(headers) > 0 {
			data.Metadata["column_count"] = fmt.Sprintf("%d", len(headers))
		}

		// Send data to output channel
		select {
		case output <- data:
			fmt.Printf("CSVReader: Sent row %d\n", rowNumber)
		case <-ctx.Done():
			fmt.Printf("CSVReader: Context cancelled while sending row %d\n", rowNumber)
			return ctx.Err()
		}
	}

	return nil
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

// moveToArchiveFolder moves the CSV file to the archive folder with timestamp
func (c *CSVReaderComponent) moveToArchiveFolder() error {
	fmt.Printf("CSVReader: Moving file to archive folder: %s\n", c.archiveFolder)

	// Create archive folder if it doesn't exist
	if err := os.MkdirAll(c.archiveFolder, 0755); err != nil {
		return fmt.Errorf("failed to create archive folder: %w", err)
	}

	// Generate new filename with timestamp
	fileName := filepath.Base(c.filePath)
	fileExt := filepath.Ext(fileName)
	fileNameWithoutExt := fileName[:len(fileName)-len(fileExt)]
	timestamp := time.Now().Format("20060102_150405")
	newFileName := fmt.Sprintf("%s_archive_%s%s", fileNameWithoutExt, timestamp, fileExt)
	newFilePath := filepath.Join(c.archiveFolder, newFileName)

	fmt.Printf("CSVReader: Renaming %s to %s\n", c.filePath, newFilePath)

	// Move (rename) the file
	if err := os.Rename(c.filePath, newFilePath); err != nil {
		return fmt.Errorf("failed to move file to archive: %w", err)
	}

	fmt.Printf("CSVReader: File archived successfully as %s\n", newFileName)
	return nil
}

// moveToErrorFolder moves the CSV file to the error folder with timestamp
func (c *CSVReaderComponent) moveToErrorFolder() error {
	fmt.Printf("CSVReader: Moving file to error folder: %s\n", c.errorFolder)

	// Create error folder if it doesn't exist
	if err := os.MkdirAll(c.errorFolder, 0755); err != nil {
		return fmt.Errorf("failed to create error folder: %w", err)
	}

	// Generate new filename with timestamp
	fileName := filepath.Base(c.filePath)
	fileExt := filepath.Ext(fileName)
	fileNameWithoutExt := fileName[:len(fileName)-len(fileExt)]
	timestamp := time.Now().Format("20060102_150405")
	newFileName := fmt.Sprintf("%s_error_%s%s", fileNameWithoutExt, timestamp, fileExt)
	newFilePath := filepath.Join(c.errorFolder, newFileName)

	fmt.Printf("CSVReader: Renaming %s to %s\n", c.filePath, newFilePath)

	// Move (rename) the file
	if err := os.Rename(c.filePath, newFilePath); err != nil {
		return fmt.Errorf("failed to move file to error folder: %w", err)
	}

	fmt.Printf("CSVReader: File moved to error folder successfully as %s\n", newFileName)
	return nil
}
