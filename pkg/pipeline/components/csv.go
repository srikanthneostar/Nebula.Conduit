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

	"github.com/Xecutables/Nebula.Conduit/pkg/logger"
	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

// CSVReaderComponent reads data from CSV files
type CSVReaderComponent struct {
	config            pipeline.ComponentConfig
	filePath          string
	delimiter         rune
	hasHeader         bool
	encoding          string
	archiveOnRead     bool
	moveOnError       bool
	archiveFolder     string
	errorFolder       string
	lazyQuotes        bool
	fieldsPerRecord   int // -1 = variable, 0 = infer from first row, >0 = exact count
	skipMalformedRows bool
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

	// Extract lazy_quotes flag (optional, defaults to false)
	// When true, allows quotes to appear in unquoted fields and non-doubled quotes in quoted fields
	lazyQuotes := false
	if lazyParam, ok := config.Parameters["lazy_quotes"].(bool); ok {
		lazyQuotes = lazyParam
	}

	// Extract fields_per_record (optional, defaults to 0 which means infer from first row)
	// -1 = variable (allow any number of fields)
	// 0 = infer from first row (default, strict checking)
	// >0 = exact field count required
	fieldsPerRecord := 0
	if fieldsParam, ok := config.Parameters["fields_per_record"].(float64); ok {
		fieldsPerRecord = int(fieldsParam)
	} else if fieldsParam, ok := config.Parameters["fields_per_record"].(int); ok {
		fieldsPerRecord = fieldsParam
	}

	// Extract skip_malformed_rows flag (optional, defaults to true)
	// When true, logs malformed rows but continues processing instead of failing
	// This is the default behavior to handle real-world CSV files gracefully
	skipMalformedRows := true
	if skipParam, ok := config.Parameters["skip_malformed_rows"].(bool); ok {
		skipMalformedRows = skipParam
	}

	return &CSVReaderComponent{
		config:            config,
		filePath:          filePath,
		delimiter:         delimiter,
		hasHeader:         hasHeader,
		encoding:          encoding,
		archiveOnRead:     archiveOnRead,
		moveOnError:       moveOnError,
		archiveFolder:     archiveFolder,
		errorFolder:       errorFolder,
		lazyQuotes:        lazyQuotes,
		fieldsPerRecord:   fieldsPerRecord,
		skipMalformedRows: skipMalformedRows,
	}, nil
}

// Execute runs the CSV Reader component logic
func (c *CSVReaderComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	return c.Start(ctx)
}

// Start implements SourceComponent interface
func (c *CSVReaderComponent) Start(ctx context.Context) (<-chan pipeline.Data, error) {
	log := logger.InitLogger("csv_reader")
	output := make(chan pipeline.Data, 10)

	log.Info().
		Str("component_id", c.config.ID).
		Str("file_path", c.filePath).
		Bool("skip_malformed_rows", c.skipMalformedRows).
		Int("fields_per_record", c.fieldsPerRecord).
		Msg("CSVReader: Starting component")

	go func() {
		defer close(output)

		err := c.readAndSend(ctx, output)

		if err != nil {
			log.Error().
				Err(err).
				Str("component_id", c.config.ID).
				Msg("CSVReader: Failed to read file")

			// Move file to error folder if move_on_error is enabled
			if c.moveOnError {
				if moveErr := c.moveToErrorFolder(); moveErr != nil {
					log.Error().Err(moveErr).Msg("CSVReader: Failed to move file to error folder")
				} else {
					log.Info().Msg("CSVReader: File moved to error folder")
				}
			}

			if !c.config.ContinueOnError {
				return
			}
		} else {
			// Move file to archive folder if archive_on_read is enabled
			if c.archiveOnRead {
				if archiveErr := c.moveToArchiveFolder(); archiveErr != nil {
					log.Error().Err(archiveErr).Msg("CSVReader: Failed to archive file")
				} else {
					log.Info().Msg("CSVReader: File archived successfully")
				}
			}
		}
	}()

	return output, nil
}

// readAndSend reads the CSV file and sends data to output channel
func (c *CSVReaderComponent) readAndSend(ctx context.Context, output chan<- pipeline.Data) error {
	log := logger.InitLogger("csv_reader")

	// Open the CSV file
	file, err := os.Open(c.filePath)
	if err != nil {
		log.Error().Err(err).Msg("CSVReader: Failed to open file")
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)
	reader.Comma = c.delimiter
	reader.TrimLeadingSpace = true
	reader.LazyQuotes = c.lazyQuotes
	reader.FieldsPerRecord = c.fieldsPerRecord

	// Read header if present
	var headers []string
	if c.hasHeader {
		headers, err = reader.Read()
		if err != nil {
			log.Error().Err(err).Msg("CSVReader: Failed to read header")
			return fmt.Errorf("failed to read header: %w", err)
		}
	}

	// Read and send rows one by one
	rowNumber := 0
	errorCount := 0

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			log.Warn().
				Int("rows_processed", rowNumber).
				Int("errors_encountered", errorCount).
				Msg("CSVReader: Context cancelled")
			return ctx.Err()
		default:
		}

		record, err := reader.Read()
		if err == io.EOF {
			log.Info().
				Int("total_rows", rowNumber).
				Int("errors_encountered", errorCount).
				Msg("CSVReader: Finished reading file")
			break
		}
		if err != nil {
			errorCount++
			log.Error().
				Err(err).
				Int("row_number", rowNumber+1).
				Msg("CSVReader: Failed to read row")

			// If skip_malformed_rows is enabled, log and continue
			if c.skipMalformedRows {
				log.Warn().
					Int("row_number", rowNumber+1).
					Msg("CSVReader: Skipping malformed row")
				continue
			}

			// Otherwise, fail
			return fmt.Errorf("failed to read row %d: %w", rowNumber+1, err)
		}

		rowNumber++

		// Create row map
		var rowMap map[string]any
		if c.hasHeader && len(headers) > 0 {
			rowMap = make(map[string]any)
			for i, value := range record {
				if i < len(headers) {
					rowMap[headers[i]] = value
				} else {
					rowMap[fmt.Sprintf("column_%d", i)] = value
				}
			}
		} else {
			rowMap = make(map[string]any)
			for i, value := range record {
				rowMap[fmt.Sprintf("column_%d", i)] = value
			}
		}

		// Convert row to JSON bytes
		jsonData, err := json.Marshal(rowMap)
		if err != nil {
			errorCount++
			log.Error().
				Err(err).
				Int("row_number", rowNumber).
				Msg("CSVReader: Failed to marshal row to JSON")
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
			// Row sent successfully (no logging for each row)
		case <-ctx.Done():
			log.Warn().
				Int("row_number", rowNumber).
				Msg("CSVReader: Context cancelled while sending row")
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
	log := logger.InitLogger("csv_reader")

	if err := os.MkdirAll(c.archiveFolder, 0755); err != nil {
		log.Error().Err(err).Msg("CSVReader: Failed to create archive folder")
		return fmt.Errorf("failed to create archive folder: %w", err)
	}

	fileName := filepath.Base(c.filePath)
	fileExt := filepath.Ext(fileName)
	fileNameWithoutExt := fileName[:len(fileName)-len(fileExt)]
	timestamp := time.Now().Format("20060102_150405")
	newFileName := fmt.Sprintf("%s_archive_%s%s", fileNameWithoutExt, timestamp, fileExt)
	newFilePath := filepath.Join(c.archiveFolder, newFileName)

	if err := os.Rename(c.filePath, newFilePath); err != nil {
		log.Error().Err(err).Msg("CSVReader: Failed to move file to archive")
		return fmt.Errorf("failed to move file to archive: %w", err)
	}

	return nil
}

// moveToErrorFolder moves the CSV file to the error folder with timestamp
func (c *CSVReaderComponent) moveToErrorFolder() error {
	log := logger.InitLogger("csv_reader")

	if err := os.MkdirAll(c.errorFolder, 0755); err != nil {
		log.Error().Err(err).Msg("CSVReader: Failed to create error folder")
		return fmt.Errorf("failed to create error folder: %w", err)
	}

	fileName := filepath.Base(c.filePath)
	fileExt := filepath.Ext(fileName)
	fileNameWithoutExt := fileName[:len(fileName)-len(fileExt)]
	timestamp := time.Now().Format("20060102_150405")
	newFileName := fmt.Sprintf("%s_error_%s%s", fileNameWithoutExt, timestamp, fileExt)
	newFilePath := filepath.Join(c.errorFolder, newFileName)

	if err := os.Rename(c.filePath, newFilePath); err != nil {
		log.Error().Err(err).Msg("CSVReader: Failed to move file to error folder")
		return fmt.Errorf("failed to move file to error folder: %w", err)
	}

	return nil
}
