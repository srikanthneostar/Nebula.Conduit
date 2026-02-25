package components

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
	_ "github.com/denisenkom/go-mssqldb" // SQL Server driver
)

// SQLQueryComponent executes SQL queries against SQL Server
type SQLQueryComponent struct {
	config           pipeline.ComponentConfig
	connectionString string
	query            string
	interval         time.Duration
	db               *sql.DB
}

// NewSQLQueryComponent creates a new SQL Query component
func NewSQLQueryComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	// Extract connection string from parameters
	connectionString, ok := config.Parameters["connection_string"].(string)
	if !ok || connectionString == "" {
		return nil, fmt.Errorf("connection_string parameter is required and must be a string")
	}

	// Extract query from parameters
	query, ok := config.Parameters["query"].(string)
	if !ok || query == "" {
		return nil, fmt.Errorf("query parameter is required and must be a string")
	}

	// Extract interval (optional, defaults to 0 for one-time execution)
	var interval time.Duration
	if intervalParam, ok := config.Parameters["interval"].(string); ok {
		parsedInterval, err := time.ParseDuration(intervalParam)
		if err != nil {
			return nil, fmt.Errorf("invalid interval format: %w", err)
		}
		interval = parsedInterval
	}

	// Open database connection
	db, err := sql.Open("sqlserver", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &SQLQueryComponent{
		config:           config,
		connectionString: connectionString,
		query:            query,
		interval:         interval,
		db:               db,
	}, nil
}

// Execute runs the SQL Query component logic
func (s *SQLQueryComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data, 10)

	go func() {
		defer close(output)
		defer s.db.Close()

		// If interval is set, run periodically; otherwise run once
		if s.interval > 0 {
			ticker := time.NewTicker(s.interval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := s.queryAndSend(ctx, output); err != nil {
						// Log error but continue if continue_on_error is true
						if !s.config.ContinueOnError {
							return
						}
					}
				}
			}
		} else {
			// One-time execution
			_ = s.queryAndSend(ctx, output)
		}
	}()

	return output, nil
}

// queryAndSend executes the SQL query and sends results to output channel
func (s *SQLQueryComponent) queryAndSend(ctx context.Context, output chan<- pipeline.Data) error {
	// Execute query with context
	rows, err := s.db.QueryContext(ctx, s.query)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	// Prepare result slice
	var results []map[string]interface{}

	// Iterate through rows
	for rows.Next() {
		// Create a slice of interface{} to hold each column value
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan the row into the value pointers
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		// Create a map for this row
		rowMap := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			// Convert byte slices to strings for better JSON serialization
			if b, ok := val.([]byte); ok {
				rowMap[col] = string(b)
			} else {
				rowMap[col] = val
			}
		}

		results = append(results, rowMap)
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
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
		TraceID:   s.config.ID,
	}

	// Add metadata
	data.Metadata["row_count"] = fmt.Sprintf("%d", len(results))
	data.Metadata["column_count"] = fmt.Sprintf("%d", len(columns))
	data.Metadata["query"] = s.query

	// Send data to output channel
	select {
	case output <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Validate checks if the component configuration is valid
func (s *SQLQueryComponent) Validate() error {
	if s.connectionString == "" {
		return fmt.Errorf("connection_string is required")
	}

	if s.query == "" {
		return fmt.Errorf("query is required")
	}

	return nil
}

// Type returns the component type identifier
func (s *SQLQueryComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeSQLQuery
}

// ID returns the unique component instance identifier
func (s *SQLQueryComponent) ID() string {
	return s.config.ID
}

// Config returns the component configuration
func (s *SQLQueryComponent) Config() pipeline.ComponentConfig {
	return s.config
}
