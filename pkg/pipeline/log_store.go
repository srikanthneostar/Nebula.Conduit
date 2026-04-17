package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// PipelineLogEntry represents a single log entry created by the print_log component.
type PipelineLogEntry struct {
	ID          string `json:"id"`
	PipelineID  string `json:"pipeline_id"`
	ExecutionID string `json:"execution_id,omitempty"`
	ComponentID string `json:"component_id"`
	LogLevel    string `json:"log_level"`
	Message     string `json:"message,omitempty"`
	Payload     string `json:"payload,omitempty"`
	Metadata    string `json:"metadata,omitempty"`
	TraceID     string `json:"trace_id,omitempty"`
	CreatedAt   string `json:"created_at"`
}

// LogStore provides read/write access to the pipeline_logs table.
type LogStore interface {
	InsertLog(ctx context.Context, entry PipelineLogEntry) error
	GetLogs(ctx context.Context, pipelineID string, limit, offset int) ([]PipelineLogEntry, int, error)
	GetLogsByExecution(ctx context.Context, executionID string, limit, offset int) ([]PipelineLogEntry, int, error)
	GetLogsByComponent(ctx context.Context, componentID string, limit, offset int) ([]PipelineLogEntry, int, error)
	DeleteOldLogs(ctx context.Context, retentionDays int) (int64, error)
}

type defaultLogStore struct {
	db *sql.DB
}

// NewLogStore creates a new LogStore backed by the given database.
func NewLogStore(db *sql.DB) LogStore {
	return &defaultLogStore{db: db}
}

func (s *defaultLogStore) InsertLog(ctx context.Context, entry PipelineLogEntry) error {
	if entry.ID == "" {
		entry.ID = uuid.New().String()
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO pipeline_logs (id, pipeline_id, execution_id, component_id, log_level, message, payload, metadata, trace_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.ID, entry.PipelineID, entry.ExecutionID, entry.ComponentID,
		entry.LogLevel, entry.Message, entry.Payload, entry.Metadata, entry.TraceID,
		time.Now(),
	)
	if err != nil {
		return fmt.Errorf("failed to insert pipeline log: %w", err)
	}
	return nil
}

func (s *defaultLogStore) GetLogs(ctx context.Context, pipelineID string, limit, offset int) ([]PipelineLogEntry, int, error) {
	var total int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipeline_logs WHERE pipeline_id = ?`, pipelineID,
	).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count logs: %w", err)
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT id, pipeline_id, execution_id, component_id, log_level, message, payload, metadata, trace_id, created_at
		 FROM pipeline_logs WHERE pipeline_id = ?
		 ORDER BY created_at DESC LIMIT ? OFFSET ?`,
		pipelineID, limit, offset,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query logs: %w", err)
	}
	defer rows.Close()

	return scanLogRows(rows, total)
}

func (s *defaultLogStore) GetLogsByExecution(ctx context.Context, executionID string, limit, offset int) ([]PipelineLogEntry, int, error) {
	var total int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipeline_logs WHERE execution_id = ?`, executionID,
	).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count logs: %w", err)
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT id, pipeline_id, execution_id, component_id, log_level, message, payload, metadata, trace_id, created_at
		 FROM pipeline_logs WHERE execution_id = ?
		 ORDER BY created_at DESC LIMIT ? OFFSET ?`,
		executionID, limit, offset,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query logs: %w", err)
	}
	defer rows.Close()

	return scanLogRows(rows, total)
}

func (s *defaultLogStore) GetLogsByComponent(ctx context.Context, componentID string, limit, offset int) ([]PipelineLogEntry, int, error) {
	var total int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipeline_logs WHERE component_id = ?`, componentID,
	).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count logs: %w", err)
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT id, pipeline_id, execution_id, component_id, log_level, message, payload, metadata, trace_id, created_at
		 FROM pipeline_logs WHERE component_id = ?
		 ORDER BY created_at DESC LIMIT ? OFFSET ?`,
		componentID, limit, offset,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query logs: %w", err)
	}
	defer rows.Close()

	return scanLogRows(rows, total)
}

func (s *defaultLogStore) DeleteOldLogs(ctx context.Context, retentionDays int) (int64, error) {
	cutoff := time.Now().AddDate(0, 0, -retentionDays)
	result, err := s.db.ExecContext(ctx,
		`DELETE FROM pipeline_logs WHERE created_at < ?`, cutoff,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to delete old logs: %w", err)
	}
	return result.RowsAffected()
}

func scanLogRows(rows *sql.Rows, total int) ([]PipelineLogEntry, int, error) {
	entries := make([]PipelineLogEntry, 0)
	for rows.Next() {
		var e PipelineLogEntry
		var execID, message, payload, metadata, traceID sql.NullString
		if err := rows.Scan(&e.ID, &e.PipelineID, &execID, &e.ComponentID,
			&e.LogLevel, &message, &payload, &metadata, &traceID, &e.CreatedAt); err != nil {
			return nil, 0, fmt.Errorf("failed to scan log row: %w", err)
		}
		e.ExecutionID = execID.String
		e.Message = message.String
		e.Payload = payload.String
		e.Metadata = metadata.String
		e.TraceID = traceID.String
		entries = append(entries, e)
	}
	return entries, total, nil
}

// PayloadToLogString converts a pipeline Data payload to a string suitable for storage.
func PayloadToLogString(payload interface{}) string {
	switch v := payload.(type) {
	case []byte:
		return string(v)
	case string:
		return v
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(b)
	}
}

// MetadataToLogString converts metadata map to a JSON string for storage.
func MetadataToLogString(metadata map[string]string) string {
	if len(metadata) == 0 {
		return "{}"
	}
	b, err := json.Marshal(metadata)
	if err != nil {
		return "{}"
	}
	return string(b)
}
