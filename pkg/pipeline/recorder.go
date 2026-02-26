package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ExecutionRecorder records pipeline and component execution details
type ExecutionRecorder interface {
	StartExecution(ctx context.Context, pipelineID string) (string, error)
	EndExecution(ctx context.Context, executionID string, status InstanceStatus, err error) error
	StartComponent(ctx context.Context, executionID, componentID string) (string, error)
	EndComponent(ctx context.Context, componentExecID string, status InstanceStatus, outputSize int64, err error) error
	GetExecutionHistory(ctx context.Context, pipelineID string, limit, offset int) ([]ExecutionRecord, int, error)
	GetExecutionDetails(ctx context.Context, executionID string) (*ExecutionRecord, error)
	DeleteOldExecutions(ctx context.Context, retentionDays int) (int64, error)
}

// defaultExecutionRecorder implements ExecutionRecorder using the database
type defaultExecutionRecorder struct {
	db *sql.DB
}

// NewExecutionRecorder creates a new execution recorder
func NewExecutionRecorder(db *sql.DB) ExecutionRecorder {
	return &defaultExecutionRecorder{db: db}
}

func (r *defaultExecutionRecorder) StartExecution(ctx context.Context, pipelineID string) (string, error) {
	executionID := uuid.New().String()
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO pipeline_executions (id, pipeline_id, status, started_at) VALUES (?, ?, ?, ?)`,
		executionID, pipelineID, InstanceStatusRunning, time.Now(),
	)
	if err != nil {
		return "", fmt.Errorf("failed to record execution start: %w", err)
	}
	return executionID, nil
}

func (r *defaultExecutionRecorder) EndExecution(ctx context.Context, executionID string, status InstanceStatus, execErr error) error {
	var errMsg *string
	if execErr != nil {
		msg := execErr.Error()
		errMsg = &msg
	}
	_, err := r.db.ExecContext(ctx,
		`UPDATE pipeline_executions SET status = ?, ended_at = ?, error_message = ? WHERE id = ?`,
		status, time.Now(), errMsg, executionID,
	)
	if err != nil {
		return fmt.Errorf("failed to record execution end: %w", err)
	}
	return nil
}

func (r *defaultExecutionRecorder) StartComponent(ctx context.Context, executionID, componentID string) (string, error) {
	componentExecID := uuid.New().String()
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO component_executions (id, pipeline_execution_id, component_id, status, started_at) VALUES (?, ?, ?, ?, ?)`,
		componentExecID, executionID, componentID, InstanceStatusRunning, time.Now(),
	)
	if err != nil {
		return "", fmt.Errorf("failed to record component start: %w", err)
	}
	return componentExecID, nil
}

func (r *defaultExecutionRecorder) EndComponent(ctx context.Context, componentExecID string, status InstanceStatus, outputSize int64, execErr error) error {
	var errMsg *string
	if execErr != nil {
		msg := execErr.Error()
		errMsg = &msg
	}
	_, err := r.db.ExecContext(ctx,
		`UPDATE component_executions SET status = ?, ended_at = ?, output_data_size = ?, error_message = ? WHERE id = ?`,
		status, time.Now(), outputSize, errMsg, componentExecID,
	)
	if err != nil {
		return fmt.Errorf("failed to record component end: %w", err)
	}
	return nil
}

func (r *defaultExecutionRecorder) GetExecutionHistory(ctx context.Context, pipelineID string, limit, offset int) ([]ExecutionRecord, int, error) {
	// Get total count
	var total int
	err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipeline_executions WHERE pipeline_id = ?`, pipelineID,
	).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count executions: %w", err)
	}

	rows, err := r.db.QueryContext(ctx,
		`SELECT id, pipeline_id, status, started_at, ended_at, error_message
		 FROM pipeline_executions WHERE pipeline_id = ?
		 ORDER BY started_at DESC LIMIT ? OFFSET ?`,
		pipelineID, limit, offset,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query execution history: %w", err)
	}
	defer rows.Close()

	var records []ExecutionRecord
	for rows.Next() {
		var rec ExecutionRecord
		if err := rows.Scan(&rec.ID, &rec.PipelineID, &rec.Status, &rec.StartedAt, &rec.EndedAt, &rec.ErrorMessage); err != nil {
			return nil, 0, fmt.Errorf("failed to scan execution record: %w", err)
		}
		records = append(records, rec)
	}
	return records, total, nil
}

func (r *defaultExecutionRecorder) GetExecutionDetails(ctx context.Context, executionID string) (*ExecutionRecord, error) {
	var rec ExecutionRecord
	err := r.db.QueryRowContext(ctx,
		`SELECT id, pipeline_id, status, started_at, ended_at, error_message
		 FROM pipeline_executions WHERE id = ?`, executionID,
	).Scan(&rec.ID, &rec.PipelineID, &rec.Status, &rec.StartedAt, &rec.EndedAt, &rec.ErrorMessage)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("execution %s not found", executionID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query execution: %w", err)
	}

	// Load component execution records
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, pipeline_execution_id, component_id, status, started_at, ended_at, output_data_size, error_message
		 FROM component_executions WHERE pipeline_execution_id = ?
		 ORDER BY started_at`, executionID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query component executions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var compRec ComponentExecutionRecord
		if err := rows.Scan(&compRec.ID, &compRec.PipelineExecutionID, &compRec.ComponentID,
			&compRec.Status, &compRec.StartedAt, &compRec.EndedAt, &compRec.OutputDataSize, &compRec.ErrorMessage); err != nil {
			return nil, fmt.Errorf("failed to scan component execution: %w", err)
		}
		rec.ComponentResults = append(rec.ComponentResults, compRec)
	}

	return &rec, nil
}

func (r *defaultExecutionRecorder) DeleteOldExecutions(ctx context.Context, retentionDays int) (int64, error) {
	cutoff := time.Now().AddDate(0, 0, -retentionDays)
	result, err := r.db.ExecContext(ctx,
		`DELETE FROM pipeline_executions WHERE ended_at IS NOT NULL AND ended_at < ?`, cutoff,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to delete old executions: %w", err)
	}
	return result.RowsAffected()
}

// ComponentExecutionTracker tracks component execution metrics
type ComponentExecutionTracker struct {
	executionID     string
	componentID     string
	componentExecID string
	recorder        ExecutionRecorder
	startTime       time.Time
	outputDataSize  int64
}

func NewComponentExecutionTracker(recorder ExecutionRecorder, executionID, componentID string) *ComponentExecutionTracker {
	return &ComponentExecutionTracker{
		executionID: executionID,
		componentID: componentID,
		recorder:    recorder,
		startTime:   time.Now(),
	}
}

func (t *ComponentExecutionTracker) Start(ctx context.Context) error {
	componentExecID, err := t.recorder.StartComponent(ctx, t.executionID, t.componentID)
	if err != nil {
		return err
	}
	t.componentExecID = componentExecID
	return nil
}

func (t *ComponentExecutionTracker) End(ctx context.Context, status InstanceStatus, err error) error {
	if t.componentExecID == "" {
		return fmt.Errorf("component execution not started")
	}
	return t.recorder.EndComponent(ctx, t.componentExecID, status, t.outputDataSize, err)
}

func (t *ComponentExecutionTracker) RecordOutputSize(size int64) {
	t.outputDataSize += size
}
