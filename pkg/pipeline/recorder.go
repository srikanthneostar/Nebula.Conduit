package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ExecutionRecorder records pipeline and component execution details
type ExecutionRecorder interface {
	// StartExecution records the start of a pipeline execution
	StartExecution(ctx context.Context, pipelineID string) (string, error)

	// EndExecution records the end of a pipeline execution
	EndExecution(ctx context.Context, executionID string, status InstanceStatus, err error) error

	// StartComponent records the start of a component execution
	StartComponent(ctx context.Context, executionID, componentID string) (string, error)

	// EndComponent records the end of a component execution
	EndComponent(ctx context.Context, componentExecID string, status InstanceStatus, outputSize int64, err error) error

	// GetExecutionHistory retrieves execution history for a pipeline
	GetExecutionHistory(ctx context.Context, pipelineID string, limit, offset int) ([]ExecutionRecord, error)

	// GetExecutionDetails retrieves details of a specific execution
	GetExecutionDetails(ctx context.Context, executionID string) (*ExecutionRecord, error)
}

// defaultExecutionRecorder implements ExecutionRecorder using the repository
type defaultExecutionRecorder struct {
	repository PipelineRepository
}

// NewExecutionRecorder creates a new execution recorder
func NewExecutionRecorder(repository PipelineRepository) ExecutionRecorder {
	return &defaultExecutionRecorder{
		repository: repository,
	}
}

// StartExecution records the start of a pipeline execution
func (r *defaultExecutionRecorder) StartExecution(ctx context.Context, pipelineID string) (string, error) {
	executionID := uuid.New().String()

	// TODO: Insert into pipeline_executions table
	// For now, just return the ID
	// In production, this would insert:
	// INSERT INTO pipeline_executions (id, pipeline_id, status, started_at)
	// VALUES (?, ?, 'running', CURRENT_TIMESTAMP)

	return executionID, nil
}

// EndExecution records the end of a pipeline execution
func (r *defaultExecutionRecorder) EndExecution(ctx context.Context, executionID string, status InstanceStatus, err error) error {
	// TODO: Update pipeline_executions table
	// For now, just log
	// In production, this would update:
	// UPDATE pipeline_executions
	// SET status = ?, ended_at = CURRENT_TIMESTAMP, error_message = ?
	// WHERE id = ?

	var errMsg *string
	if err != nil {
		msg := err.Error()
		errMsg = &msg
	}

	fmt.Printf("Execution %s ended with status %s, error: %v\n", executionID, status, errMsg)
	return nil
}

// StartComponent records the start of a component execution
func (r *defaultExecutionRecorder) StartComponent(ctx context.Context, executionID, componentID string) (string, error) {
	componentExecID := uuid.New().String()

	// TODO: Insert into component_executions table
	// For now, just return the ID
	// In production, this would insert:
	// INSERT INTO component_executions (id, pipeline_execution_id, component_id, status, started_at)
	// VALUES (?, ?, ?, 'running', CURRENT_TIMESTAMP)

	return componentExecID, nil
}

// EndComponent records the end of a component execution
func (r *defaultExecutionRecorder) EndComponent(ctx context.Context, componentExecID string, status InstanceStatus, outputSize int64, err error) error {
	// TODO: Update component_executions table
	// For now, just log
	// In production, this would update:
	// UPDATE component_executions
	// SET status = ?, ended_at = CURRENT_TIMESTAMP, output_data_size = ?, error_message = ?
	// WHERE id = ?

	var errMsg *string
	if err != nil {
		msg := err.Error()
		errMsg = &msg
	}

	fmt.Printf("Component execution %s ended with status %s, output size: %d, error: %v\n",
		componentExecID, status, outputSize, errMsg)
	return nil
}

// GetExecutionHistory retrieves execution history for a pipeline
func (r *defaultExecutionRecorder) GetExecutionHistory(ctx context.Context, pipelineID string, limit, offset int) ([]ExecutionRecord, error) {
	// TODO: Query pipeline_executions table with JOIN to component_executions
	// For now, return empty list
	// In production, this would query:
	// SELECT * FROM pipeline_executions
	// WHERE pipeline_id = ?
	// ORDER BY started_at DESC
	// LIMIT ? OFFSET ?

	return []ExecutionRecord{}, nil
}

// GetExecutionDetails retrieves details of a specific execution
func (r *defaultExecutionRecorder) GetExecutionDetails(ctx context.Context, executionID string) (*ExecutionRecord, error) {
	// TODO: Query pipeline_executions and component_executions tables
	// For now, return nil
	// In production, this would query:
	// SELECT * FROM pipeline_executions WHERE id = ?
	// And JOIN with component_executions

	return nil, fmt.Errorf("execution %s not found", executionID)
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

// NewComponentExecutionTracker creates a new component execution tracker
func NewComponentExecutionTracker(recorder ExecutionRecorder, executionID, componentID string) *ComponentExecutionTracker {
	return &ComponentExecutionTracker{
		executionID: executionID,
		componentID: componentID,
		recorder:    recorder,
		startTime:   time.Now(),
	}
}

// Start records the start of component execution
func (t *ComponentExecutionTracker) Start(ctx context.Context) error {
	componentExecID, err := t.recorder.StartComponent(ctx, t.executionID, t.componentID)
	if err != nil {
		return err
	}
	t.componentExecID = componentExecID
	return nil
}

// End records the end of component execution
func (t *ComponentExecutionTracker) End(ctx context.Context, status InstanceStatus, err error) error {
	if t.componentExecID == "" {
		return fmt.Errorf("component execution not started")
	}
	return t.recorder.EndComponent(ctx, t.componentExecID, status, t.outputDataSize, err)
}

// RecordOutputSize records the size of output data
func (t *ComponentExecutionTracker) RecordOutputSize(size int64) {
	t.outputDataSize += size
}
