package scheduler

import (
	"time"
)

// PipelineConfig represents the main configuration structure
type PipelineConfig struct {
	Pipelines []Pipeline `json:"pipelines"`
}

// Pipeline represents a single data pipeline
type Pipeline struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	CronExpr    string `json:"cron_expression"`
	Enabled     bool   `json:"enabled"`
	Jobs        []Job  `json:"jobs"`
	MaxRetries  int    `json:"max_retries,omitempty"`
	TimeoutMins int    `json:"timeout_minutes,omitempty"`
}

// Job represents a single job step in the pipeline
type Job struct {
	Name            string            `json:"name"`
	ScriptName      string            `json:"script_name"`
	Args            []string          `json:"args,omitempty"`
	Env             map[string]string `json:"env,omitempty"`
	WaitAfterSecs   int               `json:"wait_after_seconds,omitempty"`
	TimeoutMins     int               `json:"timeout_minutes,omitempty"`
	ContinueOnError bool              `json:"continue_on_error,omitempty"`
	Retries         int               `json:"retries,omitempty"`
}

// PipelineExecution represents a pipeline execution instance
type PipelineExecution struct {
	ID           string          `json:"id"`
	PipelineName string          `json:"pipeline_name"`
	Status       ExecutionStatus `json:"status"`
	StartedAt    time.Time       `json:"started_at"`
	EndedAt      *time.Time      `json:"ended_at,omitempty"`
	Jobs         []JobExecution  `json:"jobs"`
	Error        string          `json:"error,omitempty"`
}

// JobExecution represents a job execution within a pipeline
type JobExecution struct {
	JobName   string          `json:"job_name"`
	TaskID    string          `json:"task_id,omitempty"`
	Status    ExecutionStatus `json:"status"`
	StartedAt time.Time       `json:"started_at"`
	EndedAt   *time.Time      `json:"ended_at,omitempty"`
	Output    string          `json:"output,omitempty"`
	Error     string          `json:"error,omitempty"`
	ExitCode  int             `json:"exit_code,omitempty"`
	Attempt   int             `json:"attempt"`
}

// ExecutionStatus represents the status of pipeline/job execution
type ExecutionStatus string

const (
	StatusPending   ExecutionStatus = "pending"
	StatusRunning   ExecutionStatus = "running"
	StatusCompleted ExecutionStatus = "completed"
	StatusFailed    ExecutionStatus = "failed"
	StatusCancelled ExecutionStatus = "cancelled"
	StatusSkipped   ExecutionStatus = "skipped"
)
