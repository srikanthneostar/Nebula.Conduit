package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog"

	"github.com/Xecutables/Nebula.Conduit/config"
	"github.com/Xecutables/Nebula.Conduit/internal/repository"
	"github.com/Xecutables/Nebula.Conduit/pkg/executor"
	"github.com/Xecutables/Nebula.Conduit/pkg/logger"
)

// CronScheduler manages pipeline scheduling and execution
type CronScheduler struct {
	cron              *cron.Cron
	pipelines         map[string]*Pipeline
	runningExecutions sync.Map
	taskRepo          repository.TaskRepository
	pathConfig        *config.PathConfig
	logger            zerolog.Logger
	configFile        string
	defaultTimeout    time.Duration
	mu                sync.RWMutex
}

// NewCronScheduler creates a new cron scheduler instance
func NewCronScheduler(taskRepo repository.TaskRepository, pathConfig *config.PathConfig, configFile string) *CronScheduler {
	return &CronScheduler{
		cron:           cron.New(cron.WithSeconds()),
		pipelines:      make(map[string]*Pipeline),
		taskRepo:       taskRepo,
		pathConfig:     pathConfig,
		logger:         logger.InitLogger(),
		configFile:     configFile,
		defaultTimeout: 30 * time.Minute,
	}
}

// LoadPipelines loads pipeline configurations from JSON file
func (cs *CronScheduler) LoadPipelines() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	data, err := os.ReadFile(cs.configFile)
	if err != nil {
		cs.logger.Error().Err(err).Str("config_file", cs.configFile).Msg("Failed to read pipeline config file")
		return fmt.Errorf("failed to read config file: %w", err)
	}

	var config PipelineConfig
	if err := json.Unmarshal(data, &config); err != nil {
		cs.logger.Error().Err(err).Msg("Failed to parse pipeline config")
		return fmt.Errorf("failed to parse config: %w", err)
	}

	// Clear existing pipelines
	cs.pipelines = make(map[string]*Pipeline)

	// Load new pipelines
	for i := range config.Pipelines {
		pipeline := &config.Pipelines[i]
		cs.pipelines[pipeline.Name] = pipeline
		cs.logger.Info().Str("pipeline", pipeline.Name).Str("cron", pipeline.CronExpr).Msg("Loaded pipeline")
	}

	cs.logger.Info().Int("count", len(cs.pipelines)).Msg("Successfully loaded pipelines")
	return nil
}

// Start begins the cron scheduler
func (cs *CronScheduler) Start() error {
	if err := cs.LoadPipelines(); err != nil {
		return err
	}

	if err := cs.schedulePipelines(); err != nil {
		return err
	}

	cs.cron.Start()
	cs.logger.Info().Msg("Cron scheduler started")
	return nil
}

// Stop stops the cron scheduler
func (cs *CronScheduler) Stop() {
	cs.cron.Stop()
	cs.logger.Info().Msg("Cron scheduler stopped")
}

// schedulePipelines schedules all enabled pipelines
func (cs *CronScheduler) schedulePipelines() error {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	for name, pipeline := range cs.pipelines {
		if !pipeline.Enabled {
			cs.logger.Info().Str("pipeline", name).Msg("Pipeline disabled, skipping")
			continue
		}

		_, err := cs.cron.AddFunc(pipeline.CronExpr, func(p *Pipeline) func() {
			return func() {
				cs.executePipeline(p)
			}
		}(pipeline))

		if err != nil {
			cs.logger.Error().Err(err).Str("pipeline", name).Str("cron", pipeline.CronExpr).Msg("Failed to schedule pipeline")
			return fmt.Errorf("failed to schedule pipeline %s: %w", name, err)
		}

		cs.logger.Info().Str("pipeline", name).Str("cron", pipeline.CronExpr).Msg("Pipeline scheduled")
	}

	return nil
}

// executePipeline executes a single pipeline
func (cs *CronScheduler) executePipeline(pipeline *Pipeline) {
	executionID := uuid.New().String()

	execution := &PipelineExecution{
		ID:           executionID,
		PipelineName: pipeline.Name,
		Status:       StatusRunning,
		StartedAt:    time.Now(),
		Jobs:         make([]JobExecution, 0, len(pipeline.Jobs)),
	}

	cs.runningExecutions.Store(executionID, execution)
	cs.logger.Info().Str("execution_id", executionID).Str("pipeline", pipeline.Name).Msg("Starting pipeline execution")

	// Execute pipeline in goroutine
	go func() {
		defer cs.runningExecutions.Delete(executionID)
		cs.runPipelineJobs(execution, pipeline)
	}()
}

// runPipelineJobs executes all jobs in a pipeline sequentially
func (cs *CronScheduler) runPipelineJobs(execution *PipelineExecution, pipeline *Pipeline) {
	ctx := context.Background()

	// Set pipeline timeout if specified
	if pipeline.TimeoutMins > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(pipeline.TimeoutMins)*time.Minute)
		defer cancel()
	}

	for _, job := range pipeline.Jobs {
		select {
		case <-ctx.Done():
			cs.finalizePipelineExecution(execution, StatusCancelled, "Pipeline timeout exceeded")
			return
		default:
		}

		if !cs.executeJob(ctx, execution, &job, pipeline) {
			if !job.ContinueOnError {
				cs.finalizePipelineExecution(execution, StatusFailed, fmt.Sprintf("Job %s failed and continue_on_error is false", job.Name))
				return
			}
		}

		// Wait between jobs if specified
		if job.WaitAfterSecs > 0 {
			cs.logger.Info().Str("job", job.Name).Int("wait_seconds", job.WaitAfterSecs).Msg("Waiting before next job")
			time.Sleep(time.Duration(job.WaitAfterSecs) * time.Second)
		}
	}

	cs.finalizePipelineExecution(execution, StatusCompleted, "")
}

// executeJob executes a single job with retry logic
func (cs *CronScheduler) executeJob(ctx context.Context, execution *PipelineExecution, job *Job, pipeline *Pipeline) bool {
	maxRetries := job.Retries
	if maxRetries == 0 {
		maxRetries = pipeline.MaxRetries
	}
	if maxRetries == 0 {
		maxRetries = 1
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		jobExecution := JobExecution{
			JobName:   job.Name,
			Status:    StatusRunning,
			StartedAt: time.Now(),
			Attempt:   attempt,
		}

		execution.Jobs = append(execution.Jobs, jobExecution)
		jobIndex := len(execution.Jobs) - 1

		cs.logger.Info().
			Str("pipeline", execution.PipelineName).
			Str("job", job.Name).
			Int("attempt", attempt).
			Msg("Starting job execution")

		if cs.runSingleJob(ctx, &execution.Jobs[jobIndex], job) {
			cs.logger.Info().
				Str("pipeline", execution.PipelineName).
				Str("job", job.Name).
				Int("attempt", attempt).
				Msg("Job completed successfully")
			return true
		}

		if attempt < maxRetries {
			cs.logger.Warn().
				Str("pipeline", execution.PipelineName).
				Str("job", job.Name).
				Int("attempt", attempt).
				Int("max_retries", maxRetries).
				Msg("Job failed, retrying")
			time.Sleep(time.Duration(attempt) * time.Second) // Exponential backoff
		}
	}

	cs.logger.Error().
		Str("pipeline", execution.PipelineName).
		Str("job", job.Name).
		Int("max_retries", maxRetries).
		Msg("Job failed after all retry attempts")

	return false
}

// runSingleJob executes a single job using PythonExecutor
func (cs *CronScheduler) runSingleJob(ctx context.Context, jobExecution *JobExecution, job *Job) bool {
	// Determine timeout
	timeout := cs.defaultTimeout
	if job.TimeoutMins > 0 {
		timeout = time.Duration(job.TimeoutMins) * time.Minute
	}

	// Create Python executor
	pythonExecutor := executor.NewPythonExecutor(cs.taskRepo, cs.pathConfig, timeout)

	// Convert env map to slice
	var envVars []string
	for key, value := range job.Env {
		envVars = append(envVars, fmt.Sprintf("%s=%s", key, value))
	}

	// Execute task
	task, err := pythonExecutor.ExecuteWithTimeout(ctx, job.ScriptName, job.Args, envVars, 0) // Using 0 as system user
	if err != nil {
		now := time.Now()
		jobExecution.Status = StatusFailed
		jobExecution.Error = err.Error()
		jobExecution.EndedAt = &now
		jobExecution.ExitCode = -1
		return false
	}

	jobExecution.TaskID = task.ID

	// Wait for task completion with timeout
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		select {
		case <-timeoutCtx.Done():
			// Task timeout
			pythonExecutor.StopTask(task.ID)
			now := time.Now()
			jobExecution.Status = StatusCancelled
			jobExecution.Error = "Job execution timeout"
			jobExecution.EndedAt = &now
			jobExecution.ExitCode = -2
			return false

		case <-ticker.C:
			// Check task status
			updatedTask, err := cs.taskRepo.GetTask(task.ID)
			if err != nil {
				continue
			}

			switch updatedTask.Status {
			case "completed":
				now := time.Now()
				jobExecution.Status = StatusCompleted
				jobExecution.Output = updatedTask.Output
				jobExecution.EndedAt = &now
				jobExecution.ExitCode = updatedTask.ExitCode
				return true

			case "failed":
				now := time.Now()
				jobExecution.Status = StatusFailed
				jobExecution.Error = updatedTask.Error
				jobExecution.Output = updatedTask.Output
				jobExecution.EndedAt = &now
				jobExecution.ExitCode = updatedTask.ExitCode
				return false

			case "cancelled":
				now := time.Now()
				jobExecution.Status = StatusCancelled
				jobExecution.Error = updatedTask.Error
				jobExecution.EndedAt = &now
				jobExecution.ExitCode = updatedTask.ExitCode
				return false
			}
		}
	}
}

// finalizePipelineExecution finalizes the pipeline execution
func (cs *CronScheduler) finalizePipelineExecution(execution *PipelineExecution, status ExecutionStatus, errorMsg string) {
	now := time.Now()
	execution.Status = status
	execution.EndedAt = &now

	if errorMsg != "" {
		execution.Error = errorMsg
	}

	// Log execution summary
	duration := now.Sub(execution.StartedAt)
	completedJobs := 0
	failedJobs := 0

	for _, job := range execution.Jobs {
		switch job.Status {
		case StatusCompleted:
			completedJobs++
		case StatusFailed:
			failedJobs++
		}
	}

	cs.logger.Info().
		Str("execution_id", execution.ID).
		Str("pipeline", execution.PipelineName).
		Str("status", string(status)).
		Dur("duration", duration).
		Int("total_jobs", len(execution.Jobs)).
		Int("completed_jobs", completedJobs).
		Int("failed_jobs", failedJobs).
		Str("error", errorMsg).
		Msg("Pipeline execution completed")

	// Store execution history (you might want to implement a repository for this)
	cs.storeExecutionHistory(execution)
}

// storeExecutionHistory stores the execution history for audit and monitoring
func (cs *CronScheduler) storeExecutionHistory(execution *PipelineExecution) {
	// This could be implemented to store in database, file, or other persistence layer
	// For now, we'll just log the execution details
	executionJSON, err := json.MarshalIndent(execution, "", "  ")
	if err != nil {
		cs.logger.Error().Err(err).Str("execution_id", execution.ID).Msg("Failed to marshal execution history")
		return
	}

	cs.logger.Debug().
		Str("execution_id", execution.ID).
		RawJSON("execution_details", executionJSON).
		Msg("Pipeline execution history")
}

// GetRunningExecutions returns all currently running pipeline executions
func (cs *CronScheduler) GetRunningExecutions() []*PipelineExecution {
	var executions []*PipelineExecution

	cs.runningExecutions.Range(func(key, value interface{}) bool {
		if execution, ok := value.(*PipelineExecution); ok {
			executions = append(executions, execution)
		}
		return true
	})

	return executions
}

// GetPipelines returns all loaded pipelines
func (cs *CronScheduler) GetPipelines() map[string]*Pipeline {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	// Return a copy to prevent external modifications
	pipelines := make(map[string]*Pipeline)
	for name, pipeline := range cs.pipelines {
		pipelines[name] = pipeline
	}

	return pipelines
}

// ReloadPipelines reloads pipeline configurations and reschedules
func (cs *CronScheduler) ReloadPipelines() error {
	cs.logger.Info().Msg("Reloading pipeline configurations")

	// Stop current cron jobs
	cs.cron.Stop()

	// Create new cron instance
	cs.cron = cron.New(cron.WithSeconds())

	// Load and schedule pipelines
	if err := cs.LoadPipelines(); err != nil {
		return err
	}

	if err := cs.schedulePipelines(); err != nil {
		return err
	}

	// Start the scheduler
	cs.cron.Start()

	cs.logger.Info().Msg("Pipeline configurations reloaded successfully")
	return nil
}

// StopPipelineExecution stops a running pipeline execution
func (cs *CronScheduler) StopPipelineExecution(executionID string) error {
	value, ok := cs.runningExecutions.Load(executionID)
	if !ok {
		return fmt.Errorf("execution %s not found", executionID)
	}

	execution, ok := value.(*PipelineExecution)
	if !ok {
		return fmt.Errorf("invalid execution type for %s", executionID)
	}

	cs.logger.Info().Str("execution_id", executionID).Str("pipeline", execution.PipelineName).Msg("Stopping pipeline execution")

	// Stop any running jobs by stopping their tasks
	pythonExecutor := executor.NewPythonExecutor(cs.taskRepo, cs.pathConfig, cs.defaultTimeout)

	for _, job := range execution.Jobs {
		if job.Status == StatusRunning && job.TaskID != "" {
			if err := pythonExecutor.StopTask(job.TaskID); err != nil {
				cs.logger.Warn().Err(err).Str("task_id", job.TaskID).Msg("Failed to stop job task")
			}
		}
	}

	// Finalize the execution as cancelled
	cs.finalizePipelineExecution(execution, StatusCancelled, "Execution stopped by user")

	return nil
}

// TriggerPipeline manually triggers a pipeline execution
func (cs *CronScheduler) TriggerPipeline(pipelineName string) (*PipelineExecution, error) {
	cs.mu.RLock()
	pipeline, exists := cs.pipelines[pipelineName]
	cs.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("pipeline %s not found", pipelineName)
	}

	if !pipeline.Enabled {
		return nil, fmt.Errorf("pipeline %s is disabled", pipelineName)
	}

	cs.logger.Info().Str("pipeline", pipelineName).Msg("Manually triggering pipeline")

	executionID := uuid.New().String()
	execution := &PipelineExecution{
		ID:           executionID,
		PipelineName: pipeline.Name,
		Status:       StatusRunning,
		StartedAt:    time.Now(),
		Jobs:         make([]JobExecution, 0, len(pipeline.Jobs)),
	}

	cs.runningExecutions.Store(executionID, execution)

	// Execute pipeline in goroutine
	go func() {
		defer cs.runningExecutions.Delete(executionID)
		cs.runPipelineJobs(execution, pipeline)
	}()

	return execution, nil
}

// GetExecutionStatus returns the current status of a pipeline execution
func (cs *CronScheduler) GetExecutionStatus(executionID string) (*PipelineExecution, error) {
	value, ok := cs.runningExecutions.Load(executionID)
	if !ok {
		return nil, fmt.Errorf("execution %s not found or completed", executionID)
	}

	execution, ok := value.(*PipelineExecution)
	if !ok {
		return nil, fmt.Errorf("invalid execution type for %s", executionID)
	}

	return execution, nil
}

// ValidatePipelineConfig validates a pipeline configuration
func (cs *CronScheduler) ValidatePipelineConfig(configPath string) error {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	var config PipelineConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	// Validate each pipeline
	for i, pipeline := range config.Pipelines {
		if pipeline.Name == "" {
			return fmt.Errorf("pipeline at index %d: name is required", i)
		}

		if pipeline.CronExpr == "" {
			return fmt.Errorf("pipeline %s: cron_expression is required", pipeline.Name)
		}

		// Validate cron expression
		if _, err := cron.ParseStandard(pipeline.CronExpr); err != nil {
			return fmt.Errorf("pipeline %s: invalid cron expression '%s': %w", pipeline.Name, pipeline.CronExpr, err)
		}

		if len(pipeline.Jobs) == 0 {
			return fmt.Errorf("pipeline %s: at least one job is required", pipeline.Name)
		}

		// Validate each job
		for j, job := range pipeline.Jobs {
			if job.Name == "" {
				return fmt.Errorf("pipeline %s, job at index %d: name is required", pipeline.Name, j)
			}

			if job.ScriptName == "" {
				return fmt.Errorf("pipeline %s, job %s: script_name is required", pipeline.Name, job.Name)
			}
		}
	}

	cs.logger.Info().Int("pipelines", len(config.Pipelines)).Msg("Pipeline configuration validation successful")
	return nil
}
