package pipeline

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
)

// PipelineEngine is the core system that manages pipeline lifecycle, execution, and state
type PipelineEngine struct {
	repository       PipelineRepository
	scheduler        Scheduler
	executor         Executor
	lifecycleManager LifecycleManager
	componentFactory ComponentFactory
	backpressure     BackpressureSystem
	recorder         ExecutionRecorder
	logger           *zerolog.Logger
	metrics          MetricsCollector
}

// MetricsCollector collects and exposes pipeline metrics
type MetricsCollector interface {
	// RecordExecution records a pipeline execution
	RecordExecution(pipelineID string, duration float64, success bool)

	// RecordComponentError records a component error
	RecordComponentError(componentType ComponentType)

	// SetActivePipelines sets the number of active pipelines
	SetActivePipelines(count int)

	// SetRunningInstances sets the number of running instances
	SetRunningInstances(count int)

	// IncrementTotalExecutions increments the total execution counter
	IncrementTotalExecutions()
}

// PipelineEngineConfig holds configuration for the pipeline engine
type PipelineEngineConfig struct {
	MaxConcurrentInstances    int
	MaxGoroutinesPerInstance  int
	ExecutionHistoryRetention int // days
	MetricsEnabled            bool
	LogLevel                  string
}

// NewPipelineEngine creates a new pipeline engine instance
func NewPipelineEngine(
	repository PipelineRepository,
	componentFactory ComponentFactory,
	backpressure BackpressureSystem,
	logger *zerolog.Logger,
	metrics MetricsCollector,
	config PipelineEngineConfig,
) *PipelineEngine {
	// Create executor
	executor := NewExecutor(componentFactory)

	// Create recorder
	recorder := NewExecutionRecorder(repository)

	// Set recorder and backpressure on executor
	if defaultExec, ok := executor.(*defaultExecutor); ok {
		defaultExec.SetRecorder(recorder)
		defaultExec.SetBackpressure(backpressure)
	}

	// Create scheduler
	scheduler := NewScheduler(executor)

	// Create lifecycle manager
	lifecycleManager := NewLifecycleManager(repository, executor, scheduler)

	return &PipelineEngine{
		repository:       repository,
		scheduler:        scheduler,
		executor:         executor,
		lifecycleManager: lifecycleManager,
		componentFactory: componentFactory,
		backpressure:     backpressure,
		recorder:         recorder,
		logger:           logger,
		metrics:          metrics,
	}
}

// Initialize initializes the pipeline engine and loads active pipelines
func (e *PipelineEngine) Initialize(ctx context.Context) error {
	e.logger.Info().Msg("Initializing pipeline engine")

	// Start the scheduler
	e.scheduler.Start()

	// Load active pipelines
	activePipelines, err := e.repository.ListActive()
	if err != nil {
		return fmt.Errorf("failed to load active pipelines: %w", err)
	}

	e.logger.Info().Int("count", len(activePipelines)).Msg("Loading active pipelines")

	// Start each active pipeline
	for _, pipeline := range activePipelines {
		if err := e.lifecycleManager.Start(pipeline.ID); err != nil {
			e.logger.Error().
				Err(err).
				Str("pipeline_id", pipeline.ID).
				Str("pipeline_name", pipeline.Name).
				Msg("Failed to start pipeline")
			// Continue with other pipelines
			continue
		}

		e.logger.Info().
			Str("pipeline_id", pipeline.ID).
			Str("pipeline_name", pipeline.Name).
			Str("execution_mode", string(pipeline.ExecutionMode)).
			Msg("Started pipeline")
	}

	// Update metrics
	if e.metrics != nil {
		e.metrics.SetActivePipelines(len(activePipelines))
	}

	e.logger.Info().Msg("Pipeline engine initialized successfully")
	return nil
}

// Shutdown gracefully shuts down the pipeline engine
func (e *PipelineEngine) Shutdown(ctx context.Context) error {
	e.logger.Info().Msg("Shutting down pipeline engine")

	// Stop the scheduler
	e.scheduler.Stop()

	// Get all running instances
	runningInstances := e.lifecycleManager.GetRunningInstances()

	e.logger.Info().Int("count", len(runningInstances)).Msg("Stopping running pipeline instances")

	// Stop all running instances
	for _, instance := range runningInstances {
		if err := e.executor.Stop(instance.ID); err != nil {
			e.logger.Error().
				Err(err).
				Str("instance_id", instance.ID).
				Str("pipeline_id", instance.PipelineID).
				Msg("Failed to stop pipeline instance")
			// Continue with other instances
			continue
		}
	}

	e.logger.Info().Msg("Pipeline engine shut down successfully")
	return nil
}

// GetRepository returns the pipeline repository
func (e *PipelineEngine) GetRepository() PipelineRepository {
	return e.repository
}

// GetExecutor returns the executor
func (e *PipelineEngine) GetExecutor() Executor {
	return e.executor
}

// GetScheduler returns the scheduler
func (e *PipelineEngine) GetScheduler() Scheduler {
	return e.scheduler
}

// GetLifecycleManager returns the lifecycle manager
func (e *PipelineEngine) GetLifecycleManager() LifecycleManager {
	return e.lifecycleManager
}

// GetRecorder returns the execution recorder
func (e *PipelineEngine) GetRecorder() ExecutionRecorder {
	return e.recorder
}
