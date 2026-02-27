package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"time"

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
	config           PipelineEngineConfig
	db               *sql.DB
	cleanupCancel    context.CancelFunc
}

// MetricsCollector collects and exposes pipeline metrics
type MetricsCollector interface {
	RecordExecution(pipelineID string, duration float64, success bool)
	RecordComponentError(componentType ComponentType)
	SetActivePipelines(count int)
	SetRunningInstances(count int)
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
	db *sql.DB,
	repository PipelineRepository,
	componentFactory ComponentFactory,
	backpressure BackpressureSystem,
	logger *zerolog.Logger,
	metrics MetricsCollector,
	config PipelineEngineConfig,
) *PipelineEngine {
	recorder := NewExecutionRecorder(db)
	executor := NewExecutor(componentFactory)

	if defaultExec, ok := executor.(*defaultExecutor); ok {
		defaultExec.SetRecorder(recorder)
		defaultExec.SetBackpressure(backpressure)
	}

	scheduler := NewScheduler(executor)
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
		config:           config,
		db:               db,
	}
}

// Initialize initializes the pipeline engine and loads active pipelines
func (e *PipelineEngine) Initialize(ctx context.Context) error {
	e.logger.Info().Msg("Initializing pipeline engine")

	e.scheduler.Start()

	activePipelines, err := e.repository.ListActive()
	if err != nil {
		return fmt.Errorf("failed to load active pipelines: %w", err)
	}

	e.logger.Info().Int("count", len(activePipelines)).Msg("Loading active pipelines")

	for _, pipeline := range activePipelines {
		if err := e.lifecycleManager.Start(pipeline.ID); err != nil {
			e.logger.Error().Err(err).
				Str("pipeline_id", pipeline.ID).
				Str("pipeline_name", pipeline.Name).
				Msg("Failed to start pipeline")
			continue
		}
		e.logger.Info().
			Str("pipeline_id", pipeline.ID).
			Str("pipeline_name", pipeline.Name).
			Str("execution_mode", string(pipeline.ExecutionMode)).
			Msg("Started pipeline")
	}

	if e.metrics != nil {
		e.metrics.SetActivePipelines(len(activePipelines))
	}

	// Start execution history cleanup if retention is configured
	if e.config.ExecutionHistoryRetention > 0 {
		cleanupCtx, cancel := context.WithCancel(ctx)
		e.cleanupCancel = cancel
		go e.runCleanupLoop(cleanupCtx)
	}

	e.logger.Info().Msg("Pipeline engine initialized successfully")
	return nil
}

// Shutdown gracefully shuts down the pipeline engine
func (e *PipelineEngine) Shutdown(ctx context.Context) error {
	e.logger.Info().Msg("Shutting down pipeline engine")

	if e.cleanupCancel != nil {
		e.cleanupCancel()
	}

	e.scheduler.Stop()

	runningInstances := e.lifecycleManager.GetRunningInstances()
	e.logger.Info().Int("count", len(runningInstances)).Msg("Stopping running pipeline instances")

	for _, instance := range runningInstances {
		if err := e.executor.Stop(instance.ID); err != nil {
			e.logger.Error().Err(err).
				Str("instance_id", instance.ID).
				Str("pipeline_id", instance.PipelineID).
				Msg("Failed to stop pipeline instance")
			continue
		}
	}

	e.logger.Info().Msg("Pipeline engine shut down successfully")

	// Close backpressure resources
	if e.backpressure != nil {
		if closer, ok := e.backpressure.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				e.logger.Error().Err(err).Msg("Failed to close backpressure resources")
			}
		}
	}

	return nil
}

// runCleanupLoop periodically cleans up old execution records
func (e *PipelineEngine) runCleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			deleted, err := e.recorder.DeleteOldExecutions(ctx, e.config.ExecutionHistoryRetention)
			if err != nil {
				e.logger.Error().Err(err).Msg("Failed to clean up old execution records")
			} else if deleted > 0 {
				e.logger.Info().Int64("deleted", deleted).Msg("Cleaned up old execution records")
			}
		}
	}
}

func (e *PipelineEngine) GetRepository() PipelineRepository     { return e.repository }
func (e *PipelineEngine) GetExecutor() Executor                 { return e.executor }
func (e *PipelineEngine) GetScheduler() Scheduler               { return e.scheduler }
func (e *PipelineEngine) GetLifecycleManager() LifecycleManager { return e.lifecycleManager }
func (e *PipelineEngine) GetRecorder() ExecutionRecorder        { return e.recorder }
func (e *PipelineEngine) GetConfig() PipelineEngineConfig       { return e.config }
