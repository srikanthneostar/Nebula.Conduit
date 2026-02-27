package pipeline

import (
	"context"
	"fmt"
	"sync"

	"github.com/robfig/cron/v3"
)

// Scheduler manages pipeline execution scheduling
type Scheduler interface {
	// Schedule registers a pipeline for scheduled execution
	Schedule(pipeline PipelineDefinition) error

	// Unschedule removes a pipeline from the schedule
	Unschedule(pipelineID string) error

	// Trigger manually triggers a pipeline execution
	Trigger(pipelineID string) error

	// Start begins the scheduler
	Start()

	// Stop stops the scheduler
	Stop()
}

// defaultScheduler implements the Scheduler interface using cron
type defaultScheduler struct {
	cron              *cron.Cron
	executor          Executor
	entries           map[string]cron.EntryID // pipelineID -> cron entry ID
	runningExecutions map[string]bool         // pipelineID -> is running
	mu                sync.RWMutex
}

// NewScheduler creates a new scheduler instance
func NewScheduler(executor Executor) Scheduler {
	return &defaultScheduler{
		cron:              cron.New(),
		executor:          executor,
		entries:           make(map[string]cron.EntryID),
		runningExecutions: make(map[string]bool),
	}
}

// Schedule registers a pipeline for scheduled execution
func (s *defaultScheduler) Schedule(pipeline PipelineDefinition) error {
	if pipeline.ExecutionMode != ExecutionModeScheduled {
		return fmt.Errorf("pipeline %s is not in scheduled mode", pipeline.ID)
	}

	if pipeline.CronExpression == "" {
		return fmt.Errorf("pipeline %s has no cron expression", pipeline.ID)
	}

	// Validate cron expression by parsing it
	_, err := cron.ParseStandard(pipeline.CronExpression)
	if err != nil {
		return fmt.Errorf("invalid cron expression %q: %w", pipeline.CronExpression, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Remove existing schedule if present
	if entryID, exists := s.entries[pipeline.ID]; exists {
		s.cron.Remove(entryID)
		delete(s.entries, pipeline.ID)
	}

	// Add new schedule
	entryID, err := s.cron.AddFunc(pipeline.CronExpression, func() {
		// Check if pipeline is already running
		s.mu.Lock()
		isRunning := s.runningExecutions[pipeline.ID]
		if isRunning {
			s.mu.Unlock()
			fmt.Printf("⚠️  BACKPRESSURE: Pipeline %s (%s) is still running from previous execution, skipping this scheduled run\n",
				pipeline.Name, pipeline.ID)
			return
		}
		// Mark as running
		s.runningExecutions[pipeline.ID] = true
		s.mu.Unlock()

		fmt.Printf("▶️  SCHEDULER: Starting scheduled execution of pipeline %s (%s)\n", pipeline.Name, pipeline.ID)

		// Execute pipeline in background
		ctx := context.Background()
		instance, err := s.executor.Execute(ctx, pipeline)
		if err != nil {
			// Log error and mark as not running
			fmt.Printf("❌ SCHEDULER: Scheduled execution of pipeline %s failed: %v\n", pipeline.ID, err)
			s.mu.Lock()
			s.runningExecutions[pipeline.ID] = false
			s.mu.Unlock()
			return
		}

		// Monitor the instance and mark as not running when complete
		go func() {
			instance.WaitGroup.Wait()
			s.mu.Lock()
			s.runningExecutions[pipeline.ID] = false
			s.mu.Unlock()
			fmt.Printf("✅ SCHEDULER: Pipeline %s (%s) execution completed\n", pipeline.Name, pipeline.ID)
		}()
	})

	if err != nil {
		return fmt.Errorf("failed to schedule pipeline %s: %w", pipeline.ID, err)
	}

	s.entries[pipeline.ID] = entryID
	return nil
}

// Unschedule removes a pipeline from the schedule
func (s *defaultScheduler) Unschedule(pipelineID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entryID, exists := s.entries[pipelineID]
	if !exists {
		return fmt.Errorf("pipeline %s is not scheduled", pipelineID)
	}

	s.cron.Remove(entryID)
	delete(s.entries, pipelineID)
	delete(s.runningExecutions, pipelineID)
	return nil
}

// Trigger manually triggers a pipeline execution
func (s *defaultScheduler) Trigger(pipelineID string) error {
	// This method is for manual triggering, not cron-based
	// It should be implemented by the caller using the executor directly
	return fmt.Errorf("trigger should be called through the executor, not the scheduler")
}

// Start begins the scheduler
func (s *defaultScheduler) Start() {
	s.cron.Start()
}

// Stop stops the scheduler
func (s *defaultScheduler) Stop() {
	ctx := s.cron.Stop()
	<-ctx.Done()
}
