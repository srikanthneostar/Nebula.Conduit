package pipeline

import (
	"fmt"
	"sync"
)

// LifecycleManager manages pipeline lifecycle operations
type LifecycleManager interface {
	// Start activates a pipeline
	Start(pipelineID string) error

	// Stop deactivates a pipeline and stops all running instances
	Stop(pipelineID string) error

	// GetRunningInstances returns all currently running pipeline instances
	GetRunningInstances() []PipelineInstance

	// GetExecutionHistory retrieves execution history for a pipeline
	GetExecutionHistory(pipelineID string) ([]ExecutionRecord, error)
}

// defaultLifecycleManager implements the LifecycleManager interface
type defaultLifecycleManager struct {
	repository PipelineRepository
	executor   Executor
	scheduler  Scheduler
	instances  map[string][]*PipelineInstance // pipelineID -> list of instances
	mu         sync.RWMutex
}

// NewLifecycleManager creates a new lifecycle manager instance
func NewLifecycleManager(repository PipelineRepository, executor Executor, scheduler Scheduler) LifecycleManager {
	return &defaultLifecycleManager{
		repository: repository,
		executor:   executor,
		scheduler:  scheduler,
		instances:  make(map[string][]*PipelineInstance),
	}
}

// Start activates a pipeline
func (lm *defaultLifecycleManager) Start(pipelineID string) error {
	// Load pipeline definition
	pipeline, err := lm.repository.Read(pipelineID)
	if err != nil {
		return fmt.Errorf("failed to load pipeline %s: %w", pipelineID, err)
	}

	// Update status to active if not already
	if pipeline.Status != PipelineStatusActive {
		pipeline.Status = PipelineStatusActive
		if err := lm.repository.Update(pipeline); err != nil {
			return fmt.Errorf("failed to update pipeline status: %w", err)
		}
	}

	// Handle based on execution mode
	switch pipeline.ExecutionMode {
	case ExecutionModeScheduled:
		// Schedule the pipeline
		if err := lm.scheduler.Schedule(pipeline); err != nil {
			return fmt.Errorf("failed to schedule pipeline: %w", err)
		}

	case ExecutionModeContinuous:
		// Start continuous execution
		// TODO: Implement continuous execution with context management
		return fmt.Errorf("continuous execution mode not yet implemented")

	default:
		return fmt.Errorf("unknown execution mode: %s", pipeline.ExecutionMode)
	}

	return nil
}

// Stop deactivates a pipeline and stops all running instances
func (lm *defaultLifecycleManager) Stop(pipelineID string) error {
	// Load pipeline definition
	pipeline, err := lm.repository.Read(pipelineID)
	if err != nil {
		return fmt.Errorf("failed to load pipeline %s: %w", pipelineID, err)
	}

	// Update status to inactive
	pipeline.Status = PipelineStatusInactive
	if err := lm.repository.Update(pipeline); err != nil {
		return fmt.Errorf("failed to update pipeline status: %w", err)
	}

	// Unschedule if scheduled
	if pipeline.ExecutionMode == ExecutionModeScheduled {
		if err := lm.scheduler.Unschedule(pipelineID); err != nil {
			// Log error but don't fail - pipeline might not be scheduled
			fmt.Printf("warning: failed to unschedule pipeline %s: %v\n", pipelineID, err)
		}
	}

	// Stop all running instances
	lm.mu.Lock()
	instances := lm.instances[pipelineID]
	delete(lm.instances, pipelineID)
	lm.mu.Unlock()

	for _, instance := range instances {
		if err := lm.executor.Stop(instance.ID); err != nil {
			// Log error but continue stopping other instances
			fmt.Printf("warning: failed to stop instance %s: %v\n", instance.ID, err)
		}
	}

	return nil
}

// GetRunningInstances returns all currently running pipeline instances
func (lm *defaultLifecycleManager) GetRunningInstances() []PipelineInstance {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	var result []PipelineInstance
	for _, instances := range lm.instances {
		for _, instance := range instances {
			if instance.Status == InstanceStatusRunning {
				result = append(result, *instance)
			}
		}
	}

	return result
}

// GetExecutionHistory retrieves execution history for a pipeline
func (lm *defaultLifecycleManager) GetExecutionHistory(pipelineID string) ([]ExecutionRecord, error) {
	// TODO: Implement database query for execution history
	// This will be implemented when we add execution recording
	return []ExecutionRecord{}, nil
}

// RegisterInstance registers a running instance with the lifecycle manager
func (lm *defaultLifecycleManager) RegisterInstance(instance *PipelineInstance) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.instances[instance.PipelineID] = append(lm.instances[instance.PipelineID], instance)
}

// UnregisterInstance removes an instance from the lifecycle manager
func (lm *defaultLifecycleManager) UnregisterInstance(instanceID string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	for pipelineID, instances := range lm.instances {
		for i, instance := range instances {
			if instance.ID == instanceID {
				// Remove instance from slice
				lm.instances[pipelineID] = append(instances[:i], instances[i+1:]...)
				return
			}
		}
	}
}
