package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// LifecycleManager manages pipeline lifecycle operations
type LifecycleManager interface {
	Start(pipelineID string) error
	Stop(pipelineID string) error
	GetRunningInstances() []PipelineInstance
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

func NewLifecycleManager(repository PipelineRepository, executor Executor, scheduler Scheduler) LifecycleManager {
	return &defaultLifecycleManager{
		repository: repository,
		executor:   executor,
		scheduler:  scheduler,
		instances:  make(map[string][]*PipelineInstance),
	}
}

func (lm *defaultLifecycleManager) Start(pipelineID string) error {
	pipeline, err := lm.repository.Read(pipelineID)
	if err != nil {
		return fmt.Errorf("failed to load pipeline %s: %w", pipelineID, err)
	}

	if pipeline.Status != PipelineStatusActive {
		pipeline.Status = PipelineStatusActive
		if err := lm.repository.Update(pipeline); err != nil {
			return fmt.Errorf("failed to update pipeline status: %w", err)
		}
	}

	switch pipeline.ExecutionMode {
	case ExecutionModeScheduled:
		if err := lm.scheduler.Schedule(pipeline); err != nil {
			return fmt.Errorf("failed to schedule pipeline: %w", err)
		}
	case ExecutionModeContinuous:
		return lm.startContinuous(pipeline)
	default:
		return fmt.Errorf("unknown execution mode: %s", pipeline.ExecutionMode)
	}
	return nil
}

// startContinuous starts a pipeline in continuous execution mode
func (lm *defaultLifecycleManager) startContinuous(pipeline PipelineDefinition) error {
	ctx := context.Background()
	instance, err := lm.executor.Execute(ctx, pipeline)
	if err != nil {
		return fmt.Errorf("failed to start continuous pipeline: %w", err)
	}

	lm.mu.Lock()
	lm.instances[pipeline.ID] = append(lm.instances[pipeline.ID], instance)
	lm.mu.Unlock()

	// Monitor and restart on failure
	go func() {
		for {
			instance.WaitGroup.Wait()

			// Check if pipeline is still active
			p, err := lm.repository.Read(pipeline.ID)
			if err != nil || p.Status != PipelineStatusActive {
				return
			}

			// Restart after a brief delay
			time.Sleep(5 * time.Second)

			newInstance, err := lm.executor.Execute(context.Background(), pipeline)
			if err != nil {
				fmt.Printf("failed to restart continuous pipeline %s: %v\n", pipeline.ID, err)
				return
			}

			lm.mu.Lock()
			lm.instances[pipeline.ID] = append(lm.instances[pipeline.ID], newInstance)
			lm.mu.Unlock()

			instance = newInstance
		}
	}()

	return nil
}

func (lm *defaultLifecycleManager) Stop(pipelineID string) error {
	pipeline, err := lm.repository.Read(pipelineID)
	if err != nil {
		return fmt.Errorf("failed to load pipeline %s: %w", pipelineID, err)
	}

	pipeline.Status = PipelineStatusInactive
	if err := lm.repository.Update(pipeline); err != nil {
		return fmt.Errorf("failed to update pipeline status: %w", err)
	}

	if pipeline.ExecutionMode == ExecutionModeScheduled {
		if err := lm.scheduler.Unschedule(pipelineID); err != nil {
			fmt.Printf("warning: failed to unschedule pipeline %s: %v\n", pipelineID, err)
		}
	}

	lm.mu.Lock()
	instances := lm.instances[pipelineID]
	delete(lm.instances, pipelineID)
	lm.mu.Unlock()

	for _, instance := range instances {
		if err := lm.executor.Stop(instance.ID); err != nil {
			fmt.Printf("warning: failed to stop instance %s: %v\n", instance.ID, err)
		}
	}

	return nil
}

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

func (lm *defaultLifecycleManager) GetExecutionHistory(pipelineID string) ([]ExecutionRecord, error) {
	// This is now handled by the recorder directly via the API handlers
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
				lm.instances[pipelineID] = append(instances[:i], instances[i+1:]...)
				return
			}
		}
	}
}
