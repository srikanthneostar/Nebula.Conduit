package pipeline

import (
	"sync"
	"sync/atomic"
)

// DefaultMetricsCollector is a simple in-memory metrics collector
type DefaultMetricsCollector struct {
	totalExecutions    atomic.Int64
	activePipelines    atomic.Int64
	runningInstances   atomic.Int64
	componentErrors    map[ComponentType]*atomic.Int64
	executionDurations []float64
	mu                 sync.Mutex
}

func NewDefaultMetricsCollector() *DefaultMetricsCollector {
	return &DefaultMetricsCollector{
		componentErrors: make(map[ComponentType]*atomic.Int64),
	}
}

func (m *DefaultMetricsCollector) RecordExecution(pipelineID string, duration float64, success bool) {
	m.totalExecutions.Add(1)
	m.mu.Lock()
	m.executionDurations = append(m.executionDurations, duration)
	m.mu.Unlock()
}

func (m *DefaultMetricsCollector) RecordComponentError(componentType ComponentType) {
	m.mu.Lock()
	counter, exists := m.componentErrors[componentType]
	if !exists {
		counter = &atomic.Int64{}
		m.componentErrors[componentType] = counter
	}
	m.mu.Unlock()
	counter.Add(1)
}

func (m *DefaultMetricsCollector) SetActivePipelines(count int) {
	m.activePipelines.Store(int64(count))
}

func (m *DefaultMetricsCollector) SetRunningInstances(count int) {
	m.runningInstances.Store(int64(count))
}

func (m *DefaultMetricsCollector) IncrementTotalExecutions() {
	m.totalExecutions.Add(1)
}

// Snapshot returns current metrics values for reporting
type MetricsSnapshot struct {
	TotalExecutions  int64            `json:"total_executions"`
	ActivePipelines  int64            `json:"active_pipelines"`
	RunningInstances int64            `json:"running_instances"`
	ComponentErrors  map[string]int64 `json:"component_errors"`
	AvgDurationMs    float64          `json:"avg_execution_duration_ms"`
}

func (m *DefaultMetricsCollector) Snapshot() MetricsSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	errors := make(map[string]int64)
	for ct, counter := range m.componentErrors {
		errors[string(ct)] = counter.Load()
	}

	var avgDuration float64
	if len(m.executionDurations) > 0 {
		var sum float64
		for _, d := range m.executionDurations {
			sum += d
		}
		avgDuration = sum / float64(len(m.executionDurations))
	}

	return MetricsSnapshot{
		TotalExecutions:  m.totalExecutions.Load(),
		ActivePipelines:  m.activePipelines.Load(),
		RunningInstances: m.runningInstances.Load(),
		ComponentErrors:  errors,
		AvgDurationMs:    avgDuration,
	}
}
