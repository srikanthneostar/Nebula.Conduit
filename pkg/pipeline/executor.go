package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Executor manages pipeline instance execution
type Executor interface {
	// Execute runs a pipeline and returns the instance
	Execute(ctx context.Context, pipeline PipelineDefinition) (*PipelineInstance, error)

	// Stop terminates a running pipeline instance
	Stop(instanceID string) error

	// GetStatus retrieves the current status of a pipeline instance
	GetStatus(instanceID string) (ExecutionStatus, error)
}

// defaultExecutor is the default implementation of Executor
type defaultExecutor struct {
	factory      ComponentFactory
	recorder     ExecutionRecorder
	backpressure BackpressureSystem
	logStore     LogStore
	instances    map[string]*PipelineInstance
	mu           sync.RWMutex
}

// NewExecutor creates a new executor instance
func NewExecutor(factory ComponentFactory) Executor {
	return &defaultExecutor{
		factory:      factory,
		recorder:     nil, // Will be set via SetRecorder if needed
		backpressure: nil, // Will be set via SetBackpressure if needed
		instances:    make(map[string]*PipelineInstance),
	}
}

// SetRecorder sets the execution recorder
func (e *defaultExecutor) SetRecorder(recorder ExecutionRecorder) {
	e.recorder = recorder
}

// SetBackpressure sets the backpressure system
func (e *defaultExecutor) SetBackpressure(bp BackpressureSystem) {
	e.backpressure = bp
}

// SetLogStore sets the log store for print_log components
func (e *defaultExecutor) SetLogStore(store LogStore) {
	e.logStore = store
}

// Execute runs a pipeline and returns the instance
func (e *defaultExecutor) Execute(ctx context.Context, pipeline PipelineDefinition) (*PipelineInstance, error) {
	fmt.Printf("\n========== PIPELINE EXECUTION STARTED ==========\n")
	fmt.Printf("Pipeline ID: %s\n", pipeline.ID)
	fmt.Printf("Pipeline Name: %s\n", pipeline.Name)
	fmt.Printf("Components: %d\n", len(pipeline.Components))
	fmt.Printf("Connections: %d\n", len(pipeline.Connections))
	fmt.Printf("================================================\n\n")

	// Check backpressure before creating new instance
	if e.backpressure != nil {
		// Check if system is under high load
		if e.backpressure.IsHighLoad() {
			fmt.Println("warning: system under high load, queueing pipeline execution")
			// In production, this would queue the execution
			// For now, we'll proceed but log the warning
		}

		// Check if circuit breaker is open
		if e.backpressure.IsCircuitOpen() {
			fmt.Println("warning: circuit breaker open, queueing pipeline execution")
			// In production, this would queue the execution
			// For now, we'll proceed but log the warning
		}

		// Record the operation
		if err := e.backpressure.RecordOperation("pipeline_execution"); err != nil {
			fmt.Printf("warning: failed to record operation: %v\n", err)
		}
	}

	// Validate the pipeline graph
	fmt.Printf("Step 1: Validating pipeline graph...\n")
	if err := ValidateGraph(pipeline); err != nil {
		fmt.Printf("ERROR: Graph validation failed: %v\n", err)
		return nil, fmt.Errorf("invalid pipeline graph: %w", err)
	}
	fmt.Printf("✓ Graph validation passed\n\n")

	// Record execution start
	var executionID string
	if e.recorder != nil {
		var err error
		executionID, err = e.recorder.StartExecution(ctx, pipeline.ID)
		if err != nil {
			fmt.Printf("warning: failed to record execution start: %v\n", err)
		} else {
			fmt.Printf("Execution ID: %s\n\n", executionID)
		}
	}

	// Create pipeline instance
	instanceID := uuid.New().String()
	instanceCtx, cancel := context.WithCancel(ctx)

	instance := &PipelineInstance{
		ID:         instanceID,
		PipelineID: pipeline.ID,
		Status:     InstanceStatusRunning,
		Components: make(map[string]Component),
		Channels:   make(map[string]chan Data),
		Context:    instanceCtx,
		CancelFunc: cancel,
		WaitGroup:  &sync.WaitGroup{},
		StartedAt:  time.Now(),
	}

	fmt.Printf("Step 2: Creating component instances...\n")

	// Create component instances
	for i, compConfig := range pipeline.Components {
		fmt.Printf("  [%d/%d] Creating component: %s (type: %s)\n", i+1, len(pipeline.Components), compConfig.ID, compConfig.Type)

		component, err := e.factory.Create(compConfig)
		if err != nil {
			cancel()
			fmt.Printf("  ERROR: Failed to create component %s: %v\n", compConfig.ID, err)
			return nil, fmt.Errorf("failed to create component %s: %w", compConfig.ID, err)
		}
		fmt.Printf("  ✓ Component %s created successfully\n", compConfig.ID)

		// Validate component configuration
		fmt.Printf("  Validating component %s...\n", compConfig.ID)
		if err := component.Validate(); err != nil {
			cancel()
			fmt.Printf("  ERROR: Component %s validation failed: %v\n", compConfig.ID, err)
			return nil, fmt.Errorf("component %s validation failed: %w", compConfig.ID, err)
		}
		fmt.Printf("  ✓ Component %s validated\n\n", compConfig.ID)

		instance.Components[compConfig.ID] = component

		// Inject LogStore into components that support it (e.g. print_log)
		if injectable, ok := component.(LogStoreInjectable); ok && e.logStore != nil {
			injectable.SetLogStore(e.logStore)
			injectable.SetPipelineContext(pipeline.ID, executionID)
		}
	}
	fmt.Printf("✓ All components created and validated\n\n")

	// Set up channels between connected components
	fmt.Printf("Step 3: Setting up channels...\n")
	if err := e.setupChannels(instance, pipeline); err != nil {
		cancel()
		fmt.Printf("ERROR: Failed to setup channels: %v\n", err)
		return nil, fmt.Errorf("failed to setup channels: %w", err)
	}
	fmt.Printf("✓ Channels setup complete\n\n")

	// Store instance
	e.mu.Lock()
	e.instances[instanceID] = instance
	e.mu.Unlock()

	// Start component goroutines
	fmt.Printf("Step 4: Starting component goroutines...\n")
	if err := e.startComponents(instance, pipeline); err != nil {
		cancel()
		e.mu.Lock()
		delete(e.instances, instanceID)
		e.mu.Unlock()
		fmt.Printf("ERROR: Failed to start components: %v\n", err)
		return nil, fmt.Errorf("failed to start components: %w", err)
	}
	fmt.Printf("✓ All component goroutines started\n\n")

	// Start monitoring goroutine with execution recording
	go e.monitorInstance(instance, executionID)

	fmt.Printf("========== PIPELINE EXECUTION INITIALIZED ==========\n")
	fmt.Printf("Instance ID: %s\n", instanceID)
	fmt.Printf("Status: %s\n", instance.Status)
	fmt.Printf("====================================================\n\n")

	return instance, nil
}

// setupChannels creates channels between connected components
func (e *defaultExecutor) setupChannels(instance *PipelineInstance, pipeline PipelineDefinition) error {
	fmt.Printf("Setting up channels for %d components:\n", len(pipeline.Components))
	// Create a channel for each component's output
	for _, comp := range pipeline.Components {
		// Use buffered channels to prevent blocking
		instance.Channels[comp.ID] = make(chan Data, 100)
		fmt.Printf("  ✓ Channel created for component: %s (buffer: 100)\n", comp.ID)
	}
	fmt.Printf("\n")
	return nil
}

// startComponents spawns goroutines for each component
func (e *defaultExecutor) startComponents(instance *PipelineInstance, pipeline PipelineDefinition) error {
	// Build adjacency list for connections
	graph := buildAdjacencyList(pipeline)

	// Build reverse adjacency list (incoming connections)
	incomingConns := make(map[string][]string)
	for sourceID, targets := range graph {
		for _, targetID := range targets {
			incomingConns[targetID] = append(incomingConns[targetID], sourceID)
		}
	}

	fmt.Printf("Pipeline connection graph:\n")
	for sourceID, targets := range graph {
		if len(targets) > 0 {
			fmt.Printf("  %s → %v\n", sourceID, targets)
		}
	}
	fmt.Printf("\n")

	// Start each component in a goroutine
	for i, compConfig := range pipeline.Components {
		component := instance.Components[compConfig.ID]
		outputChan := instance.Channels[compConfig.ID]

		// Determine component type
		var componentType string
		if _, ok := component.(SourceComponent); ok {
			componentType = "SOURCE"
		} else if _, ok := component.(ProcessorComponent); ok {
			componentType = "PROCESSOR"
		} else if _, ok := component.(SinkComponent); ok {
			componentType = "SINK"
		} else {
			componentType = "GENERIC"
		}

		// Determine input channels for this component
		var inputChans []<-chan Data
		if sources, hasIncoming := incomingConns[compConfig.ID]; hasIncoming {
			fmt.Printf("  [%d/%d] Component %s (%s) has %d input(s): %v\n",
				i+1, len(pipeline.Components), compConfig.ID, componentType, len(sources), sources)

			// When a component has incoming connections, it reads from its own channel
			// if any of its upstream components write to it via fanout (multiple targets)
			// Otherwise, it reads from the upstream component's channel
			readFromOwnChannel := false
			for _, sourceID := range sources {
				if len(graph[sourceID]) > 1 {
					// Source has multiple targets, so it uses fanout
					readFromOwnChannel = true
					break
				}
			}

			if readFromOwnChannel {
				// Fanout case: read from own channel
				inputChans = append(inputChans, instance.Channels[compConfig.ID])
				fmt.Printf("    → Reading from own channel (fanout mode)\n")
			} else {
				// Normal case: read from source channels
				for _, sourceID := range sources {
					inputChans = append(inputChans, instance.Channels[sourceID])
					fmt.Printf("    → Reading from %s's channel\n", sourceID)
				}
			}
		} else {
			fmt.Printf("  [%d/%d] Component %s (%s) has NO inputs (source component)\n",
				i+1, len(pipeline.Components), compConfig.ID, componentType)
		}

		// Determine output channels (where to send data)
		var outputTargets []chan<- Data
		if targets, hasOutgoing := graph[compConfig.ID]; hasOutgoing {
			fmt.Printf("    → Outputs to %d target(s): %v\n", len(targets), targets)
			for _, targetID := range targets {
				outputTargets = append(outputTargets, instance.Channels[targetID])
			}
		} else {
			fmt.Printf("    → NO outputs (sink component)\n")
		}

		instance.WaitGroup.Add(1)
		fmt.Printf("    ✓ Starting goroutine for %s\n\n", compConfig.ID)
		go e.runComponent(instance, component, inputChans, outputChan, outputTargets, compConfig)
	}

	return nil
}

// runComponent executes a single component in a goroutine
func (e *defaultExecutor) runComponent(
	instance *PipelineInstance,
	component Component,
	inputChans []<-chan Data,
	outputChan chan Data,
	outputTargets []chan<- Data,
	config ComponentConfig,
) {
	defer instance.WaitGroup.Done()
	defer func() {
		if r := recover(); r != nil {
			// Recover from panic and log it with stack trace
			panicErr := fmt.Errorf("component %s panicked: %v", component.ID(), r)
			instance.Error = panicErr
			instance.Status = InstanceStatusFailed

			// Log panic with stack trace (in production, use proper logger)
			fmt.Printf("❌ PANIC in component %s: %v\n", component.ID(), r)
			// In production, this would use zerolog with stack trace
			// log.Error().Stack().Err(panicErr).Str("component_id", component.ID()).Msg("Component panicked")
		}
		fmt.Printf("Component %s goroutine exiting\n", component.ID())
	}()

	fmt.Printf("\n▶ Component %s goroutine STARTED\n", component.ID())

	// Create merged input channel if multiple inputs
	var inputChan <-chan Data
	if len(inputChans) == 0 {
		// Source component - no input
		inputChan = nil
		fmt.Printf("  %s: No input channels (source component)\n", component.ID())
	} else if len(inputChans) == 1 {
		// Single input
		inputChan = inputChans[0]
		fmt.Printf("  %s: Single input channel\n", component.ID())
	} else {
		// Multiple inputs - merge them
		inputChan = e.mergeChannels(instance.Context, inputChans)
		fmt.Printf("  %s: Merged %d input channels\n", component.ID(), len(inputChans))
	}

	// Create component executor with retry logic
	componentExecutor := NewComponentExecutor(component, config)

	// Execute component with retry and error handling
	var err error
	var componentOutput <-chan Data

	// Handle different component types
	if _, ok := component.(SourceComponent); ok {
		// Source component
		fmt.Printf("  %s: Executing as SOURCE component\n", component.ID())
		componentOutput, err = componentExecutor.ExecuteSource(instance.Context)
	} else if _, ok := component.(ProcessorComponent); ok {
		// Processor component
		fmt.Printf("  %s: Executing as PROCESSOR component\n", component.ID())
		componentOutput, err = componentExecutor.ExecuteProcessor(instance.Context, inputChan)
	} else if _, ok := component.(SinkComponent); ok {
		// Sink component
		fmt.Printf("  %s: Executing as SINK component\n", component.ID())
		err = componentExecutor.ExecuteSink(instance.Context, inputChan)
		// Sinks don't produce output
		componentOutput = nil
	} else {
		// Fallback to generic Execute with retry
		fmt.Printf("  %s: Executing as GENERIC component\n", component.ID())
		componentOutput, err = componentExecutor.Execute(instance.Context, inputChan)
	}

	if err != nil {
		fmt.Printf("❌ Component %s FAILED: %v\n", component.ID(), err)
		instance.Error = fmt.Errorf("component %s failed: %w", component.ID(), err)
		if !config.ContinueOnError {
			instance.Status = InstanceStatusFailed
			instance.CancelFunc()
		}
		return
	}

	fmt.Printf("  %s: Execution started successfully\n", component.ID())

	// Handle component output and fanout
	if componentOutput != nil {
		if len(outputTargets) > 1 {
			// Fanout case: multiple targets
			fmt.Printf("  %s: Fanning out to %d targets\n", component.ID(), len(outputTargets))
			// Write to all target channels directly
			e.fanOut(instance.Context, componentOutput, outputTargets)
			// Close all target channels after fanout completes
			for i, target := range outputTargets {
				close(target)
				fmt.Printf("  %s: Closed target channel %d\n", component.ID(), i+1)
			}
			// Close own output channel (not used in fanout)
			close(outputChan)
		} else if len(outputTargets) == 1 {
			// Single target: write to own channel, target will read from it
			fmt.Printf("  %s: Forwarding output to single target\n", component.ID())
			go func() {
				defer close(outputChan)
				for {
					select {
					case <-instance.Context.Done():
						return
					case data, ok := <-componentOutput:
						if !ok {
							fmt.Printf("  %s: Component output channel closed\n", component.ID())
							return
						}
						select {
						case outputChan <- data:
							fmt.Printf("  %s: Forwarded data (TraceID: %s)\n", component.ID(), data.TraceID)
						case <-instance.Context.Done():
							return
						}
					}
				}
			}()
		} else {
			// No targets: just drain the output
			fmt.Printf("  %s: No targets, draining output\n", component.ID())
			go func() {
				defer close(outputChan)
				for range componentOutput {
					// Drain to prevent blocking
				}
			}()
		}
	} else {
		// No output from component (sink component)
		fmt.Printf("  %s: No output (sink component completed)\n", component.ID())
		// Don't close outputChan - it may have been closed by upstream fanout
		// or it's not used at all
	}
}

// mergeChannels combines multiple input channels into one
func (e *defaultExecutor) mergeChannels(ctx context.Context, inputs []<-chan Data) <-chan Data {
	merged := make(chan Data, 100)
	var wg sync.WaitGroup

	// Start a goroutine for each input channel
	for _, input := range inputs {
		wg.Add(1)
		go func(ch <-chan Data) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case data, ok := <-ch:
					if !ok {
						return
					}
					select {
					case merged <- data:
					case <-ctx.Done():
						return
					}
				}
			}
		}(input)
	}

	// Close merged channel when all inputs are done
	go func() {
		wg.Wait()
		close(merged)
	}()

	return merged
}

// fanOut sends data from one channel to multiple output channels
// fanOut sends data from one channel to multiple output channels
// Note: This function does NOT close the output channels - the receiving components
// are responsible for closing their own channels when they're done
func (e *defaultExecutor) fanOut(ctx context.Context, input <-chan Data, outputs []chan<- Data) {
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-input:
			if !ok {
				// Input closed
				return
			}
			// Send to all output channels
			for _, output := range outputs {
				select {
				case output <- data:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// monitorInstance waits for the instance to complete and updates status
func (e *defaultExecutor) monitorInstance(instance *PipelineInstance, executionID string) {
	fmt.Printf("\n🔍 Monitor: Waiting for pipeline instance %s to complete...\n", instance.ID)

	// Wait for all components to finish
	instance.WaitGroup.Wait()

	fmt.Printf("\n🏁 Monitor: All components finished for instance %s\n", instance.ID)

	// Update completion time and status
	now := time.Now()
	instance.CompletedAt = &now

	if instance.Status == InstanceStatusRunning {
		if instance.Error != nil {
			instance.Status = InstanceStatusFailed
			fmt.Printf("❌ Pipeline instance %s FAILED: %v\n", instance.ID, instance.Error)
		} else {
			instance.Status = InstanceStatusCompleted
			fmt.Printf("✅ Pipeline instance %s COMPLETED successfully\n", instance.ID)
		}
	}

	duration := now.Sub(instance.StartedAt)
	fmt.Printf("⏱ Execution duration: %v\n", duration)

	// Record execution end
	if e.recorder != nil && executionID != "" {
		err := e.recorder.EndExecution(context.Background(), executionID, instance.Status, instance.Error)
		if err != nil {
			fmt.Printf("warning: failed to record execution end: %v\n", err)
		} else {
			fmt.Printf("✓ Execution recorded: %s\n", executionID)
		}
	}

	// Clean up channels
	for id, ch := range instance.Channels {
		// Drain and close any remaining channels
		go func(c chan Data, componentID string) {
			for range c {
				// Drain channel
			}
		}(ch, id)
	}

	fmt.Printf("\n========== PIPELINE EXECUTION COMPLETED ==========\n")
	fmt.Printf("Instance ID: %s\n", instance.ID)
	fmt.Printf("Final Status: %s\n", instance.Status)
	fmt.Printf("==================================================\n\n")
}

// Stop terminates a running pipeline instance
func (e *defaultExecutor) Stop(instanceID string) error {
	e.mu.RLock()
	instance, exists := e.instances[instanceID]
	e.mu.RUnlock()

	if !exists {
		return fmt.Errorf("instance %s not found", instanceID)
	}

	// Cancel the context to signal all goroutines to stop
	instance.CancelFunc()

	// Wait for all component goroutines to finish (with timeout)
	done := make(chan struct{})
	go func() {
		instance.WaitGroup.Wait()
		close(done)
	}()

	// Wait for completion or timeout
	select {
	case <-done:
		// All goroutines finished
		instance.Status = InstanceStatusStopped
	case <-time.After(30 * time.Second):
		// Timeout - force stop
		instance.Status = InstanceStatusStopped
		fmt.Printf("warning: pipeline instance %s did not stop gracefully within timeout\n", instanceID)
	}

	// Close all channels
	for _, ch := range instance.Channels {
		select {
		case <-ch:
			// Channel already closed or empty
		default:
			close(ch)
		}
	}

	return nil
}

// GetStatus retrieves the current status of a pipeline instance
func (e *defaultExecutor) GetStatus(instanceID string) (ExecutionStatus, error) {
	e.mu.RLock()
	instance, exists := e.instances[instanceID]
	e.mu.RUnlock()

	if !exists {
		return ExecutionStatus{}, fmt.Errorf("instance %s not found", instanceID)
	}

	status := ExecutionStatus{
		InstanceID: instance.ID,
		PipelineID: instance.PipelineID,
		Status:     instance.Status,
		StartedAt:  instance.StartedAt,
		Components: []ComponentStatus{},
	}

	// Add component statuses (simplified for now)
	for id := range instance.Components {
		status.Components = append(status.Components, ComponentStatus{
			ComponentID: id,
			Status:      instance.Status, // Simplified - all components share instance status
			DurationMs:  0,               // TODO: Track individual component durations
		})
	}

	return status, nil
}
