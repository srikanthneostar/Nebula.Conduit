package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestExecutor_Execute_BasicPipeline(t *testing.T) {
	// Create a factory with mock components
	factory := NewComponentFactory()

	factory.Register(ComponentTypeHTTPGet, func(config ComponentConfig) (Component, error) {
		return &mockSourceComponent{
			mockComponent: mockComponent{
				id:            config.ID,
				componentType: ComponentTypeHTTPGet,
				config:        config,
			},
		}, nil
	})

	factory.Register(ComponentTypeHTTPPost, func(config ComponentConfig) (Component, error) {
		return &mockSinkComponent{
			mockComponent: mockComponent{
				id:            config.ID,
				componentType: ComponentTypeHTTPPost,
				config:        config,
			},
		}, nil
	})

	// Create executor
	executor := NewExecutor(factory)

	// Create a simple pipeline: source -> sink
	pipeline := PipelineDefinition{
		ID:   "test-pipeline",
		Name: "Test Pipeline",
		Components: []ComponentConfig{
			{
				ID:   "source-1",
				Type: ComponentTypeHTTPGet,
			},
			{
				ID:   "sink-1",
				Type: ComponentTypeHTTPPost,
			},
		},
		Connections: []Connection{
			{
				SourceComponentID: "source-1",
				TargetComponentID: "sink-1",
			},
		},
	}

	// Execute pipeline
	ctx := context.Background()
	instance, err := executor.Execute(ctx, pipeline)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if instance == nil {
		t.Fatal("Expected instance to be non-nil")
	}

	if instance.Status != InstanceStatusRunning {
		t.Errorf("Expected status Running, got %s", instance.Status)
	}

	if instance.PipelineID != pipeline.ID {
		t.Errorf("Expected pipeline ID %s, got %s", pipeline.ID, instance.PipelineID)
	}

	// Wait a bit for execution to complete
	time.Sleep(100 * time.Millisecond)

	// Check status
	status, err := executor.GetStatus(instance.ID)
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}

	if status.InstanceID != instance.ID {
		t.Errorf("Expected instance ID %s, got %s", instance.ID, status.InstanceID)
	}
}

func TestExecutor_Execute_InvalidGraph(t *testing.T) {
	factory := NewComponentFactory()
	executor := NewExecutor(factory)

	// Create a pipeline with no sink (invalid)
	pipeline := PipelineDefinition{
		ID:   "invalid-pipeline",
		Name: "Invalid Pipeline",
		Components: []ComponentConfig{
			{
				ID:   "source-1",
				Type: ComponentTypeHTTPGet,
			},
		},
		Connections: []Connection{},
	}

	// Execute should fail
	ctx := context.Background()
	_, err := executor.Execute(ctx, pipeline)
	if err == nil {
		t.Fatal("Expected error for invalid graph, got nil")
	}
}

func TestExecutor_Stop(t *testing.T) {
	factory := NewComponentFactory()

	// Register a long-running source
	factory.Register(ComponentTypeHTTPGet, func(config ComponentConfig) (Component, error) {
		return &mockSourceComponent{
			mockComponent: mockComponent{
				id:            config.ID,
				componentType: ComponentTypeHTTPGet,
				config:        config,
			},
			startFunc: func(ctx context.Context) (<-chan Data, error) {
				output := make(chan Data)
				go func() {
					defer close(output)
					ticker := time.NewTicker(10 * time.Millisecond)
					defer ticker.Stop()
					for {
						select {
						case <-ctx.Done():
							return
						case <-ticker.C:
							output <- Data{Payload: "data"}
						}
					}
				}()
				return output, nil
			},
		}, nil
	})

	factory.Register(ComponentTypeHTTPPost, func(config ComponentConfig) (Component, error) {
		return &mockSinkComponent{
			mockComponent: mockComponent{
				id:            config.ID,
				componentType: ComponentTypeHTTPPost,
				config:        config,
			},
		}, nil
	})

	executor := NewExecutor(factory)

	pipeline := PipelineDefinition{
		ID:   "test-pipeline",
		Name: "Test Pipeline",
		Components: []ComponentConfig{
			{ID: "source-1", Type: ComponentTypeHTTPGet},
			{ID: "sink-1", Type: ComponentTypeHTTPPost},
		},
		Connections: []Connection{
			{SourceComponentID: "source-1", TargetComponentID: "sink-1"},
		},
	}

	ctx := context.Background()
	instance, err := executor.Execute(ctx, pipeline)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Let it run briefly
	time.Sleep(50 * time.Millisecond)

	// Stop the instance
	err = executor.Stop(instance.ID)
	if err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if instance.Status != InstanceStatusStopped {
		t.Errorf("Expected status Stopped, got %s", instance.Status)
	}
}

func TestExecutor_GetStatus_NotFound(t *testing.T) {
	factory := NewComponentFactory()
	executor := NewExecutor(factory)

	_, err := executor.GetStatus("non-existent-id")
	if err == nil {
		t.Fatal("Expected error for non-existent instance, got nil")
	}
}

func TestExecutor_ContextCancellation(t *testing.T) {
	factory := NewComponentFactory()

	factory.Register(ComponentTypeHTTPGet, func(config ComponentConfig) (Component, error) {
		return &mockSourceComponent{
			mockComponent: mockComponent{
				id:            config.ID,
				componentType: ComponentTypeHTTPGet,
				config:        config,
			},
			startFunc: func(ctx context.Context) (<-chan Data, error) {
				output := make(chan Data)
				go func() {
					defer close(output)
					<-ctx.Done() // Wait for cancellation
				}()
				return output, nil
			},
		}, nil
	})

	factory.Register(ComponentTypeHTTPPost, func(config ComponentConfig) (Component, error) {
		return &mockSinkComponent{
			mockComponent: mockComponent{
				id:            config.ID,
				componentType: ComponentTypeHTTPPost,
				config:        config,
			},
		}, nil
	})

	executor := NewExecutor(factory)

	pipeline := PipelineDefinition{
		ID:   "test-pipeline",
		Name: "Test Pipeline",
		Components: []ComponentConfig{
			{ID: "source-1", Type: ComponentTypeHTTPGet},
			{ID: "sink-1", Type: ComponentTypeHTTPPost},
		},
		Connections: []Connection{
			{SourceComponentID: "source-1", TargetComponentID: "sink-1"},
		},
	}

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	instance, err := executor.Execute(ctx, pipeline)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Cancel the context
	cancel()

	// Wait for goroutines to finish
	time.Sleep(100 * time.Millisecond)

	// Instance should still exist
	status, err := executor.GetStatus(instance.ID)
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}

	if status.InstanceID != instance.ID {
		t.Errorf("Expected instance ID %s, got %s", instance.ID, status.InstanceID)
	}
}

func TestExecutor_FanoutDataDistribution(t *testing.T) {
	factory := NewComponentFactory()

	// Track data received by each sink with mutex protection
	var mu sync.Mutex
	sink1Data := make([]Data, 0)
	sink2Data := make([]Data, 0)
	sink3Data := make([]Data, 0)

	// Register a source that produces multiple data items
	factory.Register(ComponentTypeHTTPGet, func(config ComponentConfig) (Component, error) {
		return &mockSourceComponent{
			mockComponent: mockComponent{
				id:            config.ID,
				componentType: ComponentTypeHTTPGet,
				config:        config,
			},
			startFunc: func(ctx context.Context) (<-chan Data, error) {
				output := make(chan Data)
				go func() {
					defer close(output)
					// Send 5 data items
					for i := 1; i <= 5; i++ {
						select {
						case <-ctx.Done():
							return
						case output <- Data{Payload: i}:
						}
					}
				}()
				return output, nil
			},
		}, nil
	})

	// Register sinks that collect data
	factory.Register(ComponentTypeHTTPPost, func(config ComponentConfig) (Component, error) {
		return &mockSinkComponent{
			mockComponent: mockComponent{
				id:            config.ID,
				componentType: ComponentTypeHTTPPost,
				config:        config,
			},
			writeFunc: func(ctx context.Context, input <-chan Data) error {
				for {
					select {
					case <-ctx.Done():
						return nil
					case data, ok := <-input:
						if !ok {
							return nil
						}
						// Store data based on component ID with mutex protection
						mu.Lock()
						switch config.ID {
						case "sink-1":
							sink1Data = append(sink1Data, data)
						case "sink-2":
							sink2Data = append(sink2Data, data)
						case "sink-3":
							sink3Data = append(sink3Data, data)
						}
						mu.Unlock()
					}
				}
			},
		}, nil
	})

	executor := NewExecutor(factory)

	// Create a fanout pipeline: source -> sink1, sink2, sink3
	pipeline := PipelineDefinition{
		ID:   "fanout-pipeline",
		Name: "Fanout Test Pipeline",
		Components: []ComponentConfig{
			{ID: "source-1", Type: ComponentTypeHTTPGet},
			{ID: "sink-1", Type: ComponentTypeHTTPPost},
			{ID: "sink-2", Type: ComponentTypeHTTPPost},
			{ID: "sink-3", Type: ComponentTypeHTTPPost},
		},
		Connections: []Connection{
			{SourceComponentID: "source-1", TargetComponentID: "sink-1"},
			{SourceComponentID: "source-1", TargetComponentID: "sink-2"},
			{SourceComponentID: "source-1", TargetComponentID: "sink-3"},
		},
	}

	ctx := context.Background()
	instance, err := executor.Execute(ctx, pipeline)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Wait for execution to complete
	instance.WaitGroup.Wait()
	time.Sleep(100 * time.Millisecond)

	// Verify all sinks received all 5 data items
	mu.Lock()
	defer mu.Unlock()

	if len(sink1Data) != 5 {
		t.Errorf("Sink 1 expected 5 items, got %d", len(sink1Data))
	}
	if len(sink2Data) != 5 {
		t.Errorf("Sink 2 expected 5 items, got %d", len(sink2Data))
	}
	if len(sink3Data) != 5 {
		t.Errorf("Sink 3 expected 5 items, got %d", len(sink3Data))
	}

	// Verify all sinks received the same data in the same order
	for i := 0; i < 5; i++ {
		expectedPayload := i + 1

		if len(sink1Data) > i && sink1Data[i].Payload != expectedPayload {
			t.Errorf("Sink 1 item %d: expected payload %d, got %v", i, expectedPayload, sink1Data[i].Payload)
		}
		if len(sink2Data) > i && sink2Data[i].Payload != expectedPayload {
			t.Errorf("Sink 2 item %d: expected payload %d, got %v", i, expectedPayload, sink2Data[i].Payload)
		}
		if len(sink3Data) > i && sink3Data[i].Payload != expectedPayload {
			t.Errorf("Sink 3 item %d: expected payload %d, got %v", i, expectedPayload, sink3Data[i].Payload)
		}
	}
}

func TestExecutor_FanoutWithProcessor(t *testing.T) {
	factory := NewComponentFactory()

	// Track data received by each sink
	sink1Data := make([]Data, 0)
	sink2Data := make([]Data, 0)

	// Register a source
	factory.Register(ComponentTypeHTTPGet, func(config ComponentConfig) (Component, error) {
		return &mockSourceComponent{
			mockComponent: mockComponent{
				id:            config.ID,
				componentType: ComponentTypeHTTPGet,
				config:        config,
			},
			startFunc: func(ctx context.Context) (<-chan Data, error) {
				output := make(chan Data)
				go func() {
					defer close(output)
					for i := 1; i <= 3; i++ {
						select {
						case <-ctx.Done():
							return
						case output <- Data{Payload: i}:
						}
					}
				}()
				return output, nil
			},
		}, nil
	})

	// Register a processor that doubles the value
	factory.Register(ComponentTypeLog, func(config ComponentConfig) (Component, error) {
		return &mockComponent{
			id:            config.ID,
			componentType: ComponentTypeLog,
			config:        config,
			executeFunc: func(ctx context.Context, input <-chan Data) (<-chan Data, error) {
				output := make(chan Data)
				go func() {
					defer close(output)
					for {
						select {
						case <-ctx.Done():
							return
						case data, ok := <-input:
							if !ok {
								return
							}
							// Double the value
							if val, ok := data.Payload.(int); ok {
								data.Payload = val * 2
							}
							select {
							case output <- data:
							case <-ctx.Done():
								return
							}
						}
					}
				}()
				return output, nil
			},
		}, nil
	})

	// Register sinks
	factory.Register(ComponentTypeHTTPPost, func(config ComponentConfig) (Component, error) {
		return &mockSinkComponent{
			mockComponent: mockComponent{
				id:            config.ID,
				componentType: ComponentTypeHTTPPost,
				config:        config,
			},
			writeFunc: func(ctx context.Context, input <-chan Data) error {
				for {
					select {
					case <-ctx.Done():
						return nil
					case data, ok := <-input:
						if !ok {
							return nil
						}
						switch config.ID {
						case "sink-1":
							sink1Data = append(sink1Data, data)
						case "sink-2":
							sink2Data = append(sink2Data, data)
						}
					}
				}
			},
		}, nil
	})

	executor := NewExecutor(factory)

	// Create pipeline: source -> processor -> sink1, sink2
	pipeline := PipelineDefinition{
		ID:   "fanout-processor-pipeline",
		Name: "Fanout with Processor Pipeline",
		Components: []ComponentConfig{
			{ID: "source-1", Type: ComponentTypeHTTPGet},
			{ID: "processor-1", Type: ComponentTypeLog},
			{ID: "sink-1", Type: ComponentTypeHTTPPost},
			{ID: "sink-2", Type: ComponentTypeHTTPPost},
		},
		Connections: []Connection{
			{SourceComponentID: "source-1", TargetComponentID: "processor-1"},
			{SourceComponentID: "processor-1", TargetComponentID: "sink-1"},
			{SourceComponentID: "processor-1", TargetComponentID: "sink-2"},
		},
	}

	ctx := context.Background()
	instance, err := executor.Execute(ctx, pipeline)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Wait for execution to complete
	instance.WaitGroup.Wait()
	time.Sleep(100 * time.Millisecond)

	// Verify both sinks received all 3 processed items
	if len(sink1Data) != 3 {
		t.Errorf("Sink 1 expected 3 items, got %d", len(sink1Data))
	}
	if len(sink2Data) != 3 {
		t.Errorf("Sink 2 expected 3 items, got %d", len(sink2Data))
	}

	// Verify data was processed (doubled) and both sinks got the same data
	expectedValues := []int{2, 4, 6} // Original 1,2,3 doubled
	for i := 0; i < 3; i++ {
		if len(sink1Data) > i {
			if val, ok := sink1Data[i].Payload.(int); !ok || val != expectedValues[i] {
				t.Errorf("Sink 1 item %d: expected %d, got %v", i, expectedValues[i], sink1Data[i].Payload)
			}
		}
		if len(sink2Data) > i {
			if val, ok := sink2Data[i].Payload.(int); !ok || val != expectedValues[i] {
				t.Errorf("Sink 2 item %d: expected %d, got %v", i, expectedValues[i], sink2Data[i].Payload)
			}
		}
	}
}

func TestExecutor_FanoutEmptyData(t *testing.T) {
	factory := NewComponentFactory()

	sinkCalled := false

	// Register a source that produces no data
	factory.Register(ComponentTypeHTTPGet, func(config ComponentConfig) (Component, error) {
		return &mockSourceComponent{
			mockComponent: mockComponent{
				id:            config.ID,
				componentType: ComponentTypeHTTPGet,
				config:        config,
			},
			startFunc: func(ctx context.Context) (<-chan Data, error) {
				output := make(chan Data)
				close(output) // Close immediately without sending data
				return output, nil
			},
		}, nil
	})

	// Register sink
	factory.Register(ComponentTypeHTTPPost, func(config ComponentConfig) (Component, error) {
		return &mockSinkComponent{
			mockComponent: mockComponent{
				id:            config.ID,
				componentType: ComponentTypeHTTPPost,
				config:        config,
			},
			writeFunc: func(ctx context.Context, input <-chan Data) error {
				for range input {
					sinkCalled = true
				}
				return nil
			},
		}, nil
	})

	executor := NewExecutor(factory)

	// Create fanout pipeline with empty source
	pipeline := PipelineDefinition{
		ID:   "fanout-empty-pipeline",
		Name: "Fanout Empty Pipeline",
		Components: []ComponentConfig{
			{ID: "source-1", Type: ComponentTypeHTTPGet},
			{ID: "sink-1", Type: ComponentTypeHTTPPost},
			{ID: "sink-2", Type: ComponentTypeHTTPPost},
		},
		Connections: []Connection{
			{SourceComponentID: "source-1", TargetComponentID: "sink-1"},
			{SourceComponentID: "source-1", TargetComponentID: "sink-2"},
		},
	}

	ctx := context.Background()
	instance, err := executor.Execute(ctx, pipeline)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Wait for execution to complete
	instance.WaitGroup.Wait()
	time.Sleep(100 * time.Millisecond)

	// Verify sinks were not called with data (but should complete successfully)
	if sinkCalled {
		t.Error("Expected sinks not to receive data, but they did")
	}

	// Verify instance completed successfully
	if instance.Status != InstanceStatusCompleted && instance.Status != InstanceStatusRunning {
		t.Errorf("Expected status Completed or Running, got %s. Error: %v", instance.Status, instance.Error)
	}
}

func TestExecutor_MixedFanoutAndDirectInputsDeliverAllData(t *testing.T) {
	factory := NewComponentFactory()

	var mu sync.Mutex
	sharedSinkData := make([]string, 0)
	extraSinkData := make([]string, 0)

	factory.Register(ComponentTypeHTTPGet, func(config ComponentConfig) (Component, error) {
		return &mockSourceComponent{
			mockComponent: mockComponent{
				id:            config.ID,
				componentType: ComponentTypeHTTPGet,
				config:        config,
			},
			startFunc: func(ctx context.Context) (<-chan Data, error) {
				output := make(chan Data)
				go func() {
					defer close(output)

					var payloads []string
					switch config.ID {
					case "source-fanout":
						payloads = []string{"fanout-1", "fanout-2", "fanout-3"}
					case "source-direct":
						payloads = []string{"direct-1", "direct-2"}
					default:
						payloads = []string{}
					}

					for _, payload := range payloads {
						select {
						case <-ctx.Done():
							return
						case output <- Data{Payload: payload}:
						}
					}
				}()
				return output, nil
			},
		}, nil
	})

	factory.Register(ComponentTypeHTTPPost, func(config ComponentConfig) (Component, error) {
		return &mockSinkComponent{
			mockComponent: mockComponent{
				id:            config.ID,
				componentType: ComponentTypeHTTPPost,
				config:        config,
			},
			writeFunc: func(ctx context.Context, input <-chan Data) error {
				for {
					select {
					case <-ctx.Done():
						return nil
					case data, ok := <-input:
						if !ok {
							return nil
						}

						payload, _ := data.Payload.(string)
						mu.Lock()
						if config.ID == "sink-shared" {
							sharedSinkData = append(sharedSinkData, payload)
						} else if config.ID == "sink-extra" {
							extraSinkData = append(extraSinkData, payload)
						}
						mu.Unlock()
					}
				}
			},
		}, nil
	})

	executor := NewExecutor(factory)

	pipeline := PipelineDefinition{
		ID:   "mixed-fanout-pipeline",
		Name: "Mixed Fanout Pipeline",
		Components: []ComponentConfig{
			{ID: "source-fanout", Type: ComponentTypeHTTPGet},
			{ID: "source-direct", Type: ComponentTypeHTTPGet},
			{ID: "sink-shared", Type: ComponentTypeHTTPPost},
			{ID: "sink-extra", Type: ComponentTypeHTTPPost},
		},
		Connections: []Connection{
			{SourceComponentID: "source-fanout", TargetComponentID: "sink-shared"},
			{SourceComponentID: "source-fanout", TargetComponentID: "sink-extra"},
			{SourceComponentID: "source-direct", TargetComponentID: "sink-shared"},
		},
	}

	instance, err := executor.Execute(context.Background(), pipeline)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	instance.WaitGroup.Wait()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(extraSinkData) != 3 {
		t.Fatalf("expected extra sink to receive 3 fanout items, got %d", len(extraSinkData))
	}

	if len(sharedSinkData) != 5 {
		t.Fatalf("expected shared sink to receive 5 mixed-topology items, got %d", len(sharedSinkData))
	}

	seen := make(map[string]int)
	for _, payload := range sharedSinkData {
		seen[payload]++
	}

	for _, expected := range []string{"fanout-1", "fanout-2", "fanout-3", "direct-1", "direct-2"} {
		if seen[expected] != 1 {
			t.Fatalf("expected shared sink to receive %q exactly once, got count=%d", expected, seen[expected])
		}
	}
}
