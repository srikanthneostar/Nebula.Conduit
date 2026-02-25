package components

import (
	"context"
	"testing"
	"time"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLogComponent(t *testing.T) {
	tests := []struct {
		name        string
		config      pipeline.ComponentConfig
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config with all parameters",
			config: pipeline.ComponentConfig{
				ID:   "log-1",
				Type: pipeline.ComponentTypeLog,
				Parameters: map[string]interface{}{
					"log_level":   "info",
					"format":      "json",
					"sample_size": 1000,
					"truncate":    true,
				},
			},
			expectError: false,
		},
		{
			name: "valid config with minimal parameters",
			config: pipeline.ComponentConfig{
				ID:   "log-2",
				Type: pipeline.ComponentTypeLog,
				Parameters: map[string]interface{}{
					"log_level": "debug",
				},
			},
			expectError: false,
		},
		{
			name: "missing log_level parameter",
			config: pipeline.ComponentConfig{
				ID:         "log-3",
				Type:       pipeline.ComponentTypeLog,
				Parameters: map[string]interface{}{},
			},
			expectError: true,
			errorMsg:    "log_level parameter is required",
		},
		{
			name: "invalid log_level",
			config: pipeline.ComponentConfig{
				ID:   "log-4",
				Type: pipeline.ComponentTypeLog,
				Parameters: map[string]interface{}{
					"log_level": "invalid",
				},
			},
			expectError: true,
			errorMsg:    "invalid log_level",
		},
		{
			name: "invalid format",
			config: pipeline.ComponentConfig{
				ID:   "log-5",
				Type: pipeline.ComponentTypeLog,
				Parameters: map[string]interface{}{
					"log_level": "info",
					"format":    "xml",
				},
			},
			expectError: true,
			errorMsg:    "invalid format",
		},
		{
			name: "valid text format",
			config: pipeline.ComponentConfig{
				ID:   "log-6",
				Type: pipeline.ComponentTypeLog,
				Parameters: map[string]interface{}{
					"log_level": "warn",
					"format":    "text",
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			component, err := NewLogComponent(tt.config)

			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
				assert.Nil(t, component)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, component)
				assert.Equal(t, tt.config.ID, component.ID())
				assert.Equal(t, pipeline.ComponentTypeLog, component.Type())
			}
		})
	}
}

func TestLogComponent_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      pipeline.ComponentConfig
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid configuration",
			config: pipeline.ComponentConfig{
				ID:   "log-1",
				Type: pipeline.ComponentTypeLog,
				Parameters: map[string]interface{}{
					"log_level": "info",
					"format":    "json",
				},
			},
			expectError: false,
		},
		{
			name: "all valid log levels",
			config: pipeline.ComponentConfig{
				ID:   "log-2",
				Type: pipeline.ComponentTypeLog,
				Parameters: map[string]interface{}{
					"log_level": "debug",
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			component, err := NewLogComponent(tt.config)
			require.NoError(t, err)

			err = component.Validate()
			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLogComponent_Execute(t *testing.T) {
	t.Run("passes data unchanged to downstream", func(t *testing.T) {
		config := pipeline.ComponentConfig{
			ID:   "log-1",
			Type: pipeline.ComponentTypeLog,
			Parameters: map[string]interface{}{
				"log_level": "info",
			},
		}

		component, err := NewLogComponent(config)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// Create input channel and send test data
		input := make(chan pipeline.Data, 1)
		testData := pipeline.Data{
			Payload: []byte("test payload"),
			Metadata: map[string]string{
				"key": "value",
			},
			Timestamp: time.Now(),
			TraceID:   "trace-123",
		}
		input <- testData
		close(input)

		// Execute component
		output, err := component.Execute(ctx, input)
		require.NoError(t, err)

		// Read output
		receivedData, ok := <-output
		require.True(t, ok, "should receive data from output channel")

		// Verify data is unchanged
		assert.Equal(t, testData.Payload, receivedData.Payload)
		assert.Equal(t, testData.Metadata, receivedData.Metadata)
		assert.Equal(t, testData.Timestamp, receivedData.Timestamp)
		assert.Equal(t, testData.TraceID, receivedData.TraceID)

		// Verify channel is closed
		_, ok = <-output
		assert.False(t, ok, "output channel should be closed")
	})

	t.Run("handles multiple data items", func(t *testing.T) {
		config := pipeline.ComponentConfig{
			ID:   "log-2",
			Type: pipeline.ComponentTypeLog,
			Parameters: map[string]interface{}{
				"log_level": "debug",
			},
		}

		component, err := NewLogComponent(config)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// Create input channel and send multiple data items
		input := make(chan pipeline.Data, 3)
		testData := []pipeline.Data{
			{Payload: []byte("data1"), TraceID: "trace-1", Timestamp: time.Now()},
			{Payload: []byte("data2"), TraceID: "trace-2", Timestamp: time.Now()},
			{Payload: []byte("data3"), TraceID: "trace-3", Timestamp: time.Now()},
		}
		for _, data := range testData {
			input <- data
		}
		close(input)

		// Execute component
		output, err := component.Execute(ctx, input)
		require.NoError(t, err)

		// Read all output
		var receivedData []pipeline.Data
		for data := range output {
			receivedData = append(receivedData, data)
		}

		// Verify all data received unchanged
		assert.Len(t, receivedData, 3)
		for i, data := range receivedData {
			assert.Equal(t, testData[i].Payload, data.Payload)
			assert.Equal(t, testData[i].TraceID, data.TraceID)
		}
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		config := pipeline.ComponentConfig{
			ID:   "log-3",
			Type: pipeline.ComponentTypeLog,
			Parameters: map[string]interface{}{
				"log_level": "info",
			},
		}

		component, err := NewLogComponent(config)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())

		// Create input channel but don't close it
		input := make(chan pipeline.Data, 1)

		// Execute component
		output, err := component.Execute(ctx, input)
		require.NoError(t, err)

		// Cancel context immediately
		cancel()

		// Wait a bit for goroutine to process cancellation
		time.Sleep(100 * time.Millisecond)

		// Try to send data - should not block
		select {
		case input <- pipeline.Data{Payload: []byte("test")}:
			// Data sent, but component should have stopped
		default:
			// Channel full or closed
		}

		// Output channel should eventually close
		timeout := time.After(1 * time.Second)
		select {
		case _, ok := <-output:
			if ok {
				// Might receive data if it was processed before cancellation
			}
		case <-timeout:
			t.Fatal("output channel did not close after context cancellation")
		}
	})
}

func TestLogComponent_DataTruncation(t *testing.T) {
	t.Run("truncates large byte payload", func(t *testing.T) {
		config := pipeline.ComponentConfig{
			ID:   "log-1",
			Type: pipeline.ComponentTypeLog,
			Parameters: map[string]interface{}{
				"log_level":   "info",
				"sample_size": 10.0,
				"truncate":    true,
			},
		}

		component, err := NewLogComponent(config)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// Create large payload
		largePayload := make([]byte, 100)
		for i := range largePayload {
			largePayload[i] = byte('A' + (i % 26))
		}

		input := make(chan pipeline.Data, 1)
		testData := pipeline.Data{
			Payload:   largePayload,
			Timestamp: time.Now(),
			TraceID:   "trace-123",
		}
		input <- testData
		close(input)

		// Execute component
		output, err := component.Execute(ctx, input)
		require.NoError(t, err)

		// Read output - data should be unchanged
		receivedData, ok := <-output
		require.True(t, ok)
		assert.Equal(t, largePayload, receivedData.Payload, "payload should be unchanged in output")
	})

	t.Run("truncates large string payload", func(t *testing.T) {
		config := pipeline.ComponentConfig{
			ID:   "log-2",
			Type: pipeline.ComponentTypeLog,
			Parameters: map[string]interface{}{
				"log_level":   "info",
				"sample_size": 10.0,
				"truncate":    true,
			},
		}

		component, err := NewLogComponent(config)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		largeString := "This is a very long string that should be truncated for logging purposes"

		input := make(chan pipeline.Data, 1)
		testData := pipeline.Data{
			Payload:   largeString,
			Timestamp: time.Now(),
			TraceID:   "trace-123",
		}
		input <- testData
		close(input)

		// Execute component
		output, err := component.Execute(ctx, input)
		require.NoError(t, err)

		// Read output - data should be unchanged
		receivedData, ok := <-output
		require.True(t, ok)
		assert.Equal(t, largeString, receivedData.Payload, "payload should be unchanged in output")
	})

	t.Run("does not truncate when truncate is false", func(t *testing.T) {
		config := pipeline.ComponentConfig{
			ID:   "log-3",
			Type: pipeline.ComponentTypeLog,
			Parameters: map[string]interface{}{
				"log_level":   "info",
				"sample_size": 10.0,
				"truncate":    false,
			},
		}

		component, err := NewLogComponent(config)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		largePayload := make([]byte, 100)
		input := make(chan pipeline.Data, 1)
		testData := pipeline.Data{
			Payload:   largePayload,
			Timestamp: time.Now(),
			TraceID:   "trace-123",
		}
		input <- testData
		close(input)

		// Execute component
		output, err := component.Execute(ctx, input)
		require.NoError(t, err)

		// Read output - data should be unchanged
		receivedData, ok := <-output
		require.True(t, ok)
		assert.Equal(t, largePayload, receivedData.Payload)
	})
}

func TestLogComponent_AllLogLevels(t *testing.T) {
	logLevels := []string{"debug", "info", "warn", "error"}

	for _, level := range logLevels {
		t.Run("log level "+level, func(t *testing.T) {
			config := pipeline.ComponentConfig{
				ID:   "log-" + level,
				Type: pipeline.ComponentTypeLog,
				Parameters: map[string]interface{}{
					"log_level": level,
				},
			}

			component, err := NewLogComponent(config)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			input := make(chan pipeline.Data, 1)
			testData := pipeline.Data{
				Payload:   []byte("test"),
				Timestamp: time.Now(),
				TraceID:   "trace-123",
			}
			input <- testData
			close(input)

			output, err := component.Execute(ctx, input)
			require.NoError(t, err)

			receivedData, ok := <-output
			require.True(t, ok)
			assert.Equal(t, testData.Payload, receivedData.Payload)
		})
	}
}

func TestLogComponent_Formats(t *testing.T) {
	formats := []string{"json", "text"}

	for _, format := range formats {
		t.Run("format "+format, func(t *testing.T) {
			config := pipeline.ComponentConfig{
				ID:   "log-" + format,
				Type: pipeline.ComponentTypeLog,
				Parameters: map[string]interface{}{
					"log_level": "info",
					"format":    format,
				},
			}

			component, err := NewLogComponent(config)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			input := make(chan pipeline.Data, 1)
			testData := pipeline.Data{
				Payload:   map[string]interface{}{"key": "value"},
				Timestamp: time.Now(),
				TraceID:   "trace-123",
			}
			input <- testData
			close(input)

			output, err := component.Execute(ctx, input)
			require.NoError(t, err)

			receivedData, ok := <-output
			require.True(t, ok)
			assert.Equal(t, testData.Payload, receivedData.Payload)
		})
	}
}
