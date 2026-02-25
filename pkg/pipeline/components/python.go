package components

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/rs/zerolog"

	"github.com/Xecutables/Nebula.Conduit/pkg/logger"
	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

// PythonCodeBlockComponent executes inline Python code for data transformation
type PythonCodeBlockComponent struct {
	config        pipeline.ComponentConfig
	code          string
	environment   map[string]string
	pythonCommand string
	logger        zerolog.Logger
}

// NewPythonCodeBlockComponent creates a new Python Code Block component
func NewPythonCodeBlockComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	// Extract code from parameters
	code, ok := config.Parameters["code"].(string)
	if !ok || code == "" {
		return nil, fmt.Errorf("code parameter is required and must be a non-empty string")
	}

	// Extract environment variables (optional)
	environment := make(map[string]string)
	if envParam, ok := config.Parameters["environment"].(map[string]interface{}); ok {
		for k, v := range envParam {
			if strVal, ok := v.(string); ok {
				environment[k] = strVal
			}
		}
	}

	// Determine Python command
	pythonCmd := "python"
	if _, err := exec.LookPath("py"); err == nil {
		pythonCmd = "py"
	} else if _, err := exec.LookPath("python3"); err == nil {
		pythonCmd = "python3"
	}

	return &PythonCodeBlockComponent{
		config:        config,
		code:          code,
		environment:   environment,
		pythonCommand: pythonCmd,
		logger:        logger.InitLogger(),
	}, nil
}

// Execute runs the Python Code Block component logic
func (p *PythonCodeBlockComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data, 10)

	go func() {
		defer close(output)

		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-input:
				if !ok {
					// Input channel closed, we're done
					return
				}

				// Execute Python code with input data
				result, err := p.executeCode(ctx, data)
				if err != nil {
					p.logger.Error().Err(err).Str("component_id", p.config.ID).Msg("Python code execution failed")
					// Continue if continue_on_error is true
					if !p.config.ContinueOnError {
						return
					}
					continue
				}

				// Send result to output channel
				select {
				case output <- result:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return output, nil
}

// executeCode executes the Python code with the given input data
func (p *PythonCodeBlockComponent) executeCode(ctx context.Context, inputData pipeline.Data) (pipeline.Data, error) {
	// Create a temporary directory for the script
	tempDir, err := os.MkdirTemp("", "pipeline-python-*")
	if err != nil {
		return pipeline.Data{}, fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a temporary Python script file
	scriptPath := filepath.Join(tempDir, "script.py")

	// Prepare the Python script with input/output handling
	fullScript := fmt.Sprintf(`
import sys
import json

# Input data from pipeline
input_data = '''%s'''

# User code
%s

# Output data (user should set output_data variable)
if 'output_data' in locals():
    print(output_data, end='')
else:
    print(input_data, end='')
`, p.convertPayloadToString(inputData.Payload), p.code)

	// Write script to file
	if err := os.WriteFile(scriptPath, []byte(fullScript), 0644); err != nil {
		return pipeline.Data{}, fmt.Errorf("failed to write script file: %w", err)
	}

	// Apply timeout from config
	timeout := p.config.Timeout
	if timeout == 0 {
		timeout = 60 * time.Second // Default timeout
	}

	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Prepare command
	cmd := exec.CommandContext(execCtx, p.pythonCommand, scriptPath)
	cmd.Dir = tempDir

	// Set environment variables
	cmd.Env = os.Environ()
	for k, v := range p.environment {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	// Execute and capture output
	outputBytes, err := cmd.CombinedOutput()
	if err != nil {
		// Check if it was a timeout
		if execCtx.Err() == context.DeadlineExceeded {
			return pipeline.Data{}, fmt.Errorf("Python code execution exceeded timeout of %v", timeout)
		}
		return pipeline.Data{}, fmt.Errorf("Python code execution failed: %w, output: %s", err, string(outputBytes))
	}

	// Create result data
	result := pipeline.Data{
		Payload:   outputBytes,
		Metadata:  make(map[string]string),
		Timestamp: time.Now(),
		TraceID:   inputData.TraceID,
	}

	// Copy input metadata and add execution info
	for k, v := range inputData.Metadata {
		result.Metadata[k] = v
	}
	result.Metadata["python_executed"] = "true"
	result.Metadata["execution_time"] = time.Now().Format(time.RFC3339)

	return result, nil
}

// convertPayloadToString converts the payload to a string for Python input
func (p *PythonCodeBlockComponent) convertPayloadToString(payload interface{}) string {
	switch v := payload.(type) {
	case []byte:
		return string(v)
	case string:
		return v
	default:
		// Try to convert to string
		return fmt.Sprintf("%v", v)
	}
}

// Validate checks if the component configuration is valid
func (p *PythonCodeBlockComponent) Validate() error {
	if p.code == "" {
		return fmt.Errorf("code is required and must be non-empty")
	}

	// Check if Python is available
	if _, err := exec.LookPath(p.pythonCommand); err != nil {
		return fmt.Errorf("Python interpreter not found: %w", err)
	}

	return nil
}

// Type returns the component type identifier
func (p *PythonCodeBlockComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypePythonCodeBlock
}

// ID returns the unique component instance identifier
func (p *PythonCodeBlockComponent) ID() string {
	return p.config.ID
}

// Config returns the component configuration
func (p *PythonCodeBlockComponent) Config() pipeline.ComponentConfig {
	return p.config
}
