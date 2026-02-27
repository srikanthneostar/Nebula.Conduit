package components

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

// LocalStorageReaderComponent reads data from local file system
type LocalStorageReaderComponent struct {
	id       string
	config   pipeline.ComponentConfig
	basePath string
}

// NewLocalStorageReaderComponent creates a new local storage reader component
func NewLocalStorageReaderComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	path, _ := config.Parameters["path"].(string)
	basePath, _ := config.Parameters["base_path"].(string)

	if path == "" {
		return nil, fmt.Errorf("path is required")
	}

	if basePath == "" {
		basePath = "." // Default to current directory
	}

	return &LocalStorageReaderComponent{
		id:       config.ID,
		config:   config,
		basePath: basePath,
	}, nil
}

func (c *LocalStorageReaderComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data)

	go func() {
		defer close(output)

		path, _ := c.config.Parameters["path"].(string)
		fullPath := filepath.Join(c.basePath, path)

		data, err := os.ReadFile(fullPath)
		if err != nil {
			output <- pipeline.Data{
				Payload:  nil,
				Metadata: map[string]string{"error": err.Error()},
			}
			return
		}

		output <- pipeline.Data{
			Payload: data,
			Metadata: map[string]string{
				"path": path,
				"size": fmt.Sprintf("%d", len(data)),
			},
		}
	}()

	return output, nil
}

func (c *LocalStorageReaderComponent) Validate() error {
	if c.config.Parameters["path"] == nil {
		return fmt.Errorf("path is required")
	}
	return nil
}

func (c *LocalStorageReaderComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeLocalStorageReader
}

func (c *LocalStorageReaderComponent) ID() string {
	return c.id
}

func (c *LocalStorageReaderComponent) Config() pipeline.ComponentConfig {
	return c.config
}

// LocalStorageWriterComponent writes data to local file system
type LocalStorageWriterComponent struct {
	id       string
	config   pipeline.ComponentConfig
	basePath string
}

// NewLocalStorageWriterComponent creates a new local storage writer component
func NewLocalStorageWriterComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	path, _ := config.Parameters["path"].(string)
	basePath, _ := config.Parameters["base_path"].(string)

	if path == "" {
		return nil, fmt.Errorf("path is required")
	}

	if basePath == "" {
		basePath = "." // Default to current directory
	}

	// Ensure base directory exists
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	return &LocalStorageWriterComponent{
		id:       config.ID,
		config:   config,
		basePath: basePath,
	}, nil
}

func (c *LocalStorageWriterComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data)

	go func() {
		defer close(output)

		for data := range input {
			path, _ := c.config.Parameters["path"].(string)
			fullPath := filepath.Join(c.basePath, path)

			var dataBytes []byte
			switch v := data.Payload.(type) {
			case []byte:
				dataBytes = v
			case string:
				dataBytes = []byte(v)
			default:
				jsonData, err := json.Marshal(v)
				if err != nil {
					output <- pipeline.Data{
						Payload:  nil,
						Metadata: map[string]string{"error": fmt.Sprintf("failed to marshal data: %v", err)},
					}
					continue
				}
				dataBytes = jsonData
			}

			// Ensure directory exists
			dir := filepath.Dir(fullPath)
			if err := os.MkdirAll(dir, 0755); err != nil {
				output <- pipeline.Data{
					Payload:  nil,
					Metadata: map[string]string{"error": fmt.Sprintf("failed to create directory: %v", err)},
				}
				continue
			}

			err := os.WriteFile(fullPath, dataBytes, 0644)
			if err != nil {
				output <- pipeline.Data{
					Payload:  nil,
					Metadata: map[string]string{"error": err.Error()},
				}
				continue
			}

			output <- pipeline.Data{
				Payload: data.Payload,
				Metadata: map[string]string{
					"path":   path,
					"size":   fmt.Sprintf("%d", len(dataBytes)),
					"status": "written",
				},
			}
		}
	}()

	return output, nil
}

func (c *LocalStorageWriterComponent) Validate() error {
	if c.config.Parameters["path"] == nil {
		return fmt.Errorf("path is required")
	}
	return nil
}

func (c *LocalStorageWriterComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeLocalStorageWriter
}

func (c *LocalStorageWriterComponent) ID() string {
	return c.id
}

func (c *LocalStorageWriterComponent) Config() pipeline.ComponentConfig {
	return c.config
}
