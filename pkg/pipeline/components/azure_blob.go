package components

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

// AzureBlobReaderComponent reads data from Azure Blob Storage
type AzureBlobReaderComponent struct {
	id     string
	config pipeline.ComponentConfig
	client *azblob.Client
}

// NewAzureBlobReaderComponent creates a new Azure Blob reader component
func NewAzureBlobReaderComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	accountName, _ := config.Parameters["account_name"].(string)
	accountKey, _ := config.Parameters["account_key"].(string)
	container, _ := config.Parameters["container"].(string)
	blob, _ := config.Parameters["blob"].(string)

	if accountName == "" || accountKey == "" {
		return nil, fmt.Errorf("account_name and account_key are required")
	}
	if container == "" || blob == "" {
		return nil, fmt.Errorf("container and blob are required")
	}

	// Create Azure credential
	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure credential: %w", err)
	}

	// Create Azure client
	url := fmt.Sprintf("https://%s.blob.core.windows.net", accountName)
	client, err := azblob.NewClientWithSharedKeyCredential(url, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure client: %w", err)
	}

	return &AzureBlobReaderComponent{
		id:     config.ID,
		config: config,
		client: client,
	}, nil
}

func (c *AzureBlobReaderComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data)

	go func() {
		defer close(output)

		container, _ := c.config.Parameters["container"].(string)
		blob, _ := c.config.Parameters["blob"].(string)

		downloadResponse, err := c.client.DownloadStream(ctx, container, blob, nil)
		if err != nil {
			output <- pipeline.Data{
				Payload:  nil,
				Metadata: map[string]string{"error": err.Error()},
			}
			return
		}
		defer downloadResponse.Body.Close()

		data, err := io.ReadAll(downloadResponse.Body)
		if err != nil {
			output <- pipeline.Data{
				Payload:  nil,
				Metadata: map[string]string{"error": fmt.Sprintf("failed to read data: %v", err)},
			}
			return
		}

		output <- pipeline.Data{
			Payload: data,
			Metadata: map[string]string{
				"container": container,
				"blob":      blob,
				"size":      fmt.Sprintf("%d", len(data)),
			},
		}
	}()

	return output, nil
}

func (c *AzureBlobReaderComponent) Validate() error {
	if c.config.Parameters["account_name"] == nil || c.config.Parameters["account_key"] == nil {
		return fmt.Errorf("account_name and account_key are required")
	}
	if c.config.Parameters["container"] == nil || c.config.Parameters["blob"] == nil {
		return fmt.Errorf("container and blob are required")
	}
	return nil
}

func (c *AzureBlobReaderComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeAzureBlobReader
}

func (c *AzureBlobReaderComponent) ID() string {
	return c.id
}

func (c *AzureBlobReaderComponent) Config() pipeline.ComponentConfig {
	return c.config
}

// AzureBlobWriterComponent writes data to Azure Blob Storage
type AzureBlobWriterComponent struct {
	id     string
	config pipeline.ComponentConfig
	client *azblob.Client
}

// NewAzureBlobWriterComponent creates a new Azure Blob writer component
func NewAzureBlobWriterComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	accountName, _ := config.Parameters["account_name"].(string)
	accountKey, _ := config.Parameters["account_key"].(string)
	container, _ := config.Parameters["container"].(string)
	blob, _ := config.Parameters["blob"].(string)

	if accountName == "" || accountKey == "" {
		return nil, fmt.Errorf("account_name and account_key are required")
	}
	if container == "" || blob == "" {
		return nil, fmt.Errorf("container and blob are required")
	}

	// Create Azure credential
	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure credential: %w", err)
	}

	// Create Azure client
	url := fmt.Sprintf("https://%s.blob.core.windows.net", accountName)
	client, err := azblob.NewClientWithSharedKeyCredential(url, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure client: %w", err)
	}

	return &AzureBlobWriterComponent{
		id:     config.ID,
		config: config,
		client: client,
	}, nil
}

func (c *AzureBlobWriterComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data)

	go func() {
		defer close(output)

		for data := range input {
			container, _ := c.config.Parameters["container"].(string)
			blob, _ := c.config.Parameters["blob"].(string)

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

			_, err := c.client.UploadBuffer(ctx, container, blob, dataBytes, nil)
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
					"container": container,
					"blob":      blob,
					"size":      fmt.Sprintf("%d", len(dataBytes)),
					"status":    "uploaded",
				},
			}
		}
	}()

	return output, nil
}

func (c *AzureBlobWriterComponent) Validate() error {
	if c.config.Parameters["account_name"] == nil || c.config.Parameters["account_key"] == nil {
		return fmt.Errorf("account_name and account_key are required")
	}
	if c.config.Parameters["container"] == nil || c.config.Parameters["blob"] == nil {
		return fmt.Errorf("container and blob are required")
	}
	return nil
}

func (c *AzureBlobWriterComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeAzureBlobWriter
}

func (c *AzureBlobWriterComponent) ID() string {
	return c.id
}

func (c *AzureBlobWriterComponent) Config() pipeline.ComponentConfig {
	return c.config
}
