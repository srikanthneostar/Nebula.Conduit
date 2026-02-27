package components

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
	"github.com/Xecutables/Nebula.Conduit/pkg/storage"
)

// S3ReaderComponent reads data from S3/MinIO storage
type S3ReaderComponent struct {
	id       string
	config   pipeline.ComponentConfig
	provider storage.StorageProvider
}

// NewS3ReaderComponent creates a new S3 reader component
func NewS3ReaderComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	bucket, _ := config.Parameters["bucket"].(string)
	key, _ := config.Parameters["key"].(string)
	region, _ := config.Parameters["region"].(string)
	endpoint, _ := config.Parameters["endpoint"].(string)
	accessKey, _ := config.Parameters["access_key"].(string)
	secretKey, _ := config.Parameters["secret_key"].(string)

	if bucket == "" || key == "" {
		return nil, fmt.Errorf("bucket and key are required")
	}

	storageType := "s3"
	if storageTypeParam, ok := config.Parameters["storage_type"].(string); ok {
		storageType = storageTypeParam
	}

	storageConfig := storage.StorageConfig{
		Type:      storageType,
		Endpoint:  endpoint,
		Bucket:    bucket,
		Region:    region,
		AccessKey: accessKey,
		SecretKey: secretKey,
	}

	provider, err := storage.NewStorage(storageConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage provider: %w", err)
	}

	return &S3ReaderComponent{
		id:       config.ID,
		config:   config,
		provider: provider,
	}, nil
}

func (c *S3ReaderComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data)

	go func() {
		defer close(output)

		bucket, _ := c.config.Parameters["bucket"].(string)
		key, _ := c.config.Parameters["key"].(string)

		data, err := c.provider.Download(ctx, bucket, key)
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
				"bucket": bucket,
				"key":    key,
				"size":   fmt.Sprintf("%d", len(data)),
			},
		}
	}()

	return output, nil
}

func (c *S3ReaderComponent) Validate() error {
	if c.config.Parameters["bucket"] == nil || c.config.Parameters["key"] == nil {
		return fmt.Errorf("bucket and key are required")
	}
	return nil
}

func (c *S3ReaderComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeS3Reader
}

func (c *S3ReaderComponent) ID() string {
	return c.id
}

func (c *S3ReaderComponent) Config() pipeline.ComponentConfig {
	return c.config
}

// S3WriterComponent writes data to S3/MinIO storage
type S3WriterComponent struct {
	id       string
	config   pipeline.ComponentConfig
	provider storage.StorageProvider
}

// NewS3WriterComponent creates a new S3 writer component
func NewS3WriterComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	bucket, _ := config.Parameters["bucket"].(string)
	key, _ := config.Parameters["key"].(string)
	region, _ := config.Parameters["region"].(string)
	endpoint, _ := config.Parameters["endpoint"].(string)
	accessKey, _ := config.Parameters["access_key"].(string)
	secretKey, _ := config.Parameters["secret_key"].(string)

	if bucket == "" || key == "" {
		return nil, fmt.Errorf("bucket and key are required")
	}

	storageType := "s3"
	if storageTypeParam, ok := config.Parameters["storage_type"].(string); ok {
		storageType = storageTypeParam
	}

	storageConfig := storage.StorageConfig{
		Type:      storageType,
		Endpoint:  endpoint,
		Bucket:    bucket,
		Region:    region,
		AccessKey: accessKey,
		SecretKey: secretKey,
	}

	provider, err := storage.NewStorage(storageConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage provider: %w", err)
	}

	return &S3WriterComponent{
		id:       config.ID,
		config:   config,
		provider: provider,
	}, nil
}

func (c *S3WriterComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data)

	go func() {
		defer close(output)

		for data := range input {
			bucket, _ := c.config.Parameters["bucket"].(string)
			key, _ := c.config.Parameters["key"].(string)

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

			err := c.provider.Upload(ctx, bucket, key, dataBytes)
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
					"bucket": bucket,
					"key":    key,
					"size":   fmt.Sprintf("%d", len(dataBytes)),
					"status": "uploaded",
				},
			}
		}
	}()

	return output, nil
}

func (c *S3WriterComponent) Validate() error {
	if c.config.Parameters["bucket"] == nil || c.config.Parameters["key"] == nil {
		return fmt.Errorf("bucket and key are required")
	}
	return nil
}

func (c *S3WriterComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeS3Writer
}

func (c *S3WriterComponent) ID() string {
	return c.id
}

func (c *S3WriterComponent) Config() pipeline.ComponentConfig {
	return c.config
}

// AzureBlobReaderComponent reads data from Azure Blob Storage
type AzureBlobReaderComponent struct {
	id       string
	config   pipeline.ComponentConfig
	provider storage.StorageProvider
}

// NewAzureBlobReaderComponent creates a new Azure Blob reader component
func NewAzureBlobReaderComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	container, _ := config.Parameters["container"].(string)
	blob, _ := config.Parameters["blob"].(string)
	accountName, _ := config.Parameters["account_name"].(string)
	accountKey, _ := config.Parameters["account_key"].(string)

	if container == "" || blob == "" {
		return nil, fmt.Errorf("container and blob are required")
	}

	storageConfig := storage.StorageConfig{
		Type:        "azure",
		Bucket:      container,
		AccountName: accountName,
		AccountKey:  accountKey,
	}

	provider, err := storage.NewStorage(storageConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage provider: %w", err)
	}

	return &AzureBlobReaderComponent{
		id:       config.ID,
		config:   config,
		provider: provider,
	}, nil
}

func (c *AzureBlobReaderComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data)

	go func() {
		defer close(output)

		container, _ := c.config.Parameters["container"].(string)
		blob, _ := c.config.Parameters["blob"].(string)

		data, err := c.provider.Download(ctx, container, blob)
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
				"container": container,
				"blob":      blob,
				"size":      fmt.Sprintf("%d", len(data)),
			},
		}
	}()

	return output, nil
}

func (c *AzureBlobReaderComponent) Validate() error {
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
	id       string
	config   pipeline.ComponentConfig
	provider storage.StorageProvider
}

// NewAzureBlobWriterComponent creates a new Azure Blob writer component
func NewAzureBlobWriterComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	container, _ := config.Parameters["container"].(string)
	blob, _ := config.Parameters["blob"].(string)
	accountName, _ := config.Parameters["account_name"].(string)
	accountKey, _ := config.Parameters["account_key"].(string)

	if container == "" || blob == "" {
		return nil, fmt.Errorf("container and blob are required")
	}

	storageConfig := storage.StorageConfig{
		Type:        "azure",
		Bucket:      container,
		AccountName: accountName,
		AccountKey:  accountKey,
	}

	provider, err := storage.NewStorage(storageConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage provider: %w", err)
	}

	return &AzureBlobWriterComponent{
		id:       config.ID,
		config:   config,
		provider: provider,
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

			err := c.provider.Upload(ctx, container, blob, dataBytes)
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

// LocalStorageReaderComponent reads data from local file system
type LocalStorageReaderComponent struct {
	id       string
	config   pipeline.ComponentConfig
	provider storage.StorageProvider
}

// NewLocalStorageReaderComponent creates a new local storage reader component
func NewLocalStorageReaderComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	path, _ := config.Parameters["path"].(string)
	basePath, _ := config.Parameters["base_path"].(string)

	if path == "" {
		return nil, fmt.Errorf("path is required")
	}

	storageConfig := storage.StorageConfig{
		Type:   "local",
		Bucket: basePath,
	}

	provider, err := storage.NewStorage(storageConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage provider: %w", err)
	}

	return &LocalStorageReaderComponent{
		id:       config.ID,
		config:   config,
		provider: provider,
	}, nil
}

func (c *LocalStorageReaderComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data)

	go func() {
		defer close(output)

		path, _ := c.config.Parameters["path"].(string)
		basePath, _ := c.config.Parameters["base_path"].(string)

		data, err := c.provider.Download(ctx, basePath, path)
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
	provider storage.StorageProvider
}

// NewLocalStorageWriterComponent creates a new local storage writer component
func NewLocalStorageWriterComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	path, _ := config.Parameters["path"].(string)
	basePath, _ := config.Parameters["base_path"].(string)

	if path == "" {
		return nil, fmt.Errorf("path is required")
	}

	storageConfig := storage.StorageConfig{
		Type:   "local",
		Bucket: basePath,
	}

	provider, err := storage.NewStorage(storageConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage provider: %w", err)
	}

	return &LocalStorageWriterComponent{
		id:       config.ID,
		config:   config,
		provider: provider,
	}, nil
}

func (c *LocalStorageWriterComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data)

	go func() {
		defer close(output)

		for data := range input {
			path, _ := c.config.Parameters["path"].(string)
			basePath, _ := c.config.Parameters["base_path"].(string)

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

			err := c.provider.Upload(ctx, basePath, path, dataBytes)
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
