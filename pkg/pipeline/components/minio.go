package components

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// MinIOReaderComponent reads data from MinIO storage
type MinIOReaderComponent struct {
	id     string
	config pipeline.ComponentConfig
	client *s3.S3
}

// NewMinIOReaderComponent creates a new MinIO reader component
func NewMinIOReaderComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	endpoint, _ := config.Parameters["endpoint"].(string)
	accessKey, _ := config.Parameters["access_key"].(string)
	secretKey, _ := config.Parameters["secret_key"].(string)
	bucket, _ := config.Parameters["bucket"].(string)
	key, _ := config.Parameters["key"].(string)
	region, _ := config.Parameters["region"].(string)

	if endpoint == "" {
		return nil, fmt.Errorf("endpoint is required")
	}
	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("access_key and secret_key are required")
	}
	if bucket == "" || key == "" {
		return nil, fmt.Errorf("bucket and key are required")
	}

	if region == "" {
		region = "us-east-1" // Default region for MinIO
	}

	// Create MinIO client
	creds := credentials.NewStaticCredentials(accessKey, secretKey, "")
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(endpoint),
		Region:           aws.String(region),
		Credentials:      creds,
		S3ForcePathStyle: aws.Bool(true), // MinIO requires path style
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create MinIO session: %w", err)
	}

	return &MinIOReaderComponent{
		id:     config.ID,
		config: config,
		client: s3.New(sess),
	}, nil
}

func (c *MinIOReaderComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
	output := make(chan pipeline.Data)

	go func() {
		defer close(output)

		bucket, _ := c.config.Parameters["bucket"].(string)
		key, _ := c.config.Parameters["key"].(string)

		result, err := c.client.GetObjectWithContext(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			output <- pipeline.Data{
				Payload:  nil,
				Metadata: map[string]string{"error": err.Error()},
			}
			return
		}
		defer result.Body.Close()

		data, err := io.ReadAll(result.Body)
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
				"bucket": bucket,
				"key":    key,
				"size":   fmt.Sprintf("%d", len(data)),
			},
		}
	}()

	return output, nil
}

func (c *MinIOReaderComponent) Validate() error {
	if c.config.Parameters["endpoint"] == nil {
		return fmt.Errorf("endpoint is required")
	}
	if c.config.Parameters["access_key"] == nil || c.config.Parameters["secret_key"] == nil {
		return fmt.Errorf("access_key and secret_key are required")
	}
	if c.config.Parameters["bucket"] == nil || c.config.Parameters["key"] == nil {
		return fmt.Errorf("bucket and key are required")
	}
	return nil
}

func (c *MinIOReaderComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeMinIOReader
}

func (c *MinIOReaderComponent) ID() string {
	return c.id
}

func (c *MinIOReaderComponent) Config() pipeline.ComponentConfig {
	return c.config
}

// MinIOWriterComponent writes data to MinIO storage
type MinIOWriterComponent struct {
	id     string
	config pipeline.ComponentConfig
	client *s3.S3
}

// NewMinIOWriterComponent creates a new MinIO writer component
func NewMinIOWriterComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	endpoint, _ := config.Parameters["endpoint"].(string)
	accessKey, _ := config.Parameters["access_key"].(string)
	secretKey, _ := config.Parameters["secret_key"].(string)
	bucket, _ := config.Parameters["bucket"].(string)
	key, _ := config.Parameters["key"].(string)
	region, _ := config.Parameters["region"].(string)

	if endpoint == "" {
		return nil, fmt.Errorf("endpoint is required")
	}
	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("access_key and secret_key are required")
	}
	if bucket == "" || key == "" {
		return nil, fmt.Errorf("bucket and key are required")
	}

	if region == "" {
		region = "us-east-1" // Default region for MinIO
	}

	// Create MinIO client
	creds := credentials.NewStaticCredentials(accessKey, secretKey, "")
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(endpoint),
		Region:           aws.String(region),
		Credentials:      creds,
		S3ForcePathStyle: aws.Bool(true), // MinIO requires path style
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create MinIO session: %w", err)
	}

	return &MinIOWriterComponent{
		id:     config.ID,
		config: config,
		client: s3.New(sess),
	}, nil
}

func (c *MinIOWriterComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
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

			_, err := c.client.PutObjectWithContext(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   aws.ReadSeekCloser(bytes.NewReader(dataBytes)),
			})
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

func (c *MinIOWriterComponent) Validate() error {
	if c.config.Parameters["endpoint"] == nil {
		return fmt.Errorf("endpoint is required")
	}
	if c.config.Parameters["access_key"] == nil || c.config.Parameters["secret_key"] == nil {
		return fmt.Errorf("access_key and secret_key are required")
	}
	if c.config.Parameters["bucket"] == nil || c.config.Parameters["key"] == nil {
		return fmt.Errorf("bucket and key are required")
	}
	return nil
}

func (c *MinIOWriterComponent) Type() pipeline.ComponentType {
	return pipeline.ComponentTypeMinIOWriter
}

func (c *MinIOWriterComponent) ID() string {
	return c.id
}

func (c *MinIOWriterComponent) Config() pipeline.ComponentConfig {
	return c.config
}
