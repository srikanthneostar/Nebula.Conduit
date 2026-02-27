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

// S3ReaderComponent reads data from AWS S3 storage
type S3ReaderComponent struct {
	id     string
	config pipeline.ComponentConfig
	client *s3.S3
}

// NewS3ReaderComponent creates a new S3 reader component
func NewS3ReaderComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	accessKey, _ := config.Parameters["access_key"].(string)
	secretKey, _ := config.Parameters["secret_key"].(string)
	region, _ := config.Parameters["region"].(string)
	bucket, _ := config.Parameters["bucket"].(string)
	key, _ := config.Parameters["key"].(string)

	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("access_key and secret_key are required")
	}
	if region == "" {
		return nil, fmt.Errorf("region is required")
	}
	if bucket == "" || key == "" {
		return nil, fmt.Errorf("bucket and key are required")
	}

	// Create S3 client
	creds := credentials.NewStaticCredentials(accessKey, secretKey, "")
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: creds,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 session: %w", err)
	}

	return &S3ReaderComponent{
		id:     config.ID,
		config: config,
		client: s3.New(sess),
	}, nil
}

func (c *S3ReaderComponent) Execute(ctx context.Context, input <-chan pipeline.Data) (<-chan pipeline.Data, error) {
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

func (c *S3ReaderComponent) Validate() error {
	if c.config.Parameters["access_key"] == nil || c.config.Parameters["secret_key"] == nil {
		return fmt.Errorf("access_key and secret_key are required")
	}
	if c.config.Parameters["region"] == nil {
		return fmt.Errorf("region is required")
	}
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

// S3WriterComponent writes data to AWS S3 storage
type S3WriterComponent struct {
	id     string
	config pipeline.ComponentConfig
	client *s3.S3
}

// NewS3WriterComponent creates a new S3 writer component
func NewS3WriterComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
	accessKey, _ := config.Parameters["access_key"].(string)
	secretKey, _ := config.Parameters["secret_key"].(string)
	region, _ := config.Parameters["region"].(string)
	bucket, _ := config.Parameters["bucket"].(string)
	key, _ := config.Parameters["key"].(string)

	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("access_key and secret_key are required")
	}
	if region == "" {
		return nil, fmt.Errorf("region is required")
	}
	if bucket == "" || key == "" {
		return nil, fmt.Errorf("bucket and key are required")
	}

	// Create S3 client
	creds := credentials.NewStaticCredentials(accessKey, secretKey, "")
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: creds,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 session: %w", err)
	}

	return &S3WriterComponent{
		id:     config.ID,
		config: config,
		client: s3.New(sess),
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

func (c *S3WriterComponent) Validate() error {
	if c.config.Parameters["access_key"] == nil || c.config.Parameters["secret_key"] == nil {
		return fmt.Errorf("access_key and secret_key are required")
	}
	if c.config.Parameters["region"] == nil {
		return fmt.Errorf("region is required")
	}
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
