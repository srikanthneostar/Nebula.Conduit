package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// S3Storage implements StorageProvider for AWS S3 and S3-compatible services (MinIO)
type S3Storage struct {
	client *s3.S3
	bucket string
}

// NewS3Storage creates a new S3 storage client
func NewS3Storage(config StorageConfig) (*S3Storage, error) {
	// Build credentials
	creds := credentials.NewStaticCredentials(config.AccessKey, config.SecretKey, "")

	// Build session
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(config.Endpoint),
		Region:           aws.String(config.Region),
		Credentials:      creds,
		S3ForcePathStyle: aws.Bool(config.Type == "minio"), // MinIO requires path style
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 session: %w", err)
	}

	return &S3Storage{
		client: s3.New(sess),
		bucket: config.Bucket,
	}, nil
}

// Upload uploads data to S3
func (s *S3Storage) Upload(ctx context.Context, bucket, key string, data []byte) error {
	if bucket == "" {
		bucket = s.bucket
	}
	_, err := s.client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   aws.ReadSeekCloser(bytes.NewReader(data)),
	})
	return err
}

// Download downloads data from S3
func (s *S3Storage) Download(ctx context.Context, bucket, key string) ([]byte, error) {
	if bucket == "" {
		bucket = s.bucket
	}
	result, err := s.client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer result.Body.Close()

	return io.ReadAll(result.Body)
}

// List lists objects in S3
func (s *S3Storage) List(ctx context.Context, bucket, prefix string) ([]string, error) {
	if bucket == "" {
		bucket = s.bucket
	}
	result, err := s.client.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return nil, err
	}

	keys := make([]string, len(result.Contents))
	for i, obj := range result.Contents {
		keys[i] = *obj.Key
	}
	return keys, nil
}

// Delete deletes an object from S3
func (s *S3Storage) Delete(ctx context.Context, bucket, key string) error {
	if bucket == "" {
		bucket = s.bucket
	}
	_, err := s.client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return err
}

// Exists checks if an object exists in S3
func (s *S3Storage) Exists(ctx context.Context, bucket, key string) (bool, error) {
	if bucket == "" {
		bucket = s.bucket
	}
	_, err := s.client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func isNotFound(err error) bool {
	// Check for common S3 not found errors
	if err == nil {
		return false
	}
	errStr := err.Error()
	return containsAny(errStr, []string{
		"NotFound",
		"NoSuchKey",
		"404",
	})
}

func containsAny(str string, substrs []string) bool {
	for _, substr := range substrs {
		if len(str) >= len(substr) {
			for i := 0; i <= len(str)-len(substr); i++ {
				if str[i:i+len(substr)] == substr {
					return true
				}
			}
		}
	}
	return false
}
