package storage

import "context"

// StorageProvider defines the interface for cloud storage operations
type StorageProvider interface {
	// Upload uploads data to storage
	Upload(ctx context.Context, bucket, key string, data []byte) error

	// Download downloads data from storage
	Download(ctx context.Context, bucket, key string) ([]byte, error)

	// List lists objects in a bucket/prefix
	List(ctx context.Context, bucket, prefix string) ([]string, error)

	// Delete deletes an object from storage
	Delete(ctx context.Context, bucket, key string) error

	// Exists checks if an object exists
	Exists(ctx context.Context, bucket, key string) (bool, error)
}

// StorageConfig holds configuration for storage providers
type StorageConfig struct {
	Type        string `mapstructure:"type"`         // "s3", "minio", "azure", "local"
	Endpoint    string `mapstructure:"endpoint"`     // For S3/MinIO
	Bucket      string `mapstructure:"bucket"`       // Storage bucket/container name
	Region      string `mapstructure:"region"`       // AWS region
	AccessKey   string `mapstructure:"access_key"`   // Access key/username
	SecretKey   string `mapstructure:"secret_key"`   // Secret key/password
	AccountName string `mapstructure:"account_name"` // Azure account name
	AccountKey  string `mapstructure:"account_key"`  // Azure account key
}
