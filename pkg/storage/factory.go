package storage

import (
	"fmt"
)

// NewStorage creates a new storage client based on type
func NewStorage(config StorageConfig) (StorageProvider, error) {
	switch config.Type {
	case "s3", "minio":
		return NewS3Storage(config)
	case "azure":
		return NewAzureStorage(config)
	case "local":
		return NewLocalStorage(config)
	default:
		return nil, fmt.Errorf("unknown storage type: %s", config.Type)
	}
}
