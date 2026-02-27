package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// LocalStorage implements StorageProvider for local file system
type LocalStorage struct {
	basePath string
}

// NewLocalStorage creates a new local storage client
func NewLocalStorage(config StorageConfig) (*LocalStorage, error) {
	// Use bucket as the base path for local storage
	basePath := config.Bucket
	if basePath == "" {
		basePath = "."
	}

	// Ensure directory exists
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create local directory: %w", err)
	}

	return &LocalStorage{
		basePath: basePath,
	}, nil
}

// Upload uploads data to local file system
func (l *LocalStorage) Upload(ctx context.Context, bucket, key string, data []byte) error {
	basePath := l.basePath
	if bucket != "" {
		basePath = bucket
	}
	fullPath := filepath.Join(basePath, key)

	// Ensure directory exists
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	return os.WriteFile(fullPath, data, 0644)
}

// Download downloads data from local file system
func (l *LocalStorage) Download(ctx context.Context, bucket, key string) ([]byte, error) {
	basePath := l.basePath
	if bucket != "" {
		basePath = bucket
	}
	fullPath := filepath.Join(basePath, key)
	return os.ReadFile(fullPath)
}

// List lists files in local directory
func (l *LocalStorage) List(ctx context.Context, bucket, prefix string) ([]string, error) {
	basePath := l.basePath
	if bucket != "" {
		basePath = bucket
	}
	fullPath := filepath.Join(basePath, prefix)

	entries, err := os.ReadDir(fullPath)
	if err != nil {
		return nil, err
	}

	var keys []string
	for _, entry := range entries {
		if !entry.IsDir() {
			keys = append(keys, entry.Name())
		}
	}
	return keys, nil
}

// Delete deletes a file from local file system
func (l *LocalStorage) Delete(ctx context.Context, bucket, key string) error {
	basePath := l.basePath
	if bucket != "" {
		basePath = bucket
	}
	fullPath := filepath.Join(basePath, key)
	return os.Remove(fullPath)
}

// Exists checks if a file exists in local file system
func (l *LocalStorage) Exists(ctx context.Context, bucket, key string) (bool, error) {
	basePath := l.basePath
	if bucket != "" {
		basePath = bucket
	}
	fullPath := filepath.Join(basePath, key)
	_, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
