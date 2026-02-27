package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// AzureStorage implements StorageProvider for Azure Blob Storage
type AzureStorage struct {
	client *azblob.Client
	bucket string // Container name
}

// NewAzureStorage creates a new Azure Blob Storage client
func NewAzureStorage(config StorageConfig) (*AzureStorage, error) {
	// Create credential
	var cred *azblob.SharedKeyCredential
	if config.AccountName != "" && config.AccountKey != "" {
		var err error
		cred, err = azblob.NewSharedKeyCredential(config.AccountName, config.AccountKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure credential: %w", err)
		}
	} else {
		return nil, fmt.Errorf("Azure account_name and account_key are required")
	}

	// Create client
	url := fmt.Sprintf("https://%s.blob.core.windows.net", config.AccountName)
	client, err := azblob.NewClientWithSharedKeyCredential(url, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure client: %w", err)
	}

	return &AzureStorage{
		client: client,
		bucket: config.Bucket,
	}, nil
}

// Upload uploads data to Azure Blob Storage
func (a *AzureStorage) Upload(ctx context.Context, bucket, key string, data []byte) error {
	if bucket == "" {
		bucket = a.bucket
	}
	_, err := a.client.UploadBuffer(ctx, bucket, key, data, nil)
	return err
}

// Download downloads data from Azure Blob Storage
func (a *AzureStorage) Download(ctx context.Context, bucket, key string) ([]byte, error) {
	if bucket == "" {
		bucket = a.bucket
	}
	downloadResponse, err := a.client.DownloadStream(ctx, bucket, key, nil)
	if err != nil {
		return nil, err
	}
	defer downloadResponse.Body.Close()

	return io.ReadAll(downloadResponse.Body)
}

// List lists blobs in Azure Blob Storage
func (a *AzureStorage) List(ctx context.Context, bucket, prefix string) ([]string, error) {
	if bucket == "" {
		bucket = a.bucket
	}
	// List blobs with prefix
	pager := a.client.NewListBlobsFlatPager(bucket, &azblob.ListBlobsFlatOptions{
		Prefix: &prefix,
	})

	var keys []string
	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, blob := range resp.Segment.BlobItems {
			if blob.Name != nil {
				keys = append(keys, *blob.Name)
			}
		}
	}
	return keys, nil
}

// Delete deletes a blob from Azure Blob Storage
func (a *AzureStorage) Delete(ctx context.Context, bucket, key string) error {
	if bucket == "" {
		bucket = a.bucket
	}
	_, err := a.client.DeleteBlob(ctx, bucket, key, nil)
	return err
}

// Exists checks if a blob exists in Azure Blob Storage
func (a *AzureStorage) Exists(ctx context.Context, bucket, key string) (bool, error) {
	if bucket == "" {
		bucket = a.bucket
	}
	blobClient := a.client.ServiceClient().NewContainerClient(bucket).NewBlobClient(key)
	_, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
