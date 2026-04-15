package s3bucket

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Xecutables/Nebula.Conduit/config"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// S3BucketService provides file download capabilities from an S3-compatible bucket.
// Configuration is resolved from environment variables first, then falls back to
// config.yaml values, and finally to hardcoded defaults.
type S3BucketService struct {
	client   *s3.S3
	bucket   string
	endpoint string
}

// S3Config holds the resolved S3 connection parameters.
type S3Config struct {
	Endpoint  string
	Bucket    string
	UseSSL    bool
	Region    string
	AccessKey string
	SecretKey string
}

// defaults matching the user-provided values
const (
	defaultEndpoint  = "192.168.1.40:9000"
	defaultBucket    = "nebula-configurations"
	defaultUseSSL    = "false"
	defaultRegion    = "us-east-1"
	defaultAccessKey = "minioadmin"
	defaultSecretKey = "minioadmin123"
)

// resolveConfig builds S3Config by checking env vars, then config file values, then defaults.
func resolveConfig(cfg *config.Config) S3Config {
	return S3Config{
		Endpoint:  firstNonEmpty(os.Getenv("S3_ENDPOINT"), cfgVal(cfg, "endpoint"), defaultEndpoint),
		Bucket:    firstNonEmpty(os.Getenv("S3_BUCKET"), cfgVal(cfg, "bucket"), defaultBucket),
		UseSSL:    strings.EqualFold(firstNonEmpty(os.Getenv("S3_USE_SSL"), cfgVal(cfg, "usessl"), defaultUseSSL), "true"),
		Region:    firstNonEmpty(os.Getenv("S3_REGION"), cfgVal(cfg, "region"), defaultRegion),
		AccessKey: firstNonEmpty(os.Getenv("S3_ACCESS_KEY"), cfgVal(cfg, "accesskey"), defaultAccessKey),
		SecretKey: firstNonEmpty(os.Getenv("S3_SECRET_KEY"), cfgVal(cfg, "secretkey"), defaultSecretKey),
	}
}

func cfgVal(cfg *config.Config, field string) string {
	if cfg == nil {
		return ""
	}
	switch field {
	case "endpoint":
		return cfg.S3.Endpoint
	case "bucket":
		return cfg.S3.Bucket
	case "usessl":
		return cfg.S3.UseSSL
	case "region":
		return cfg.S3.Region
	case "accesskey":
		return cfg.S3.AccessKey
	case "secretkey":
		return cfg.S3.SecretKey
	}
	return ""
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// NewS3BucketService creates a new service using the resolved configuration.
// Pass nil for cfg if you only want to rely on environment variables / defaults.
func NewS3BucketService(cfg *config.Config) (*S3BucketService, error) {
	resolved := resolveConfig(cfg)

	scheme := "http"
	if resolved.UseSSL {
		scheme = "https"
	}
	endpoint := resolved.Endpoint
	if !strings.HasPrefix(endpoint, "http") {
		endpoint = fmt.Sprintf("%s://%s", scheme, endpoint)
	}

	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(endpoint),
		Region:           aws.String(resolved.Region),
		Credentials:      credentials.NewStaticCredentials(resolved.AccessKey, resolved.SecretKey, ""),
		S3ForcePathStyle: aws.Bool(true),
		DisableSSL:       aws.Bool(!resolved.UseSSL),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 session: %w", err)
	}

	return &S3BucketService{
		client:   s3.New(sess),
		bucket:   resolved.Bucket,
		endpoint: endpoint,
	}, nil
}

// DownloadFile downloads the object at the given key from the configured bucket
// and writes it to the specified local destination path.
func (svc *S3BucketService) DownloadFile(key string, destPath string) error {
	result, err := svc.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(svc.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to download %s from bucket %s: %w", key, svc.bucket, err)
	}
	defer result.Body.Close()

	outFile, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create local file %s: %w", destPath, err)
	}
	defer outFile.Close()

	written, err := io.Copy(outFile, result.Body)
	if err != nil {
		return fmt.Errorf("failed to write to %s: %w", destPath, err)
	}

	fmt.Printf("Downloaded %s (%d bytes) -> %s\n", key, written, destPath)
	return nil
}

// DownloadFileToBytes downloads the object at the given key and returns its contents as bytes.
func (svc *S3BucketService) DownloadFileToBytes(key string) ([]byte, error) {
	result, err := svc.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(svc.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download %s from bucket %s: %w", key, svc.bucket, err)
	}
	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", key, err)
	}

	return data, nil
}

// Bucket returns the configured bucket name.
func (svc *S3BucketService) Bucket() string {
	return svc.bucket
}
