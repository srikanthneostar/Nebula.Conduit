# Cloud Storage Integration

The pipeline system now supports multiple cloud storage providers for reading and writing data.

## Available Storage Components

The following storage components are now integrated into the pipeline system:

### Source Components (Readers)
- `s3_reader` - Read from AWS S3 or MinIO
- `azure_blob_reader` - Read from Azure Blob Storage
- `local_storage_reader` - Read from local file system

### Sink Components (Writers)
- `s3_writer` - Write to AWS S3 or MinIO
- `azure_blob_writer` - Write to Azure Blob Storage
- `local_storage_writer` - Write to local file system

## Supported Providers

| Provider | Type | Use Case |
|----------|------|----------|
| **S3** | AWS | Amazon S3 cloud storage |
| **MinIO** | S3-compatible | On-premises S3-compatible storage |
| **Azure Blob** | Azure | Microsoft Azure Blob Storage |
| **Local** | File system | Local file system (default) |

## Configuration

Add to `commons/config.yaml`:

```yaml
storage:
  type: "s3"  # Options: "local", "s3", "minio", "azure"
  endpoint: "s3.amazonaws.com"  # For S3/MinIO
  bucket: "my-bucket-name"
  region: "us-east-1"
  access_key: "your-access-key"
  secret_key: "your-secret-key"
  account_name: ""  # For Azure
  account_key: ""   # For Azure
```

### S3 Configuration

```yaml
storage:
  type: "s3"
  endpoint: "s3.amazonaws.com"
  bucket: "my-bucket"
  region: "us-east-1"
  access_key: "AKIA..."
  secret_key: "your-secret"
```

### MinIO Configuration

```yaml
storage:
  type: "minio"
  endpoint: "http://localhost:9000"
  bucket: "my-bucket"
  region: "us-east-1"
  access_key: "minioadmin"
  secret_key: "minioadmin"
```

### Azure Blob Storage Configuration

```yaml
storage:
  type: "azure"
  bucket: "my-container"
  account_name: "mystorageaccount"
  account_key: "your-account-key"
```

### Local Storage (Default)

```yaml
storage:
  type: "local"
  bucket: "/path/to/local/directory"
```

## Pipeline Components

### S3 Reader Component

Reads data from S3/MinIO buckets:

```json
{
  "id": "s3-reader-1",
  "type": "s3_reader",
  "parameters": {
    "bucket": "my-bucket",
    "key": "data/input.csv",
    "region": "us-east-1",
    "storage_type": "s3",
    "endpoint": "s3.amazonaws.com",
    "access_key": "your-access-key",
    "secret_key": "your-secret-key"
  }
}
```

Parameters:
- `bucket` (required): S3 bucket name
- `key` (required): Object key/path
- `region` (optional): AWS region (default: us-east-1)
- `storage_type` (optional): "s3" or "minio" (default: s3)
- `endpoint` (optional): S3 endpoint URL
- `access_key` (optional): AWS access key
- `secret_key` (optional): AWS secret key

### S3 Writer Component

Writes data to S3/MinIO buckets:

```json
{
  "id": "s3-writer-1",
  "type": "s3_writer",
  "parameters": {
    "bucket": "my-bucket",
    "key": "data/output.csv",
    "region": "us-east-1",
    "storage_type": "s3",
    "endpoint": "s3.amazonaws.com",
    "access_key": "your-access-key",
    "secret_key": "your-secret-key"
  }
}
```

Parameters:
- `bucket` (required): S3 bucket name
- `key` (required): Object key/path
- `region` (optional): AWS region
- `storage_type` (optional): "s3" or "minio" (default: s3)
- `endpoint` (optional): S3 endpoint URL
- `access_key` (optional): AWS access key
- `secret_key` (optional): AWS secret key

### Azure Blob Reader Component

Reads data from Azure Blob Storage:

```json
{
  "id": "azure-reader-1",
  "type": "azure_blob_reader",
  "parameters": {
    "container": "my-container",
    "blob": "data/input.csv",
    "account_name": "mystorageaccount",
    "account_key": "your-account-key"
  }
}
```

Parameters:
- `container` (required): Azure container name
- `blob` (required): Blob name/path
- `account_name` (required): Azure storage account name
- `account_key` (required): Azure storage account key

### Azure Blob Writer Component

Writes data to Azure Blob Storage:

```json
{
  "id": "azure-writer-1",
  "type": "azure_blob_writer",
  "parameters": {
    "container": "my-container",
    "blob": "data/output.csv",
    "account_name": "mystorageaccount",
    "account_key": "your-account-key"
  }
}
```

Parameters:
- `container` (required): Azure container name
- `blob` (required): Blob name/path
- `account_name` (required): Azure storage account name
- `account_key` (required): Azure storage account key

### Local Storage Reader Component

Reads data from local file system:

```json
{
  "id": "local-reader-1",
  "type": "local_storage_reader",
  "parameters": {
    "path": "data/input.csv",
    "base_path": "/path/to/storage"
  }
}
```

Parameters:
- `path` (required): File path relative to base_path
- `base_path` (optional): Base directory path

### Local Storage Writer Component

Writes data to local file system:

```json
{
  "id": "local-writer-1",
  "type": "local_storage_writer",
  "parameters": {
    "path": "data/output.csv",
    "base_path": "/path/to/storage"
  }
}
```

Parameters:
- `path` (required): File path relative to base_path
- `base_path` (optional): Base directory path

## Example Pipelines

### S3 to S3 Pipeline

```json
{
  "name": "S3 to S3 Pipeline",
  "components": [
    {
      "id": "s3-reader-1",
      "type": "s3_reader",
      "parameters": {
        "bucket": "source-bucket",
        "key": "data/input.csv",
        "region": "us-east-1",
        "access_key": "your-access-key",
        "secret_key": "your-secret-key"
      }
    },
    {
      "id": "s3-writer-1",
      "type": "s3_writer",
      "parameters": {
        "bucket": "destination-bucket",
        "key": "data/output.csv",
        "region": "us-east-1",
        "access_key": "your-access-key",
        "secret_key": "your-secret-key"
      }
    }
  ],
  "connections": [
    {
      "source_component_id": "s3-reader-1",
      "target_component_id": "s3-writer-1"
    }
  ]
}
```

### Azure to S3 Pipeline

```json
{
  "name": "Azure to S3 Pipeline",
  "components": [
    {
      "id": "azure-reader-1",
      "type": "azure_blob_reader",
      "parameters": {
        "container": "source-container",
        "blob": "data/input.csv",
        "account_name": "mystorageaccount",
        "account_key": "your-account-key"
      }
    },
    {
      "id": "s3-writer-1",
      "type": "s3_writer",
      "parameters": {
        "bucket": "destination-bucket",
        "key": "data/output.csv",
        "region": "us-east-1",
        "access_key": "your-access-key",
        "secret_key": "your-secret-key"
      }
    }
  ],
  "connections": [
    {
      "source_component_id": "azure-reader-1",
      "target_component_id": "s3-writer-1"
    }
  ]
}
```

### Local to Azure Pipeline with Processing

```json
{
  "name": "Local to Azure with Processing",
  "components": [
    {
      "id": "local-reader-1",
      "type": "local_storage_reader",
      "parameters": {
        "path": "data/input.csv",
        "base_path": "/var/data"
      }
    },
    {
      "id": "processor-1",
      "type": "python_code_block",
      "parameters": {
        "code": "import json\ndata = json.loads(input_data)\ndata['processed'] = True\noutput_data = json.dumps(data)"
      }
    },
    {
      "id": "azure-writer-1",
      "type": "azure_blob_writer",
      "parameters": {
        "container": "processed-data",
        "blob": "output/processed.json",
        "account_name": "mystorageaccount",
        "account_key": "your-account-key"
      }
    }
  ],
  "connections": [
    {
      "source_component_id": "local-reader-1",
      "target_component_id": "processor-1"
    },
    {
      "source_component_id": "processor-1",
      "target_component_id": "azure-writer-1"
    }
  ]
}
```

## Benefits

1. **Scalability** - Store and process petabytes of data
2. **Durability** - 99.999999999% (11 9's) data durability
3. **Availability** - 99.99% availability SLA
4. **Security** - Encryption at rest and in transit
5. **Cost-effective** - Pay only for what you use

## Best Practices

1. **Use IAM roles** instead of access keys when possible
2. **Enable versioning** on buckets for data protection
3. **Set up lifecycle policies** to manage old data
4. **Use server-side encryption** for sensitive data
5. **Monitor storage metrics** for cost optimization

## Troubleshooting

### Connection Failed

Check:
1. Credentials are correct
2. Network connectivity to storage endpoint
3. Bucket/container exists and is accessible

### Permission Denied

Check:
1. IAM role/credentials have required permissions
2. Bucket policy allows access
3. Region is correct

### File Not Found

Check:
1. Key/path is correct
2. File exists in the bucket
3. Prefix is correct for listing operations
