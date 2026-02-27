# Cloud Storage Integration

The pipeline system supports multiple cloud storage providers for reading and writing data. Each storage component takes credentials and configuration as parameters, allowing different components in the same pipeline to use different storage accounts or providers.

## Available Storage Components

The following storage components are integrated into the pipeline system:

### Source Components (Readers)
- `s3_reader` - Read from AWS S3
- `minio_reader` - Read from MinIO (S3-compatible)
- `azure_blob_reader` - Read from Azure Blob Storage
- `local_storage_reader` - Read from local file system

### Sink Components (Writers)
- `s3_writer` - Write to AWS S3
- `minio_writer` - Write to MinIO (S3-compatible)
- `azure_blob_writer` - Write to Azure Blob Storage
- `local_storage_writer` - Write to local file system

## Supported Providers

| Provider | Type | Component Types | Use Case |
|----------|------|-----------------|----------|
| **AWS S3** | Cloud | `s3_reader`, `s3_writer` | Amazon S3 cloud storage |
| **MinIO** | S3-compatible | `minio_reader`, `minio_writer` | On-premises S3-compatible storage |
| **Azure Blob** | Cloud | `azure_blob_reader`, `azure_blob_writer` | Microsoft Azure Blob Storage |
| **Local** | File system | `local_storage_reader`, `local_storage_writer` | Local file system |

## Configuration

**Important:** Storage components no longer use global configuration from `config.yaml`. Instead, all credentials and settings are passed as parameters directly to each component. This provides:

- **Flexibility**: Different components can use different storage accounts
- **Security**: Credentials can be managed per-pipeline
- **Isolation**: No shared global state

## Pipeline Components

### S3 Reader Component

Reads data from AWS S3 buckets:

```json
{
  "id": "s3-reader-1",
  "type": "s3_reader",
  "parameters": {
    "access_key": "AKIA...",
    "secret_key": "your-secret-key",
    "region": "us-east-1",
    "bucket": "my-bucket",
    "key": "data/input.csv"
  }
}
```

**Required Parameters:**
- `access_key`: AWS access key ID
- `secret_key`: AWS secret access key
- `region`: AWS region (e.g., "us-east-1")
- `bucket`: S3 bucket name
- `key`: Object key/path in the bucket

### S3 Writer Component

Writes data to AWS S3 buckets:

```json
{
  "id": "s3-writer-1",
  "type": "s3_writer",
  "parameters": {
    "access_key": "AKIA...",
    "secret_key": "your-secret-key",
    "region": "us-east-1",
    "bucket": "my-bucket",
    "key": "data/output.csv"
  }
}
```

**Required Parameters:**
- `access_key`: AWS access key ID
- `secret_key`: AWS secret access key
- `region`: AWS region
- `bucket`: S3 bucket name
- `key`: Object key/path in the bucket

### MinIO Reader Component

Reads data from MinIO storage:

```json
{
  "id": "minio-reader-1",
  "type": "minio_reader",
  "parameters": {
    "endpoint": "http://localhost:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "bucket": "my-bucket",
    "key": "data/input.csv",
    "region": "us-east-1"
  }
}
```

**Required Parameters:**
- `endpoint`: MinIO server endpoint URL
- `access_key`: MinIO access key
- `secret_key`: MinIO secret key
- `bucket`: Bucket name
- `key`: Object key/path

**Optional Parameters:**
- `region`: Region (default: "us-east-1")

### MinIO Writer Component

Writes data to MinIO storage:

```json
{
  "id": "minio-writer-1",
  "type": "minio_writer",
  "parameters": {
    "endpoint": "http://localhost:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "bucket": "my-bucket",
    "key": "data/output.csv",
    "region": "us-east-1"
  }
}
```

**Required Parameters:**
- `endpoint`: MinIO server endpoint URL
- `access_key`: MinIO access key
- `secret_key`: MinIO secret key
- `bucket`: Bucket name
- `key`: Object key/path

**Optional Parameters:**
- `region`: Region (default: "us-east-1")

### Azure Blob Reader Component

Reads data from Azure Blob Storage:

```json
{
  "id": "azure-reader-1",
  "type": "azure_blob_reader",
  "parameters": {
    "account_name": "mystorageaccount",
    "account_key": "your-account-key",
    "container": "my-container",
    "blob": "data/input.csv"
  }
}
```

**Required Parameters:**
- `account_name`: Azure storage account name
- `account_key`: Azure storage account key
- `container`: Azure container name
- `blob`: Blob name/path

### Azure Blob Writer Component

Writes data to Azure Blob Storage:

```json
{
  "id": "azure-writer-1",
  "type": "azure_blob_writer",
  "parameters": {
    "account_name": "mystorageaccount",
    "account_key": "your-account-key",
    "container": "my-container",
    "blob": "data/output.csv"
  }
}
```

**Required Parameters:**
- `account_name`: Azure storage account name
- `account_key`: Azure storage account key
- `container`: Azure container name
- `blob`: Blob name/path

### Local Storage Reader Component

Reads data from local file system:

```json
{
  "id": "local-reader-1",
  "type": "local_storage_reader",
  "parameters": {
    "path": "data/input.csv",
    "base_path": "/var/data"
  }
}
```

**Required Parameters:**
- `path`: File path relative to base_path

**Optional Parameters:**
- `base_path`: Base directory path (default: current directory)

### Local Storage Writer Component

Writes data to local file system:

```json
{
  "id": "local-writer-1",
  "type": "local_storage_writer",
  "parameters": {
    "path": "data/output.csv",
    "base_path": "/var/data"
  }
}
```

**Required Parameters:**
- `path`: File path relative to base_path

**Optional Parameters:**
- `base_path`: Base directory path (default: current directory)

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
        "access_key": "AKIA...",
        "secret_key": "your-secret-key",
        "region": "us-east-1",
        "bucket": "source-bucket",
        "key": "data/input.csv"
      }
    },
    {
      "id": "s3-writer-1",
      "type": "s3_writer",
      "parameters": {
        "access_key": "AKIA...",
        "secret_key": "your-secret-key",
        "region": "us-west-2",
        "bucket": "destination-bucket",
        "key": "data/output.csv"
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

### MinIO to S3 Pipeline

```json
{
  "name": "MinIO to S3 Pipeline",
  "components": [
    {
      "id": "minio-reader-1",
      "type": "minio_reader",
      "parameters": {
        "endpoint": "http://localhost:9000",
        "access_key": "minioadmin",
        "secret_key": "minioadmin",
        "bucket": "source-bucket",
        "key": "data/input.csv"
      }
    },
    {
      "id": "s3-writer-1",
      "type": "s3_writer",
      "parameters": {
        "access_key": "AKIA...",
        "secret_key": "your-secret-key",
        "region": "us-east-1",
        "bucket": "destination-bucket",
        "key": "data/output.csv"
      }
    }
  ],
  "connections": [
    {
      "source_component_id": "minio-reader-1",
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
        "account_name": "mystorageaccount",
        "account_key": "your-account-key",
        "container": "source-container",
        "blob": "data/input.csv"
      }
    },
    {
      "id": "s3-writer-1",
      "type": "s3_writer",
      "parameters": {
        "access_key": "AKIA...",
        "secret_key": "your-secret-key",
        "region": "us-east-1",
        "bucket": "destination-bucket",
        "key": "data/output.csv"
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
        "account_name": "mystorageaccount",
        "account_key": "your-account-key",
        "container": "processed-data",
        "blob": "output/processed.json"
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

### Multi-Account S3 Pipeline

This example shows how different components can use different AWS accounts:

```json
{
  "name": "Multi-Account S3 Pipeline",
  "components": [
    {
      "id": "s3-reader-account-a",
      "type": "s3_reader",
      "parameters": {
        "access_key": "AKIA_ACCOUNT_A",
        "secret_key": "secret-for-account-a",
        "region": "us-east-1",
        "bucket": "account-a-bucket",
        "key": "data/input.csv"
      }
    },
    {
      "id": "processor-1",
      "type": "log",
      "parameters": {
        "message": "Processing data from Account A"
      }
    },
    {
      "id": "s3-writer-account-b",
      "type": "s3_writer",
      "parameters": {
        "access_key": "AKIA_ACCOUNT_B",
        "secret_key": "secret-for-account-b",
        "region": "eu-west-1",
        "bucket": "account-b-bucket",
        "key": "data/output.csv"
      }
    }
  ],
  "connections": [
    {
      "source_component_id": "s3-reader-account-a",
      "target_component_id": "processor-1"
    },
    {
      "source_component_id": "processor-1",
      "target_component_id": "s3-writer-account-b"
    }
  ]
}
```

## Benefits

1. **Flexibility** - Each component can use different storage accounts and credentials
2. **Scalability** - Store and process petabytes of data across multiple providers
3. **Durability** - 99.999999999% (11 9's) data durability with cloud providers
4. **Availability** - 99.99% availability SLA
5. **Security** - Credentials isolated per component, encryption at rest and in transit
6. **Cost-effective** - Pay only for what you use
7. **Multi-cloud** - Mix and match storage providers in the same pipeline

## Best Practices

1. **Credential Management**
   - Store credentials securely (use environment variables or secret managers)
   - Rotate credentials regularly
   - Use IAM roles when running on cloud infrastructure
   - Never commit credentials to version control

2. **Storage Organization**
   - Use consistent naming conventions for buckets/containers
   - Organize data with clear folder structures
   - Enable versioning for critical data

3. **Performance**
   - Choose regions close to your compute resources
   - Use appropriate storage classes for your access patterns
   - Consider data transfer costs between regions

4. **Security**
   - Enable encryption at rest
   - Use HTTPS/TLS for data in transit
   - Implement least-privilege access policies
   - Enable audit logging

5. **Cost Optimization**
   - Set up lifecycle policies to manage old data
   - Monitor storage metrics and usage
   - Use appropriate storage tiers
   - Clean up unused data regularly

## Troubleshooting

### Connection Failed

**Symptoms:** Component fails to connect to storage service

**Check:**
1. Credentials (access_key, secret_key, account_key) are correct
2. Network connectivity to storage endpoint
3. Endpoint URL is correct (especially for MinIO)
4. Firewall rules allow outbound connections

### Permission Denied

**Symptoms:** "Access Denied" or "403 Forbidden" errors

**Check:**
1. Credentials have required permissions (read/write)
2. Bucket/container exists and is accessible
3. Bucket policies allow the operation
4. Region is correct (for S3)
5. Account name matches the storage account (for Azure)

### File Not Found

**Symptoms:** "NoSuchKey" or "BlobNotFound" errors

**Check:**
1. Key/blob path is correct (case-sensitive)
2. File exists in the bucket/container
3. Bucket/container name is correct
4. No typos in the path

### Invalid Credentials

**Symptoms:** "InvalidAccessKeyId" or "AuthenticationFailed" errors

**Check:**
1. Access key and secret key are correct
2. Credentials haven't expired
3. Account name and key match (for Azure)
4. No extra spaces in credential strings

### Endpoint Issues (MinIO)

**Symptoms:** Connection timeout or "no such host" errors

**Check:**
1. MinIO server is running
2. Endpoint URL includes protocol (http:// or https://)
3. Port number is correct (default: 9000)
4. Network can reach the MinIO server

## Component Type Reference

| Component Type | Provider | Operation | Required Parameters |
|----------------|----------|-----------|---------------------|
| `s3_reader` | AWS S3 | Read | access_key, secret_key, region, bucket, key |
| `s3_writer` | AWS S3 | Write | access_key, secret_key, region, bucket, key |
| `minio_reader` | MinIO | Read | endpoint, access_key, secret_key, bucket, key |
| `minio_writer` | MinIO | Write | endpoint, access_key, secret_key, bucket, key |
| `azure_blob_reader` | Azure | Read | account_name, account_key, container, blob |
| `azure_blob_writer` | Azure | Write | account_name, account_key, container, blob |
| `local_storage_reader` | Local | Read | path |
| `local_storage_writer` | Local | Write | path |
