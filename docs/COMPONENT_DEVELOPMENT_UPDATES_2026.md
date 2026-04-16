# Component Development Guide - 2026 Updates

## Latest Fixes and Enhancements

This document supplements the main [Component Development Guide](component-development-guide.md) with all the latest fixes, enhancements, and best practices added in 2026.

## Table of Contents

- [Pipeline Scheduling System](#pipeline-scheduling-system)
- [Complete Component Reference](#complete-component-reference)
  - [Source Components](#source-components)
    - [HTTP GET](#http-get-component)
    - [SQL Query](#sql-query-component)
    - [CSV Reader](#csv-reader-component)
    - [TCP Read](#tcp-read-component)
    - [Kafka Consumer](#kafka-consumer-component)
    - [RabbitMQ Consumer](#rabbitmq-consumer-component)
    - [HL7 Reader](#hl7-reader-component)
    - [S3 Reader](#s3-reader-component)
    - [MinIO Reader](#minio-reader-component)
    - [Azure Blob Reader](#azure-blob-reader-component)
    - [Local Storage Reader](#local-storage-reader-component)
  - [Processor Components](#processor-components)
    - [Log](#log-component)
    - [Attribute Update](#attribute-update-component)
    - [JSON Extractor](#json-extractor-component)
    - [JSON Transform](#json-transform-component)
    - [Python Code Block](#python-code-block-component)
    - [Lychgate Response](#lychgate-response-component)
  - [Sink Components](#sink-components)
    - [HTTP POST](#http-post-component)
    - [Log Sink](#log-sink-component)
    - [TCP Write](#tcp-write-component)
    - [Kafka Producer](#kafka-producer-component)
    - [RabbitMQ Producer](#rabbitmq-producer-component)
    - [S3 Writer](#s3-writer-component)
    - [MinIO Writer](#minio-writer-component)
    - [Azure Blob Writer](#azure-blob-writer-component)
    - [Local Storage Writer](#local-storage-writer-component)
- [HTTP GET URL Template Resolution Fix](#http-get-url-template-resolution-fix)
- [Database Management](#database-management)
- [Pipeline Execution Monitoring](#pipeline-execution-monitoring)
- [GitLab CI/CD Integration](#gitlab-cicd-integration)
- [React Project Integration](#react-project-integration)
- [Best Practices](#best-practices)

---

## Pipeline Scheduling System

### Overview

Pipelines now support automatic scheduling with cron expressions. When you create a pipeline with `execution_mode: "scheduled"` and `status: "active"`, it automatically schedules and executes per the cron expression.

### Key Features

1. **Real-time Scheduling** - Pipelines schedule immediately on creation (no server restart needed)
2. **Dynamic Updates** - Status and cron changes take effect immediately
3. **Singleton Pattern** - Pipeline engine is a server-level singleton
4. **Automatic Cleanup** - Pipelines unschedule automatically on deletion

### Implementation Details

**Files Modified:**
- `internal/api/server.go` - Pipeline engine as singleton field
- `pkg/pipeline/api_handlers.go` - Immediate scheduling on create/update
- `pkg/pipeline/scheduler.go` - Cron-based scheduler
- `pkg/pipeline/lifecycle.go` - Pipeline lifecycle management

### Usage Example

```json
{
  "name": "Hourly Data Sync",
  "execution_mode": "scheduled",
  "cron_expression": "0 * * * *",
  "status": "active",
  "components": [...]
}
```

**Cron Expression Examples:**
- `*/5 * * * *` - Every 5 minutes
- `0 * * * *` - Every hour
- `0 0 * * *` - Daily at midnight
- `0 9 * * 1-5` - Weekdays at 9 AM

### API Operations

**Create Pipeline (Schedules Immediately):**
```bash
POST /api/v1/pipelines
```

**Update Status (Schedules/Unschedules Immediately):**
```bash
PUT /api/v1/pipelines/{id}
{"status": "active"}  # Schedules
{"status": "inactive"}  # Unschedules
```

**Manual Trigger (Works Anytime):**
```bash
POST /api/v1/pipelines/{id}/trigger
```

---

## Complete Component Reference

All components share these common configuration fields:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `id` | string | required | Unique component instance identifier |
| `type` | string | required | Component type identifier |
| `parameters` | object | required | Component-specific configuration |
| `retry_count` | int | 0 | Number of retries on failure |
| `retry_delay` | duration | 0 | Delay between retries (nanoseconds) |
| `continue_on_error` | bool | false | Continue pipeline on component error |
| `timeout` | duration | 0 | Component execution timeout (nanoseconds) |

---

### Source Components

Source components generate or read data from external systems and emit it into the pipeline.

---

#### HTTP GET Component

**Type:** `http_get`
**Category:** Source / Processor (dual-mode)
**File:** `pkg/pipeline/components/http.go`

Performs HTTP GET requests. Runs as a source (no input) or as a processor (receives input data with template variables for dynamic URL/header resolution).

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `url` | string | yes | - | Target URL. Supports `{{var}}` templates when used as processor |
| `headers` | object | no | `{}` | HTTP headers as key-value pairs. Supports `{{var}}` templates |
| `interval` | string | no | `"0"` | Polling interval (e.g. `"30s"`, `"5m"`). `0` = one-time execution |

**Output Metadata:**
- `status_code` - HTTP response status code
- `content_type` - Response Content-Type header
- `content_length` - Response body size in bytes

**Pipeline Node Example (Source mode - one-time):**
```json
{
  "id": "fetch-employees",
  "type": "http_get",
  "parameters": {
    "url": "http://frappe-host:8001/api/resource/Employee?fields=[\"name\",\"employee_name\"]&limit_page_length=100",
    "headers": {
      "Authorization": "token api_key:api_secret",
      "Accept": "application/json"
    }
  },
  "timeout": 30000000000
}
```

**Pipeline Node Example (Source mode - polling):**
```json
{
  "id": "poll-orders",
  "type": "http_get",
  "parameters": {
    "url": "http://api.example.com/orders?status=pending",
    "headers": {
      "Authorization": "Bearer my-token"
    },
    "interval": "60s"
  }
}
```

**Pipeline Node Example (Processor mode - dynamic URL from upstream metadata):**
```json
{
  "id": "fetch-with-cursor",
  "type": "http_get",
  "parameters": {
    "url": "http://frappe-host:8001/api/resource/Employee?filters=[[\"modified\",\">\",\"{{last_modified}}\"]]&limit_page_length=100",
    "headers": {
      "Authorization": "token {{api_key}}:{{api_secret}}"
    }
  }
}
```

---

#### SQL Query Component

**Type:** `sql_query`
**Category:** Source
**File:** `pkg/pipeline/components/sql.go`

Executes SQL queries against SQL Server and emits results as JSON.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `connection_string` | string | yes | - | SQL Server connection string |
| `query` | string | yes | - | SQL query to execute |
| `interval` | string | no | `"0"` | Polling interval. `0` = one-time execution |

**Output Metadata:**
- `row_count` - Number of rows returned
- `column_count` - Number of columns
- `query` - The executed query

**Pipeline Node Example:**
```json
{
  "id": "query-users",
  "type": "sql_query",
  "parameters": {
    "connection_string": "sqlserver://user:password@host:1433?database=mydb",
    "query": "SELECT id, name, email, status FROM users WHERE active = 1",
    "interval": "5m"
  },
  "timeout": 60000000000
}
```

---

#### CSV Reader Component

**Type:** `csv_reader`
**Category:** Source
**File:** `pkg/pipeline/components/csv.go`

Reads CSV files and emits each row as a Data item. Supports automatic archiving and error handling.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `file_path` | string | yes | - | Path to the CSV file |
| `has_header` | bool | no | `false` | Whether the first row is a header |
| `delimiter` | string | no | `","` | Field delimiter character |
| `archive_on_read` | bool | no | `false` | Move file to archive folder after successful read |
| `move_on_error` | bool | no | `false` | Move file to error folder on failure |
| `archive_folder` | string | no | `"./archive"` | Custom archive folder path |
| `error_folder` | string | no | `"./error"` | Custom error folder path |

**Pipeline Node Example:**
```json
{
  "id": "csv-reader-1",
  "type": "csv_reader",
  "parameters": {
    "file_path": "/data/input/employees.csv",
    "has_header": true,
    "delimiter": ",",
    "archive_on_read": true,
    "move_on_error": true,
    "archive_folder": "/data/archive",
    "error_folder": "/data/error"
  },
  "retry_count": 2,
  "continue_on_error": false,
  "timeout": 30000000000
}
```

### CSV Reader Behavior

**Successful Processing:**
1. CSV file is read
2. Each row is emitted as Data
3. File is moved to archive folder (if `archive_on_read: true`)
4. Original file no longer exists at source location

**Failed Processing:**
1. CSV file read fails
2. File is moved to error folder (if `move_on_error: true`)
3. Error is logged
4. Pipeline continues or stops based on `continue_on_error`

**Benefits:**
- **Prevents Reprocessing** - Archived files won't be read again
- **Error Tracking** - Failed files isolated for investigation
- **Audit Trail** - Timestamped filenames show when processed
- **Clean Source Folder** - Processed files automatically removed

---

#### TCP Read Component

**Type:** `tcp_read`
**Category:** Source
**File:** `pkg/pipeline/components/tcp.go`

Reads data from TCP connections. Can operate as a TCP server (listens for connections) or client (connects to a remote host).

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `address` | string | yes | - | TCP address (e.g. `"0.0.0.0:9000"` for server, `"host:port"` for client) |
| `mode` | string | no | `"server"` | `"server"` or `"client"` |
| `connection_timeout` | string | no | `"30s"` | Connection timeout duration |
| `max_connections` | int | no | `10` | Maximum concurrent connections (server mode) |

**Pipeline Node Example (Server mode):**
```json
{
  "id": "tcp-listener",
  "type": "tcp_read",
  "parameters": {
    "address": "0.0.0.0:9000",
    "mode": "server",
    "max_connections": 50,
    "connection_timeout": "60s"
  }
}
```

**Pipeline Node Example (Client mode):**
```json
{
  "id": "tcp-client",
  "type": "tcp_read",
  "parameters": {
    "address": "192.168.1.100:8080",
    "mode": "client",
    "connection_timeout": "10s"
  }
}
```

---

#### Kafka Consumer Component

**Type:** `kafka_consumer`
**Category:** Source
**File:** `pkg/pipeline/components/kafka.go`

Consumes messages from Kafka topics using consumer groups.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `brokers` | array | yes | - | List of Kafka broker addresses |
| `topic` | string | yes | - | Kafka topic to consume from |
| `group_id` | string | no | component ID | Consumer group ID |
| `start_offset` | int | no | `-1` (latest) | Start offset (`-1` = latest, `-2` = earliest) |
| `max_wait` | string | no | `"10s"` | Maximum wait time for new messages |
| `min_bytes` | int | no | `1` | Minimum bytes to fetch |
| `max_bytes` | int | no | `10000000` | Maximum bytes to fetch (10MB) |

**Output Metadata:**
- `topic` - Kafka topic name
- `partition` - Partition number
- `offset` - Message offset
- `key` - Message key

**Pipeline Node Example:**
```json
{
  "id": "kafka-in",
  "type": "kafka_consumer",
  "parameters": {
    "brokers": ["kafka-1:9092", "kafka-2:9092"],
    "topic": "employee-events",
    "group_id": "nebula-pipeline-group",
    "start_offset": -2,
    "max_wait": "5s"
  }
}
```

---

#### RabbitMQ Consumer Component

**Type:** `rabbitmq_consumer`
**Category:** Source
**File:** `pkg/pipeline/components/rabbitmq.go`

Consumes messages from a RabbitMQ queue.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `connection_url` | string | yes | - | AMQP connection URL |
| `queue` | string | yes | - | Queue name to consume from |
| `auto_ack` | bool | no | `false` | Auto-acknowledge messages |
| `prefetch_count` | int | no | `10` | Prefetch count (QoS) |

**Output Metadata:**
- `routing_key` - Message routing key
- `exchange` - Source exchange
- `message_id` - Message ID
- `content_type` - Message content type

**Pipeline Node Example:**
```json
{
  "id": "rabbitmq-in",
  "type": "rabbitmq_consumer",
  "parameters": {
    "connection_url": "amqp://user:pass@rabbitmq-host:5672/",
    "queue": "employee-updates",
    "auto_ack": false,
    "prefetch_count": 20
  }
}
```

---

#### HL7 Reader Component

**Type:** `hl7_reader`
**Category:** Source
**File:** `pkg/pipeline/components/hl7.go`

Reads and parses HL7 v2.x flat files. Each HL7 message (delimited by MSH segments) is emitted as a separate Data item with parsed segments.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `file_path` | string | yes | - | Path to the HL7 file |

**Output Metadata:**
- `source` - Source file path
- `message_index` - Message sequence number
- `message_type` - HL7 message type (from MSH-9)

**Output Payload Structure:**
```json
{
  "raw_message": "MSH|^~\\&|...\rPID|...",
  "segments": {
    "MSH": [{"MSH.0": "MSH", "MSH.1": "|", ...}],
    "PID": [{"PID.0": "PID", "PID.1": "1", ...}]
  }
}
```

**Pipeline Node Example:**
```json
{
  "id": "hl7-reader",
  "type": "hl7_reader",
  "parameters": {
    "file_path": "/data/hl7/patient_admissions.hl7"
  }
}
```

---

#### S3 Reader Component

**Type:** `s3_reader`
**Category:** Source
**File:** `pkg/pipeline/components/s3.go`

Reads a single object from AWS S3.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `access_key` | string | yes | - | AWS access key ID |
| `secret_key` | string | yes | - | AWS secret access key |
| `region` | string | yes | - | AWS region |
| `bucket` | string | yes | - | S3 bucket name |
| `key` | string | yes | - | S3 object key |

**Output Metadata:**
- `bucket` - Bucket name
- `key` - Object key
- `size` - Object size in bytes

**Pipeline Node Example:**
```json
{
  "id": "s3-read",
  "type": "s3_reader",
  "parameters": {
    "access_key": "AKIAIOSFODNN7EXAMPLE",
    "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "region": "us-east-1",
    "bucket": "my-data-bucket",
    "key": "imports/employees.json"
  }
}
```

---

#### MinIO Reader Component

**Type:** `minio_reader`
**Category:** Source
**File:** `pkg/pipeline/components/minio.go`

Reads a single object from MinIO storage (S3-compatible).

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `endpoint` | string | yes | - | MinIO endpoint URL |
| `access_key` | string | yes | - | Access key |
| `secret_key` | string | yes | - | Secret key |
| `bucket` | string | yes | - | Bucket name |
| `key` | string | yes | - | Object key |
| `region` | string | no | `"us-east-1"` | Region |

**Output Metadata:**
- `bucket`, `key`, `size`

**Pipeline Node Example:**
```json
{
  "id": "minio-read",
  "type": "minio_reader",
  "parameters": {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "bucket": "data",
    "key": "input/records.json"
  }
}
```

---

#### Azure Blob Reader Component

**Type:** `azure_blob_reader`
**Category:** Source
**File:** `pkg/pipeline/components/azure_blob.go`

Reads a single blob from Azure Blob Storage.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `account_name` | string | yes | - | Azure storage account name |
| `account_key` | string | yes | - | Azure storage account key |
| `container` | string | yes | - | Blob container name |
| `blob` | string | yes | - | Blob name |

**Output Metadata:**
- `container`, `blob`, `size`

**Pipeline Node Example:**
```json
{
  "id": "azure-read",
  "type": "azure_blob_reader",
  "parameters": {
    "account_name": "mystorageaccount",
    "account_key": "base64encodedkey==",
    "container": "imports",
    "blob": "employees.json"
  }
}
```

---

#### Local Storage Reader Component

**Type:** `local_storage_reader`
**Category:** Source
**File:** `pkg/pipeline/components/local_storage.go`

Reads a file from the local file system.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `path` | string | yes | - | Relative file path |
| `base_path` | string | no | `"."` | Base directory path |

**Output Metadata:**
- `path`, `size`

**Pipeline Node Example:**
```json
{
  "id": "local-read",
  "type": "local_storage_reader",
  "parameters": {
    "path": "data/input.json",
    "base_path": "/opt/pipeline"
  }
}
```

---

### Processor Components

Processor components transform data flowing through the pipeline. They receive input, apply transformations, and emit output.

---

#### Log Component

**Type:** `log`
**Category:** Processor (pass-through)
**File:** `pkg/pipeline/components/log.go`

Logs data for inspection and passes it through unchanged to downstream components. Useful for debugging pipelines.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `log_level` | string | yes | - | Log level: `debug`, `info`, `warn`, `error` |
| `format` | string | no | `"json"` | Output format: `"json"` or `"text"` |
| `sample_size` | int | no | `0` | Max payload size for logging (0 = no limit) |
| `truncate` | bool | no | `false` | Truncate payload to `sample_size` in logs |

**Pipeline Node Example:**
```json
{
  "id": "debug-log",
  "type": "log",
  "parameters": {
    "log_level": "debug",
    "format": "json",
    "sample_size": 1000,
    "truncate": true
  }
}
```

---

#### Attribute Update Component

**Type:** `attribute_update`
**Category:** Processor
**File:** `pkg/pipeline/components/attribute_update.go`

Creates or updates metadata fields on Data items using template expressions. Essential for setting dynamic configuration values that downstream components reference via `{{variable}}` placeholders.

Supports dot-notation for accessing nested payload fields (e.g. `{{address.city}}`), and built-in variables `{{_timestamp}}` and `{{_trace_id}}`.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `mappings` | array | yes | - | Array of `{name, expression}` objects |

**Mapping Object:**

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Metadata key to set |
| `expression` | string | Value (static or `{{var}}` template) |

**Pipeline Node Example:**
```json
{
  "id": "set-config",
  "type": "attribute_update",
  "parameters": {
    "mappings": [
      {"name": "log_output_path", "expression": "/logs/{{department}}_report.log"},
      {"name": "log_output_level", "expression": "info"},
      {"name": "api_key", "expression": "token_abc123"},
      {"name": "full_label", "expression": "{{employee_name}} ({{designation}})"}
    ]
  }
}
```

### Attribute Update Common Patterns

#### 1. Dynamic Log Configuration

```json
{
  "components": [
    {"id": "source", "type": "csv_reader", "parameters": {"file_path": "/data/input.csv", "has_header": true}},
    {
      "id": "set-log-config",
      "type": "attribute_update",
      "parameters": {
        "mappings": [
          {"name": "log_path", "expression": "/logs/{{date}}.log"},
          {"name": "log_level", "expression": "debug"}
        ]
      }
    },
    {
      "id": "sink",
      "type": "log_sink",
      "parameters": {
        "file_path": "{{log_path}}",
        "log_level": "{{log_level}}"
      }
    }
  ]
}
```

#### 2. Adding Processing Context

```json
{
  "id": "add-context",
  "type": "attribute_update",
  "parameters": {
    "mappings": [
      {"name": "processed_by", "expression": "pipeline-v2"},
      {"name": "environment", "expression": "production"},
      {"name": "timestamp", "expression": "2026-02-26T14:30:00Z"}
    ]
  }
}
```

#### 3. Routing Information

```json
{
  "id": "set-routing",
  "type": "attribute_update",
  "parameters": {
    "mappings": [
      {"name": "destination_queue", "expression": "high-priority"},
      {"name": "retry_policy", "expression": "exponential"}
    ]
  }
}
```

#### 4. Log Sink with Dynamic Template Variables

```json
{
  "components": [
    {
      "id": "csv-reader-1",
      "type": "csv_reader",
      "parameters": {
        "file_path": "/data/input.csv",
        "has_header": true,
        "archive_on_read": true
      }
    },
    {
      "id": "attr-update-1",
      "type": "attribute_update",
      "parameters": {
        "mappings": [
          {"name": "log_output_path", "expression": "/logs/pipeline.log"},
          {"name": "log_output_level", "expression": "info"}
        ]
      }
    },
    {
      "id": "log-sink-1",
      "type": "log_sink",
      "parameters": {
        "file_path": "{{log_output_path}}",
        "log_level": "{{log_output_level}}",
        "format": "json",
        "include_data": true,
        "decode_payload": true
      }
    }
  ],
  "connections": [
    {"source_component_id": "csv-reader-1", "target_component_id": "attr-update-1"},
    {"source_component_id": "attr-update-1", "target_component_id": "log-sink-1"}
  ]
}
```

**Best Practices:**
1. **Place Before Template-Using Components** - attribute_update must come before components that use `{{variables}}`
2. **Use Descriptive Names** - Clear metadata keys make debugging easier
3. **Document Mappings** - Comment what each mapping is for
4. **Validate Downstream** - Ensure downstream components expect the metadata keys you set

---

#### JSON Extractor Component

**Type:** `json_extractor`
**Category:** Processor
**File:** `pkg/pipeline/components/json_extractor.go`
**Added:** April 2026

Extracts aggregated values from JSON array payloads and writes results into metadata. Designed for use cases like extracting the max `modified` timestamp from an API response to use as a cursor in the next polling cycle.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `extractions` | array | yes | - | Array of extraction rule objects |

**Extraction Rule Object:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `array_path` | string | no | Dot-notation path to the JSON array (e.g. `"data"`, `"response.items"`). Empty = root is the array |
| `field` | string | yes* | Field name to extract from each element. *Not required for `count` |
| `operation` | string | yes | Aggregation: `max`, `min`, `first`, `last`, `count` |
| `output_key` | string | yes | Metadata key where the result is stored |

**Supported Operations:**

| Operation | Description | Example Result |
|-----------|-------------|----------------|
| `max` | Maximum value (lexicographic, works for datetime strings) | `"2026-04-15 14:22:00"` |
| `min` | Minimum value | `"2026-04-10 08:30:00"` |
| `first` | Value from the first array element | `"HR-EMP-00001"` |
| `last` | Value from the last array element | `"HR-EMP-00003"` |
| `count` | Number of elements in the array | `"3"` |

**Pipeline Node Example (Frappe polling cursor):**
```json
{
  "id": "extract-cursor",
  "type": "json_extractor",
  "parameters": {
    "extractions": [
      {
        "array_path": "data",
        "field": "modified",
        "operation": "max",
        "output_key": "last_modified"
      },
      {
        "array_path": "data",
        "field": "name",
        "operation": "count",
        "output_key": "record_count"
      }
    ]
  }
}
```

**Pipeline Node Example (Root-level array):**
```json
{
  "id": "extract-stats",
  "type": "json_extractor",
  "parameters": {
    "extractions": [
      {
        "array_path": "",
        "field": "score",
        "operation": "max",
        "output_key": "highest_score"
      },
      {
        "array_path": "",
        "field": "id",
        "operation": "first",
        "output_key": "first_id"
      }
    ]
  }
}
```

**Example Pipeline Flow (Frappe HR Polling with Cursor):**
```json
{
  "name": "Frappe Employee Sync",
  "execution_mode": "scheduled",
  "cron_expression": "*/5 * * * *",
  "status": "active",
  "components": [
    {
      "id": "fetch-employees",
      "type": "http_get",
      "parameters": {
        "url": "http://frappe-host:8001/api/resource/Employee?filters=[[\"modified\",\">\",\"{{last_modified}}\"]]&order_by=modified asc&limit_page_length=100",
        "headers": {
          "Authorization": "token api_key:api_secret",
          "Accept": "application/json"
        }
      }
    },
    {
      "id": "extract-cursor",
      "type": "json_extractor",
      "parameters": {
        "extractions": [
          {"array_path": "data", "field": "modified", "operation": "max", "output_key": "last_modified"}
        ]
      }
    },
    {
      "id": "log-output",
      "type": "log_sink",
      "parameters": {
        "file_path": "/logs/frappe_sync.log",
        "log_level": "info",
        "format": "json"
      }
    }
  ],
  "connections": [
    {"source_component_id": "fetch-employees", "target_component_id": "extract-cursor"},
    {"source_component_id": "extract-cursor", "target_component_id": "log-output"}
  ]
}
```

---

#### JSON Transform Component

**Type:** `json_transform`
**Category:** Processor
**File:** `pkg/pipeline/components/json_transform.go`
**Added:** April 2026

Applies field-level transformation rules to JSON payloads. Auto-detects whether the payload (or a nested path within it) is a single object or an array and applies every rule to each object.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `array_path` | string | no | `""` | Dot-notation path to the array inside the payload. Empty = root |
| `rules` | array | yes | - | Array of transformation rule objects |

**Transformation Rule Object:**

| Field | Type | Used By | Description |
|-------|------|---------|-------------|
| `operation` | string | all | Operation type (see table below) |
| `field` | string | all | Target field name |
| `to` | string | rename, copy | Destination field name |
| `value` | string | add | Value to set (supports `{{var}}` templates from the object and metadata) |
| `to_type` | string | convert | Target type: `string`, `number`, `bool` |
| `mapping` | object | map_values | Key-value lookup table |
| `default` | string | map_values | Fallback value when no mapping matches |
| `separator` | string | flatten | Separator for flattened keys (default: `_`) |

**Supported Operations:**

| Operation | Description | Example |
|-----------|-------------|---------|
| `rename` | Rename a field | `{"operation":"rename", "field":"employee_name", "to":"full_name"}` |
| `remove` | Delete a field | `{"operation":"remove", "field":"modified_by"}` |
| `add` | Add/overwrite a field with static or template value | `{"operation":"add", "field":"source", "value":"frappe_hr"}` |
| `copy` | Copy a field to another key | `{"operation":"copy", "field":"email", "to":"primary_email"}` |
| `map_values` | Map discrete values via a lookup table | `{"operation":"map_values", "field":"status", "mapping":{"Active":"1","Left":"0"}, "default":"unknown"}` |
| `convert` | Type conversion | `{"operation":"convert", "field":"age", "to_type":"string"}` |
| `flatten` | Pull nested object fields up with separator | `{"operation":"flatten", "field":"address", "separator":"_"}` |

**Pipeline Node Example (Frappe employee transformation):**
```json
{
  "id": "transform-employees",
  "type": "json_transform",
  "parameters": {
    "array_path": "data",
    "rules": [
      {"operation": "rename", "field": "employee_name", "to": "full_name"},
      {"operation": "rename", "field": "company_email", "to": "email"},
      {"operation": "remove", "field": "modified_by"},
      {"operation": "map_values", "field": "status", "mapping": {"Active": "active", "Left": "inactive"}, "default": "unknown"},
      {"operation": "add", "field": "source_system", "value": "frappe_hr"},
      {"operation": "add", "field": "label", "value": "{{full_name}} ({{designation}})"}
    ]
  }
}
```

**Pipeline Node Example (Flatten nested address):**
```json
{
  "id": "flatten-address",
  "type": "json_transform",
  "parameters": {
    "rules": [
      {"operation": "flatten", "field": "address", "separator": "_"},
      {"operation": "convert", "field": "age", "to_type": "string"},
      {"operation": "remove", "field": "internal_id"}
    ]
  }
}
```

Input: `{"name":"Alice","address":{"city":"NY","zip":"10001"},"age":30,"internal_id":"x"}`
Output: `{"name":"Alice","address_city":"NY","address_zip":"10001","age":"30"}`

**Pipeline Node Example (Root-level array):**
```json
{
  "id": "transform-array",
  "type": "json_transform",
  "parameters": {
    "rules": [
      {"operation": "rename", "field": "old_key", "to": "new_key"},
      {"operation": "add", "field": "processed", "value": "true"}
    ]
  }
}
```

Input: `[{"old_key":"v1"},{"old_key":"v2"}]`
Output: `[{"new_key":"v1","processed":"true"},{"new_key":"v2","processed":"true"}]`

---

#### Python Code Block Component

**Type:** `python_code_block`
**Category:** Processor
**File:** `pkg/pipeline/components/python.go`

Executes inline Python code for data transformation. The input data is available as the `input_data` variable. Set `output_data` to control what is passed downstream.

Requires Python to be installed on the host system.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `code` | string | yes | - | Python code to execute |
| `environment` | object | no | `{}` | Environment variables for the Python process |

**Output Metadata:**
- All input metadata is preserved
- `python_executed` = `"true"`
- `execution_time` = ISO 8601 timestamp

**Pipeline Node Example:**
```json
{
  "id": "python-transform",
  "type": "python_code_block",
  "parameters": {
    "code": "import json\ndata = json.loads(input_data)\nfor item in data:\n    item['full_name'] = item.get('first_name','') + ' ' + item.get('last_name','')\noutput_data = json.dumps(data)",
    "environment": {
      "PYTHONPATH": "/opt/libs"
    }
  },
  "timeout": 60000000000
}
```

---

#### Lychgate Response Component

**Type:** `lychgate_response`
**Category:** Processor
**File:** `pkg/pipeline/components/lychgate/lychgate_response.go`

Processes responses from the Lychgate integration system. Specialized component for Lychgate API workflows.

**Pipeline Node Example:**
```json
{
  "id": "lychgate-resp",
  "type": "lychgate_response",
  "parameters": {}
}
```

---

### Sink Components

Sink components consume data and write it to external destinations. They are the terminal nodes of a pipeline.

---

#### HTTP POST Component

**Type:** `http_post`
**Category:** Sink / Processor
**File:** `pkg/pipeline/components/http.go`

Sends HTTP POST requests. Can forward pipeline data as the request body or use a static body template. Supports JWT extraction from responses.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `url` | string | yes | - | Target URL. Supports `{{var}}` templates |
| `content_type` | string | no | `"application/json"` | Request Content-Type |
| `body` | string | no | `""` | Static body template (supports `{{var}}`). If empty, forwards pipeline data |
| `headers` | object | no | `{}` | HTTP headers (supports `{{var}}` templates) |
| `extract_jwt` | bool | no | `false` | Extract JWT token from response |
| `jwt_field` | string | no | `"token"` | JSON field name containing the JWT |
| `jwt_metadata_key` | string | no | `"jwt_token"` | Metadata key to store extracted JWT |

**Pipeline Node Example (Forward data):**
```json
{
  "id": "post-to-api",
  "type": "http_post",
  "parameters": {
    "url": "http://target-system:8080/api/employees",
    "headers": {
      "Authorization": "Bearer {{jwt_token}}",
      "Content-Type": "application/json"
    }
  },
  "timeout": 30000000000
}
```

**Pipeline Node Example (JWT login):**
```json
{
  "id": "login",
  "type": "http_post",
  "parameters": {
    "url": "http://api.example.com/auth/login",
    "body": "{\"username\":\"admin\",\"password\":\"secret\"}",
    "extract_jwt": true,
    "jwt_field": "access_token",
    "jwt_metadata_key": "auth_token"
  }
}
```

---

#### Log Sink Component

**Type:** `log_sink`
**Category:** Sink
**File:** `pkg/pipeline/components/log_sink.go`

Writes pipeline data to a text log file. Supports dynamic file paths via `{{var}}` templates resolved from metadata.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `file_path` | string | yes | - | Log file path. Supports `{{var}}` templates |
| `log_level` | string | yes | - | Log level (supports `{{var}}` template) |
| `format` | string | no | `"text"` | Output format: `"json"` or `"text"` |
| `include_data` | bool | no | `true` | Include full data payload in logs |
| `decode_payload` | bool | no | `true` | Decode JSON byte arrays to readable objects |

**Pipeline Node Example (Static path):**
```json
{
  "id": "log-sink",
  "type": "log_sink",
  "parameters": {
    "file_path": "/logs/pipeline_output.log",
    "log_level": "info",
    "format": "json",
    "include_data": true,
    "decode_payload": true
  }
}
```

**Pipeline Node Example (Dynamic path from metadata):**
```json
{
  "id": "dynamic-log",
  "type": "log_sink",
  "parameters": {
    "file_path": "{{log_output_path}}",
    "log_level": "{{log_output_level}}",
    "format": "json",
    "include_data": false
  }
}
```

---

#### TCP Write Component

**Type:** `tcp_write`
**Category:** Sink
**File:** `pkg/pipeline/components/tcp.go`

Writes data to TCP connections. Can operate as a TCP server (accepts connections) or client (connects to remote host).

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `address` | string | yes | - | TCP address |
| `mode` | string | no | `"client"` | `"server"` or `"client"` |
| `connection_timeout` | string | no | `"30s"` | Connection timeout |
| `max_connections` | int | no | `10` | Max concurrent connections (server mode) |

**Pipeline Node Example:**
```json
{
  "id": "tcp-out",
  "type": "tcp_write",
  "parameters": {
    "address": "downstream-service:9001",
    "mode": "client",
    "connection_timeout": "10s"
  }
}
```

---

#### Kafka Producer Component

**Type:** `kafka_producer`
**Category:** Sink
**File:** `pkg/pipeline/components/kafka.go`

Produces messages to a Kafka topic.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `brokers` | array | yes | - | List of Kafka broker addresses |
| `topic` | string | yes | - | Target Kafka topic |
| `compression` | string | no | `"none"` | Compression: `none`, `gzip`, `snappy`, `lz4`, `zstd` |
| `async` | bool | no | `false` | Async writes (fire-and-forget) |

**Pipeline Node Example:**
```json
{
  "id": "kafka-out",
  "type": "kafka_producer",
  "parameters": {
    "brokers": ["kafka-1:9092", "kafka-2:9092"],
    "topic": "processed-employees",
    "compression": "snappy",
    "async": false
  }
}
```

---

#### RabbitMQ Producer Component

**Type:** `rabbitmq_producer`
**Category:** Sink
**File:** `pkg/pipeline/components/rabbitmq.go`

Publishes messages to a RabbitMQ exchange.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `connection_url` | string | yes | - | AMQP connection URL |
| `exchange` | string | yes | - | Exchange name |
| `routing_key` | string | no | `""` | Default routing key (overridden by `routing_key` in metadata) |
| `mandatory` | bool | no | `false` | Mandatory flag |
| `persistent` | bool | no | `true` | Persistent delivery mode |
| `content_type` | string | no | `"application/octet-stream"` | Message content type |

**Pipeline Node Example:**
```json
{
  "id": "rabbitmq-out",
  "type": "rabbitmq_producer",
  "parameters": {
    "connection_url": "amqp://user:pass@rabbitmq-host:5672/",
    "exchange": "employee-exchange",
    "routing_key": "employee.updated",
    "persistent": true,
    "content_type": "application/json"
  }
}
```

---

#### S3 Writer Component

**Type:** `s3_writer`
**Category:** Sink
**File:** `pkg/pipeline/components/s3.go`

Writes data to AWS S3.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `access_key` | string | yes | - | AWS access key ID |
| `secret_key` | string | yes | - | AWS secret access key |
| `region` | string | yes | - | AWS region |
| `bucket` | string | yes | - | S3 bucket name |
| `key` | string | yes | - | S3 object key |

**Output Metadata:**
- `bucket`, `key`, `size`, `status` = `"uploaded"`

**Pipeline Node Example:**
```json
{
  "id": "s3-write",
  "type": "s3_writer",
  "parameters": {
    "access_key": "AKIAIOSFODNN7EXAMPLE",
    "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "region": "us-east-1",
    "bucket": "my-output-bucket",
    "key": "exports/employees.json"
  }
}
```

---

#### MinIO Writer Component

**Type:** `minio_writer`
**Category:** Sink
**File:** `pkg/pipeline/components/minio.go`

Writes data to MinIO storage.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `endpoint` | string | yes | - | MinIO endpoint URL |
| `access_key` | string | yes | - | Access key |
| `secret_key` | string | yes | - | Secret key |
| `bucket` | string | yes | - | Bucket name |
| `key` | string | yes | - | Object key |
| `region` | string | no | `"us-east-1"` | Region |

**Output Metadata:**
- `bucket`, `key`, `size`, `status` = `"uploaded"`

**Pipeline Node Example:**
```json
{
  "id": "minio-write",
  "type": "minio_writer",
  "parameters": {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "bucket": "output",
    "key": "processed/employees.json"
  }
}
```

---

#### Azure Blob Writer Component

**Type:** `azure_blob_writer`
**Category:** Sink
**File:** `pkg/pipeline/components/azure_blob.go`

Writes data to Azure Blob Storage.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `account_name` | string | yes | - | Azure storage account name |
| `account_key` | string | yes | - | Azure storage account key |
| `container` | string | yes | - | Blob container name |
| `blob` | string | yes | - | Blob name |

**Output Metadata:**
- `container`, `blob`, `size`, `status` = `"uploaded"`

**Pipeline Node Example:**
```json
{
  "id": "azure-write",
  "type": "azure_blob_writer",
  "parameters": {
    "account_name": "mystorageaccount",
    "account_key": "base64encodedkey==",
    "container": "exports",
    "blob": "employees.json"
  }
}
```

---

#### Local Storage Writer Component

**Type:** `local_storage_writer`
**Category:** Sink
**File:** `pkg/pipeline/components/local_storage.go`

Writes data to the local file system.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `path` | string | yes | - | Relative file path |
| `base_path` | string | no | `"."` | Base directory path |

**Output Metadata:**
- `path`, `size`, `status` = `"written"`

**Pipeline Node Example:**
```json
{
  "id": "local-write",
  "type": "local_storage_writer",
  "parameters": {
    "path": "output/employees.json",
    "base_path": "/opt/pipeline"
  }
}
```

---

## HTTP GET URL Template Resolution Fix

**Fixed:** April 2026
**File:** `pkg/pipeline/components/http.go`

The HTTP GET component's `fetchAndSendWithData` method now resolves `{{var}}` template placeholders in the URL itself, not just in headers. Previously, template variables were only resolved in header values, which meant dynamic URLs (e.g. with cursor-based pagination filters) did not work in processor mode.

**Before (broken):**
```go
req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.url, nil) // raw URL, no resolution
```

**After (fixed):**
```go
resolvedURL := replaceTemplateVars(h.url, inputData.Metadata) // resolve {{var}} in URL
req, err := http.NewRequestWithContext(ctx, http.MethodGet, resolvedURL, nil)
```

This enables patterns like:
```
http://api.example.com/resource?filters=[["modified",">","{{last_modified}}"]]
```

---

## Database Management

### Foreign Key Constraints

**Fixed:** Pipeline deletion now properly cleans up related components.

**Implementation:**
- Enabled SQLite foreign key constraints with `PRAGMA foreign_keys = ON`
- Added explicit CASCADE DELETE in repository Delete method
- Update method now cleans up old components before inserting new ones

**Files Modified:**
- `pkg/database/database.go` - Enable foreign keys
- `pkg/pipeline/repository.go` - CASCADE DELETE implementation

### Cleanup Scripts

**Location:** `scripts/` directory

1. **`cleanup_database.ps1`** - Full database cleanup
2. **`quick_cleanup.ps1`** - Quick cleanup for development

**Usage:**
```powershell
# Full cleanup
.\scripts\cleanup_database.ps1

# Quick cleanup
.\scripts\quick_cleanup.ps1
```

### Database Schema

**Tables:**
- `pipelines` - Pipeline definitions
- `pipeline_components` - Component configurations (CASCADE DELETE on pipeline deletion)
- `pipeline_connections` - Component connections (CASCADE DELETE on pipeline deletion)
- `pipeline_executions` - Execution history

**Key Relationships:**
```
pipelines (1) ---> (N) pipeline_components [CASCADE DELETE]
pipelines (1) ---> (N) pipeline_connections [CASCADE DELETE]
pipelines (1) ---> (N) pipeline_executions
```

---

## Pipeline Execution Monitoring

### Execution History API

**Get Execution History:**
```bash
GET /api/v1/pipelines/{pipeline_id}/executions?page=1&page_size=20
```

**Response:**
```json
{
  "executions": [
    {
      "id": "exec-123",
      "pipeline_id": "pipe-456",
      "status": "completed",
      "started_at": "2026-02-26T14:30:00Z",
      "ended_at": "2026-02-26T14:30:15Z",
      "duration_ms": 15000,
      "components_executed": 3,
      "error": null
    }
  ],
  "total": 50,
  "page": 1,
  "page_size": 20
}
```

**Get Execution Details:**
```bash
GET /api/v1/pipelines/executions/{execution_id}
```

### Debug Logging

**Comprehensive logging added to:**
- Pipeline start/end
- Component creation
- Channel setup
- Component execution
- Data flow
- Completion status

**Log Location:**
- Configured via `NEBULA_CONDUIT_HOME` environment variable
- Default: `$NEBULA_CONDUIT_HOME/logs/app.log`

**Log Format:**
```json
{
  "level": "debug",
  "time": "2026-02-26T14:30:00+05:30",
  "message": "Starting pipeline execution",
  "pipeline_id": "pipe-456",
  "pipeline_name": "CSV Processing"
}
```

---

## GitLab CI/CD Integration

### Pipeline Configuration

**File:** `.gitlab-ci.yml`

### Stages

1. **docker-login** - Authenticate with Docker Hub
2. **image-build-push** - Build and push Docker image
3. **kubernetes-deploy** - Deploy to Kubernetes cluster

### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `DOCKER_USR` | Docker Hub username | `myusername` |
| `DOCKER_PASS` | Docker Hub password (masked) | `mypassword` |
| `DEPLOY_TO` | Deployment environment | `production` |

### Docker Images

Two tags are created:
1. **Pipeline-specific:** `${DOCKER_USR}/nebula-conduit:${CI_PIPELINE_ID}`
2. **Latest:** `${DOCKER_USR}/nebula-conduit:latest`

### Kubernetes Deployment

- **Namespace:** `core-insights-nebula`
- **Deployment:** `nebula-conduit`
- **Service:** `nebula-conduit`
- **Rollout Timeout:** 900 seconds (15 minutes)

---

## React Project Integration

### API Client Setup

Create an API client for your React project:

```typescript
// src/api/pipelineClient.ts
import axios from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8080';

export interface Pipeline {
  id: string;
  name: string;
  description: string;
  execution_mode: 'scheduled' | 'manual';
  cron_expression?: string;
  status: 'active' | 'inactive';
  components: Component[];
  connections: Connection[];
  created_at: string;
  updated_at: string;
}

export interface Component {
  id: string;
  type: string;
  parameters: Record<string, any>;
  retry_count?: number;
  retry_delay?: number;
  continue_on_error?: boolean;
  timeout?: number;
}

export interface Connection {
  source_component_id: string;
  target_component_id: string;
}

export interface ExecutionRecord {
  id: string;
  pipeline_id: string;
  status: 'running' | 'completed' | 'failed';
  started_at: string;
  ended_at?: string;
  duration_ms?: number;
  error?: string;
}

class PipelineClient {
  private token: string | null = null;

  setToken(token: string) {
    this.token = token;
  }

  private getHeaders() {
    return {
      'Content-Type': 'application/json',
      ...(this.token && { Authorization: `Bearer ${this.token}` }),
    };
  }

  async listPipelines(status?: string): Promise<Pipeline[]> {
    const params = status ? { status } : {};
    const response = await axios.get(`${API_BASE_URL}/api/v1/pipelines`, {
      headers: this.getHeaders(),
      params,
    });
    return response.data.pipelines;
  }

  async getPipeline(id: string): Promise<Pipeline> {
    const response = await axios.get(`${API_BASE_URL}/api/v1/pipelines/${id}`, {
      headers: this.getHeaders(),
    });
    return response.data;
  }

  async createPipeline(pipeline: Omit<Pipeline, 'id' | 'created_at' | 'updated_at'>): Promise<Pipeline> {
    const response = await axios.post(`${API_BASE_URL}/api/v1/pipelines`, pipeline, {
      headers: this.getHeaders(),
    });
    return response.data;
  }

  async updatePipeline(id: string, updates: Partial<Pipeline>): Promise<Pipeline> {
    const response = await axios.put(`${API_BASE_URL}/api/v1/pipelines/${id}`, updates, {
      headers: this.getHeaders(),
    });
    return response.data;
  }

  async deletePipeline(id: string): Promise<void> {
    await axios.delete(`${API_BASE_URL}/api/v1/pipelines/${id}`, {
      headers: this.getHeaders(),
    });
  }

  async triggerPipeline(id: string): Promise<{ instance_id: string }> {
    const response = await axios.post(`${API_BASE_URL}/api/v1/pipelines/${id}/trigger`, {}, {
      headers: this.getHeaders(),
    });
    return response.data;
  }

  async getExecutionHistory(pipelineId: string, page = 1, pageSize = 20): Promise<{
    executions: ExecutionRecord[];
    total: number;
    page: number;
    page_size: number;
  }> {
    const response = await axios.get(`${API_BASE_URL}/api/v1/pipelines/${pipelineId}/executions`, {
      headers: this.getHeaders(),
      params: { page, page_size: pageSize },
    });
    return response.data;
  }

  async getExecutionDetails(executionId: string): Promise<ExecutionRecord> {
    const response = await axios.get(`${API_BASE_URL}/api/v1/pipelines/executions/${executionId}`, {
      headers: this.getHeaders(),
    });
    return response.data.execution_record;
  }

  async login(username: string, password: string): Promise<string> {
    const response = await axios.post(`${API_BASE_URL}/login`, {
      username,
      password,
    });
    this.setToken(response.data.token);
    return response.data.token;
  }
}

export const pipelineClient = new PipelineClient();
```

### React Component Example

```typescript
// src/components/PipelineList.tsx
import React, { useEffect, useState } from 'react';
import { pipelineClient, Pipeline } from '../api/pipelineClient';

export const PipelineList: React.FC = () => {
  const [pipelines, setPipelines] = useState<Pipeline[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    loadPipelines();
  }, []);

  const loadPipelines = async () => {
    try {
      setLoading(true);
      const data = await pipelineClient.listPipelines();
      setPipelines(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load pipelines');
    } finally {
      setLoading(false);
    }
  };

  const handleTrigger = async (id: string) => {
    try {
      await pipelineClient.triggerPipeline(id);
      alert('Pipeline triggered successfully');
    } catch (err) {
      alert('Failed to trigger pipeline');
    }
  };

  const handleToggleStatus = async (pipeline: Pipeline) => {
    try {
      const newStatus = pipeline.status === 'active' ? 'inactive' : 'active';
      await pipelineClient.updatePipeline(pipeline.id, { status: newStatus });
      loadPipelines();
    } catch (err) {
      alert('Failed to update pipeline status');
    }
  };

  if (loading) return <div>Loading...</div>;
  if (error) return <div>Error: {error}</div>;

  return (
    <div>
      <h1>Pipelines</h1>
      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Status</th>
            <th>Execution Mode</th>
            <th>Cron Expression</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {pipelines.map((pipeline) => (
            <tr key={pipeline.id}>
              <td>{pipeline.name}</td>
              <td>
                <span className={`status-${pipeline.status}`}>
                  {pipeline.status}
                </span>
              </td>
              <td>{pipeline.execution_mode}</td>
              <td>{pipeline.cron_expression || 'N/A'}</td>
              <td>
                <button onClick={() => handleTrigger(pipeline.id)}>
                  Trigger
                </button>
                <button onClick={() => handleToggleStatus(pipeline)}>
                  {pipeline.status === 'active' ? 'Deactivate' : 'Activate'}
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};
```

### Environment Configuration

```bash
# .env
REACT_APP_API_URL=http://localhost:8080
```

### CORS Configuration

The Nebula Conduit server already has CORS enabled in `cmd/server/main.go`:

```go
func corsMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Access-Control-Allow-Origin", "*")
        w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
        if r.Method == "OPTIONS" {
            w.WriteHeader(http.StatusNoContent)
            return
        }
        next.ServeHTTP(w, r)
    })
}
```

---

## Best Practices

### Component Development

1. **Use Base Structs** - Always use `BaseSource`, `BaseProcessor`, or `BaseSink`
2. **Parameter Validation** - Use helper functions from `pkg/pipeline/helpers.go`
3. **Error Handling** - Return descriptive errors with context
4. **Context Awareness** - Always respect `ctx.Done()` for cancellation
5. **Test Thoroughly** - Use `ComponentTestHarness` for isolated testing
6. **Document Parameters** - Clear documentation for all component parameters

### Pipeline Design

1. **Start Simple** - Begin with basic pipelines, add complexity gradually
2. **Use Attribute Update** - Set metadata before components that use templates
3. **Enable Archiving** - Use `archive_on_read` to prevent reprocessing
4. **Monitor Execution** - Check execution history regularly
5. **Set Timeouts** - Configure appropriate timeouts for all components
6. **Handle Errors** - Use `continue_on_error` strategically

### Scheduling

1. **Test Manually First** - Use manual trigger before enabling scheduling
2. **Start Inactive** - Create pipelines as inactive, test, then activate
3. **Appropriate Intervals** - Don't schedule too frequently
4. **Monitor First Run** - Watch logs for the first scheduled execution
5. **Use Cron Validators** - Verify cron expressions before deployment

### Database Management

1. **Regular Cleanup** - Run cleanup scripts periodically
2. **Monitor Size** - Check database size growth
3. **Backup Before Cleanup** - Always backup before running cleanup scripts
4. **Test Deletions** - Verify CASCADE DELETE works correctly

### Logging

1. **Use Appropriate Levels** - debug, info, warn, error
2. **Include Context** - Add metadata for traceability
3. **Control Data Logging** - Use `include_data: false` for sensitive data
4. **Rotate Logs** - Implement log rotation for production
5. **Monitor Log Size** - Large payloads can create huge log files

### Security

1. **Mask Sensitive Data** - Use `include_data: false` for PII
2. **Secure Credentials** - Store credentials in environment variables
3. **Use HTTPS** - Enable TLS for production deployments
4. **Validate Input** - Always validate component parameters
5. **Limit Access** - Use authentication for API endpoints

### Performance

1. **Optimize Queries** - Use efficient database queries
2. **Batch Operations** - Process data in batches when possible
3. **Connection Pooling** - Reuse database connections
4. **Monitor Resources** - Watch CPU and memory usage
5. **Profile Bottlenecks** - Identify and optimize slow components

### Deployment

1. **Use GitLab CI** - Automate builds and deployments
2. **Version Images** - Tag Docker images with pipeline IDs
3. **Test in Staging** - Deploy to staging before production
4. **Monitor Rollouts** - Watch Kubernetes rollout status
5. **Have Rollback Plan** - Know how to rollback deployments

### React Integration

1. **Type Safety** - Use TypeScript interfaces for API responses
2. **Error Handling** - Handle API errors gracefully
3. **Loading States** - Show loading indicators during API calls
4. **Token Management** - Store auth tokens securely
5. **Environment Config** - Use environment variables for API URLs

---

## Quick Reference

### Component Base Types

| Type | Base Struct | Method | Use Case |
|------|-------------|--------|----------|
| Source | `BaseSource` | `Generate` | Read data from external sources |
| Processor | `BaseProcessor` | `Transform` | Transform data one item at a time |
| Sink | `BaseSink` | `Consume` | Write data to external destinations |

### All Component Types

| Type | Category | Description |
|------|----------|-------------|
| `http_get` | Source/Processor | HTTP GET requests with polling and template support |
| `sql_query` | Source | SQL Server queries |
| `csv_reader` | Source | CSV file reader with archive/error handling |
| `tcp_read` | Source | TCP server/client reader |
| `kafka_consumer` | Source | Kafka topic consumer |
| `rabbitmq_consumer` | Source | RabbitMQ queue consumer |
| `hl7_reader` | Source | HL7 v2.x flat file parser |
| `s3_reader` | Source | AWS S3 object reader |
| `minio_reader` | Source | MinIO object reader |
| `azure_blob_reader` | Source | Azure Blob Storage reader |
| `local_storage_reader` | Source | Local file system reader |
| `log` | Processor | Debug logger (pass-through) |
| `attribute_update` | Processor | Metadata field setter with templates |
| `json_extractor` | Processor | JSON array aggregation (max/min/first/last/count) to metadata |
| `json_transform` | Processor | JSON field-level transformation (rename/remove/add/copy/map/convert/flatten) |
| `python_code_block` | Processor | Inline Python code execution |
| `lychgate_response` | Processor | Lychgate integration response handler |
| `http_post` | Sink/Processor | HTTP POST with JWT extraction support |
| `log_sink` | Sink | File-based log writer with dynamic paths |
| `tcp_write` | Sink | TCP server/client writer |
| `kafka_producer` | Sink | Kafka topic producer |
| `rabbitmq_producer` | Sink | RabbitMQ exchange publisher |
| `s3_writer` | Sink | AWS S3 object writer |
| `minio_writer` | Sink | MinIO object writer |
| `azure_blob_writer` | Sink | Azure Blob Storage writer |
| `local_storage_writer` | Sink | Local file system writer |

### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/pipelines` | List all pipelines |
| GET | `/api/v1/pipelines/{id}` | Get pipeline details |
| POST | `/api/v1/pipelines` | Create pipeline |
| PUT | `/api/v1/pipelines/{id}` | Update pipeline |
| DELETE | `/api/v1/pipelines/{id}` | Delete pipeline |
| POST | `/api/v1/pipelines/{id}/trigger` | Trigger pipeline execution |
| GET | `/api/v1/pipelines/{id}/executions` | Get execution history |

### Cron Expression Format

```
* * * * *
│ │ │ │ │
│ │ │ │ └─── Day of week (0-6, Sunday=0)
│ │ │ └───── Month (1-12)
│ │ └─────── Day of month (1-31)
│ └───────── Hour (0-23)
└─────────── Minute (0-59)
```

### File Locations

| File | Purpose |
|------|---------|
| `pkg/pipeline/models.go` | Component types and data structures |
| `pkg/pipeline/framework.go` | Base structs (BaseSource, BaseProcessor, BaseSink) |
| `pkg/pipeline/component.go` | Component interface definitions |
| `pkg/pipeline/components/*.go` | Component implementations |
| `pkg/pipeline/components/register.go` | Component factory registration |
| `pkg/pipeline/validation.go` | Component configuration validation |
| `pkg/pipeline/graph.go` | Pipeline graph validation |
| `pkg/pipeline/helpers.go` | Parameter and data helpers |
| `pkg/pipeline/executor.go` | Pipeline execution engine |
| `pkg/pipeline/test_harness.go` | Component testing utilities |
| `.gitlab-ci.yml` | CI/CD pipeline configuration |

---

## See Also

- [Component Development Guide](component-development-guide.md) - Main guide
- [Pipeline Scheduling Guide](pipeline_scheduling_guide.md) - Scheduling details
- [Log Sink Configuration Guide](log_sink_configuration_guide.md) - Log sink parameters
- [CSV Reader Archive Feature](csv_reader_archive_feature.md) - CSV archiving
- [GitLab CI Configuration](GITLAB_CI_CONFIGURATION.md) - CI/CD setup
- [Logging and Monitoring](LOGGING_AND_MONITORING.md) - Monitoring guide
- [Pipeline API Setup](pipeline_api_setup.md) - API reference

---

## Changelog

### 2026-04-16

- ✅ Added `json_extractor` component for JSON array aggregation (max/min/first/last/count) to metadata
- ✅ Added `json_transform` component for field-level JSON transformations (rename/remove/add/copy/map_values/convert/flatten)
- ✅ Fixed HTTP GET `fetchAndSendWithData` to resolve `{{var}}` templates in URL (not just headers)
- ✅ Added complete component reference with pipeline node examples for all 26 components
- ✅ Updated document structure with full Table of Contents

### 2026-02-26

- ✅ Added real-time pipeline scheduling
- ✅ Enhanced log_sink with `include_data` and `decode_payload` parameters
- ✅ Added CSV reader archive and error handling
- ✅ Implemented template variable resolution
- ✅ Fixed database foreign key constraints
- ✅ Added comprehensive debug logging
- ✅ Simplified GitLab CI configuration
- ✅ Created React integration examples

---

**Last Updated:** April 16, 2026
**Version:** 3.0
**Status:** Production Ready
