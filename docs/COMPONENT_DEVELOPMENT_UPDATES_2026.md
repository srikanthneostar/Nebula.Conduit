# Component Development Guide - 2026 Updates

## Latest Fixes and Enhancements

This document supplements the main [Component Development Guide](component-development-guide.md) with all the latest fixes, enhancements, and best practices added in 2026.

## Table of Contents

- [Pipeline Scheduling System](#pipeline-scheduling-system)
- [Log Sink Component Enhancements](#log-sink-component-enhancements)
- [CSV Reader Component Enhancements](#csv-reader-component-enhancements)
- [Attribute Update Component](#attribute-update-component)
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

**See Also:** [Pipeline Scheduling Guide](pipeline_scheduling_guide.md)

---


## Log Sink Component Enhancements

### New Parameters

The log_sink component now supports two powerful parameters for controlling log output:

#### 1. `include_data` (boolean, default: true)

Controls whether to include the full data payload in logs.

**When true:**
```json
{
  "timestamp": "2026-02-26T13:15:47+05:30",
  "level": "info",
  "trace_id": "csv-reader-1-row-1",
  "metadata": {...},
  "payload": {"id": "1", "name": "Alice", ...}
}
```

**When false:**
```json
{
  "timestamp": "2026-02-26T13:15:47+05:30",
  "level": "info",
  "trace_id": "csv-reader-1-row-1",
  "metadata": {...},
  "message": "Data logged (payload excluded)"
}
```

**Use Cases:**
- Privacy/compliance (exclude sensitive data)
- Performance (reduce log file size by 70%)
- High-volume processing

#### 2. `decode_payload` (boolean, default: true)

Controls whether to decode JSON byte arrays to readable objects.

**When true (default):**
- JSON byte arrays are decoded to readable JSON objects
- Human-readable logs

**When false:**
- Payloads kept as-is (raw bytes/base64)
- Useful for binary data or when raw format needed

### Template Variable Resolution

Log sink now resolves template variables from metadata:

```json
{
  "id": "log-sink-1",
  "type": "log_sink",
  "parameters": {
    "file_path": "{{log_output_path}}",
    "log_level": "{{log_output_level}}",
    "format": "{{log_format}}",
    "include_data": true,
    "decode_payload": true
  }
}
```

Use with `attribute_update` component to set dynamic values:

```json
{
  "id": "attr-update-1",
  "type": "attribute_update",
  "parameters": {
    "mappings": [
      {"name": "log_output_path", "expression": "C:/logs/output.log"},
      {"name": "log_output_level", "expression": "info"},
      {"name": "log_format", "expression": "json"}
    ]
  }
}
```

### Complete Example

```json
{
  "components": [
    {
      "id": "csv-reader-1",
      "type": "csv_reader",
      "parameters": {
        "file_path": "C:/data/input.csv",
        "has_header": true,
        "archive_on_read": true
      }
    },
    {
      "id": "attr-update-1",
      "type": "attribute_update",
      "parameters": {
        "mappings": [
          {"name": "log_output_path", "expression": "C:/logs/pipeline.log"},
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

**See Also:** [Log Sink Configuration Guide](log_sink_configuration_guide.md)

---


## CSV Reader Component Enhancements

### Archive and Error Handling

The CSV reader now supports automatic file management after processing:

#### New Parameters

1. **`archive_on_read`** (boolean, default: false)
   - Moves file to archive folder after successful read
   - File renamed with timestamp: `filename_archive_YYYYMMDD_HHMMSS.csv`

2. **`move_on_error`** (boolean, default: false)
   - Moves file to error folder on failure
   - File renamed with timestamp: `filename_error_YYYYMMDD_HHMMSS.csv`

3. **`archive_folder`** (string, optional)
   - Custom archive folder path
   - Defaults to `./archive` relative to CSV file location

4. **`error_folder`** (string, optional)
   - Custom error folder path
   - Defaults to `./error` relative to CSV file location

### Usage Example

```json
{
  "id": "csv-reader-1",
  "type": "csv_reader",
  "parameters": {
    "file_path": "C:/data/input.csv",
    "has_header": true,
    "delimiter": ",",
    "archive_on_read": true,
    "move_on_error": true,
    "archive_folder": "C:/data/archive",
    "error_folder": "C:/data/error"
  },
  "retry_count": 2,
  "retry_delay": 5000000000,
  "continue_on_error": false,
  "timeout": 30000000000
}
```

### Behavior

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

### Benefits

- **Prevents Reprocessing** - Archived files won't be read again
- **Error Tracking** - Failed files isolated for investigation
- **Audit Trail** - Timestamped filenames show when processed
- **Clean Source Folder** - Processed files automatically removed

**See Also:** [CSV Reader Archive Feature](csv_reader_archive_feature.md)

---


## Attribute Update Component

### Overview

The `attribute_update` component is a processor that adds or updates metadata fields on Data items flowing through the pipeline. It's essential for:

- Setting dynamic configuration values
- Adding context to data
- Enabling template variable resolution in downstream components

### Parameters

**`mappings`** (array of objects, required)
- Each mapping has `name` and `expression` fields
- `name`: The metadata key to set
- `expression`: The value to set (can be static or dynamic)

### Usage Example

```json
{
  "id": "attr-update-1",
  "type": "attribute_update",
  "parameters": {
    "mappings": [
      {
        "name": "log_output_path",
        "expression": "C:/logs/pipeline.log"
      },
      {
        "name": "log_output_level",
        "expression": "info"
      },
      {
        "name": "log_format",
        "expression": "json"
      },
      {
        "name": "pipeline_name",
        "expression": "Data Processing Pipeline"
      }
    ]
  }
}
```

### Common Patterns

#### 1. Dynamic Log Configuration

```json
{
  "components": [
    {"id": "source", "type": "csv_reader", ...},
    {
      "id": "set-log-config",
      "type": "attribute_update",
      "parameters": {
        "mappings": [
          {"name": "log_path", "expression": "C:/logs/{{date}}.log"},
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

### Best Practices

1. **Place Before Template-Using Components** - attribute_update must come before components that use `{{variables}}`
2. **Use Descriptive Names** - Clear metadata keys make debugging easier
3. **Document Mappings** - Comment what each mapping is for
4. **Validate Downstream** - Ensure downstream components expect the metadata keys you set

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

**See Also:** [Logging and Monitoring](LOGGING_AND_MONITORING.md)

---


## GitLab CI/CD Integration

### Pipeline Configuration

**File:** `.gitlab-ci.yml`

The CI/CD pipeline builds Docker images and deploys to Kubernetes automatically.

### Stages

1. **docker-login** - Authenticate with Docker Hub
2. **image-build-push** - Build and push Docker image
3. **kubernetes-deploy** - Deploy to Kubernetes cluster

### Required Variables

Set in GitLab CI/CD settings:

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

### Usage

```bash
# Commit and push to trigger pipeline
git add .
git commit -m "Update pipeline"
git push

# Or manually trigger in GitLab UI
```

**See Also:** [GitLab CI Configuration](GITLAB_CI_CONFIGURATION.md)

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

  // Pipeline CRUD
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

  // Pipeline Execution
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

  // Authentication
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

### Component Types

| Type | Base Struct | Method | Use Case |
|------|-------------|--------|----------|
| Source | `BaseSource` | `Generate` | Read data from external sources |
| Processor | `BaseProcessor` | `Transform` | Transform data one item at a time |
| Sink | `BaseSink` | `Consume` | Write data to external destinations |

### Common Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `retry_count` | int | 0 | Number of retries on failure |
| `retry_delay` | duration | 0 | Delay between retries |
| `continue_on_error` | bool | false | Continue pipeline on component error |
| `timeout` | duration | 0 | Component execution timeout |

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
| `pkg/pipeline/components/*.go` | Component implementations |
| `pkg/pipeline/validation.go` | Component configuration validation |
| `pkg/pipeline/graph.go` | Pipeline graph validation |
| `pkg/pipeline/helpers.go` | Parameter and data helpers |
| `pkg/pipeline/test_harness.go` | Component testing utilities |
| `.gitlab-ci.yml` | CI/CD pipeline configuration |
| `docs/*.md` | Documentation files |

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

### 2026-02-26

- ✅ Added real-time pipeline scheduling
- ✅ Enhanced log_sink with `include_data` and `decode_payload` parameters
- ✅ Added CSV reader archive and error handling
- ✅ Implemented template variable resolution
- ✅ Fixed database foreign key constraints
- ✅ Added comprehensive debug logging
- ✅ Simplified GitLab CI configuration
- ✅ Created React integration examples
- ✅ Updated all documentation

---

**Last Updated:** February 26, 2026
**Version:** 2.0
**Status:** Production Ready
