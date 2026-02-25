# Design Document: Data Pipeline Engine

## Overview

The Data Pipeline Engine is a graph-based data processing system that enables users to construct and execute data workflows in Go. Inspired by DigitalOcean's Firebolt architecture, the engine follows a Source → Processing Nodes → Sinks pattern where components are connected in a directed acyclic graph (DAG) to form pipelines.

### Key Design Principles

1. **Modularity**: The engine is implemented as a standalone module (`pkg/pipeline`) that integrates with existing infrastructure without modifying core systems
2. **Concurrency**: Leverages Go's goroutines and channels for efficient parallel component execution
3. **Extensibility**: Component interface design allows easy addition of new component types
4. **Observability**: Deep integration with zerolog for comprehensive logging and metrics
5. **Resilience**: Built-in error handling, retry logic, and backpressure integration

### Architecture Pattern

The engine implements a **Pipeline Execution Model** where:
- **Pipelines** are directed graphs of connected components
- **Components** are goroutines that communicate via channels
- **Data flows** through channels from sources to sinks
- **Execution modes** support both scheduled (cron) and continuous (streaming) processing

## Architecture

### High-Level System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        API Layer                                 │
│  /api/v1/pipelines (CRUD, Start, Stop, Status, History)        │
└────────────────────┬────────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────────┐
│                   Pipeline Engine Core                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │  Scheduler   │  │   Executor   │  │  Lifecycle   │         │
│  │  (Cron)      │  │  (Goroutine  │  │  Manager     │         │
│  │              │  │   Pool)      │  │              │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
└────────────────────┬────────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────────┐
│                  Component Registry                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │ Sources  │  │Processors│  │  Sinks   │  │ Factory  │       │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘       │
└────────────────────┬────────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────────┐
│                Integration Layer                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │  Database    │  │ Backpressure │  │   Logger     │         │
│  │  (SQLite)    │  │   System     │  │  (zerolog)   │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
└─────────────────────────────────────────────────────────────────┘
```

### Component Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Pipeline Instance                         │
│                                                                  │
│  ┌──────────┐      ┌──────────┐      ┌──────────┐             │
│  │  Source  │─────▶│Processor │─────▶│   Sink   │             │
│  │Component │      │Component │      │Component │             │
│  │(Goroutine)      │(Goroutine)      │(Goroutine)             │
│  └──────────┘      └──────────┘      └──────────┘             │
│       │                  │                  │                   │
│       └──────────────────┴──────────────────┘                   │
│                          │                                       │
│                    Data Channels                                 │
│                                                                  │
│  Context (cancellation) ────────────────────────────────────▶   │
│  WaitGroup (coordination) ───────────────────────────────────▶  │
└─────────────────────────────────────────────────────────────────┘
```

### Execution Flow

1. **Pipeline Loading**: Engine loads active pipeline definitions from database
2. **Scheduling**: Scheduler determines when to execute based on execution mode
3. **Instantiation**: Executor creates component instances with configurations
4. **Graph Validation**: Validates DAG structure and type compatibility
5. **Goroutine Spawning**: Creates goroutine for each component
6. **Channel Setup**: Establishes channels between connected components
7. **Execution**: Components process data concurrently
8. **Monitoring**: Tracks execution status and metrics
9. **Completion**: Waits for all components, records results
10. **Cleanup**: Closes channels, cancels contexts, releases resources

## Components and Interfaces

### Core Component Interface

```go
// Component is the base interface all pipeline components must implement
type Component interface {
    // Execute runs the component logic with the given context and input channel
    // Returns an output channel for downstream components
    Execute(ctx context.Context, input <-chan Data) (<-chan Data, error)
    
    // Validate checks if the component configuration is valid
    Validate() error
    
    // Type returns the component type identifier
    Type() ComponentType
    
    // ID returns the unique component instance identifier
    ID() string
    
    // Config returns the component configuration
    Config() ComponentConfig
}
```

### Component Categories

#### Source Components
Generate or read data from external systems. No input channel required.

```go
type SourceComponent interface {
    Component
    // Start begins data generation/reading
    Start(ctx context.Context) (<-chan Data, error)
}
```

**Supported Sources:**
- `HTTPGetComponent`: Fetches data via HTTP GET requests
- `SQLQueryComponent`: Executes SQL queries against SQL Server
- `CSVReaderComponent`: Reads data from CSV files
- `KafkaConsumerComponent`: Consumes messages from Kafka topics
- `RabbitMQConsumerComponent`: Consumes messages from RabbitMQ queues
- `HL7ReaderComponent`: Reads HL7 flat files
- `TCPReadComponent`: Reads data from TCP connections (server or client mode)

#### Processor Components
Transform data between sources and sinks.

```go
type ProcessorComponent interface {
    Component
    // Process transforms input data to output data
    Process(ctx context.Context, input <-chan Data) (<-chan Data, error)
}
```

**Supported Processors:**
- `PythonCodeBlockComponent`: Executes inline Python code for data transformation
- `LogComponent`: Logs data for inspection and passes it through unchanged

#### Sink Components
Write or send data to external systems.

```go
type SinkComponent interface {
    Component
    // Write consumes data and writes to destination
    Write(ctx context.Context, input <-chan Data) error
}
```

**Supported Sinks:**
- `HTTPPostComponent`: Sends data via HTTP POST requests
- `KafkaProducerComponent`: Produces messages to Kafka topics
- `RabbitMQProducerComponent`: Produces messages to RabbitMQ exchanges
- `TCPWriteComponent`: Writes data to TCP connections (server or client mode)

### Component Factory

```go
type ComponentFactory interface {
    // Create instantiates a component from its configuration
    Create(config ComponentConfig) (Component, error)
    
    // Register adds a new component type to the factory
    Register(componentType ComponentType, constructor ComponentConstructor)
    
    // ListTypes returns all registered component types
    ListTypes() []ComponentType
}

type ComponentConstructor func(config ComponentConfig) (Component, error)
```

### Data Model

```go
// Data represents the payload passed between components
type Data struct {
    Payload   interface{}       // The actual data
    Metadata  map[string]string // Contextual information
    Timestamp time.Time         // When data was created
    TraceID   string            // For distributed tracing
}

// ComponentConfig holds component-specific configuration
type ComponentConfig struct {
    ID              string                 `json:"id"`
    Type            ComponentType          `json:"type"`
    Parameters      map[string]interface{} `json:"parameters"`
    RetryCount      int                    `json:"retry_count"`
    RetryDelay      time.Duration          `json:"retry_delay"`
    ContinueOnError bool                   `json:"continue_on_error"`
    Timeout         time.Duration          `json:"timeout"`
}
```

### Pipeline Engine Core

```go
type PipelineEngine struct {
    repository      PipelineRepository
    scheduler       Scheduler
    executor        Executor
    lifecycleManager LifecycleManager
    componentFactory ComponentFactory
    backpressure    BackpressureSystem
    logger          *zerolog.Logger
    metrics         MetricsCollector
}

type PipelineRepository interface {
    Create(def PipelineDefinition) error
    Read(id string) (PipelineDefinition, error)
    Update(def PipelineDefinition) error
    Delete(id string) error
    List() ([]PipelineDefinition, error)
    ListActive() ([]PipelineDefinition, error)
}

type Scheduler interface {
    Schedule(pipeline PipelineDefinition) error
    Unschedule(pipelineID string) error
    Trigger(pipelineID string) error
}

type Executor interface {
    Execute(ctx context.Context, pipeline PipelineDefinition) (*PipelineInstance, error)
    Stop(instanceID string) error
    GetStatus(instanceID string) (ExecutionStatus, error)
}

type LifecycleManager interface {
    Start(pipelineID string) error
    Stop(pipelineID string) error
    GetRunningInstances() []PipelineInstance
    GetExecutionHistory(pipelineID string) ([]ExecutionRecord, error)
}
```

## Data Models

### Pipeline Definition

```go
type PipelineDefinition struct {
    ID            string            `json:"id" db:"id"`
    Name          string            `json:"name" db:"name"`
    Description   string            `json:"description" db:"description"`
    ExecutionMode ExecutionMode     `json:"execution_mode" db:"execution_mode"`
    CronExpression string           `json:"cron_expression" db:"cron_expression"`
    Status        PipelineStatus    `json:"status" db:"status"`
    Components    []ComponentConfig `json:"components"`
    Connections   []Connection      `json:"connections"`
    CreatedAt     time.Time         `json:"created_at" db:"created_at"`
    UpdatedAt     time.Time         `json:"updated_at" db:"updated_at"`
}

type ExecutionMode string

const (
    ExecutionModeScheduled  ExecutionMode = "scheduled"
    ExecutionModeContinuous ExecutionMode = "continuous"
)

type PipelineStatus string

const (
    PipelineStatusActive   PipelineStatus = "active"
    PipelineStatusInactive PipelineStatus = "inactive"
)

type Connection struct {
    SourceComponentID string `json:"source_component_id"`
    TargetComponentID string `json:"target_component_id"`
}
```

### Pipeline Instance

```go
type PipelineInstance struct {
    ID              string
    PipelineID      string
    Status          InstanceStatus
    Components      map[string]Component
    Channels        map[string]chan Data
    Context         context.Context
    CancelFunc      context.CancelFunc
    WaitGroup       *sync.WaitGroup
    StartedAt       time.Time
    CompletedAt     *time.Time
    Error           error
}

type InstanceStatus string

const (
    InstanceStatusRunning   InstanceStatus = "running"
    InstanceStatusCompleted InstanceStatus = "completed"
    InstanceStatusFailed    InstanceStatus = "failed"
    InstanceStatusStopped   InstanceStatus = "stopped"
)
```

### Execution Records

```go
type ExecutionRecord struct {
    ID              string         `json:"id" db:"id"`
    PipelineID      string         `json:"pipeline_id" db:"pipeline_id"`
    Status          InstanceStatus `json:"status" db:"status"`
    StartedAt       time.Time      `json:"started_at" db:"started_at"`
    EndedAt         *time.Time     `json:"ended_at" db:"ended_at"`
    ErrorMessage    *string        `json:"error_message" db:"error_message"`
    ComponentResults []ComponentExecutionRecord `json:"component_results"`
}

type ComponentExecutionRecord struct {
    ID                 string         `json:"id" db:"id"`
    PipelineExecutionID string        `json:"pipeline_execution_id" db:"pipeline_execution_id"`
    ComponentID        string         `json:"component_id" db:"component_id"`
    Status             InstanceStatus `json:"status" db:"status"`
    StartedAt          time.Time      `json:"started_at" db:"started_at"`
    EndedAt            *time.Time     `json:"ended_at" db:"ended_at"`
    OutputDataSize     int64          `json:"output_data_size" db:"output_data_size"`
    ErrorMessage       *string        `json:"error_message" db:"error_message"`
}
```

### Database Schema

```sql
-- Pipelines table
CREATE TABLE pipelines (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    execution_mode TEXT NOT NULL CHECK(execution_mode IN ('scheduled', 'continuous')),
    cron_expression TEXT,
    status TEXT NOT NULL CHECK(status IN ('active', 'inactive')),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Pipeline components table
CREATE TABLE pipeline_components (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    component_type TEXT NOT NULL,
    component_config TEXT NOT NULL, -- JSON
    position_in_graph INTEGER NOT NULL,
    FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE
);

-- Pipeline connections table
CREATE TABLE pipeline_connections (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    source_component_id TEXT NOT NULL,
    target_component_id TEXT NOT NULL,
    FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE,
    FOREIGN KEY (source_component_id) REFERENCES pipeline_components(id) ON DELETE CASCADE,
    FOREIGN KEY (target_component_id) REFERENCES pipeline_components(id) ON DELETE CASCADE
);

-- Pipeline executions table
CREATE TABLE pipeline_executions (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('running', 'completed', 'failed', 'stopped')),
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    error_message TEXT,
    FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE
);

-- Component executions table
CREATE TABLE component_executions (
    id TEXT PRIMARY KEY,
    pipeline_execution_id TEXT NOT NULL,
    component_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('running', 'completed', 'failed', 'stopped')),
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    output_data_size INTEGER,
    error_message TEXT,
    FOREIGN KEY (pipeline_execution_id) REFERENCES pipeline_executions(id) ON DELETE CASCADE,
    FOREIGN KEY (component_id) REFERENCES pipeline_components(id) ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX idx_pipelines_status ON pipelines(status);
CREATE INDEX idx_pipeline_components_pipeline_id ON pipeline_components(pipeline_id);
CREATE INDEX idx_pipeline_connections_pipeline_id ON pipeline_connections(pipeline_id);
CREATE INDEX idx_pipeline_executions_pipeline_id ON pipeline_executions(pipeline_id);
CREATE INDEX idx_pipeline_executions_started_at ON pipeline_executions(started_at);
CREATE INDEX idx_component_executions_pipeline_execution_id ON component_executions(pipeline_execution_id);
```

### Component Configuration Examples

#### HTTP GET Component
```json
{
  "id": "http-source-1",
  "type": "http_get",
  "parameters": {
    "url": "https://api.example.com/data",
    "headers": {
      "Authorization": "Bearer token123"
    },
    "interval": "30s"
  },
  "retry_count": 3,
  "retry_delay": "5s",
  "timeout": "30s"
}
```

#### Python Code Block Component
```json
{
  "id": "transform-1",
  "type": "python_code_block",
  "parameters": {
    "code": "import json\ndata = json.loads(input_data)\ndata['processed'] = True\noutput_data = json.dumps(data)",
    "environment": {
      "PYTHONPATH": "/custom/path"
    }
  },
  "timeout": "60s",
  "continue_on_error": false
}
```

#### TCP Read Component (Server Mode)
```json
{
  "id": "tcp-server-1",
  "type": "tcp_read",
  "parameters": {
    "mode": "server",
    "host": "0.0.0.0",
    "port": 8080,
    "encoding": "utf-8",
    "connection_timeout": "30s",
    "max_connections": 10
  },
  "retry_count": 0
}
```

#### Log Component
```json
{
  "id": "logger-1",
  "type": "log",
  "parameters": {
    "log_level": "info",
    "format": "json",
    "sample_size": 1000,
    "truncate": true
  }
}
```

#### Kafka Producer Component
```json
{
  "id": "kafka-sink-1",
  "type": "kafka_producer",
  "parameters": {
    "brokers": ["localhost:9092"],
    "topic": "processed-data",
    "compression": "snappy"
  },
  "retry_count": 5,
  "retry_delay": "10s"
}
```


## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system—essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Pipeline Definition Round-Trip Serialization

*For any* valid pipeline definition with components and connections, serializing to JSON and deserializing back should produce an equivalent pipeline definition with all component configurations preserved.

**Validates: Requirements 1.3, 1.7**

### Property 2: Pipeline Definition Structure Completeness

*For any* pipeline definition created or retrieved from the repository, it must contain all required fields: unique identifier, name, execution mode, status (active or inactive), and for scheduled pipelines, a valid cron expression.

**Validates: Requirements 1.2, 1.6, 3.6**

### Property 3: Graph Validation Rejects Invalid Topologies

*For any* pipeline definition, if the component graph lacks at least one source component or at least one sink component, the repository validation must reject it with an error.

**Validates: Requirements 1.5**

### Property 4: Component Configuration Validation

*For any* component type and configuration, if the configuration is missing required parameters (e.g., URL for HTTP components, connection string for SQL components, host/port for TCP components, code for Python components, log level for Log components), the repository validation must reject it with a descriptive error message.

**Validates: Requirements 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 8.9, 8.10, 8.11, 8.12, 8.13, 8.14, 8.15**

### Property 5: Invalid Cron Expression Rejection

*For any* pipeline definition with execution mode set to scheduled, if the cron expression is invalid or malformed, the repository validation must reject it with an error.

**Validates: Requirements 3.7**

### Property 6: Component Type Registration

*For any* registered component type in the factory, creating a component with valid configuration for that type should succeed and return a component instance of the correct type.

**Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10, 2.15, 2.20, 2.27**

### Property 7: Python Component Input Data Flow

*For any* Python code block component in a pipeline, the input data from the previous component must be accessible to the Python execution environment, and any environment variables configured must be available during execution.

**Validates: Requirements 2.22, 2.23**

### Property 8: Python Component Timeout Enforcement

*For any* Python code block component with a configured timeout, if the Python code execution exceeds the timeout duration, the component must terminate execution and return an error.

**Validates: Requirements 2.25, 2.26**

### Property 9: Log Component Data Passthrough

*For any* log component receiving input data, the component must log the data at the configured level and pass the exact same data unchanged to all downstream components.

**Validates: Requirements 2.32**

### Property 10: Log Component Truncation

*For any* log component configured with truncation enabled and a maximum size, if the input data exceeds the maximum size, the logged output must be truncated while the full data is still passed to downstream components.

**Validates: Requirements 2.30**

### Property 11: Component Configuration Loading

*For any* component instantiated from a pipeline definition, the component's configuration must match the configuration stored in the pipeline definition.

**Validates: Requirements 2.33**

### Property 12: Scheduled Pipeline Execution Recording

*For any* scheduled pipeline that completes execution, an execution record must be created in the database containing the pipeline ID, status, start time, end time, and any error messages.

**Validates: Requirements 3.4, 6.6**

### Property 13: Topological Execution Order

*For any* pipeline instance with a directed acyclic graph of components, components must execute in an order that respects the graph topology—no component should execute before all its upstream dependencies have produced output.

**Validates: Requirements 4.2**

### Property 14: Fanout Data Distribution

*For any* component with multiple downstream components, when the component produces output data, that exact data must be routed to all connected downstream components.

**Validates: Requirements 4.5, 4.6, 12.3**

### Property 15: Type Compatibility Validation

*For any* two components connected in a pipeline, if the output type of the source component is incompatible with the input type of the target component, the pipeline validation must reject the connection with an error.

**Validates: Requirements 4.8**

### Property 16: Rate Limiting Compliance

*For any* pipeline engine operation, the operation must respect the configured rate limits from the backpressure system, ensuring that operation rates do not exceed the specified limits.

**Validates: Requirements 5.3**

### Property 17: Pipeline Status Change Propagation

*For any* pipeline definition, when its status changes from active to inactive, all running pipeline instances for that definition must be stopped.

**Validates: Requirements 6.2**

### Property 18: Execution History Completeness

*For any* completed pipeline instance, the execution history must contain records for the pipeline execution and all component executions, including duration, status, and any errors.

**Validates: Requirements 7.8**

### Property 19: Component Retry Behavior

*For any* component with retry count N > 0 and retry delay D, if the component fails, it must be retried up to N times with delay D between attempts, and only marked as failed after all retries are exhausted.

**Validates: Requirements 7.3, 7.4**

### Property 20: Continue-On-Error Behavior

*For any* component with continue_on_error flag, if the component fails and the flag is true, downstream components must continue executing; if the flag is false, the pipeline instance must stop and be marked as failed.

**Validates: Requirements 7.6, 7.7**

### Property 21: TCP Component Encoding Round-Trip

*For any* TCP read or TCP write component with a configured encoding, data encoded with that encoding and then decoded should produce equivalent data.

**Validates: Requirements 2.13, 2.14, 2.18, 2.19**

### Property 22: Pipeline Instance Termination

*For any* running pipeline instance, when a stop signal is sent, all component goroutines must terminate gracefully, and the instance status must be updated to stopped.

**Validates: Requirements 3.5, 12.5**

### Property 23: Panic Recovery and Logging

*For any* component goroutine that panics during execution, the pipeline engine must recover from the panic, log the panic with full context, mark the component as failed, and continue managing other components.

**Validates: Requirements 12.7**

## Error Handling

### Error Categories

The pipeline engine handles four categories of errors:

1. **Configuration Errors**: Invalid pipeline definitions, malformed component configurations, invalid cron expressions
2. **Execution Errors**: Component failures, timeout errors, connection failures, data processing errors
3. **System Errors**: Database errors, resource exhaustion, backpressure triggers
4. **Panic Errors**: Unexpected panics in component goroutines

### Error Handling Strategy

#### Configuration Validation
- All configuration errors are caught during pipeline creation/update
- Validation occurs before any execution begins
- Descriptive error messages indicate which field is invalid and why
- Failed validations prevent pipeline from being saved or activated

#### Component Execution Errors
- Each component has configurable retry logic (count and delay)
- Retries use exponential backoff for transient failures
- After retry exhaustion, component is marked as failed
- `continue_on_error` flag determines if pipeline continues or stops
- All errors are logged with full context (component ID, config, input data size)
- Errors are recorded in component_executions table

#### Timeout Handling
- Each component supports configurable timeout
- Timeouts use context.Context for cancellation
- Timeout expiration cancels component execution
- Timeout errors are treated as execution errors (subject to retry logic)

#### Panic Recovery
- All component goroutines are wrapped with recover()
- Panics are caught, logged with stack trace, and converted to errors
- Panicked components are marked as failed
- Pipeline continues managing other components after recovery

#### Backpressure Integration
- High load conditions pause new pipeline instance creation
- Circuit breaker open state queues execution requests
- Resource threshold violations throttle continuous pipelines
- All backpressure events are logged for observability

#### Database Errors
- Database operations use transactions where appropriate
- Failed transactions are rolled back
- Database errors are logged and propagated to callers
- Retry logic for transient database errors (connection issues)

### Error Propagation

```
Component Error
    ↓
Retry Logic (if configured)
    ↓
Continue-On-Error Check
    ↓
├─ True: Log error, continue pipeline
└─ False: Stop pipeline, mark as failed
    ↓
Record in execution history
    ↓
Log with zerolog
    ↓
Update metrics
```

### Error Response Format

API endpoints return structured error responses:

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Pipeline validation failed",
    "details": {
      "field": "components[0].parameters.url",
      "reason": "Invalid URL format"
    }
  }
}
```

## Testing Strategy

### Dual Testing Approach

The pipeline engine requires both unit tests and property-based tests for comprehensive coverage:

- **Unit tests** verify specific examples, edge cases, and integration points
- **Property-based tests** verify universal properties across all inputs
- Together they provide comprehensive coverage: unit tests catch concrete bugs, property tests verify general correctness

### Property-Based Testing

**Framework**: Use `gopter` (Go property testing library) for property-based tests

**Configuration**: Each property test must run minimum 100 iterations to ensure adequate randomization coverage

**Test Organization**: Each correctness property from the design document must be implemented as a single property-based test

**Tagging Convention**: Each test must include a comment tag referencing the design property:
```go
// Feature: data-pipeline-engine, Property 1: Pipeline Definition Round-Trip Serialization
func TestProperty_PipelineDefinitionRoundTrip(t *testing.T) { ... }
```

**Property Test Examples**:

1. **Round-Trip Properties**: Test serialization/deserialization (Property 1, 21)
2. **Validation Properties**: Test that invalid inputs are rejected (Properties 3, 4, 5, 15)
3. **Invariant Properties**: Test that certain conditions always hold (Properties 9, 14, 18)
4. **Behavioral Properties**: Test that operations behave correctly across inputs (Properties 7, 8, 19, 20)

### Unit Testing

**Focus Areas**:
- Component factory registration and creation
- API endpoint handlers (create, read, update, delete, trigger, stop, status, history)
- Database schema validation
- Scheduler integration with cron library
- Backpressure system integration
- Logging integration with zerolog
- Specific execution modes (scheduled vs continuous)
- Specific component types (HTTP, SQL, Kafka, etc.)
- Error scenarios (network failures, timeouts, panics)

**Test Organization**:
```
pkg/pipeline/
  ├── engine_test.go          # Engine lifecycle tests
  ├── repository_test.go      # Database CRUD tests
  ├── executor_test.go        # Execution logic tests
  ├── scheduler_test.go       # Scheduling tests
  ├── components/
  │   ├── http_test.go        # HTTP component tests
  │   ├── sql_test.go         # SQL component tests
  │   ├── kafka_test.go       # Kafka component tests
  │   ├── python_test.go      # Python component tests
  │   └── ...
  └── properties/
      ├── roundtrip_test.go   # Round-trip property tests
      ├── validation_test.go  # Validation property tests
      ├── execution_test.go   # Execution property tests
      └── ...
```

### Integration Testing

**Test Scenarios**:
1. End-to-end pipeline execution (source → processor → sink)
2. Multi-component fanout patterns
3. Error handling and retry logic
4. Scheduled pipeline execution with cron
5. Continuous pipeline lifecycle (start, run, stop)
6. Backpressure integration under load
7. Database persistence and retrieval
8. API endpoint workflows

**Test Environment**:
- Use testcontainers for external dependencies (Kafka, RabbitMQ, SQL Server)
- Use in-memory SQLite for database tests
- Mock backpressure system for controlled testing
- Capture and verify log output

### Test Coverage Goals

- **Line Coverage**: Minimum 80% for all packages
- **Branch Coverage**: Minimum 75% for error handling paths
- **Property Coverage**: 100% of design properties implemented as tests
- **Component Coverage**: 100% of component types have unit tests

### Continuous Testing

- All tests run on every commit via CI/CD
- Property tests run with increased iterations (1000+) in nightly builds
- Integration tests run against real external systems in staging environment
- Performance benchmarks track execution time and resource usage


## API Design

### REST API Endpoints

All pipeline endpoints are registered under `/api/v1/pipelines`

#### Pipeline Management

**Create Pipeline**
```
POST /api/v1/pipelines
Content-Type: application/json

{
  "name": "data-ingestion-pipeline",
  "description": "Ingests data from API and stores in Kafka",
  "execution_mode": "scheduled",
  "cron_expression": "0 */5 * * *",
  "status": "active",
  "components": [...],
  "connections": [...]
}

Response: 201 Created
{
  "id": "pipeline-uuid",
  "name": "data-ingestion-pipeline",
  ...
}
```

**Get Pipeline**
```
GET /api/v1/pipelines/{id}

Response: 200 OK
{
  "id": "pipeline-uuid",
  "name": "data-ingestion-pipeline",
  "components": [...],
  "connections": [...]
}
```

**List Pipelines**
```
GET /api/v1/pipelines?status=active&execution_mode=scheduled

Response: 200 OK
{
  "pipelines": [
    {
      "id": "pipeline-uuid",
      "name": "data-ingestion-pipeline",
      ...
    }
  ],
  "total": 1
}
```

**Update Pipeline**
```
PUT /api/v1/pipelines/{id}
Content-Type: application/json

{
  "name": "updated-pipeline-name",
  "status": "inactive",
  ...
}

Response: 200 OK
```

**Delete Pipeline**
```
DELETE /api/v1/pipelines/{id}

Response: 204 No Content
```

#### Pipeline Execution Control

**Trigger Pipeline**
```
POST /api/v1/pipelines/{id}/trigger

Response: 202 Accepted
{
  "execution_id": "execution-uuid",
  "pipeline_id": "pipeline-uuid",
  "status": "running",
  "started_at": "2024-01-15T10:30:00Z"
}
```

**Stop Pipeline Instance**
```
POST /api/v1/pipelines/{id}/instances/{instance_id}/stop

Response: 200 OK
{
  "instance_id": "instance-uuid",
  "status": "stopped"
}
```

**Get Instance Status**
```
GET /api/v1/pipelines/{id}/instances/{instance_id}

Response: 200 OK
{
  "instance_id": "instance-uuid",
  "pipeline_id": "pipeline-uuid",
  "status": "running",
  "started_at": "2024-01-15T10:30:00Z",
  "components": [
    {
      "component_id": "comp-1",
      "status": "completed",
      "duration_ms": 1234
    }
  ]
}
```

**List Running Instances**
```
GET /api/v1/pipelines/instances?status=running

Response: 200 OK
{
  "instances": [
    {
      "instance_id": "instance-uuid",
      "pipeline_id": "pipeline-uuid",
      "status": "running",
      "started_at": "2024-01-15T10:30:00Z"
    }
  ]
}
```

#### Execution History

**Get Execution History**
```
GET /api/v1/pipelines/{id}/executions?limit=10&offset=0

Response: 200 OK
{
  "executions": [
    {
      "id": "execution-uuid",
      "pipeline_id": "pipeline-uuid",
      "status": "completed",
      "started_at": "2024-01-15T10:30:00Z",
      "ended_at": "2024-01-15T10:35:00Z",
      "duration_ms": 300000,
      "component_results": [...]
    }
  ],
  "total": 42
}
```

**Get Execution Details**
```
GET /api/v1/pipelines/{id}/executions/{execution_id}

Response: 200 OK
{
  "id": "execution-uuid",
  "pipeline_id": "pipeline-uuid",
  "status": "completed",
  "component_results": [
    {
      "component_id": "comp-1",
      "component_type": "http_get",
      "status": "completed",
      "started_at": "2024-01-15T10:30:00Z",
      "ended_at": "2024-01-15T10:30:05Z",
      "output_data_size": 1024,
      "error_message": null
    }
  ]
}
```

### Metrics Endpoint Integration

Pipeline metrics are exposed at the existing `/metrics` endpoint:

```
# HELP pipeline_total Total number of pipeline definitions
# TYPE pipeline_total gauge
pipeline_total 15

# HELP pipeline_active Number of active pipeline definitions
# TYPE pipeline_active gauge
pipeline_active 10

# HELP pipeline_instances_running Number of currently running pipeline instances
# TYPE pipeline_instances_running gauge
pipeline_instances_running 5

# HELP pipeline_executions_total Total number of pipeline executions
# TYPE pipeline_executions_total counter
pipeline_executions_total 1234

# HELP pipeline_execution_duration_seconds Pipeline execution duration
# TYPE pipeline_execution_duration_seconds histogram
pipeline_execution_duration_seconds_bucket{pipeline="data-ingestion",le="1"} 10
pipeline_execution_duration_seconds_bucket{pipeline="data-ingestion",le="5"} 45
pipeline_execution_duration_seconds_bucket{pipeline="data-ingestion",le="10"} 80

# HELP pipeline_component_errors_total Total number of component errors
# TYPE pipeline_component_errors_total counter
pipeline_component_errors_total{component_type="http_get"} 12
pipeline_component_errors_total{component_type="kafka_producer"} 3
```

## Integration Patterns

### Backpressure System Integration

The pipeline engine integrates with the existing backpressure system through defined interfaces:

```go
type BackpressureSystem interface {
    // IsHighLoad returns true if system is under high load
    IsHighLoad() bool
    
    // IsCircuitOpen returns true if circuit breaker is open
    IsCircuitOpen() bool
    
    // GetRateLimit returns the current rate limit for operations
    GetRateLimit() int
    
    // RecordOperation records an operation for rate limiting
    RecordOperation(operationType string) error
}
```

**Integration Points**:
1. Before creating new pipeline instances, check `IsHighLoad()`
2. Before executing operations, check `IsCircuitOpen()`
3. Record all pipeline operations with `RecordOperation()`
4. Respect rate limits from `GetRateLimit()`

### Database Integration

The pipeline engine uses the existing `pkg/database` package:

```go
// Use existing database connection
db := database.GetConnection()

// Use existing transaction support
tx, err := db.Begin()
defer tx.Rollback()
// ... operations ...
tx.Commit()

// Use existing migration system
database.RegisterMigrations("pipeline", pipelineMigrations)
```

**Migration Files**:
- `migrations/001_create_pipelines_table.sql`
- `migrations/002_create_pipeline_components_table.sql`
- `migrations/003_create_pipeline_connections_table.sql`
- `migrations/004_create_pipeline_executions_table.sql`
- `migrations/005_create_component_executions_table.sql`

### Logging Integration

The pipeline engine uses the existing `pkg/logger` package with zerolog:

```go
// Get logger instance
log := logger.GetLogger()

// Structured logging for pipeline events
log.Info().
    Str("pipeline_id", pipelineID).
    Str("execution_id", executionID).
    Dur("duration", duration).
    Msg("Pipeline execution completed")

// Error logging with context
log.Error().
    Err(err).
    Str("component_id", componentID).
    Str("component_type", componentType).
    Interface("config", config).
    Msg("Component execution failed")
```

### Python Executor Integration

The Python code block component integrates with existing Python executor infrastructure:

```go
type PythonExecutor interface {
    // Execute runs Python code with input data and environment
    Execute(ctx context.Context, code string, input interface{}, env map[string]string) (interface{}, error)
}

// Python component uses existing executor
executor := python.GetExecutor()
output, err := executor.Execute(ctx, component.Code, inputData, component.Environment)
```

## Data Flow Patterns

### Synchronous Flow

Components execute sequentially, waiting for each to complete:

```
Source → Process → Sink
  ↓        ↓        ↓
 Wait    Wait    Complete
```

**Use Cases**: Simple ETL, data validation, sequential processing

### Asynchronous Flow

Components execute concurrently with channel-based communication:

```
Source ──channel──→ Process ──channel──→ Sink
  ↓                    ↓                    ↓
Goroutine          Goroutine            Goroutine
```

**Use Cases**: High-throughput streaming, parallel processing

### Fanout Pattern

One component feeds multiple downstream components:

```
        ┌──→ Processor 1 ──→ Sink 1
Source ─┼──→ Processor 2 ──→ Sink 2
        └──→ Processor 3 ──→ Sink 3
```

**Implementation**: Data is duplicated and sent to all downstream channels

**Use Cases**: Multi-destination routing, parallel transformations

### Fan-In Pattern

Multiple components feed one downstream component:

```
Source 1 ──┐
Source 2 ──┼──→ Processor ──→ Sink
Source 3 ──┘
```

**Implementation**: Processor reads from multiple input channels using select

**Use Cases**: Data aggregation, multi-source merging

## Configuration Management

### Pipeline Configuration File Format

Pipelines can be defined in YAML or JSON configuration files:

```yaml
name: data-ingestion-pipeline
description: Ingests data from API and stores in Kafka
execution_mode: scheduled
cron_expression: "0 */5 * * *"
status: active

components:
  - id: http-source
    type: http_get
    parameters:
      url: https://api.example.com/data
      headers:
        Authorization: Bearer ${API_TOKEN}
      interval: 30s
    retry_count: 3
    retry_delay: 5s
    timeout: 30s

  - id: transform
    type: python_code_block
    parameters:
      code: |
        import json
        data = json.loads(input_data)
        data['processed_at'] = datetime.now().isoformat()
        output_data = json.dumps(data)
      environment:
        PYTHONPATH: /custom/path
    timeout: 60s
    continue_on_error: false

  - id: kafka-sink
    type: kafka_producer
    parameters:
      brokers:
        - localhost:9092
      topic: processed-data
      compression: snappy
    retry_count: 5
    retry_delay: 10s

connections:
  - source: http-source
    target: transform
  - source: transform
    target: kafka-sink
```

### Environment Variable Substitution

Configuration supports environment variable substitution using `${VAR_NAME}` syntax:

```json
{
  "parameters": {
    "url": "${API_URL}",
    "api_key": "${API_KEY}"
  }
}
```

### Configuration Validation

All configurations are validated before pipeline creation:
- Required fields presence
- Type correctness (URLs, ports, etc.)
- Value constraints (positive numbers, valid enums)
- Graph structure (DAG, sources, sinks)
- Component compatibility (type matching)

## Implementation Guidance

### Package Structure

```
pkg/pipeline/
├── engine.go              # Main engine implementation
├── repository.go          # Database operations
├── executor.go            # Pipeline execution logic
├── scheduler.go           # Cron scheduling
├── lifecycle.go           # Lifecycle management
├── factory.go             # Component factory
├── graph.go               # Graph validation and traversal
├── models.go              # Data models
├── errors.go              # Error types
├── metrics.go             # Metrics collection
├── components/
│   ├── component.go       # Base component interface
│   ├── http.go            # HTTP components
│   ├── sql.go             # SQL components
│   ├── kafka.go           # Kafka components
│   ├── rabbitmq.go        # RabbitMQ components
│   ├── csv.go             # CSV components
│   ├── hl7.go             # HL7 components
│   ├── tcp.go             # TCP components
│   ├── python.go          # Python components
│   └── log.go             # Log components
└── api/
    ├── handlers.go        # HTTP handlers
    ├── routes.go          # Route registration
    └── middleware.go      # API middleware
```

### Implementation Phases

**Phase 1: Core Infrastructure**
1. Define interfaces and data models
2. Implement repository with database schema
3. Implement component factory and base interfaces
4. Set up API routes and handlers

**Phase 2: Basic Components**
1. Implement HTTP GET/POST components
2. Implement Log component
3. Implement basic executor with goroutine orchestration
4. Implement graph validation

**Phase 3: Execution Engine**
1. Implement scheduler with cron support
2. Implement lifecycle manager
3. Implement error handling and retry logic
4. Implement backpressure integration

**Phase 4: Advanced Components**
1. Implement SQL query component
2. Implement CSV reader component
3. Implement Python code block component
4. Implement TCP read/write components

**Phase 5: Messaging Components**
1. Implement Kafka consumer/producer
2. Implement RabbitMQ consumer/producer
3. Implement HL7 reader

**Phase 6: Observability**
1. Implement comprehensive logging
2. Implement metrics collection
3. Implement execution history tracking
4. Add monitoring dashboards

### Key Implementation Considerations

**Goroutine Management**:
- Use worker pools to limit concurrent goroutines
- Implement graceful shutdown with context cancellation
- Use WaitGroups for coordination
- Recover from panics in all goroutines

**Channel Management**:
- Use buffered channels to prevent blocking
- Close channels when components complete
- Handle channel closure gracefully in readers
- Avoid channel leaks with proper cleanup

**Resource Management**:
- Close database connections properly
- Release file handles after reading
- Close network connections after use
- Implement connection pooling for external systems

**Concurrency Safety**:
- Use mutexes for shared state
- Prefer channels over shared memory
- Make data structures immutable where possible
- Use atomic operations for counters

**Performance Optimization**:
- Batch database operations where possible
- Use connection pooling for external systems
- Implement data streaming for large payloads
- Profile and optimize hot paths

### Security Considerations

**Configuration Security**:
- Store sensitive credentials in environment variables
- Support secret management system integration
- Validate and sanitize all user inputs
- Prevent code injection in Python components

**Network Security**:
- Support TLS for all network components
- Validate SSL certificates
- Implement authentication for external systems
- Rate limit API endpoints

**Data Security**:
- Support data encryption at rest
- Support data encryption in transit
- Implement audit logging for sensitive operations
- Prevent data leakage in logs

## Deployment Considerations

### Configuration

The pipeline engine is configured through the main application configuration:

```yaml
pipeline:
  enabled: true
  max_concurrent_instances: 10
  max_goroutines_per_instance: 100
  execution_history_retention_days: 30
  metrics_enabled: true
  log_level: info
```

### Resource Requirements

**Minimum Requirements**:
- CPU: 2 cores
- Memory: 512 MB
- Disk: 1 GB for database and logs

**Recommended for Production**:
- CPU: 4+ cores
- Memory: 2+ GB
- Disk: 10+ GB with SSD for database

### Monitoring

**Key Metrics to Monitor**:
- Pipeline execution success rate
- Average execution duration
- Component error rates
- Active pipeline instances
- Database query performance
- Memory and CPU usage

**Alerting Thresholds**:
- Pipeline failure rate > 5%
- Execution duration > 2x average
- Component error rate > 10%
- Active instances > max_concurrent_instances
- Memory usage > 80%

### Scaling Considerations

**Horizontal Scaling**:
- Multiple engine instances can run concurrently
- Use distributed locking for scheduled pipelines
- Share execution state through database
- Load balance API requests

**Vertical Scaling**:
- Increase max_concurrent_instances for more parallelism
- Increase max_goroutines_per_instance for larger pipelines
- Allocate more memory for data-intensive pipelines
- Use faster storage for database operations

