# Implementation Plan: Data Pipeline Engine

## Overview

This implementation plan breaks down the Data Pipeline Engine into 6 phases following the design architecture. The engine is a graph-based data processing system in Go that enables users to construct and execute data workflows with 13 component types, scheduled/continuous execution modes, and comprehensive error handling. Each task builds incrementally on previous work, with property-based tests using gopter to validate the 23 correctness properties from the design.

## Tasks

- [ ] 1. Phase 1: Core Infrastructure
  - [x] 1.1 Create package structure and core interfaces
    - Create `pkg/pipeline/` directory structure
    - Define `Component`, `SourceComponent`, `ProcessorComponent`, `SinkComponent` interfaces
    - Define `ComponentFactory`, `PipelineRepository`, `Scheduler`, `Executor`, `LifecycleManager` interfaces
    - Define `BackpressureSystem` interface for integration
    - _Requirements: 11.1, 11.2_

  - [x] 1.2 Implement data models and types
    - Create `models.go` with `PipelineDefinition`, `PipelineInstance`, `ExecutionRecord`, `ComponentExecutionRecord` structs
    - Define `Data`, `ComponentConfig`, `Connection` structs
    - Define enums: `ExecutionMode`, `PipelineStatus`, `InstanceStatus`, `ComponentType`
    - _Requirements: 1.2, 1.3, 1.6_

  - [ ]* 1.3 Write property test for data model serialization
    - **Property 1: Pipeline Definition Round-Trip Serialization**
    - **Validates: Requirements 1.3, 1.7**

  - [x] 1.4 Create database schema and migrations
    - Create migration files for 5 tables: `pipelines`, `pipeline_components`, `pipeline_connections`, `pipeline_executions`, `component_executions`
    - Add indexes for performance optimization
    - Integrate with existing `pkg/database` migration system
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 9.8, 11.3_

  - [x] 1.5 Implement PipelineRepository with CRUD operations
    - Implement `Create`, `Read`, `Update`, `Delete`, `List`, `ListActive` methods
    - Use existing `pkg/database` package for database operations
    - Add transaction support for atomic operations
    - _Requirements: 1.1, 1.4, 11.3_

  - [ ]* 1.6 Write property test for pipeline definition structure
    - **Property 2: Pipeline Definition Structure Completeness**
    - **Validates: Requirements 1.2, 1.6, 3.6**

  - [x] 1.7 Implement graph validation logic
    - Create `graph.go` with DAG validation functions
    - Validate at least one source and one sink component
    - Validate no cycles in component connections
    - Validate type compatibility between connected components
    - _Requirements: 1.5, 4.1, 4.7, 4.8_

  - [ ]* 1.8 Write property tests for graph validation
    - **Property 3: Graph Validation Rejects Invalid Topologies**
    - **Validates: Requirements 1.5**
    - **Property 15: Type Compatibility Validation**
    - **Validates: Requirements 4.8**

  - [x] 1.9 Implement component configuration validation
    - Create validation functions for each component type's required parameters
    - Validate URLs, connection strings, file paths, host/port combinations
    - Return descriptive error messages for validation failures
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 8.9, 8.10, 8.11, 8.12, 8.13, 8.14, 8.15_

  - [ ]* 1.10 Write property tests for component configuration validation
    - **Property 4: Component Configuration Validation**
    - **Validates: Requirements 8.1-8.15**
    - **Property 5: Invalid Cron Expression Rejection**
    - **Validates: Requirements 3.7**

  - [x] 1.11 Set up API routes and basic handlers
    - Create `api/routes.go` to register routes under `/api/v1/pipelines`
    - Create `api/handlers.go` with handler stubs for CRUD operations
    - Implement request/response models for API endpoints
    - Add error response formatting
    - _Requirements: 11.6_

  - [x] 1.12 Checkpoint - Ensure all tests pass
    - Ensure all tests pass, ask the user if questions arise.

- [ ] 2. Phase 2: Basic Components and Factory
  - [x] 2.1 Implement ComponentFactory
    - Create `factory.go` with 
    component registration and creation logic
    - Implement `Create`, `Register`, `ListTypes` methods
    - Add constructor function type for component creation
    - _Requirements: 2.33_

  - [ ]* 2.2 Write property test for component factory
    - **Property 6: Component Type Registration**
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10, 2.15, 2.20, 2.27**
    - **Property 11: Component Configuration Loading**
    - **Validates: Requirements 2.33**

  - [x] 2.3 Implement HTTP GET component
    - Create `components/http.go` with `HTTPGetComponent` struct
    - Implement `Execute`, `Validate`, `Type`, `ID`, `Config` methods
    - Support URL, headers, interval configuration
    - Integrate with HTTP client for GET requests
    - _Requirements: 2.1, 8.2_

  - [x] 2.4 Implement HTTP POST component
    - Add `HTTPPostComponent` to `components/http.go`
    - Implement sink component interface methods
    - Support URL, headers, content type configuration
    - Integrate with HTTP client for POST requests
    - _Requirements: 2.2, 8.3_

  - [ ]* 2.5 Write unit tests for HTTP components
    - Test HTTP GET with mock server
    - Test HTTP POST with mock server
    - Test error handling and retries
    - Test timeout behavior

  - [x] 2.6 Implement Log component
    - Create `components/log.go` with `LogComponent` struct
    - Implement processor component interface methods
    - Support log levels (debug, info, warn, error)
    - Support JSON and plain text formatting
    - Support data sampling and truncation
    - Integrate with existing `pkg/logger` zerolog infrastructure
    - Pass data unchanged to downstream components
    - _Requirements: 2.27, 2.28, 2.29, 2.30, 2.31, 2.32, 8.14, 11.4_

  - [ ]* 2.7 Write property tests for Log component
    - **Property 9: Log Component Data Passthrough**
    - **Validates: Requirements 2.32**
    - **Property 10: Log Component Truncation**
    - **Validates: Requirements 2.30**

  - [ ] 2.8 Register basic components in factory
    - Register HTTP GET, HTTP POST, and Log components
    - Add component type constants
    - Test component creation through factory
    - _Requirements: 2.1, 2.2, 2.27_

  - [x] 2.9 Checkpoint - Ensure all tests pass
    - Ensure all tests pass, ask the user if questions arise.

- [ ] 3. Phase 3: Execution Engine Core
  - [x] 3.1 Implement basic Executor
    - Create `executor.go` with `Executor` struct
    - Implement `Execute` method to create pipeline instances
    - Create goroutine for each component
    - Set up channels between connected components
    - Use `context.Context` for cancellation
    - Use `sync.WaitGroup` for coordination
    - _Requirements: 3.3, 12.1, 12.2, 12.4, 12.6_

  - [ ]* 3.2 Write property test for topological execution
    - **Property 13: Topological Execution Order**
    - **Validates: Requirements 4.2**

  - [x] 3.3 Implement fanout data distribution
    - Add logic to duplicate data for multiple downstream components
    - Create separate channels for each downstream component
    - Ensure all downstream components receive the same data
    - _Requirements: 4.5, 4.6_

  - [ ]* 3.4 Write property test for fanout distribution
    - **Property 14: Fanout Data Distribution**
    - **Validates: Requirements 4.5, 4.6, 12.3**

  - [x] 3.5 Implement Scheduler with cron support
    - Create `scheduler.go` with `Scheduler` struct
    - Integrate with cron library for scheduled execution
    - Implement `Schedule`, `Unschedule`, `Trigger` methods
    - Validate cron expressions during scheduling
    - _Requirements: 3.1, 3.6, 3.7_

  - [x] 3.6 Implement LifecycleManager
    - Create `lifecycle.go` with `LifecycleManager` struct
    - Implement `Start`, `Stop`, `GetRunningInstances`, `GetExecutionHistory` methods
    - Track active pipeline instances in memory
    - Handle pipeline status changes (active/inactive)
    - _Requirements: 6.1, 6.2, 6.5_

  - [ ]* 3.7 Write property test for pipeline status propagation
    - **Property 17: Pipeline Status Change Propagation**
    - **Validates: Requirements 6.2**

  - [x] 3.8 Implement error handling and retry logic
    - Add retry logic with configurable count and delay
    - Implement exponential backoff for retries
    - Support `continue_on_error` flag behavior
    - Record errors in execution history
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8_

  - [ ]* 3.9 Write property tests for error handling
    - **Property 19: Component Retry Behavior**
    - **Validates: Requirements 7.3, 7.4**
    - **Property 20: Continue-On-Error Behavior**
    - **Validates: Requirements 7.6, 7.7**

  - [x] 3.10 Implement panic recovery
    - Wrap all component goroutines with `recover()`
    - Log panics with stack trace using zerolog
    - Mark panicked components as failed
    - Continue managing other components after recovery
    - _Requirements: 12.7_

  - [ ]* 3.11 Write property test for panic recovery
    - **Property 23: Panic Recovery and Logging**
    - **Validates: Requirements 12.7**

  - [x] 3.12 Implement execution recording
    - Record pipeline execution start, end, duration, status
    - Record component execution details in database
    - Store execution records in `pipeline_executions` and `component_executions` tables
    - _Requirements: 3.4, 6.6, 7.8_

  - [ ]* 3.13 Write property tests for execution recording
    - **Property 12: Scheduled Pipeline Execution Recording**
    - **Validates: Requirements 3.4, 6.6**
    - **Property 18: Execution History Completeness**
    - **Validates: Requirements 7.8**

  - [x] 3.14 Integrate with backpressure system
    - Check `IsHighLoad()` before creating new instances
    - Check `IsCircuitOpen()` before executing operations
    - Call `RecordOperation()` for all pipeline operations
    - Respect rate limits from `GetRateLimit()`
    - Queue execution requests when circuit breaker is open
    - Log backpressure events with zerolog
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.6, 11.5_

  - [ ]* 3.15 Write property test for rate limiting
    - **Property 16: Rate Limiting Compliance**
    - **Validates: Requirements 5.3**

  - [x] 3.16 Implement PipelineEngine main struct
    - Create `engine.go` with `PipelineEngine` struct
    - Wire together repository, scheduler, executor, lifecycle manager, factory
    - Add initialization and shutdown methods
    - Load active pipelines on startup
    - _Requirements: 6.1_

  - [x] 3.17 Implement graceful shutdown for continuous pipelines
    - Signal all component goroutines to terminate
    - Wait for all goroutines to complete
    - Close all channels properly
    - Update instance status to stopped
    - _Requirements: 3.5, 12.5_

  - [ ]* 3.18 Write property test for pipeline termination
    - **Property 22: Pipeline Instance Termination**
    - **Validates: Requirements 3.5, 12.5**

  - [x] 3.19 Checkpoint - Ensure all tests pass
    - Ensure all tests pass, ask the user if questions arise.

- [ ] 4. Phase 4: Advanced Components
  - [x] 4.1 Implement SQL Query component
    - Create `components/sql.go` with `SQLQueryComponent` struct
    - Implement source component interface methods
    - Support SQL Server connection string and query configuration
    - Execute queries and convert results to Data format
    - Handle connection pooling
    - _Requirements: 2.3, 8.4_

  - [ ]* 4.2 Write unit tests for SQL component
    - Test query execution with mock database
    - Test connection error handling
    - Test result set conversion

  - [x] 4.3 Implement CSV Reader component
    - Create `components/csv.go` with `CSVReaderComponent` struct
    - Implement source component interface methods
    - Support file path configuration
    - Read CSV files and convert rows to Data format
    - Handle file encoding and delimiters
    - _Requirements: 2.4, 8.5_

  - [ ]* 4.4 Write unit tests for CSV component
    - Test CSV reading with sample files
    - Test file not found error handling
    - Test malformed CSV handling

  - [x] 4.5 Implement Python Code Block component
    - Create `components/python.go` with `PythonCodeBlockComponent` struct
    - Implement processor component interface methods
    - Integrate with existing Python executor infrastructure
    - Pass input data to Python execution environment
    - Support environment variables configuration
    - Support configurable timeout
    - Terminate execution on timeout
    - _Requirements: 2.20, 2.21, 2.22, 2.23, 2.24, 2.25, 2.26, 8.13_

  - [ ]* 4.6 Write property tests for Python component
    - **Property 7: Python Component Input Data Flow**
    - **Validates: Requirements 2.22, 2.23**
    - **Property 8: Python Component Timeout Enforcement**
    - **Validates: Requirements 2.25, 2.26**

  - [x] 4.7 Implement TCP Read component
    - Create `components/tcp.go` with `TCPReadComponent` struct
    - Implement source component interface methods
    - Support server mode (listen on host:port)
    - Support client mode (connect to host:port)
    - Support configurable connection timeout and retry settings
    - Support configurable data encoding/decoding
    - Handle multiple concurrent connections in server mode
    - _Requirements: 2.10, 2.11, 2.12, 2.13, 2.14, 8.11_

  - [x] 4.8 Implement TCP Write component
    - Add `TCPWriteComponent` to `components/tcp.go`
    - Implement sink component interface methods
    - Support server mode (listen on host:port)
    - Support client mode (connect to host:port)
    - Support configurable connection timeout and retry settings
    - Support configurable data encoding/decoding
    - Handle multiple concurrent connections in server mode
    - _Requirements: 2.15, 2.16, 2.17, 2.18, 2.19, 8.12_

  - [ ]* 4.9 Write property test for TCP encoding
    - **Property 21: TCP Component Encoding Round-Trip**
    - **Validates: Requirements 2.13, 2.14, 2.18, 2.19**

  - [ ]* 4.10 Write unit tests for TCP components
    - Test TCP server mode with mock client
    - Test TCP client mode with mock server
    - Test connection timeout handling
    - Test encoding/decoding with various formats

  - [x] 4.11 Register advanced components in factory
    - Register SQL Query, CSV Reader, Python Code Block, TCP Read, TCP Write components
    - Add component type constants
    - Test component creation through factory
    - _Requirements: 2.3, 2.4, 2.10, 2.15, 2.20_

  - [x] 4.12 Checkpoint - Ensure all tests pass
    - Ensure all tests pass, ask the user if questions arise.

- [ ] 5. Phase 5: Messaging Components
  - [x] 5.1 Implement Kafka Consumer component
    - Create `components/kafka.go` with `KafkaConsumerComponent` struct
    - Implement source component interface methods
    - Support broker addresses and topic configuration
    - Integrate with Kafka client library
    - Handle consumer group management
    - Support offset management
    - _Requirements: 2.5, 8.6_

  - [x] 5.2 Implement Kafka Producer component
    - Add `KafkaProducerComponent` to `components/kafka.go`
    - Implement sink component interface methods
    - Support broker addresses, topic, and compression configuration
    - Integrate with Kafka client library
    - Handle producer acknowledgments
    - _Requirements: 2.6, 8.7_

  - [ ]* 5.3 Write unit tests for Kafka components
    - Test Kafka consumer with mock broker
    - Test Kafka producer with mock broker
    - Test connection error handling
    - Test message serialization

  - [-] 5.4 Implement RabbitMQ Consumer component
    - Create `components/rabbitmq.go` with `RabbitMQConsumerComponent` struct
    - Implement source component interface methods
    - Support connection URL and queue name configuration
    - Integrate with RabbitMQ client library
    - Handle connection recovery
    - Support message acknowledgment
    - _Requirements: 2.7, 8.8_

  - [ ] 5.5 Implement RabbitMQ Producer component
    - Add `RabbitMQProducerComponent` to `components/rabbitmq.go`
    - Implement sink component interface methods
    - Support connection URL and exchange name configuration
    - Integrate with RabbitMQ client library
    - Handle connection recovery
    - Support message persistence
    - _Requirements: 2.8, 8.9_

  - [ ]* 5.6 Write unit tests for RabbitMQ components
    - Test RabbitMQ consumer with mock broker
    - Test RabbitMQ producer with mock broker
    - Test connection error handling
    - Test message acknowledgment

  - [ ] 5.7 Implement HL7 Reader component
    - Create `components/hl7.go` with `HL7ReaderComponent` struct
    - Implement source component interface methods
    - Support file path configuration
    - Parse HL7 flat file format
    - Convert HL7 messages to Data format
    - _Requirements: 2.9, 8.10_

  - [ ]* 5.8 Write unit tests for HL7 component
    - Test HL7 file parsing with sample files
    - Test file not found error handling
    - Test malformed HL7 handling

  - [ ] 5.9 Register messaging components in factory
    - Register Kafka Consumer, Kafka Producer, RabbitMQ Consumer, RabbitMQ Producer, HL7 Reader components
    - Add component type constants
    - Test component creation through factory
    - _Requirements: 2.5, 2.6, 2.7, 2.8, 2.9_

  - [ ] 5.10 Checkpoint - Ensure all tests pass
    - Ensure all tests pass, ask the user if questions arise.

- [ ] 6. Phase 6: API Implementation and Observability
  - [ ] 6.1 Implement pipeline CRUD API handlers
    - Implement `CreatePipeline` handler with validation
    - Implement `GetPipeline` handler
    - Implement `ListPipelines` handler with filtering
    - Implement `UpdatePipeline` handler
    - Implement `DeletePipeline` handler
    - Add request validation and error handling
    - _Requirements: 1.1, 1.2, 1.7_

  - [ ]* 6.2 Write integration tests for CRUD endpoints
    - Test create pipeline with valid/invalid data
    - Test get pipeline by ID
    - Test list pipelines with filters
    - Test update pipeline
    - Test delete pipeline

  - [ ] 6.3 Implement pipeline execution control API handlers
    - Implement `TriggerPipeline` handler for manual execution
    - Implement `StopPipelineInstance` handler
    - Implement `GetInstanceStatus` handler
    - Implement `ListRunningInstances` handler
    - _Requirements: 6.3, 6.4, 6.5_

  - [ ]* 6.4 Write integration tests for execution control endpoints
    - Test trigger pipeline execution
    - Test stop running instance
    - Test get instance status
    - Test list running instances

  - [ ] 6.5 Implement execution history API handlers
    - Implement `GetExecutionHistory` handler with pagination
    - Implement `GetExecutionDetails` handler
    - Return execution records with component results
    - _Requirements: 6.7_

  - [ ]* 6.6 Write integration tests for history endpoints
    - Test get execution history with pagination
    - Test get execution details
    - Test filtering by status and date range

  - [ ] 6.7 Implement comprehensive logging
    - Log pipeline instance start with name, execution ID, start time
    - Log pipeline instance completion with name, execution ID, duration, status
    - Log component start with type, ID, input data size
    - Log component completion with type, ID, duration, output data size
    - Log component errors with full context including configuration
    - Use structured logging with zerolog
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 11.4_

  - [ ]* 6.8 Write unit tests for logging
    - Test log output for pipeline lifecycle events
    - Test log output for component lifecycle events
    - Test error logging with context
    - Verify structured log format

  - [ ] 6.9 Implement metrics collection
    - Create `metrics.go` with metrics collector
    - Track total pipelines, active pipelines, running instances, total executions
    - Track pipeline execution duration histogram
    - Track component error counters by type
    - Integrate with existing `/metrics` endpoint
    - _Requirements: 5.5, 10.7, 10.8_

  - [ ]* 6.10 Write unit tests for metrics
    - Test metrics increment on pipeline operations
    - Test metrics exposed at /metrics endpoint
    - Test histogram recording for execution duration

  - [ ] 6.11 Add configuration support
    - Add pipeline engine configuration to main application config
    - Support `enabled`, `max_concurrent_instances`, `max_goroutines_per_instance`, `execution_history_retention_days` settings
    - Implement configuration loading and validation
    - Support disabling pipeline engine via configuration
    - _Requirements: 11.7_

  - [ ] 6.12 Implement execution history cleanup
    - Create background job to clean old execution records
    - Respect `execution_history_retention_days` configuration
    - Run cleanup periodically
    - Log cleanup operations

  - [ ] 6.13 Final integration and end-to-end testing
    - Test complete pipeline lifecycle: create → start → execute → stop → delete
    - Test scheduled pipeline execution with cron
    - Test continuous pipeline execution
    - Test fanout patterns with multiple downstream components
    - Test error handling and retry logic across components
    - Test backpressure integration under load
    - Test all 13 component types in real pipelines

  - [ ] 6.14 Final checkpoint - Ensure all tests pass
    - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional property-based and unit tests that can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation at the end of each phase
- Property tests validate universal correctness properties using gopter with minimum 100 iterations
- Unit tests validate specific examples, edge cases, and integration points
- All 23 correctness properties from the design document are covered by property tests
- Implementation uses Go with goroutines, channels, and existing infrastructure (SQLite, zerolog, backpressure system)
- The engine is implemented as a standalone module in `pkg/pipeline` without modifying existing packages
