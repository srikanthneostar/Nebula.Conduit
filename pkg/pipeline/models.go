package pipeline

import (
	"context"
	"sync"
	"time"
)

// ExecutionMode defines how a pipeline runs
type ExecutionMode string

const (
	ExecutionModeScheduled  ExecutionMode = "scheduled"
	ExecutionModeContinuous ExecutionMode = "continuous"
)

// PipelineStatus defines the operational state of a pipeline
type PipelineStatus string

const (
	PipelineStatusActive   PipelineStatus = "active"
	PipelineStatusInactive PipelineStatus = "inactive"
)

// InstanceStatus defines the state of a running pipeline instance
type InstanceStatus string

const (
	InstanceStatusRunning   InstanceStatus = "running"
	InstanceStatusCompleted InstanceStatus = "completed"
	InstanceStatusFailed    InstanceStatus = "failed"
	InstanceStatusStopped   InstanceStatus = "stopped"
)

// ComponentType identifies the type of component
type ComponentType string

const (
	// Source components
	ComponentTypeHTTPGet          ComponentType = "http_get"
	ComponentTypeSQLQuery         ComponentType = "sql_query"
	ComponentTypeCSVReader        ComponentType = "csv_reader"
	ComponentTypeKafkaConsumer    ComponentType = "kafka_consumer"
	ComponentTypeRabbitMQConsumer ComponentType = "rabbitmq_consumer"
	ComponentTypeHL7Reader        ComponentType = "hl7_reader"
	ComponentTypeTCPRead          ComponentType = "tcp_read"

	// Processor components
	ComponentTypePythonCodeBlock ComponentType = "python_code_block"
	ComponentTypeLog             ComponentType = "log"

	// Sink components
	ComponentTypeHTTPPost         ComponentType = "http_post"
	ComponentTypeKafkaProducer    ComponentType = "kafka_producer"
	ComponentTypeRabbitMQProducer ComponentType = "rabbitmq_producer"
	ComponentTypeTCPWrite         ComponentType = "tcp_write"
)

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

// Connection represents a link between two components
type Connection struct {
	SourceComponentID string `json:"source_component_id"`
	TargetComponentID string `json:"target_component_id"`
}

// PipelineDefinition represents a pipeline configuration
type PipelineDefinition struct {
	ID             string            `json:"id" db:"id"`
	Name           string            `json:"name" db:"name"`
	Description    string            `json:"description" db:"description"`
	ExecutionMode  ExecutionMode     `json:"execution_mode" db:"execution_mode"`
	CronExpression string            `json:"cron_expression" db:"cron_expression"`
	Status         PipelineStatus    `json:"status" db:"status"`
	Components     []ComponentConfig `json:"components"`
	Connections    []Connection      `json:"connections"`
	CreatedAt      time.Time         `json:"created_at" db:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at" db:"updated_at"`
}

// PipelineInstance represents a running execution of a pipeline
type PipelineInstance struct {
	ID          string
	PipelineID  string
	Status      InstanceStatus
	Components  map[string]Component
	Channels    map[string]chan Data
	Context     context.Context
	CancelFunc  context.CancelFunc
	WaitGroup   *sync.WaitGroup
	StartedAt   time.Time
	CompletedAt *time.Time
	Error       error
}

// ExecutionRecord represents a completed pipeline execution
type ExecutionRecord struct {
	ID               string                     `json:"id" db:"id"`
	PipelineID       string                     `json:"pipeline_id" db:"pipeline_id"`
	Status           InstanceStatus             `json:"status" db:"status"`
	StartedAt        time.Time                  `json:"started_at" db:"started_at"`
	EndedAt          *time.Time                 `json:"ended_at" db:"ended_at"`
	ErrorMessage     *string                    `json:"error_message" db:"error_message"`
	ComponentResults []ComponentExecutionRecord `json:"component_results"`
}

// ComponentExecutionRecord represents a component execution result
type ComponentExecutionRecord struct {
	ID                  string         `json:"id" db:"id"`
	PipelineExecutionID string         `json:"pipeline_execution_id" db:"pipeline_execution_id"`
	ComponentID         string         `json:"component_id" db:"component_id"`
	Status              InstanceStatus `json:"status" db:"status"`
	StartedAt           time.Time      `json:"started_at" db:"started_at"`
	EndedAt             *time.Time     `json:"ended_at" db:"ended_at"`
	OutputDataSize      int64          `json:"output_data_size" db:"output_data_size"`
	ErrorMessage        *string        `json:"error_message" db:"error_message"`
}

// ExecutionStatus represents the current status of a pipeline instance
type ExecutionStatus struct {
	InstanceID string
	PipelineID string
	Status     InstanceStatus
	StartedAt  time.Time
	Components []ComponentStatus
}

// ComponentStatus represents the status of a component in an execution
type ComponentStatus struct {
	ComponentID string
	Status      InstanceStatus
	DurationMs  int64
}
