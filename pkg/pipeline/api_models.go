package pipeline

// API request and response models for pipeline endpoints

// CreatePipelineRequest represents the request body for creating a pipeline
type CreatePipelineRequest struct {
	Name           string            `json:"name"`
	Description    string            `json:"description"`
	ExecutionMode  ExecutionMode     `json:"execution_mode"`
	CronExpression string            `json:"cron_expression,omitempty"`
	Status         PipelineStatus    `json:"status"`
	Components     []ComponentConfig `json:"components"`
	Connections    []Connection      `json:"connections"`
}

// UpdatePipelineRequest represents the request body for updating a pipeline
type UpdatePipelineRequest struct {
	Name           *string            `json:"name,omitempty"`
	Description    *string            `json:"description,omitempty"`
	ExecutionMode  *ExecutionMode     `json:"execution_mode,omitempty"`
	CronExpression *string            `json:"cron_expression,omitempty"`
	Status         *PipelineStatus    `json:"status,omitempty"`
	Components     *[]ComponentConfig `json:"components,omitempty"`
	Connections    *[]Connection      `json:"connections,omitempty"`
}

// PipelineResponse represents a pipeline in API responses
type PipelineResponse struct {
	ID             string            `json:"id"`
	Name           string            `json:"name"`
	Description    string            `json:"description"`
	ExecutionMode  ExecutionMode     `json:"execution_mode"`
	CronExpression string            `json:"cron_expression,omitempty"`
	Status         PipelineStatus    `json:"status"`
	Components     []ComponentConfig `json:"components"`
	Connections    []Connection      `json:"connections"`
	CreatedAt      string            `json:"created_at"`
	UpdatedAt      string            `json:"updated_at"`
}

// ListPipelinesResponse represents the response for listing pipelines
type ListPipelinesResponse struct {
	Pipelines []PipelineResponse `json:"pipelines"`
	Total     int                `json:"total"`
}

// TriggerPipelineRequest represents the request to manually trigger a pipeline
type TriggerPipelineRequest struct {
	PipelineID string `json:"pipeline_id"`
}

// TriggerPipelineResponse represents the response after triggering a pipeline
type TriggerPipelineResponse struct {
	InstanceID string `json:"instance_id"`
	PipelineID string `json:"pipeline_id"`
	Status     string `json:"status"`
	Message    string `json:"message"`
}

// StopInstanceRequest represents the request to stop a pipeline instance
type StopInstanceRequest struct {
	InstanceID string `json:"instance_id"`
}

// StopInstanceResponse represents the response after stopping an instance
type StopInstanceResponse struct {
	InstanceID string `json:"instance_id"`
	Status     string `json:"status"`
	Message    string `json:"message"`
}

// GetInstanceStatusResponse represents the status of a pipeline instance
type GetInstanceStatusResponse struct {
	InstanceID string            `json:"instance_id"`
	PipelineID string            `json:"pipeline_id"`
	Status     InstanceStatus    `json:"status"`
	StartedAt  string            `json:"started_at"`
	Components []ComponentStatus `json:"components"`
}

// ListRunningInstancesResponse represents the list of running instances
type ListRunningInstancesResponse struct {
	Instances []GetInstanceStatusResponse `json:"instances"`
	Total     int                         `json:"total"`
}

// GetExecutionHistoryResponse represents execution history for a pipeline
type GetExecutionHistoryResponse struct {
	Executions []ExecutionRecord `json:"executions"`
	Total      int               `json:"total"`
	Page       int               `json:"page"`
	PageSize   int               `json:"page_size"`
}

// GetExecutionDetailsResponse represents detailed execution information
type GetExecutionDetailsResponse struct {
	ExecutionRecord
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error   string                 `json:"error"`
	Code    string                 `json:"code,omitempty"`
	Details map[string]interface{} `json:"details,omitempty"`
}

// MessageResponse represents a simple message response
type MessageResponse struct {
	Message string `json:"message"`
}
