package pipeline

import (
	"context"
	"fmt"
	"time"
)

// MonitorPipeline represents a pipeline that fetches all pipelines and logs them
type MonitorPipeline struct {
	id          string
	name        string
	description string
	cronExpr    string
	status      PipelineStatus
	logFilePath string
	apiURL      string
	authToken   string
}

// NewMonitorPipeline creates a new monitor pipeline instance
func NewMonitorPipeline(apiURL, logFilePath, authToken string) *MonitorPipeline {
	return &MonitorPipeline{
		name:        "Pipeline Monitor - Auto Fetch",
		description: "Automatically fetches all pipelines on a schedule and writes results to log file",
		cronExpr:    "*/5 * * * *",
		status:      PipelineStatusInactive,
		logFilePath: logFilePath,
		apiURL:      apiURL,
		authToken:   authToken,
	}
}

// GetDefinition returns the pipeline definition for this monitor pipeline
func (mp *MonitorPipeline) GetDefinition() PipelineDefinition {
	return PipelineDefinition{
		Name:           mp.name,
		Description:    mp.description,
		ExecutionMode:  ExecutionModeScheduled,
		CronExpression: mp.cronExpr,
		Status:         mp.status,
		Components: []ComponentConfig{
			{
				ID:   "http-get-pipelines-1",
				Type: ComponentTypeHTTPGet,
				Parameters: map[string]interface{}{
					"url": mp.apiURL,
					"headers": map[string]string{
						"Authorization": fmt.Sprintf("Bearer %s", mp.authToken),
					},
				},
				RetryCount:      2,
				RetryDelay:      5 * time.Second,
				ContinueOnError: false,
				Timeout:         30 * time.Second,
			},
			{
				ID:   "log-sink-1",
				Type: ComponentTypeLogSink,
				Parameters: map[string]interface{}{
					"file_path": mp.logFilePath,
					"log_level": "info",
					"format":    "json",
				},
				RetryCount:      0,
				RetryDelay:      0,
				ContinueOnError: true,
				Timeout:         10 * time.Second,
			},
		},
		Connections: []Connection{
			{
				SourceComponentID: "http-get-pipelines-1",
				TargetComponentID: "log-sink-1",
			},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

// Execute runs the monitor pipeline
func (mp *MonitorPipeline) Execute(ctx context.Context) error {
	// This would be called by the scheduler
	// The actual execution is handled by the pipeline engine
	return nil
}

// SetCronExpression updates the cron schedule
func (mp *MonitorPipeline) SetCronExpression(expr string) {
	mp.cronExpr = expr
}

// SetStatus updates the pipeline status
func (mp *MonitorPipeline) SetStatus(status PipelineStatus) {
	mp.status = status
}

// GetStatus returns the current pipeline status
func (mp *MonitorPipeline) GetStatus() PipelineStatus {
	return mp.status
}
