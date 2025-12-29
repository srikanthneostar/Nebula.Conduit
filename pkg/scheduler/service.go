package scheduler

import (
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"

	"github.com/Xecutables/Nebula.Conduit/config"
	"github.com/Xecutables/Nebula.Conduit/internal/repository"
	"github.com/Xecutables/Nebula.Conduit/pkg/logger"
)

// SchedulerService provides HTTP endpoints for scheduler management
type SchedulerService struct {
	scheduler *CronScheduler
	logger    zerolog.Logger
	mu        sync.RWMutex
}

// NewSchedulerService creates a new scheduler service
func NewSchedulerService(taskRepo repository.TaskRepository, pathConfig *config.PathConfig, configFile string) *SchedulerService {
	return &SchedulerService{
		scheduler: NewCronScheduler(taskRepo, pathConfig, configFile),
		logger:    logger.InitLogger(),
	}
}

// Start starts the scheduler service
func (ss *SchedulerService) Start() error {
	return ss.scheduler.Start()
}

// Stop stops the scheduler service
func (ss *SchedulerService) Stop() {
	ss.scheduler.Stop()
}

// RegisterRoutes registers HTTP routes for scheduler management
func (ss *SchedulerService) RegisterRoutes(router *gin.Engine) {
	api := router.Group("/api/v1/scheduler")
	{
		api.GET("/pipelines", ss.getPipelines)
		api.GET("/pipelines/:name", ss.getPipeline)
		api.POST("/pipelines/:name/trigger", ss.triggerPipeline)
		api.GET("/executions", ss.getRunningExecutions)
		api.GET("/executions/:id", ss.getExecutionStatus)
		api.DELETE("/executions/:id", ss.stopExecution)
		api.POST("/reload", ss.reloadPipelines)
		api.POST("/validate", ss.validateConfig)
		api.GET("/health", ss.getHealth)
		api.GET("/stats", ss.getStats)
	}
}

// HTTP Handlers

func (ss *SchedulerService) getPipelines(c *gin.Context) {
	pipelines := ss.scheduler.GetPipelines()
	c.JSON(http.StatusOK, gin.H{
		"pipelines": pipelines,
		"count":     len(pipelines),
	})
}

func (ss *SchedulerService) getPipeline(c *gin.Context) {
	name := c.Param("name")
	pipelines := ss.scheduler.GetPipelines()

	pipeline, exists := pipelines[name]
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "Pipeline not found"})
		return
	}

	c.JSON(http.StatusOK, pipeline)
}

func (ss *SchedulerService) triggerPipeline(c *gin.Context) {
	name := c.Param("name")

	execution, err := ss.scheduler.TriggerPipeline(name)
	if err != nil {
		ss.logger.Error().Err(err).Str("pipeline", name).Msg("Failed to trigger pipeline")
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ss.logger.Info().Str("pipeline", name).Str("execution_id", execution.ID).Msg("Pipeline triggered successfully")
	c.JSON(http.StatusOK, gin.H{
		"message":      "Pipeline triggered successfully",
		"execution_id": execution.ID,
		"pipeline":     name,
		"started_at":   execution.StartedAt,
	})
}

func (ss *SchedulerService) getRunningExecutions(c *gin.Context) {
	executions := ss.scheduler.GetRunningExecutions()
	c.JSON(http.StatusOK, gin.H{
		"executions": executions,
		"count":      len(executions),
	})
}

func (ss *SchedulerService) getExecutionStatus(c *gin.Context) {
	executionID := c.Param("id")

	execution, err := ss.scheduler.GetExecutionStatus(executionID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, execution)
}

func (ss *SchedulerService) stopExecution(c *gin.Context) {
	executionID := c.Param("id")

	err := ss.scheduler.StopPipelineExecution(executionID)
	if err != nil {
		ss.logger.Error().Err(err).Str("execution_id", executionID).Msg("Failed to stop execution")
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ss.logger.Info().Str("execution_id", executionID).Msg("Execution stopped successfully")
	c.JSON(http.StatusOK, gin.H{
		"message":      "Execution stopped successfully",
		"execution_id": executionID,
	})
}

func (ss *SchedulerService) reloadPipelines(c *gin.Context) {
	err := ss.scheduler.ReloadPipelines()
	if err != nil {
		ss.logger.Error().Err(err).Msg("Failed to reload pipelines")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	pipelines := ss.scheduler.GetPipelines()
	ss.logger.Info().Int("count", len(pipelines)).Msg("Pipelines reloaded successfully")

	c.JSON(http.StatusOK, gin.H{
		"message": "Pipelines reloaded successfully",
		"count":   len(pipelines),
	})
}

func (ss *SchedulerService) validateConfig(c *gin.Context) {
	var request struct {
		ConfigPath string `json:"config_path" binding:"required"`
	}

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err := ss.scheduler.ValidatePipelineConfig(request.ConfigPath)
	if err != nil {
		ss.logger.Error().Err(err).Str("config_path", request.ConfigPath).Msg("Pipeline config validation failed")
		c.JSON(http.StatusBadRequest, gin.H{
			"valid": false,
			"error": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"valid":   true,
		"message": "Configuration is valid",
	})
}

func (ss *SchedulerService) getHealth(c *gin.Context) {
	pipelines := ss.scheduler.GetPipelines()
	executions := ss.scheduler.GetRunningExecutions()

	health := gin.H{
		"status":             "healthy",
		"scheduler_running":  true,
		"pipelines_loaded":   len(pipelines),
		"running_executions": len(executions),
		"timestamp":          c.Request.Context().Value("timestamp"),
	}

	c.JSON(http.StatusOK, health)
}

func (ss *SchedulerService) getStats(c *gin.Context) {
	pipelines := ss.scheduler.GetPipelines()
	executions := ss.scheduler.GetRunningExecutions()

	enabledPipelines := 0
	totalJobs := 0
	runningJobs := 0

	for _, pipeline := range pipelines {
		if pipeline.Enabled {
			enabledPipelines++
		}
		totalJobs += len(pipeline.Jobs)
	}

	for _, execution := range executions {
		for _, job := range execution.Jobs {
			if job.Status == StatusRunning {
				runningJobs++
			}
		}
	}

	stats := gin.H{
		"total_pipelines":    len(pipelines),
		"enabled_pipelines":  enabledPipelines,
		"total_jobs":         totalJobs,
		"running_executions": len(executions),
		"running_jobs":       runningJobs,
	}

	c.JSON(http.StatusOK, stats)
}

// GetScheduler returns the underlying cron scheduler (for advanced usage)
func (ss *SchedulerService) GetScheduler() *CronScheduler {
	return ss.scheduler
}

// Middleware for request logging
func (ss *SchedulerService) LoggingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := c.Request.Context().Value("start_time")
		c.Set("timestamp", start)

		ss.logger.Info().
			Str("method", c.Request.Method).
			Str("path", c.Request.URL.Path).
			Str("ip", c.ClientIP()).
			Msg("Scheduler API request")

		c.Next()
	}
}

// ErrorHandlingMiddleware handles panics and errors
func (ss *SchedulerService) ErrorHandlingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				ss.logger.Error().
					Interface("error", err).
					Str("path", c.Request.URL.Path).
					Msg("Panic in scheduler API")

				c.JSON(http.StatusInternalServerError, gin.H{
					"error": "Internal server error",
				})
				c.Abort()
			}
		}()

		c.Next()
	}
}
