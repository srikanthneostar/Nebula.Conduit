package pipeline

// This file provides an example of how to integrate pipeline routes into the main server.
// This is for reference only and should not be used directly.

/*
Example integration in internal/api/server.go:

import (
	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline/components"
)

func NewServer(db *sql.DB, cfg *config.Config) *Server {
	// ... existing server setup ...

	// Create component factory and register all components
	factory := pipeline.NewComponentFactory()
	components.RegisterComponents(factory)

	// Create pipeline repository
	repo := pipeline.NewSQLPipelineRepository(db)

	// Create logger
	logger := logger.InitLogger()

	// Create metrics collector
	metrics := pipeline.NewDefaultMetricsCollector()

	// Configure pipeline engine
	engineConfig := pipeline.PipelineEngineConfig{
		MaxConcurrentInstances:    10,
		MaxGoroutinesPerInstance:  50,
		ExecutionHistoryRetention: 30, // days
		MetricsEnabled:            true,
	}

	// Initialize pipeline engine
	engine := pipeline.NewPipelineEngine(db, repo, factory, nil, &logger, metrics, engineConfig)

	// Register pipeline routes
	pipeline.RegisterRoutes(s.Router, engine)

	// Initialize engine (loads active pipelines)
	engine.Initialize(context.Background())

	return s
}

The pipeline routes will be available at:
- POST   /api/v1/pipelines                    - Create pipeline
- GET    /api/v1/pipelines                    - List pipelines
- GET    /api/v1/pipelines/{id}               - Get pipeline
- PUT    /api/v1/pipelines/{id}               - Update pipeline
- DELETE /api/v1/pipelines/{id}               - Delete pipeline
- POST   /api/v1/pipelines/{id}/trigger       - Trigger pipeline
- GET    /api/v1/pipelines/{id}/executions    - Get execution history
- GET    /api/v1/pipelines/executions/{id}    - Get execution details
- GET    /api/v1/pipelines/instances          - List running instances
- GET    /api/v1/pipelines/instances/{id}     - Get instance status
- POST   /api/v1/pipelines/instances/{id}/stop - Stop instance
*/
