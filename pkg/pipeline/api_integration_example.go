package pipeline

// This file provides an example of how to integrate pipeline routes into the main server.
// This is for reference only and should not be used directly.

/*
Example integration in internal/api/server.go:

import (
	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

func NewServer(db *sql.DB, cfg *config.Config) *Server {
	// ... existing server setup ...

	// Initialize pipeline engine (when implemented)
	// pipelineEngine := pipeline.NewPipelineEngine(db, cfg)

	// Register pipeline routes (when engine is ready)
	// pipeline.RegisterRoutes(s.Router, pipelineEngine)

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
