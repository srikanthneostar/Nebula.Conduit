package pipeline

import (
	"github.com/go-chi/chi/v5"
)

// RegisterRoutes registers all pipeline API routes under /api/v1/pipelines
// This function should be called from the main server setup
func RegisterRoutes(r chi.Router, engine *PipelineEngine) {
	handlers := NewPipelineHandlers(engine)

	// Pipeline CRUD operations
	r.Route("/api/v1/pipelines", func(r chi.Router) {
		// Pipeline definition endpoints
		r.Post("/", handlers.CreatePipeline)              // Create new pipeline
		r.Get("/", handlers.ListPipelines)                // List all pipelines
		r.Get("/{id}", handlers.GetPipeline)              // Get specific pipeline
		r.Put("/{id}", handlers.UpdatePipeline)           // Update pipeline
		r.Delete("/{id}", handlers.DeletePipeline)        // Delete pipeline
		r.Post("/{id}/trigger", handlers.TriggerPipeline) // Manually trigger pipeline

		// Execution history endpoints
		r.Get("/{id}/executions", handlers.GetExecutionHistory) // Get execution history for pipeline
		r.Get("/executions/{id}", handlers.GetExecutionDetails) // Get specific execution details

		// Pipeline instance endpoints
		r.Get("/instances", handlers.ListRunningInstances)            // List all running instances
		r.Get("/instances/{id}", handlers.GetInstanceStatus)          // Get instance status
		r.Post("/instances/{id}/stop", handlers.StopPipelineInstance) // Stop running instance
	})
}
