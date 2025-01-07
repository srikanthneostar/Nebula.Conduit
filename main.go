package main

import (
	"context"
	"net/http"

	"Nebula.Conduit/api"
	"Nebula.Conduit/services"
	"Nebula.Conduit/spouts"
)

// Data represents a single data point

func main() {
	executePipelines() // Run pipeline execution in a goroutine

	// Start web API server in a goroutine
	go func() {
		// Create embedding service for the web API
		embeddingService := services.NewEmbeddingService("http://0.0.0.0:11434", "mistral-max")

		// Initialize web API with embedding service
		webAPI := api.NewWebAPI(embeddingService)

		// Start HTTP server
		if err := http.ListenAndServe(":8080", webAPI); err != nil {
			panic(err)
		}
	}()

	// Keep main goroutine alive
	select {}
}

func executePipelines() {
	ctx := context.Background()

	pipelineService := services.NewPipelineService(ctx)
	pipeline, newCtx := spouts.CreateEmbedingsPipeline()

	//pipelineService.AddPipeline(spouts.CsvToJsonPipeline())
	pipelineService.AddPipeline(pipeline)
	// pipelineService.AddPipeline(spouts.NewEventsItenlligence())
	go pipelineService.Execute(newCtx)
}
